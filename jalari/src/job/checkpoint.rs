use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::run::current_job;
use crate::{JobError, JobResult};

const KEEP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Command {
    Stop,
    Cancel,
}

impl Command {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "stop" => Some(Self::Stop),
            "cancel" => Some(Self::Cancel),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Cancel => "cancel",
        }
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Stop => "the job was asked to stop",
            Self::Cancel => "the job was asked to cancel",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Stop => 1,
            Self::Cancel => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Stop),
            2 => Some(Self::Cancel),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Halt {
    Command(Command),
    Shutdown,
}

impl Halt {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Command(command) => command.message(),
            Self::Shutdown => "the worker is shutting down",
        }
    }
}

#[derive(Debug, Default)]
struct Slot {
    latest: Option<Value>,
    kept_at: Option<Instant>,
    dirty: bool,
}

#[derive(Debug)]
pub(crate) struct RunControl {
    job_id: i64,
    lease: i64,
    saved: Option<Value>,
    shutdown: CancellationToken,
    command: AtomicU8,
    slot: Mutex<Slot>,
}

impl RunControl {
    pub(crate) fn new(
        job_id: i64,
        lease: i64,
        saved: Option<Value>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            job_id,
            lease,
            saved,
            shutdown,
            command: AtomicU8::new(0),
            slot: Mutex::new(Slot::default()),
        }
    }

    pub(crate) fn job_id(&self) -> i64 {
        self.job_id
    }

    pub(crate) fn lease(&self) -> i64 {
        self.lease
    }

    pub(crate) fn shutdown(&self) -> &CancellationToken {
        &self.shutdown
    }

    pub(crate) fn request(&self, command: Command) {
        self.command.fetch_max(command.code(), Ordering::SeqCst);
    }

    fn halt(&self) -> Option<Halt> {
        match Command::from_code(self.command.load(Ordering::SeqCst)) {
            Some(command) => Some(Halt::Command(command)),
            None => self.shutdown.is_cancelled().then_some(Halt::Shutdown),
        }
    }

    fn is_due(&self) -> bool {
        self.slot
            .lock()
            .unwrap()
            .kept_at
            .is_none_or(|kept_at| kept_at.elapsed() >= KEEP_INTERVAL)
    }

    fn keep(&self, progress: Value) {
        let mut slot = self.slot.lock().unwrap();
        slot.latest = Some(progress);
        slot.kept_at = Some(Instant::now());
        slot.dirty = true;
    }

    pub(crate) fn latest(&self) -> Option<Value> {
        self.slot.lock().unwrap().latest.clone()
    }

    pub(crate) fn take_dirty(&self) -> Option<Value> {
        let mut slot = self.slot.lock().unwrap();
        if !slot.dirty {
            return None;
        }
        slot.dirty = false;
        slot.latest.clone()
    }

    pub(crate) fn mark_dirty(&self) {
        let mut slot = self.slot.lock().unwrap();
        slot.dirty = slot.latest.is_some();
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.slot.lock().unwrap().dirty
    }

    async fn stop_point(&self) -> JobResult {
        if let Some(halt) = self.halt() {
            return Err(JobError::halted(halt));
        }
        tokio::task::consume_budget().await;
        Ok(())
    }
}

/// Saves a running job's progress so a later run continues from it; created by [`resume`].
///
/// The type parameter is the job's own progress type, so the type it saves is the type
/// [`resume`] loads.
pub struct Checkpoint<P> {
    control: Option<Arc<RunControl>>,
    progress: PhantomData<fn(&P)>,
}

impl<P> Clone for Checkpoint<P> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
            progress: PhantomData,
        }
    }
}

impl<P> fmt::Debug for Checkpoint<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkpoint")
            .field(
                "job_id",
                &self.control.as_ref().map(|control| control.job_id),
            )
            .finish()
    }
}

impl<P: Serialize> Checkpoint<P> {
    /// Records `progress` and returns an error when the job should stop.
    ///
    /// Call it between units of work and propagate its error with `?`. It never touches the
    /// database: it keeps a JSON copy of `progress` at most once a second, which the worker
    /// writes every few seconds, so a crash loses only the last few seconds of progress. When
    /// the job was asked to stop, the copy it takes then is saved together with the stopped
    /// state. Outside a job it does nothing and returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The job was asked to stop or to cancel, or its worker is shutting down
    /// - `progress` cannot be serialized to JSON (a [`JobError::permanent`])
    pub async fn save(&self, progress: &P) -> JobResult {
        let Some(control) = &self.control else {
            return Ok(());
        };
        if control.halt().is_some() || control.is_due() {
            control.keep(serde_json::to_value(progress).map_err(JobError::permanent)?);
        }
        control.stop_point().await
    }
}

/// Loads the progress an earlier run of the current job saved, and the [`Checkpoint`] that
/// saves it from now on.
///
/// The progress is `None` on the first run, after an
/// [`OnConflict::Replace`](crate::OnConflict::Replace) overwrote the job, and outside a job.
/// Retries and [`restart`](crate::restart) both see the latest saved progress; a job that
/// finishes discards it.
///
/// # Errors
///
/// Returns a [`JobError::permanent`] if the saved JSON does not deserialize into `P`, for
/// example after the progress type changed between deployments.
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # struct Batch { last_id: i64 }
/// # async fn fetch(after: i64) -> Result<Option<Batch>, std::io::Error> { Ok(None) }
/// # async fn upload(batch: &Batch) -> Result<(), std::io::Error> { Ok(()) }
/// #[derive(Serialize, Deserialize, Default)]
/// struct Cursor {
///     last_id: i64,
/// }
///
/// # async fn example() -> jalari::JobResult {
/// let (saved, checkpoint) = jalari::resume::<Cursor>()?;
/// let mut cursor = saved.unwrap_or_default();
/// while let Some(batch) = fetch(cursor.last_id).await? {
///     upload(&batch).await?;
///     cursor.last_id = batch.last_id;
///     checkpoint.save(&cursor).await?;
/// }
/// # Ok(())
/// # }
/// ```
pub fn resume<P: Serialize + DeserializeOwned>()
-> std::result::Result<(Option<P>, Checkpoint<P>), JobError> {
    let control = current_job().map(|job| Arc::clone(job.control()));
    let saved = control
        .as_ref()
        .and_then(|control| control.saved.clone())
        .map(serde_json::from_value)
        .transpose()
        .map_err(JobError::permanent)?;
    Ok((
        saved,
        Checkpoint {
            control,
            progress: PhantomData,
        },
    ))
}

/// Returns an error once the running job should stop, so it can unwind with `?`.
///
/// Call it between units of work in jobs that keep no progress; jobs that do use
/// [`Checkpoint::save`]. It never touches the database: it checks a flag the worker sets within
/// about a second of [`stop`](crate::stop) or [`cancel`](crate::cancel), or when the worker
/// starts shutting down, and yields once so a job that loops without other `.await`s can still
/// be aborted. A job that never calls it is aborted
/// [`WorkerConfig::stop_timeout`](crate::WorkerConfig::stop_timeout) after the request. Outside
/// a job it does nothing and returns `Ok`.
///
/// # Errors
///
/// Returns an error if:
/// - The job was asked to stop or to cancel, or its worker is shutting down
///
/// # Examples
///
/// ```rust,no_run
/// # async fn export(id: i64) -> jalari::JobResult { Ok(()) }
/// # async fn example(ids: Vec<i64>) -> jalari::JobResult {
/// for id in ids {
///     export(id).await?;
///     jalari::checkpoint().await?;
/// }
/// # Ok(())
/// # }
/// ```
pub async fn checkpoint() -> JobResult {
    match current_job() {
        Some(job) => job.control().stop_point().await,
        None => Ok(()),
    }
}
