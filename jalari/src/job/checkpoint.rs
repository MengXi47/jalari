use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::run::current_job;
use crate::{JobError, JobResult};

const KEEP_INTERVAL: Duration = Duration::from_secs(1);

/// A request sent to a job with [`stop`](crate::stop) or [`cancel`](crate::cancel) that a
/// worker has not acted on yet.
///
/// A cancel outranks a stop: once a cancel is pending, a later stop does not replace it. New
/// commands may be added in minor releases, so a `match` needs a `_` arm.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Command {
    /// Stop the job and keep it for [`restart`](crate::restart).
    Stop,
    /// Stop the job and mark it deleted.
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
    Lost,
    Shutdown,
}

impl Halt {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Command(command) => command.message(),
            Self::Lost => "the worker no longer owns the job",
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
    lost: CancellationToken,
    halted: CancellationToken,
    command: AtomicU8,
    commanded: Notify,
    slot: Mutex<Slot>,
}

impl RunControl {
    pub(crate) fn new(
        job_id: i64,
        lease: i64,
        saved: Option<Value>,
        shutdown: CancellationToken,
        lost: CancellationToken,
    ) -> Self {
        Self {
            job_id,
            lease,
            saved,
            halted: shutdown.child_token(),
            shutdown,
            lost,
            command: AtomicU8::new(0),
            commanded: Notify::new(),
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

    pub(crate) fn lost(&self) -> &CancellationToken {
        &self.lost
    }

    pub(crate) fn request(&self, command: Command) {
        if self.command.fetch_max(command.code(), Ordering::SeqCst) < command.code() {
            self.halted.cancel();
            self.commanded.notify_one();
        }
    }

    pub(crate) async fn halted(&self) -> JobError {
        tokio::select! {
            () = self.halted.cancelled() => {}
            () = self.lost.cancelled() => {}
        }
        JobError::halted(self.halt().unwrap_or(Halt::Shutdown))
    }

    pub(crate) async fn commanded(&self) {
        self.commanded.notified().await;
    }

    pub(crate) fn command(&self) -> Option<Command> {
        Command::from_code(self.command.load(Ordering::SeqCst))
    }

    fn halt(&self) -> Option<Halt> {
        if let Some(command) = self.command() {
            Some(Halt::Command(command))
        } else if self.lost.is_cancelled() {
            Some(Halt::Lost)
        } else {
            self.shutdown.is_cancelled().then_some(Halt::Shutdown)
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
}

async fn stop_point(halt: Option<Halt>) -> JobResult {
    if let Some(halt) = halt {
        return Err(JobError::halted(halt));
    }
    tokio::task::consume_budget().await;
    Ok(())
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
        let halt = control.halt();
        if halt.is_some() || control.is_due() {
            control.keep(serde_json::to_value(progress).map_err(JobError::permanent)?);
        }
        stop_point(halt).await
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
        Some(job) => stop_point(job.control().halt()).await,
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::run::{self, JobRun, interrupted};

    const PENDING: Duration = Duration::from_millis(100);
    const PROMPT: Duration = Duration::from_secs(1);

    struct Run {
        control: Arc<RunControl>,
        shutdown: CancellationToken,
        lost: CancellationToken,
    }

    impl Run {
        fn new() -> Self {
            let shutdown = CancellationToken::new();
            let lost = CancellationToken::new();
            let control = Arc::new(RunControl::new(
                7,
                1,
                None,
                shutdown.child_token(),
                lost.child_token(),
            ));
            Self {
                control,
                shutdown,
                lost,
            }
        }

        fn job(&self) -> JobRun {
            JobRun::for_test(7, Arc::clone(&self.control))
        }

        async fn halts(&self) -> (JobError, JobError) {
            run::within(self.job(), async {
                let error = tokio::time::timeout(PROMPT, interrupted())
                    .await
                    .expect("interrupted() completes once the job is halted");
                (error, checkpoint().await.unwrap_err())
            })
            .await
        }
    }

    #[tokio::test]
    async fn test_interrupted_returns_the_checkpoint_error_for_a_stop() {
        let run = Run::new();
        run.control.request(Command::Stop);
        let (interrupted, checkpointed) = run.halts().await;
        assert_eq!(interrupted, checkpointed);
        assert_eq!(interrupted.msg(), Command::Stop.message());
        assert!(interrupted.is_interruption());
    }

    #[tokio::test]
    async fn test_cancel_outranks_stop_in_interrupted() {
        for order in [
            [Command::Stop, Command::Cancel],
            [Command::Cancel, Command::Stop],
        ] {
            let run = Run::new();
            for command in order {
                run.control.request(command);
            }
            let (interrupted, checkpointed) = run.halts().await;
            assert_eq!(interrupted.msg(), Command::Cancel.message(), "{order:?}");
            assert_eq!(interrupted, checkpointed);
        }
    }

    #[tokio::test]
    async fn test_interrupted_resolves_on_shutdown() {
        let run = Run::new();
        let waiting = tokio::spawn(run::within(run.job(), async { interrupted().await }));
        tokio::time::sleep(PENDING).await;
        assert!(!waiting.is_finished());
        run.shutdown.cancel();
        let error = tokio::time::timeout(PROMPT, waiting)
            .await
            .expect("interrupted() completes on shutdown")
            .unwrap();
        assert_eq!(error, JobError::halted(Halt::Shutdown));
        assert!(error.is_interruption());
        assert_eq!(run.halts().await.1, error);
    }

    #[tokio::test]
    async fn test_interrupted_resolves_on_lost() {
        let run = Run::new();
        let waiting = tokio::spawn(run.job().interrupted());
        tokio::time::sleep(PENDING).await;
        assert!(!waiting.is_finished());
        run.lost.cancel();
        let error = tokio::time::timeout(PROMPT, waiting)
            .await
            .expect("interrupted() completes when the job is lost")
            .unwrap();
        assert_eq!(error.msg(), "the worker no longer owns the job");
        assert!(error.is_interruption());
        let (interrupted, checkpointed) = run.halts().await;
        assert_eq!(interrupted, error);
        assert_eq!(checkpointed, error);
    }

    #[tokio::test]
    async fn test_request_before_await_is_not_missed() {
        let run = Run::new();
        let error = run::within(run.job(), async {
            let created = interrupted();
            run.control.request(Command::Stop);
            tokio::time::timeout(PROMPT, created)
                .await
                .expect("a stop requested before the await still completes it")
        })
        .await;
        assert_eq!(error.msg(), Command::Stop.message());
    }

    #[tokio::test]
    async fn test_interrupted_is_pending_outside_a_job() {
        assert!(tokio::time::timeout(PENDING, interrupted()).await.is_err());
        let run = Run::new();
        run.control.request(Command::Cancel);
        let (spawned, handed) = run::within(run.job(), async {
            let handed = run::current_job().unwrap().interrupted();
            tokio::spawn(async move {
                let spawned = tokio::time::timeout(PENDING, interrupted()).await;
                (spawned.is_err(), tokio::time::timeout(PROMPT, handed).await)
            })
            .await
            .unwrap()
        })
        .await;
        assert!(spawned, "interrupted() in a spawned task never completes");
        assert_eq!(handed.unwrap().msg(), Command::Cancel.message());
    }
}
