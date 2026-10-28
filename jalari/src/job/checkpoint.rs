use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::fence::{Fenced, raised, write_progress, write_progress_in};
use super::run::current_job;
use super::step::StepKeys;
use crate::enqueue::EnqueueRequest;
use crate::init::config;
use crate::{Error, JobError, JobResult};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProgressCopy {
    pub(crate) value: Value,
    pub(crate) seq: i64,
}

#[derive(Debug)]
struct Slot {
    latest: Option<ProgressCopy>,
    seq: i64,
    kept_at: Option<Instant>,
    dirty: bool,
}

impl Slot {
    fn numbered_from(seq: i64) -> Self {
        Self {
            latest: None,
            seq,
            kept_at: None,
            dirty: false,
        }
    }

    fn next_seq(&mut self) -> i64 {
        self.seq += 1;
        self.seq
    }
}

#[derive(Default)]
struct Children(Mutex<Vec<EnqueueRequest>>);

impl fmt::Debug for Children {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Children")
            .field("buffered", &self.0.lock().unwrap().len())
            .finish()
    }
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
    started: Instant,
    reached_millis: AtomicU64,
    checkpoints: bool,
    slot: Mutex<Slot>,
    children: Children,
    steps: StepKeys,
}

impl RunControl {
    pub(crate) fn new(
        job_id: i64,
        lease: i64,
        saved: Option<Value>,
        saved_seq: i64,
        shutdown: CancellationToken,
        lost: CancellationToken,
        checkpoints: bool,
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
            started: Instant::now(),
            reached_millis: AtomicU64::new(0),
            checkpoints,
            slot: Mutex::new(Slot::numbered_from(saved_seq)),
            children: Children::default(),
            steps: StepKeys::default(),
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

    pub(crate) fn reach(&self) -> Option<Halt> {
        let reached = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.reached_millis.store(reached, Ordering::Relaxed);
        self.halt()
    }

    pub(crate) fn declares_checkpoints(&self) -> bool {
        self.checkpoints
    }

    pub(crate) fn since_checkpoint(&self) -> Duration {
        let reached = Duration::from_millis(self.reached_millis.load(Ordering::Relaxed));
        self.started.elapsed().saturating_sub(reached)
    }

    fn is_due(&self) -> bool {
        self.slot
            .lock()
            .unwrap()
            .kept_at
            .is_none_or(|kept_at| kept_at.elapsed() >= KEEP_INTERVAL)
    }

    fn keep(&self, value: Value) {
        let mut slot = self.slot.lock().unwrap();
        let seq = slot.next_seq();
        slot.latest = Some(ProgressCopy { value, seq });
        slot.kept_at = Some(Instant::now());
        slot.dirty = true;
    }

    fn keep_now(&self, value: Value) -> i64 {
        let mut slot = self.slot.lock().unwrap();
        let seq = slot.next_seq();
        slot.latest = Some(ProgressCopy { value, seq });
        slot.kept_at = Some(Instant::now());
        slot.dirty = true;
        seq
    }

    fn next_seq(&self) -> i64 {
        self.slot.lock().unwrap().next_seq()
    }

    fn mark_clean(&self, seq: i64) {
        let mut slot = self.slot.lock().unwrap();
        if slot.latest.as_ref().is_some_and(|latest| latest.seq == seq) {
            slot.dirty = false;
        }
    }

    pub(crate) fn lose(&self) -> JobError {
        self.lost.cancel();
        JobError::halted(Halt::Lost)
    }

    pub(crate) fn latest(&self) -> Option<ProgressCopy> {
        self.slot.lock().unwrap().latest.clone()
    }

    pub(crate) fn take_dirty(&self) -> Option<ProgressCopy> {
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

    pub(crate) fn buffer(&self, request: EnqueueRequest) {
        self.children.0.lock().unwrap().push(request);
    }

    pub(crate) fn take_children(&self) -> Vec<EnqueueRequest> {
        std::mem::take(&mut *self.children.0.lock().unwrap())
    }

    pub(crate) fn steps(&self) -> &StepKeys {
        &self.steps
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
    /// state. When a crash must not lose the copy, use [`save_now`](Self::save_now) or
    /// [`save_in`](Self::save_in). Outside a job it does nothing and returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The job was asked to stop or to cancel, or its worker is shutting down
    /// - `progress` cannot be serialized to JSON (a [`JobError::permanent`])
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize, Default)]
    /// struct Cursor {
    ///     next_row: u64,
    /// }
    ///
    /// # async fn copy_row(_row: u64) {}
    /// async fn copy_rows(rows: u64) -> jalari::JobResult {
    ///     let (saved, checkpoint) = jalari::resume::<Cursor>()?;
    ///     let mut cursor = saved.unwrap_or_default();
    ///     while cursor.next_row < rows {
    ///         copy_row(cursor.next_row).await;
    ///         cursor.next_row += 1;
    ///         checkpoint.save(&cursor).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn save(&self, progress: &P) -> JobResult {
        let Some(control) = &self.control else {
            return Ok(());
        };
        let halt = control.reach();
        if halt.is_some() || control.is_due() {
            control.keep(serde_json::to_value(progress).map_err(JobError::permanent)?);
        }
        stop_point(halt).await
    }

    /// Writes `progress` to the database and returns once it is stored.
    ///
    /// Use it after work whose effects must not be repeated if the worker dies right after, such
    /// as a page of rows sent to another system. [`save`](Self::save) keeps a copy in memory and
    /// costs nothing; `save_now` costs one short transaction. The write is fenced by the run's
    /// lease, so a run that another worker has taken over cannot store anything: the call then
    /// returns the same error as [`checkpoint`](crate::checkpoint) for a lost job, and the job is
    /// aborted at its next `.await` even when that error is ignored. A newer copy that is already
    /// stored, for example from [`save_in`](Self::save_in), is kept. Like `save`, it is a stop
    /// point: when the job was asked to stop, it returns that error and the copy is saved
    /// together with the stopped state. Outside a job it does nothing and returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The job was asked to stop or to cancel, or its worker is shutting down
    /// - Another worker has taken the job over (the job is aborted)
    /// - The job's control row is missing, because the pool given to [`init`](crate::init)
    ///   points at another database or schema than the worker's (a [`JobError::permanent`])
    /// - `progress` cannot be serialized to JSON (a [`JobError::permanent`])
    /// - The database could not be reached or refused the write; the progress may have been
    ///   stored all the same, and the worker writes the copy again later
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize, Default)]
    /// struct Cursor {
    ///     next_page: u32,
    /// }
    ///
    /// # async fn send_page(_page: u32) -> Result<bool, std::io::Error> { Ok(false) }
    /// async fn export_pages() -> jalari::JobResult {
    ///     let (saved, checkpoint) = jalari::resume::<Cursor>()?;
    ///     let mut cursor = saved.unwrap_or_default();
    ///     while send_page(cursor.next_page).await? {
    ///         cursor.next_page += 1;
    ///         checkpoint.save_now(&cursor).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn save_now(&self, progress: &P) -> JobResult {
        let Some(control) = &self.control else {
            return Ok(());
        };
        let value = serde_json::to_value(progress).map_err(JobError::permanent)?;
        let halt = control.reach();
        let seq = control.keep_now(value.clone());
        if let Some(halt) = halt {
            return Err(JobError::halted(halt));
        }
        let config = config().map_err(JobError::permanent)?;
        let written =
            write_progress(&config.pool.pool(), &config.schema, control, &value, seq).await;
        match written {
            Ok(Fenced::Written) => {
                control.mark_clean(seq);
                Ok(())
            }
            Ok(Fenced::Lost) => Err(control.lose()),
            Ok(Fenced::Missing) => Err(missing_row(control, config.schema.name())),
            Err(e) => Err(JobError::new(format!(
                "could not save the progress, it may have been saved: {}",
                Error::from(e)
            ))),
        }
    }

    /// Writes `progress` inside `transaction`, so it is stored if and only if the transaction
    /// commits.
    ///
    /// Use it when each unit of work writes to tables in the database jalari uses: the rows and
    /// the progress then commit together, and a retry, a takeover or a restart never applies a
    /// unit twice. It does not commit `transaction`; call it as the last statement before
    /// `commit`. It writes only to the database and never changes the copy that
    /// [`save`](Self::save) keeps, so a transaction that rolls back leaves no trace.
    ///
    /// The write is fenced by the run's lease. When another worker has taken the job over, the
    /// statement fails, which aborts `transaction`: its `commit` then returns `Ok` and stores
    /// nothing, as PostgreSQL ends an aborted transaction with a rollback. The job is aborted at
    /// its next `.await`, also when the error is ignored. The other rules:
    ///
    /// - The transaction must run on the database and schema of [`init`](crate::init), at the
    ///   `READ COMMITTED` isolation level, PostgreSQL's default. Under `REPEATABLE READ` or
    ///   `SERIALIZABLE`, a progress write of the same run that commits after the transaction's
    ///   snapshot makes this call fail with a serialization error.
    /// - Rolling back to a savepoint taken before the call keeps the transaction alive without
    ///   the progress, so pass the outermost transaction or let a failure end it.
    /// - The job's control row stays locked until the transaction ends. Commit within 10 seconds:
    ///   the call sets `idle_in_transaction_session_timeout` to 10 seconds for the rest of the
    ///   transaction, unless a shorter limit is set already, so that a frozen worker cannot keep
    ///   the job from being taken over. Make no other jalari call for the same job on another
    ///   connection before the commit, such as [`save_now`](Self::save_now) or
    ///   [`step`](crate::step), which wait for the lock and fail after 2 seconds.
    ///
    /// Like [`save`](Self::save), it is a stop point: when the job was asked to stop, it writes
    /// the progress and then returns the stop error, so the transaction can roll back with the
    /// unit it carried. Outside a job it does nothing and returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The job was asked to stop or to cancel, or its worker is shutting down
    /// - Another worker has taken the job over (the job is aborted, and the transaction can no
    ///   longer commit anything)
    /// - The job's control row is missing, because the transaction runs on another database or
    ///   schema than the worker's (a [`JobError::permanent`]; the transaction is aborted)
    /// - `progress` cannot be serialized to JSON (a [`JobError::permanent`])
    /// - The statement failed, for example with a serialization failure under `REPEATABLE READ`;
    ///   retrying the unit in a new transaction can succeed
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize, Default)]
    /// struct Cursor {
    ///     last_id: i64,
    /// }
    ///
    /// async fn archive(pool: jalari::sqlx::PgPool) -> jalari::JobResult {
    ///     let (saved, checkpoint) = jalari::resume::<Cursor>()?;
    ///     let mut cursor = saved.unwrap_or_default();
    ///     loop {
    ///         let mut transaction = pool.begin().await?;
    ///         let moved: Option<i64> = jalari::sqlx::query_scalar(
    ///             "WITH moved AS (
    ///                  DELETE FROM orders WHERE id IN (
    ///                      SELECT id FROM orders WHERE id > $1 ORDER BY id LIMIT 100
    ///                  )
    ///                  RETURNING *
    ///              )
    ///              INSERT INTO archived_orders SELECT * FROM moved RETURNING id",
    ///         )
    ///         .bind(cursor.last_id)
    ///         .fetch_all(&mut *transaction)
    ///         .await?
    ///         .into_iter()
    ///         .max();
    ///         let Some(last_id) = moved else {
    ///             return Ok(());
    ///         };
    ///         cursor.last_id = last_id;
    ///         checkpoint.save_in(&mut transaction, &cursor).await?;
    ///         transaction.commit().await?;
    ///     }
    /// }
    /// ```
    pub async fn save_in(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        progress: &P,
    ) -> JobResult {
        let Some(control) = &self.control else {
            return Ok(());
        };
        let value = serde_json::to_value(progress).map_err(JobError::permanent)?;
        let halt = control.reach();
        let seq = control.next_seq();
        let config = config().map_err(JobError::permanent)?;
        let written = write_progress_in(transaction, &config.schema, control, &value, seq).await;
        match written {
            Ok(()) => stop_point(halt).await,
            Err(e) => Err(match raised(&e) {
                Some(Fenced::Lost) => control.lose(),
                Some(Fenced::Missing) => missing_row(control, config.schema.name()),
                Some(Fenced::Written) | None => JobError::new(format!(
                    "could not save the progress in the transaction: {}",
                    Error::from(e)
                )),
            }),
        }
    }
}

fn missing_row(control: &RunControl, schema: &str) -> JobError {
    JobError::permanent(std::io::Error::other(format!(
        "job {} has no control row in schema {schema}; jalari must reach the database and \
         schema the worker uses",
        control.job_id()
    )))
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
        Some(job) => stop_point(job.control().reach()).await,
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
                0,
                shutdown.child_token(),
                lost.child_token(),
                false,
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
    async fn test_copies_are_numbered_after_the_stored_copy() {
        let control = RunControl::new(
            7,
            1,
            None,
            41,
            CancellationToken::new(),
            CancellationToken::new(),
            false,
        );
        run::within(JobRun::for_test(7, Arc::new(control)), async {
            let (_, checkpoint) = resume::<u32>().unwrap();
            checkpoint.save(&1).await.unwrap();
            let control = run::current_job().unwrap().control().clone();
            let first = control.take_dirty().unwrap();
            assert_eq!(
                first,
                ProgressCopy {
                    value: 1.into(),
                    seq: 42
                }
            );
            assert_eq!(control.take_dirty(), None);
            control.request(Command::Stop);
            checkpoint.save(&2).await.unwrap_err();
            assert_eq!(
                control.latest(),
                Some(ProgressCopy {
                    value: 2.into(),
                    seq: 43
                })
            );
        })
        .await;
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
