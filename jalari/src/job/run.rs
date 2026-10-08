use std::future::Future;
use std::sync::Arc;

use tokio::task::futures::TaskLocalFuture;
use tokio_util::sync::CancellationToken;

use crate::JobId;

tokio::task_local! {
    static CURRENT_JOB: JobRun;
}

/// The attempt of a job that is running right now, as seen from inside it.
///
/// Get it with [`current_job`]. It is also available to every
/// [`JobMiddleware`](crate::JobMiddleware) through [`JobMeta`](crate::JobMeta).
#[derive(Debug, Clone)]
pub struct JobRun {
    id: JobId,
    name: &'static str,
    queue: Arc<str>,
    attempt: i32,
    max_attempts: i32,
    cancellation: CancellationToken,
}

impl JobRun {
    pub(crate) fn new(
        id: JobId,
        name: &'static str,
        queue: Arc<str>,
        attempt: i32,
        max_attempts: i32,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            id,
            name,
            queue,
            attempt,
            max_attempts,
            cancellation,
        }
    }

    /// Id of the job; it stays the same across retries, so it works as an idempotency key.
    pub fn id(&self) -> JobId {
        self.id
    }

    /// [`Job::NAME`](crate::Job::NAME) of the job.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Queue the job was taken from.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Number of this attempt, starting at 1; it changes on every retry.
    pub fn attempt(&self) -> i32 {
        self.attempt
    }

    /// Attempts allowed before the job is marked failed.
    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Cancelled when the worker starts shutting down; see [`shutdown_requested`].
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// Returns the attempt that is running, or `None` outside a job.
///
/// Spawned tasks do not inherit it, so call it from the job's own task.
///
/// # Examples
///
/// ```rust
/// assert!(jalari::current_job().is_none());
/// ```
pub fn current_job() -> Option<JobRun> {
    CURRENT_JOB.try_with(Clone::clone).ok()
}

/// Completes when the worker running the current job starts shutting down.
///
/// Running jobs get [`WorkerConfig::shutdown_timeout`](crate::WorkerConfig::shutdown_timeout)
/// to finish after that; a job can save its progress and return early instead of being aborted.
/// Outside a job it never completes.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn export(id: i64) -> jalari::JobResult { Ok(()) }
/// # async fn example(ids: Vec<i64>) -> jalari::JobResult {
/// for id in ids {
///     tokio::select! {
///         () = jalari::shutdown_requested() => {
///             return Err(jalari::JobError::new("stopped for shutdown".to_owned()));
///         }
///         result = export(id) => result?,
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub async fn shutdown_requested() {
    match current_job() {
        Some(job) => job.cancellation.cancelled().await,
        None => std::future::pending().await,
    }
}

pub(crate) fn within<F: Future>(job: JobRun, future: F) -> TaskLocalFuture<JobRun, F> {
    CURRENT_JOB.scope(job, future)
}
