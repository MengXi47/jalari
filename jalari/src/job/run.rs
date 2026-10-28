use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::task::futures::TaskLocalFuture;
use tokio_util::sync::CancellationToken;

use super::checkpoint::RunControl;
use crate::{JobError, JobId, Priority};

tokio::task_local! {
    static CURRENT_JOB: JobRun;
}

/// The attempt of a job that is running right now, as seen from inside it.
///
/// Get it with [`current_job`]. It is also available to every
/// [`JobMiddleware`](crate::JobMiddleware) through [`JobMeta`](crate::JobMeta).
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::{Job, JobResult};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct ChargeOrder {
///     order_id: i64,
/// }
///
/// #[jalari::job(name = "charge_order", queue = "payments")]
/// impl Job for ChargeOrder {
///     async fn run(self) -> JobResult {
///         let run = jalari::current_job().expect("inside a job");
///         let idempotency_key = format!("charge-{}", run.id());
///         if let Some(error) = run.last_error() {
///             println!("attempt {} after {error}", run.attempt());
///         }
///         println!(
///             "charging order {} with key {idempotency_key}, enqueued by {:?}",
///             self.order_id,
///             run.parent_id()
///         );
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct JobRun {
    pub(crate) id: JobId,
    pub(crate) name: &'static str,
    pub(crate) queue: Arc<str>,
    pub(crate) attempt: i32,
    pub(crate) max_attempts: i32,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) scheduled_at: DateTime<Utc>,
    pub(crate) priority: Priority,
    pub(crate) last_error: Option<Arc<str>>,
    pub(crate) parent_id: Option<JobId>,
    pub(crate) root_id: JobId,
    pub(crate) cancellation: CancellationToken,
    pub(crate) control: Arc<RunControl>,
}

impl JobRun {
    pub(crate) fn control(&self) -> &Arc<RunControl> {
        &self.control
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

    /// When the job was first enqueued; an [`OnConflict::Replace`](crate::OnConflict::Replace)
    /// keeps it.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// When this attempt became due: the requested run time, the end of a retry's backoff, or
    /// the time another worker took the job over after its worker stopped.
    ///
    /// The time the attempt waited for a worker is `Utc::now() - scheduled_at()`.
    pub fn scheduled_at(&self) -> DateTime<Utc> {
        self.scheduled_at
    }

    /// [`Priority`] the job was claimed with.
    pub fn priority(&self) -> Priority {
        self.priority
    }

    /// Error of the previous attempt, kept until an attempt succeeds.
    ///
    /// For a job another worker took over after its worker stopped, it is the interruption
    /// message. An [`OnConflict::Replace`](crate::OnConflict::Replace) clears it. The full error
    /// history is in [`admin::job`](crate::admin::job).
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Id of the job that was running when this one was enqueued, if any.
    ///
    /// Only an enqueue from the running job's own task records a parent; jobs enqueued from
    /// tasks it spawns have none.
    pub fn parent_id(&self) -> Option<JobId> {
        self.parent_id
    }

    /// Id of the first job in this chain: the parent's root, or this job's own id when it has no
    /// parent.
    pub fn root_id(&self) -> JobId {
        self.root_id
    }

    /// Cancelled when the worker starts shutting down; see [`shutdown_requested`].
    ///
    /// It covers shutdown only. [`JobRun::interrupted`] also completes on a stop, a cancel or a
    /// lost job, with the error to return.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Same as [`interrupted`] for this attempt; usable from tasks the job spawns.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # async fn upload() {}
    /// # async fn example() {
    /// let interrupted = jalari::current_job().map(|run| run.interrupted());
    /// tokio::spawn(async move {
    ///     if let Some(interrupted) = interrupted {
    ///         tokio::select! {
    ///             error = interrupted => eprintln!("upload interrupted: {error}"),
    ///             () = upload() => {}
    ///         }
    ///     }
    /// });
    /// # }
    /// ```
    pub fn interrupted(&self) -> impl Future<Output = JobError> + Send + 'static + use<> {
        let control = Arc::clone(&self.control);
        async move { control.halted().await }
    }

    /// Returns `true` once this attempt should stop; the synchronous twin of
    /// [`JobRun::interrupted`].
    ///
    /// It turns `true` at the same moment `interrupted` completes: on a [`stop`](crate::stop) or
    /// a [`cancel`](crate::cancel), when the worker starts shutting down, and when the worker has
    /// lost the job. It never turns back to `false`. Poll it from blocking code, which cannot
    /// await a future. The free functions [`current_job`] and [`interrupted`] read a task-local
    /// that does not exist inside `spawn_blocking`, so take the handle in the job's own task and
    /// move it in. Once the loop has noticed, return the error from `interrupted`, which is
    /// ready at once, so a stop, a cancel or a shutdown settles the job the way it does for
    /// async code.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # fn process(chunk: u32) {}
    /// # async fn example(chunks: Vec<u32>) -> jalari::JobResult {
    /// let run = jalari::current_job().expect("inside a job");
    /// let polled = run.clone();
    /// let finished = tokio::task::spawn_blocking(move || {
    ///     for chunk in chunks {
    ///         if polled.is_interrupted() {
    ///             return false;
    ///         }
    ///         process(chunk);
    ///     }
    ///     true
    /// })
    /// .await?;
    /// if !finished {
    ///     return Err(run.interrupted().await);
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub fn is_interrupted(&self) -> bool {
        self.control.command().is_some()
            || self.control.lost().is_cancelled()
            || self.control.shutdown().is_cancelled()
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
/// It covers shutdown only; prefer [`interrupted`], which also completes on a stop, a cancel or
/// a lost job and hands over the error to return. Return the [`checkpoint`](crate::checkpoint)
/// error from this branch: any other error counts as a failed attempt.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn export(id: i64) -> jalari::JobResult { Ok(()) }
/// # async fn example(ids: Vec<i64>) -> jalari::JobResult {
/// for id in ids {
///     tokio::select! {
///         () = jalari::shutdown_requested() => jalari::checkpoint().await?,
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

/// Completes with the error to return once the running job should stop.
///
/// It resolves when the job is asked to [`stop`](crate::stop) or [`cancel`](crate::cancel), when
/// its worker starts shutting down, or when the worker has lost the job. The error is the one
/// [`checkpoint`](crate::checkpoint) would return, so returning it releases or stops the job
/// without counting the attempt. During a shutdown any other error counts the attempt; a stop
/// or a cancel settles the job whatever it returns. Race it against long
/// `.await`s with `tokio::select!`. Outside a job, and in tasks the job spawns, it never
/// completes; use [`JobRun::interrupted`] there.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn export(id: i64) -> jalari::JobResult { Ok(()) }
/// # async fn example(ids: Vec<i64>) -> jalari::JobResult {
/// for id in ids {
///     tokio::select! {
///         error = jalari::interrupted() => return Err(error),
///         result = export(id) => result?,
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub fn interrupted() -> impl Future<Output = JobError> + Send + 'static {
    let control = current_job().map(|job| job.control);
    async move {
        match control {
            Some(control) => control.halted().await,
            None => std::future::pending().await,
        }
    }
}

pub(crate) fn within<F: Future>(job: JobRun, future: F) -> TaskLocalFuture<JobRun, F> {
    CURRENT_JOB.scope(job, future)
}

#[cfg(test)]
impl JobRun {
    pub(crate) fn for_test(id: i64, control: Arc<RunControl>) -> Self {
        Self {
            id: JobId(id),
            name: "test_job",
            queue: Arc::from("default"),
            attempt: 1,
            max_attempts: 3,
            created_at: Utc::now(),
            scheduled_at: Utc::now(),
            priority: Priority::Normal,
            last_error: None,
            parent_id: None,
            root_id: JobId(id),
            cancellation: control.shutdown().clone(),
            control,
        }
    }
}
