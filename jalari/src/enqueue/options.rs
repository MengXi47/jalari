use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::JobId;

/// What to do when a job with the same `job_key` is already waiting or running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnConflict {
    /// Keep the existing job and drop the new one.
    #[default]
    KeepExisting,
    /// Overwrite the waiting job's payload, queue, run time and options, and reset its attempts.
    ///
    /// A job that is already running cannot be replaced; the enqueue returns
    /// [`EnqueueOutcome::Running`] instead.
    Replace,
}

/// Result of an enqueue, carrying the id of the job that will run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// A new job was inserted.
    Inserted(JobId),
    /// A job with the same `job_key` was already waiting or running and was kept.
    Kept(JobId),
    /// A waiting job with the same `job_key` was overwritten.
    Replaced(JobId),
    /// A job with the same `job_key` is running, so it could not be replaced.
    Running(JobId),
}

impl EnqueueOutcome {
    /// Id of the inserted, kept, replaced or running job.
    pub fn id(&self) -> JobId {
        match *self {
            Self::Inserted(id) | Self::Kept(id) | Self::Replaced(id) | Self::Running(id) => id,
        }
    }
}

/// Options for [`enqueue_with`](crate::enqueue_with) and [`enqueue_in`](crate::enqueue_in).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// let options = jalari::EnqueueOptions::new()
///     .queue("emails")
///     .delay(Duration::from_secs(300))
///     .job_key("digest:user:42")
///     .on_conflict(jalari::OnConflict::Replace);
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct EnqueueOptions {
    pub(super) queue: Option<String>,
    pub(super) run_at: Option<DateTime<Utc>>,
    pub(super) delay: Option<Duration>,
    pub(super) job_key: Option<String>,
    pub(super) queue_key: Option<String>,
    pub(super) timeout: Option<Duration>,
    pub(super) on_conflict: OnConflict,
}

impl EnqueueOptions {
    /// Creates options that run the job now, on its default queue, with no keys.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sends the job to this queue instead of the one in `#[jalari::job(queue = ...)]`.
    ///
    /// Only workers that configure the queue with
    /// [`WorkerBuilder::queue`](crate::WorkerBuilder::queue) will run it.
    pub fn queue(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_owned());
        self
    }

    /// Runs the job no earlier than `delay` from now, measured by the database clock.
    ///
    /// Replaces any earlier [`run_at`](Self::run_at).
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self.run_at = None;
        self
    }

    /// Runs the job no earlier than `run_at`.
    ///
    /// Replaces any earlier [`delay`](Self::delay).
    pub fn run_at(mut self, run_at: DateTime<Utc>) -> Self {
        self.run_at = Some(run_at);
        self.delay = None;
        self
    }

    /// Deduplicates by key: at most one job with this key waits or runs at a time.
    ///
    /// What happens when one already exists is decided by [`on_conflict`](Self::on_conflict).
    /// Once the job finishes, the key is free again.
    pub fn job_key(mut self, job_key: &str) -> Self {
        self.job_key = Some(job_key.to_owned());
        self
    }

    /// Serializes execution by key: jobs that share it never run at the same time.
    ///
    /// Jobs with the same key may still be waiting together; workers run them one after another.
    pub fn queue_key(mut self, queue_key: &str) -> Self {
        self.queue_key = Some(queue_key.to_owned());
        self
    }

    /// Overrides [`Job::TIMEOUT`](crate::Job::TIMEOUT) for this job.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Chooses what happens when the [`job_key`](Self::job_key) is already taken; defaults to
    /// [`OnConflict::KeepExisting`].
    pub fn on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.on_conflict = on_conflict;
        self
    }
}
