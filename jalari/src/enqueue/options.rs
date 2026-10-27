use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::job::context::Captured;
use crate::{JobId, Priority};

/// What to do when a job with the same `job_key` is already waiting, running or stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnConflict {
    /// Keep the existing job and drop the new one.
    #[default]
    KeepExisting,
    /// Overwrite the waiting job's payload, queue, run time and options, and reset its attempts.
    ///
    /// A running job cannot be replaced; the enqueue returns [`EnqueueOutcome::Running`] at
    /// once. That includes a job whose worker died less than about 8 seconds ago, until another
    /// worker records the interrupted attempt; after that the job is waiting again and is
    /// replaced, or, when that attempt was its last, it has failed and a new job is inserted.
    /// A waiting job that another transaction keeps locked for about a second ends the enqueue
    /// with [`JobKeyContention`](crate::ErrorKind::JobKeyContention).
    ///
    /// A [stopped](crate::stop) job, or one with a pending [`stop`](crate::stop) or
    /// [`cancel`](crate::cancel), is never replaced; the enqueue returns
    /// [`EnqueueOutcome::Kept`] and the pending request still applies. Replacing a job discards
    /// the progress its earlier attempts saved.
    Replace,
}

/// Result of an enqueue, carrying the id of the job that will run.
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::{EnqueueOptions, EnqueueOutcome, OnConflict};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendDigest { user_id: i64 }
/// # #[jalari::job(name = "send_digest")]
/// # impl jalari::Job for SendDigest {
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
///
/// # async fn example() -> jalari::Result<()> {
/// let options = EnqueueOptions::new()
///     .job_key("digest:42")
///     .on_conflict(OnConflict::Replace);
/// match jalari::enqueue_with(&SendDigest { user_id: 42 }, options).await? {
///     EnqueueOutcome::Running(id) => println!("job {id} is running and keeps its payload"),
///     outcome => println!("job {} sends the new digest", outcome.id()),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// A new job was inserted.
    Inserted(JobId),
    /// A job with the same `job_key` was already waiting, running or stopped and was kept.
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
    pub(super) context: ContextChoice,
    pub(super) priority: Option<Priority>,
    pub(super) max_attempts: Option<i32>,
    pub(super) queue_key_limit: Option<u16>,
    pub(super) expires_at: Option<DateTime<Utc>>,
    pub(super) expire_after: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub(super) enum ContextChoice {
    #[default]
    Inherit,
    Set(Captured),
    Clear,
}

impl ContextChoice {
    pub(super) fn inherits(&self) -> bool {
        matches!(self, Self::Inherit)
    }

    pub(super) fn resolve(self) -> Captured {
        match self {
            Self::Inherit => crate::job::context::current(),
            Self::Set(captured) => captured,
            Self::Clear => Captured::Empty,
        }
    }
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

    /// Lets up to `limit` jobs that share the [`queue_key`](Self::queue_key) run at the same
    /// time; defaults to 1.
    ///
    /// Each job runs only when one of the first `limit` slots of its key is free, so jobs of one
    /// key with different limits share the slots: a job with the default limit of 1 needs the
    /// first slot. The enqueue returns [`InvalidOptions`](crate::ErrorKind::InvalidOptions) for
    /// a limit of 0, a limit above 32767, or a limit without a `queue_key`.
    pub fn queue_key_limit(mut self, limit: u16) -> Self {
        self.queue_key_limit = Some(limit);
        self
    }

    /// Overrides [`Job::TIMEOUT`](crate::Job::TIMEOUT) for this job.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Overrides [`Job::PRIORITY`](crate::Job::PRIORITY) for this job.
    ///
    /// With [`OnConflict::Replace`] the replaced job takes this priority; a job kept by
    /// [`OnConflict::KeepExisting`] keeps its own.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Overrides [`Job::MAX_ATTEMPTS`](crate::Job::MAX_ATTEMPTS) for this job.
    ///
    /// [`admin::retry`](crate::admin::retry) gives the job this many attempts again, and an
    /// [`OnConflict::Replace`] writes it over the replaced job's. The enqueue returns
    /// [`InvalidOptions`](crate::ErrorKind::InvalidOptions) if it is below 1.
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Discards the job if it is still waiting at `expires_at`; a running attempt is never cut
    /// short.
    ///
    /// The rule holds for every wait, including retries, snoozes, releases and restarts. The job
    /// ends [`Discarded`](crate::JobState::Discarded) and frees its `job_key`. Replaces any
    /// earlier [`expire_after`](Self::expire_after).
    pub fn expires_at(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self.expire_after = None;
        self
    }

    /// Discards the job if it is still waiting `after` the enqueue, measured by the database
    /// clock and including any [`delay`](Self::delay); a running attempt is never cut short.
    ///
    /// Otherwise it behaves like [`expires_at`](Self::expires_at), which it replaces.
    pub fn expire_after(mut self, after: Duration) -> Self {
        self.expire_after = Some(after);
        self.expires_at = None;
        self
    }

    /// Chooses what happens when the [`job_key`](Self::job_key) is already taken; defaults to
    /// [`OnConflict::KeepExisting`].
    pub fn on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    /// Stores `context` with the job instead of the one from the surrounding
    /// [`scope`](crate::scope).
    ///
    /// Workers pass it to their [`JobMiddleware`](crate::JobMiddleware) through
    /// [`JobMeta::context`](crate::JobMeta::context). It is serialized here; a value that cannot
    /// be serialized makes the enqueue fail with
    /// [`PayloadEncodeFailed`](crate::ErrorKind::PayloadEncodeFailed).
    pub fn context<C: Serialize>(mut self, context: &C) -> Self {
        self.context = ContextChoice::Set(Captured::encode(context));
        self
    }

    /// Stores no context with the job, even inside a [`scope`](crate::scope) or a running job.
    pub fn no_context(mut self) -> Self {
        self.context = ContextChoice::Clear;
        self
    }
}
