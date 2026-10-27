use std::future::Future;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{JobError, Priority, RetryPolicy};

const DEFAULT_MAX_ATTEMPTS: i32 = 10;

/// Result of [`Job::run`]: `Ok(())` marks the job succeeded, `Err` marks the attempt failed.
pub type JobResult = std::result::Result<(), JobError>;

/// A unit of background work.
///
/// The struct's fields are the job's arguments. They are serialized to JSON when the job is
/// enqueued with [`jalari::enqueue`](crate::enqueue) and deserialized by the worker that runs
/// it. Implement the trait inside `#[jalari::job]` so workers in the same binary can find the
/// type by [`NAME`](Self::NAME).
///
/// A job runs at least once: if its worker dies mid-run, another worker runs it again, so `run`
/// should be idempotent.
///
/// With `#[jalari::job(fingerprint = true)]`, every enqueued job is stamped with a fingerprint
/// of the code it reaches. A worker runs a job only when the job has no fingerprint or when the
/// worker was built from the same code, so a job enqueued by another build waits the way a job
/// under an unknown [`NAME`](Self::NAME) does. Fingerprints are off by default;
/// [`#[jalari::job]`](crate::job) lists what they cover.
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::{Job, JobResult};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct ResizeImage {
///     path: String,
///     width: u32,
/// }
///
/// #[jalari::job(name = "resize_image", queue = "images", max_attempts = 3, timeout = 60)]
/// impl Job for ResizeImage {
///     async fn run(self) -> JobResult {
///         let bytes = std::fs::read(&self.path)?;
///         println!("resizing {} bytes to {}px", bytes.len(), self.width);
///         Ok(())
///     }
/// }
///
/// # async fn example() -> jalari::Result<()> {
/// jalari::enqueue(&ResizeImage { path: "/tmp/a.png".to_owned(), width: 320 }).await?;
/// # Ok(())
/// # }
/// ```
pub trait Job: Serialize + DeserializeOwned + Send + 'static {
    /// Stable name stored with each job and used to find the type that runs it.
    ///
    /// `#[jalari::job(name = "...")]` can set it instead of the impl. When neither sets it,
    /// `#[jalari::job]` uses the type's name as written, so renaming the type renames the job.
    ///
    /// Must be unique within a binary, among the names and [`ALIASES`](Self::ALIASES) of every
    /// linked job. Jobs stored under a name no linked job declares wait until a worker that
    /// knows it picks them up; to rename a job, keep the old name in `ALIASES`. Jobs enqueued
    /// by another build of the same name wait the same way, for a worker of that build.
    ///
    /// Must not be empty; `#[jalari::job]` rejects an empty name at compile time:
    ///
    /// ```rust,compile_fail,E0080
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Ping;
    /// #[jalari::job]
    /// impl Job for Ping {
    ///     const NAME: &'static str = "";
    ///
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    const NAME: &'static str;

    /// Number of attempts before the job is marked failed, including the first; defaults to 10.
    ///
    /// An attempt counts as soon as it starts, so attempts cut short by a crashed worker count
    /// too, and a job that keeps crashing its worker stops once the limit is reached.
    ///
    /// Must be at least 1; `#[jalari::job]` rejects a smaller value at compile time:
    ///
    /// ```rust,compile_fail,E0080
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Ping;
    /// #[jalari::job]
    /// impl Job for Ping {
    ///     const NAME: &'static str = "ping";
    ///     const MAX_ATTEMPTS: i32 = 0;
    ///
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    ///
    /// The same holds when `#[jalari::job(max_attempts = ...)]` sets it:
    ///
    /// ```rust,compile_fail,E0080
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Ping;
    /// #[jalari::job(name = "ping", max_attempts = 0)]
    /// impl Job for Ping {
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    const MAX_ATTEMPTS: i32 = DEFAULT_MAX_ATTEMPTS;

    /// Longest time one attempt may run before it is cancelled and counted as failed.
    ///
    /// `None`, the default, means no limit.
    /// [`EnqueueOptions::timeout`](crate::EnqueueOptions::timeout) overrides it per job.
    ///
    /// A limit must be longer than zero and at most `i32::MAX` milliseconds, about 24.8 days;
    /// `#[jalari::job]` rejects any other value at compile time:
    ///
    /// ```rust,compile_fail,E0080
    /// # use std::time::Duration;
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Ping;
    /// #[jalari::job]
    /// impl Job for Ping {
    ///     const NAME: &'static str = "ping";
    ///     const TIMEOUT: Option<Duration> = Some(Duration::ZERO);
    ///
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    ///
    /// ```rust,compile_fail,E0080
    /// # use std::time::Duration;
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Ping;
    /// #[jalari::job]
    /// impl Job for Ping {
    ///     const NAME: &'static str = "ping";
    ///     const TIMEOUT: Option<Duration> = Some(Duration::from_secs(3_000_000));
    ///
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    ///
    /// The same holds when `#[jalari::job(timeout = ...)]` sets it in seconds:
    ///
    /// ```rust,compile_fail,E0080
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Ping;
    /// #[jalari::job(name = "ping", timeout = 0)]
    /// impl Job for Ping {
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    const TIMEOUT: Option<Duration> = None;

    /// Priority of every job of this type unless the enqueue sets one; defaults to
    /// [`Normal`](Priority::Normal).
    ///
    /// Workers take the due jobs of a queue from the highest [`Priority`] down.
    const PRIORITY: Priority = Priority::Normal;

    /// Earlier names of this job whose waiting jobs and schedules this type still runs.
    ///
    /// New jobs are always enqueued under [`NAME`](Self::NAME). Jobs stored under an alias run
    /// through this type, and [`JobRun::name`](crate::JobRun::name) reports `NAME` for them. A
    /// runtime schedule stored under an alias is moved to `NAME` the next time it fires. The
    /// code fingerprint leaves out `NAME` and `ALIASES`, so with the old name kept here, jobs
    /// that waited under it stay runnable by the renamed build. Once no job and no schedule
    /// uses an alias, it can be removed.
    ///
    /// An alias must not be empty, equal `NAME` or repeat; `#[jalari::job]` rejects such a list
    /// at compile time:
    ///
    /// ```rust,compile_fail,E0080
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct SendEmail;
    /// #[jalari::job]
    /// impl Job for SendEmail {
    ///     const NAME: &'static str = "send_email";
    ///     const ALIASES: &'static [&'static str] = &["send_email"];
    ///
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    ///
    /// ```rust,compile_fail,E0080
    /// # use jalari::{Job, JobResult};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct SendEmail;
    /// #[jalari::job(name = "send_email", aliases = ["old", "old"])]
    /// impl Job for SendEmail {
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    ///
    /// An alias must also not be the `NAME` or an alias of another job linked into the same
    /// binary; [`WorkerBuilder::build`](crate::WorkerBuilder::build) rejects that.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use jalari::{Job, JobResult};
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct SendWelcome;
    ///
    /// #[jalari::job(name = "send_welcome", queue = "emails", aliases = ["welcome_email"])]
    /// impl Job for SendWelcome {
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    const ALIASES: &'static [&'static str] = &[];

    /// Retry policy for this job type, used instead of the worker's; `None`, the default, uses
    /// [`WorkerBuilder::retry_policy`](crate::WorkerBuilder::retry_policy).
    ///
    /// Any [`RetryPolicy`] that can be built in a const or named as a `static` works. The code
    /// fingerprint leaves the policy out, so jobs already waiting when a new policy is deployed
    /// are retried by the new one.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::time::Duration;
    ///
    /// use jalari::{ExponentialBackoff, Job, JobResult, RetryPolicy};
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct DeliverWebhook {
    ///     url: String,
    /// }
    ///
    /// #[jalari::job(name = "deliver_webhook", queue = "webhooks", max_attempts = 20)]
    /// impl Job for DeliverWebhook {
    ///     const RETRY_POLICY: Option<&'static dyn RetryPolicy> = Some(&ExponentialBackoff {
    ///         base: Duration::from_secs(30),
    ///         max: Duration::from_secs(6 * 60 * 60),
    ///         jitter: 0.3,
    ///     });
    ///
    ///     async fn run(self) -> JobResult {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    const RETRY_POLICY: Option<&'static dyn RetryPolicy> = None;

    /// Value the job receives in [`run`](Self::run), provided by the worker's
    /// [`JobMiddleware`](crate::JobMiddleware).
    ///
    /// `#[jalari::job]` sets it to `()` when the impl leaves it out, which needs no middleware.
    /// For any other type, [`WorkerBuilder::build`](crate::WorkerBuilder::build) checks that a
    /// middleware whose [`Provides`](crate::JobMiddleware::Provides) is this type is registered.
    type Context: Send + Sync + 'static;

    /// Runs the job.
    ///
    /// An `Err` is retried according to [`RETRY_POLICY`](Self::RETRY_POLICY), or the worker's
    /// [`RetryPolicy`](crate::RetryPolicy) when it is `None`, until
    /// [`MAX_ATTEMPTS`](Self::MAX_ATTEMPTS) is reached, unless it is [`JobError::permanent`].
    /// A panic or a timeout counts as a failed attempt and does not affect other jobs.
    ///
    /// Inside `#[jalari::job]` the `context` parameter may be left out when it is not needed.
    fn run(self, context: &Self::Context) -> impl Future<Output = JobResult> + Send;
}
