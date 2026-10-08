use std::future::Future;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::JobError;

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
/// # Examples
///
/// ```rust,no_run
/// use std::time::Duration;
///
/// use jalari::{Job, JobResult};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct ResizeImage {
///     path: String,
///     width: u32,
/// }
///
/// #[jalari::job(queue = "images")]
/// impl Job for ResizeImage {
///     const NAME: &'static str = "resize_image";
///     const MAX_ATTEMPTS: i32 = 3;
///     const TIMEOUT: Option<Duration> = Some(Duration::from_secs(60));
///
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
    /// Must be unique within a binary. Jobs already enqueued under an old name keep waiting
    /// until a worker that knows that name picks them up.
    const NAME: &'static str;

    /// Number of attempts before the job is marked failed, including the first; defaults to 10.
    ///
    /// An attempt counts as soon as it starts, so attempts cut short by a crashed worker count
    /// too, and a job that keeps crashing its worker stops once the limit is reached.
    const MAX_ATTEMPTS: i32 = DEFAULT_MAX_ATTEMPTS;

    /// Longest time one attempt may run before it is cancelled and counted as failed.
    ///
    /// `None`, the default, means no limit.
    /// [`EnqueueOptions::timeout`](crate::EnqueueOptions::timeout) overrides it per job.
    const TIMEOUT: Option<Duration> = None;

    /// Value the job receives in [`run`](Self::run), provided by the worker's
    /// [`JobMiddleware`](crate::JobMiddleware).
    ///
    /// `#[jalari::job]` sets it to `()` when the impl leaves it out, which needs no middleware.
    /// For any other type, [`WorkerBuilder::build`](crate::WorkerBuilder::build) checks that a
    /// middleware whose [`Provides`](crate::JobMiddleware::Provides) is this type is registered.
    type Context: Send + Sync + 'static;

    /// Runs the job.
    ///
    /// An `Err` is retried according to the worker's [`RetryPolicy`](crate::RetryPolicy) until
    /// [`MAX_ATTEMPTS`](Self::MAX_ATTEMPTS) is reached, unless it is [`JobError::permanent`].
    /// A panic or a timeout counts as a failed attempt and does not affect other jobs.
    ///
    /// Inside `#[jalari::job]` the `context` parameter may be left out when it is not needed.
    fn run(self, context: &Self::Context) -> impl Future<Output = JobResult> + Send;
}
