use std::future::Future;

use sqlx::PgConnection;

use super::{EnqueueOptions, EnqueueOutcome, EnqueueRequest};
use crate::{Job, Result};

/// Enqueues a job to run as soon as a worker is free.
///
/// The job goes to the queue named in `#[jalari::job(queue = ...)]`, or `default`. It is
/// serialized right away, so the returned future does not borrow it and can be spawned.
///
/// # Errors
///
/// Returns an error if:
/// - [`init`](crate::init) has not completed
///   ([`NotInitialized`](crate::ErrorKind::NotInitialized))
/// - The job cannot be serialized
///   ([`PayloadEncodeFailed`](crate::ErrorKind::PayloadEncodeFailed))
/// - The database query fails
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendEmail { to: String }
/// # #[jalari::job]
/// # impl jalari::Job for SendEmail {
/// #     const NAME: &'static str = "send_email";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn example() -> jalari::Result<()> {
/// let outcome = jalari::enqueue(&SendEmail { to: "a@example.com".to_owned() }).await?;
/// println!("enqueued job {}", outcome.id());
/// # Ok(())
/// # }
/// ```
pub fn enqueue<T: Job>(job: &T) -> impl Future<Output = Result<EnqueueOutcome>> + Send + use<T> {
    enqueue_with(job, EnqueueOptions::new())
}

/// Enqueues a job with options such as a delay, a queue or a `job_key`.
///
/// Commits in its own transaction and wakes the workers listening on the queue.
///
/// # Errors
///
/// Returns an error if:
/// - Any condition listed for [`enqueue`] occurs
/// - The timeout exceeds about 24 days
///   ([`TimeoutTooLong`](crate::ErrorKind::TimeoutTooLong))
/// - A `job_key` replace keeps racing with other writers
///   ([`JobKeyContention`](crate::ErrorKind::JobKeyContention))
///
/// # Examples
///
/// ```rust,no_run
/// # use std::time::Duration;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendEmail { to: String }
/// # #[jalari::job]
/// # impl jalari::Job for SendEmail {
/// #     const NAME: &'static str = "send_email";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn example() -> jalari::Result<()> {
/// let options = jalari::EnqueueOptions::new()
///     .delay(Duration::from_secs(60))
///     .job_key("welcome:42");
/// jalari::enqueue_with(&SendEmail { to: "a@example.com".to_owned() }, options).await?;
/// # Ok(())
/// # }
/// ```
pub fn enqueue_with<T: Job>(
    job: &T,
    options: EnqueueOptions,
) -> impl Future<Output = Result<EnqueueOutcome>> + Send + use<T> {
    let request = EnqueueRequest::build(job, options);
    async move { request?.submit().await }
}

/// Enqueues a job on a connection you control, usually inside your own transaction.
///
/// Workers see the job only after your transaction commits, and it disappears on rollback,
/// so a job is never run for data that was not saved.
///
/// # Errors
///
/// Returns an error under the same conditions as [`enqueue_with`].
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendWelcome { user_id: i64 }
/// # #[jalari::job]
/// # impl jalari::Job for SendWelcome {
/// #     const NAME: &'static str = "send_welcome";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn example(pool: jalari::sqlx::PgPool) -> jalari::Result<()> {
/// let mut transaction = pool.begin().await?;
/// let user_id: i64 =
///     jalari::sqlx::query_scalar("INSERT INTO users DEFAULT VALUES RETURNING id")
///         .fetch_one(&mut *transaction)
///         .await?;
/// jalari::enqueue_in(
///     &mut transaction,
///     &SendWelcome { user_id },
///     jalari::EnqueueOptions::new(),
/// )
/// .await?;
/// transaction.commit().await?;
/// # Ok(())
/// # }
/// ```
pub fn enqueue_in<'c, T: Job>(
    connection: &'c mut PgConnection,
    job: &T,
    options: EnqueueOptions,
) -> impl Future<Output = Result<EnqueueOutcome>> + Send + use<'c, T> {
    let request = EnqueueRequest::build(job, options);
    async move { request?.submit_in(connection).await }
}
