use crate::{EnqueueOptions, Job, Result};

/// Enqueues a job when the running job succeeds, in the same statement that records the success.
///
/// The child is serialized now and inherits the running job's context. It is inserted only if
/// this attempt ends as succeeded; a retry, failure, stop, cancel, shutdown or crash drops it,
/// and the next attempt enqueues again. Its delay counts from the success. A child with the
/// running job's own `job_key` is inserted, since that key is released by the same statement.
/// The call returns no id; use [`enqueue`](crate::enqueue) when you need one.
///
/// # Errors
///
/// Returns an error if:
/// - It is called outside a running job's own task ([`NotInJob`](crate::ErrorKind::NotInJob))
/// - The job or its context cannot be serialized
///   ([`PayloadEncodeFailed`](crate::ErrorKind::PayloadEncodeFailed))
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct EmailReport { id: i64 }
/// # #[jalari::job]
/// # impl jalari::Job for EmailReport {
/// #     const NAME: &'static str = "email_report";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn build_report() -> Result<i64, std::io::Error> { Ok(1) }
/// # async fn run() -> jalari::JobResult {
/// let id = build_report().await?;
/// jalari::enqueue_on_success(&EmailReport { id })?;
/// Ok(())
/// # }
/// ```
#[expect(unused_variables, reason = "filled in by W2")]
pub fn enqueue_on_success<T: Job>(job: &T) -> Result<()> {
    todo!("W2")
}

/// Like [`enqueue_on_success`], with options such as a delay, a queue or a `job_key`.
///
/// Only [`OnConflict::KeepExisting`](crate::OnConflict::KeepExisting) is accepted: a child whose
/// `job_key` is taken when the running job succeeds is dropped.
///
/// # Errors
///
/// Returns an error if:
/// - Any condition listed for [`enqueue_on_success`] occurs
/// - The options ask for [`OnConflict::Replace`](crate::OnConflict::Replace) or hold a value out
///   of range ([`InvalidOptions`](crate::ErrorKind::InvalidOptions))
/// - The timeout exceeds about 24 days
///   ([`TimeoutTooLong`](crate::ErrorKind::TimeoutTooLong))
///
/// # Examples
///
/// ```rust,no_run
/// # use std::time::Duration;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct PollFeed { feed: String }
/// # #[jalari::job]
/// # impl jalari::Job for PollFeed {
/// #     const NAME: &'static str = "poll_feed";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # fn run(feed: String) -> jalari::JobResult {
/// let options = jalari::EnqueueOptions::new()
///     .job_key(&format!("poll:{feed}"))
///     .delay(Duration::from_secs(60));
/// jalari::enqueue_on_success_with(&PollFeed { feed }, options)?;
/// # Ok(())
/// # }
/// ```
#[expect(unused_variables, reason = "filled in by W2")]
pub fn enqueue_on_success_with<T: Job>(job: &T, options: EnqueueOptions) -> Result<()> {
    todo!("W2")
}
