use std::future::Future;

use sqlx::PgConnection;

use super::{EnqueueOptions, EnqueueOutcome};
use crate::{Job, Result};

/// Enqueues many jobs of one type with a single statement per few thousand jobs.
///
/// The outcomes come back in input order. A `job_key` that appears twice in the batch answers
/// [`Kept`](EnqueueOutcome::Kept) with the first job's id. The whole batch commits in one
/// transaction or not at all, and the workers of each queue get one notification.
///
/// # Errors
///
/// Returns an error if:
/// - Any condition listed for [`enqueue_with`](crate::enqueue_with) occurs for any job
/// - Any options ask for [`OnConflict::Replace`](crate::OnConflict::Replace)
///   ([`InvalidOptions`](crate::ErrorKind::InvalidOptions))
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendDigest { user_id: i64 }
/// # #[jalari::job]
/// # impl jalari::Job for SendDigest {
/// #     const NAME: &'static str = "send_digest";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn example(user_ids: Vec<i64>) -> jalari::Result<()> {
/// let digests: Vec<_> = user_ids
///     .into_iter()
///     .map(|user_id| {
///         let options = jalari::EnqueueOptions::new().job_key(&format!("digest:{user_id}"));
///         (SendDigest { user_id }, options)
///     })
///     .collect();
/// let outcomes = jalari::enqueue_many(&digests).await?;
/// println!("{} digests enqueued or kept", outcomes.len());
/// # Ok(())
/// # }
/// ```
#[expect(unused_variables, reason = "filled in by W3")]
pub fn enqueue_many<T: Job>(
    jobs: &[(T, EnqueueOptions)],
) -> impl Future<Output = Result<Vec<EnqueueOutcome>>> + Send + use<T> {
    async move { todo!("W3") }
}

/// Enqueues many jobs on a connection you control, such as an open transaction.
///
/// Nothing is visible to workers until your transaction commits, and nothing is enqueued if it
/// rolls back. Otherwise it behaves like [`enqueue_many`].
///
/// # Errors
///
/// Returns an error if any condition listed for [`enqueue_many`] occurs.
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct ImportChunk { index: u32 }
/// # #[jalari::job]
/// # impl jalari::Job for ImportChunk {
/// #     const NAME: &'static str = "import_chunk";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn example(pool: jalari::sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let chunks: Vec<_> = (0..10)
///     .map(|index| (ImportChunk { index }, jalari::EnqueueOptions::new()))
///     .collect();
/// let mut transaction = pool.begin().await?;
/// jalari::enqueue_many_in(&mut transaction, &chunks).await?;
/// transaction.commit().await?;
/// # Ok(())
/// # }
/// ```
#[expect(unused_variables, reason = "filled in by W3")]
pub fn enqueue_many_in<'c, T: Job>(
    connection: &'c mut PgConnection,
    jobs: &[(T, EnqueueOptions)],
) -> impl Future<Output = Result<Vec<EnqueueOutcome>>> + Send + use<'c, T> {
    async move { todo!("W3") }
}
