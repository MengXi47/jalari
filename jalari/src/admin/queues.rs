use chrono::{DateTime, Utc};

use crate::Result;

/// A paused queue, as returned by [`paused_queues`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PausedQueue {
    /// Name of the queue.
    pub queue: String,
    /// When [`pause_queue`] paused it.
    pub paused_at: DateTime<Utc>,
}

/// Stops workers from starting new jobs of `queue` until [`resume_queue`] and returns whether
/// the queue was not paused before.
///
/// Running jobs finish, and enqueueing continues; the new jobs wait. Once this returns, no claim
/// that starts afterwards takes a job of the queue, on any worker, and the pause survives worker
/// restarts. Any name is accepted, including a queue no worker serves yet.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn maintenance() {}
/// # async fn example() -> jalari::Result<()> {
/// jalari::admin::pause_queue("exports").await?;
/// maintenance().await;
/// jalari::admin::resume_queue("exports").await?;
/// # Ok(())
/// # }
/// ```
#[expect(unused_variables, reason = "filled in by W5")]
pub async fn pause_queue(queue: &str) -> Result<bool> {
    todo!("W5")
}

/// Lets workers start jobs of `queue` again and returns whether it was paused.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// if jalari::admin::resume_queue("exports").await? {
///     println!("exports runs again");
/// }
/// # Ok(())
/// # }
/// ```
#[expect(unused_variables, reason = "filled in by W5")]
pub async fn resume_queue(queue: &str) -> Result<bool> {
    todo!("W5")
}

/// Lists the paused queues, sorted by name.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// for paused in jalari::admin::paused_queues().await? {
///     println!("{} paused since {}", paused.queue, paused.paused_at);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn paused_queues() -> Result<Vec<PausedQueue>> {
    todo!("W5")
}
