use chrono::{DateTime, Utc};
use sqlx::AssertSqlSafe;

use crate::Result;
use crate::init::{Config, config};
use crate::storage::QUEUE_CHANNEL;

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
pub async fn pause_queue(queue: &str) -> Result<bool> {
    let config = config()?;
    changed(
        config,
        format!(
            "WITH changed AS (
                INSERT INTO {queue_pause} (queue, paused_at) VALUES ($1, statement_timestamp())
                ON CONFLICT (queue) DO NOTHING RETURNING queue
            )
            SELECT EXISTS (SELECT 1 FROM changed), pg_notify($2, $1)",
            queue_pause = config.schema.table("queue_pause"),
        ),
        queue,
    )
    .await
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
pub async fn resume_queue(queue: &str) -> Result<bool> {
    let config = config()?;
    changed(
        config,
        format!(
            "WITH changed AS (DELETE FROM {queue_pause} WHERE queue = $1 RETURNING queue)
            SELECT EXISTS (SELECT 1 FROM changed), pg_notify($2, $1)",
            queue_pause = config.schema.table("queue_pause"),
        ),
        queue,
    )
    .await
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
    let config = config()?;
    let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT queue, paused_at FROM {queue_pause} ORDER BY queue",
        queue_pause = config.schema.table("queue_pause"),
    )))
    .fetch_all(&config.pool.pool())
    .await?;
    Ok(rows
        .into_iter()
        .map(|(queue, paused_at)| PausedQueue { queue, paused_at })
        .collect())
}

async fn changed(config: &Config, statement: String, queue: &str) -> Result<bool> {
    let changed = sqlx::query_scalar(AssertSqlSafe(statement))
        .bind(queue)
        .bind(config.schema.notify_channel(QUEUE_CHANNEL))
        .fetch_one(&config.pool.pool())
        .await?;
    Ok(changed)
}
