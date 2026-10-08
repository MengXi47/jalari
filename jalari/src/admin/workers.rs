use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::AssertSqlSafe;
use sqlx::types::Json;

use crate::Result;
use crate::init::config;

/// Queue a worker processes and how many of its jobs run at once.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct QueueInfo {
    /// Queue name.
    pub name: String,
    /// Number of jobs from this queue the worker runs concurrently.
    pub concurrency: usize,
}

/// Running worker as recorded by its heartbeat.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    /// UUID generated when the worker started.
    pub id: String,
    /// Host the worker runs on.
    pub hostname: String,
    /// Process id on that host.
    pub pid: i32,
    /// Queues the worker processes.
    pub queues: Vec<QueueInfo>,
    /// Whether the worker runs the recurring scheduler.
    pub scheduler: bool,
    /// When the worker registered.
    pub started_at: DateTime<Utc>,
    /// Last heartbeat.
    pub heartbeat_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct WorkerRow {
    id: String,
    hostname: String,
    pid: i32,
    queues: Json<Vec<QueueInfo>>,
    scheduler: bool,
    started_at: DateTime<Utc>,
    heartbeat_at: DateTime<Utc>,
}

impl From<WorkerRow> for WorkerInfo {
    fn from(row: WorkerRow) -> Self {
        Self {
            id: row.id,
            hostname: row.hostname,
            pid: row.pid,
            queues: row.queues.0,
            scheduler: row.scheduler,
            started_at: row.started_at,
            heartbeat_at: row.heartbeat_at,
        }
    }
}

/// Lists the workers that sent a heartbeat within the cluster's
/// [`worker_timeout`](crate::ClusterConfig::worker_timeout), oldest first.
///
/// A worker appears shortly after [`Worker::run`](crate::Worker::run) starts and disappears
/// once it shuts down cleanly. If no listed worker has `scheduler` set, recurring schedules are
/// not firing.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// for worker in jalari::admin::workers().await? {
///     println!("{}@{} last seen {}", worker.pid, worker.hostname, worker.heartbeat_at);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn workers() -> Result<Vec<WorkerInfo>> {
    let config = config()?;
    let rows: Vec<WorkerRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id::text AS id, hostname, pid, queues, scheduler, started_at, heartbeat_at
         FROM {worker}
         ORDER BY started_at, id",
        worker = config.schema.table("worker"),
    )))
    .fetch_all(&config.pool.pool())
    .await?;
    Ok(rows.into_iter().map(WorkerInfo::from).collect())
}
