use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::job::fingerprint::compatible;
use crate::storage::interval;
use crate::{Result, Schema};

const STALE_FLOOR: Duration = Duration::from_secs(12);

/// Jobs that no live worker will run, grouped by queue, task and fingerprint.
///
/// A group appears when the jobs are due and no worker with a recent heartbeat both processes
/// the queue and runs the task at a matching fingerprint, which covers jobs enqueued by another
/// build of the task and jobs whose task no live worker knows at all. It also appears when the
/// jobs are running on a worker whose heartbeat is older than the cluster's
/// [`worker_timeout`](crate::ClusterConfig::worker_timeout) and no live worker is left to take
/// them over.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrandedJobs {
    /// Queue the jobs wait in.
    pub queue: String,
    /// [`Job::NAME`](crate::Job::NAME) of the jobs.
    pub task: String,
    /// Fingerprint the jobs were enqueued with; `None` for jobs without one.
    pub fingerprint: Option<String>,
    /// How many jobs are in the group.
    pub count: i64,
    /// Earliest `run_at` in the group, or the earliest last heartbeat of a dead worker.
    pub since: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct StrandedRow {
    queue: String,
    task: String,
    fingerprint: Option<String>,
    count: i64,
    since: DateTime<Utc>,
}

/// Lists due jobs that no live worker can run, by queue, task and fingerprint.
///
/// A job counts when it is due now and no worker that sent a heartbeat within the cluster's
/// [`worker_timeout`](crate::ClusterConfig::worker_timeout) processes its queue and runs its
/// task at a matching fingerprint. Jobs that only wait for a busy worker are not listed. A
/// worker on a jalari release without fingerprints counts as able to run any task of its
/// queues. Running jobs count when their worker's heartbeat is older than the worker timeout,
/// or than 12 seconds when the timeout is shorter, which happens only when no worker is left
/// to take them over.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// for group in jalari::admin::stranded().await? {
///     println!(
///         "{} {} jobs of {} in {} wait for a build with fingerprint {:?}",
///         group.count, group.task, group.since, group.queue, group.fingerprint
///     );
/// }
/// # Ok(())
/// # }
/// ```
pub async fn stranded() -> Result<Vec<StrandedJobs>> {
    let config = config()?;
    let mut connection = config.pool.pool().acquire().await?;
    stranded_in(&mut connection, &config.schema, None).await
}

pub(crate) async fn stranded_in(
    connection: &mut PgConnection,
    schema: &Schema,
    queues: Option<&[String]>,
) -> Result<Vec<StrandedJobs>> {
    let rows: Vec<StrandedRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job.queue, job.task, job.fingerprint, count(*) AS count, min(job.run_at) AS since
         FROM (
             SELECT job.queue, job.task, job.fingerprint, job.run_at
             FROM {job} AS job
             WHERE job.state IN ('scheduled', 'enqueued')
                 AND job.run_at <= now()
                 AND ($1::text[] IS NULL OR job.queue = ANY($1))
                 AND NOT EXISTS (
                     SELECT 1 FROM {worker} AS worker
                     WHERE worker.heartbeat_at >= now() - (SELECT worker_timeout FROM {config})
                         AND worker.queues @> jsonb_build_array(jsonb_build_object('name', job.queue))
                         AND (worker.tasks IS NULL OR {compatible})
                 )
             UNION ALL
             SELECT job.queue, job.task, job.fingerprint,
                 coalesce(owner.heartbeat_at, job.locked_at) AS run_at
             FROM {job} AS job
             LEFT JOIN {worker} AS owner ON owner.id = job.locked_by
             WHERE job.state = 'running'
                 AND ($1::text[] IS NULL OR job.queue = ANY($1))
                 AND coalesce(owner.heartbeat_at, job.locked_at) < now() - greatest(
                     (SELECT worker_timeout FROM {config}), $2::interval)
         ) AS job
         GROUP BY job.queue, job.task, job.fingerprint
         ORDER BY job.queue, job.task, job.fingerprint NULLS FIRST",
        job = schema.table("job"),
        worker = schema.table("worker"),
        config = schema.table("config"),
        compatible = compatible("worker.tasks"),
    )))
    .bind(queues)
    .bind(interval(STALE_FLOOR))
    .fetch_all(connection)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| StrandedJobs {
            queue: row.queue,
            task: row.task,
            fingerprint: row.fingerprint,
            count: row.count,
            since: row.since,
        })
        .collect())
}
