use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::job::fingerprint::compatible;
use crate::{Result, Schema};

/// Jobs that are due but that no live worker can run, grouped by queue, task and fingerprint.
///
/// A group appears when no worker with a recent heartbeat both processes the queue and runs the
/// task at a matching fingerprint. That covers jobs enqueued by another build of the task and
/// jobs whose task no live worker knows at all.
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
    /// Earliest `run_at` in the group, which is how long the oldest has been waiting.
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
/// queues. Each worker also logs a warning for the groups in its own queues at startup and
/// every five minutes.
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
         GROUP BY job.queue, job.task, job.fingerprint
         ORDER BY job.queue, job.task, job.fingerprint NULLS FIRST",
        job = schema.table("job"),
        worker = schema.table("worker"),
        config = schema.table("config"),
        compatible = compatible("worker.tasks"),
    )))
    .bind(queues)
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
