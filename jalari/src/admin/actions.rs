use std::collections::BTreeSet;

use sqlx::{AssertSqlSafe, Connection, PgConnection, Postgres, Transaction};

use super::jobs::{FILTER_SQL, JobFilter, bind_filter};
use crate::init::config;
use crate::job::JOB_KEY_STATES;
use crate::storage::JOB_CHANNEL;
use crate::worker::{INTERRUPTED, TAKEOVER_DELAY};
use crate::{Error, ErrorKind, JobId, JobState, Result, Schema};

const BATCH_SIZE: i64 = 100;
const UNIQUE_VIOLATION: &str = "23505";

/// What [`retry`] did with the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// The job is waiting again with its attempts reset; a worker runs it as soon as it can.
    Retried,
    /// The job has not failed: it is waiting, running, stopped, succeeded or deleted.
    NotFailed,
    /// Another job with the same `job_key` is waiting, running or stopped, so this one stays
    /// failed; the id is that other job's.
    KeyTaken(JobId),
    /// No job has this id, or housekeeping already removed it.
    NotFound,
}

/// What [`retry_failed`] did.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetrySummary {
    /// Jobs that are waiting again.
    pub retried: u64,
    /// Jobs left failed because another job holds their `job_key`.
    pub key_taken: u64,
}

/// What [`run_now`] did with the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunNowOutcome {
    /// The job is due now; a worker runs it as soon as it can.
    Enqueued,
    /// A worker holds the job, or its last run was interrupted less than 6 seconds ago.
    Running,
    /// The job is stopped, succeeded, failed or deleted; [`restart`](crate::restart) a
    /// stopped job and [`retry`] a failed one instead.
    NotWaiting,
    /// No job has this id, or housekeeping already removed it.
    NotFound,
}

/// Makes a failed job run again from its first attempt.
///
/// The job becomes due now with `attempts` reset to zero, so it gets its full `max_attempts`
/// again. `last_error` stays until an attempt succeeds, the change is recorded in the job's
/// history, and workers of its queue are notified. A job whose `job_key` is held by another
/// waiting, running or stopped job stays failed, because two such jobs cannot share a key.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example(id: jalari::JobId) -> jalari::Result<()> {
/// match jalari::admin::retry(id).await? {
///     jalari::admin::RetryOutcome::Retried => println!("job {id} runs again"),
///     jalari::admin::RetryOutcome::KeyTaken(other) => println!("job {other} holds the key"),
///     outcome => println!("{outcome:?}"),
/// }
/// # Ok(())
/// # }
/// ```
pub async fn retry(id: JobId) -> Result<RetryOutcome> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let locked: Option<(String, String)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, queue FROM {job} WHERE id = $1 FOR UPDATE",
        job = schema.table("job"),
    )))
    .bind(id.0)
    .fetch_optional(&mut *transaction)
    .await?;
    let outcome = match locked {
        None => RetryOutcome::NotFound,
        Some((state, _)) if state != JobState::Failed.as_str() => RetryOutcome::NotFailed,
        Some((_, queue)) => {
            let outcome = retry_locked(&mut transaction, schema, id).await?;
            if outcome == RetryOutcome::Retried {
                notify(&mut transaction, schema, &queue).await?;
            }
            outcome
        }
    };
    transaction.commit().await?;
    Ok(outcome)
}

/// Retries every failed job matching `filter`, in batches of 100 that commit separately.
///
/// Each job is handled as by [`retry`]; the filter's states are ignored, only failed jobs are
/// considered. A batch that fails leaves the earlier batches retried and returns the error.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or a database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// let filter = jalari::admin::JobFilter::new().queue("emails");
/// let summary = jalari::admin::retry_failed(&filter).await?;
/// println!("{} retried, {} blocked by their job_key", summary.retried, summary.key_taken);
/// # Ok(())
/// # }
/// ```
pub async fn retry_failed(filter: &JobFilter) -> Result<RetrySummary> {
    let config = config()?;
    let schema = &config.schema;
    let pool = config.pool.pool();
    let filter = filter.only_failed();
    let mut summary = RetrySummary::default();
    let mut after = 0_i64;
    loop {
        let query = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT id FROM {job} WHERE {FILTER_SQL} AND id > $9 ORDER BY id LIMIT $10",
            job = schema.table("job"),
        )));
        let ids: Vec<i64> = bind_filter!(query, filter)
            .bind(after)
            .bind(BATCH_SIZE)
            .fetch_all(&pool)
            .await?;
        let Some(&last) = ids.last() else {
            return Ok(summary);
        };
        after = last;

        let mut transaction = pool.begin().await?;
        let locked: Vec<(i64, String)> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT id, queue FROM {job} WHERE id = ANY($1) AND state = 'failed'
             ORDER BY id FOR UPDATE SKIP LOCKED",
            job = schema.table("job"),
        )))
        .bind(&ids)
        .fetch_all(&mut *transaction)
        .await?;
        let mut queues = BTreeSet::new();
        for (id, queue) in locked {
            match retry_locked(&mut transaction, schema, JobId(id)).await? {
                RetryOutcome::Retried => {
                    summary.retried += 1;
                    queues.insert(queue);
                }
                RetryOutcome::KeyTaken(_) => summary.key_taken += 1,
                RetryOutcome::NotFailed | RetryOutcome::NotFound => {}
            }
        }
        for queue in queues {
            notify(&mut transaction, schema, &queue).await?;
        }
        transaction.commit().await?;
    }
}

/// Makes a waiting job due now instead of at its `run_at`.
///
/// Works for a scheduled job and for a retry waiting for its backoff. The change is recorded
/// in the job's history and workers of its queue are notified. A pending
/// [`stop`](crate::stop) or [`cancel`](crate::cancel) still applies when a worker picks the
/// job up.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example(id: jalari::JobId) -> jalari::Result<()> {
/// if jalari::admin::run_now(id).await? == jalari::admin::RunNowOutcome::Enqueued {
///     println!("job {id} runs as soon as a worker is free");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn run_now(id: JobId) -> Result<RunNowOutcome> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let queue: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "WITH idle AS (
             SELECT id FROM {job} AS job
             WHERE id = $1 AND {idle}
             FOR UPDATE SKIP LOCKED
         ),
         moved AS (
             UPDATE {job} AS job
             SET state = 'enqueued', run_at = statement_timestamp(),
                 updated_at = statement_timestamp()
             WHERE id IN (SELECT id FROM idle)
             RETURNING id, attempts, queue
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, created_at)
             SELECT id, 'enqueued', attempts, statement_timestamp() FROM moved
         )
         SELECT queue FROM moved",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
        idle = schema.idle_job("job", "$2", "$3"),
    )))
    .bind(id.0)
    .bind(INTERRUPTED)
    .bind(TAKEOVER_DELAY)
    .fetch_optional(&mut *transaction)
    .await?;
    let outcome = match queue {
        Some(queue) => {
            notify(&mut transaction, schema, &queue).await?;
            RunNowOutcome::Enqueued
        }
        None => {
            let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT state FROM {job} WHERE id = $1",
                job = schema.table("job"),
            )))
            .bind(id.0)
            .fetch_optional(&mut *transaction)
            .await?;
            match state.as_deref() {
                None => RunNowOutcome::NotFound,
                Some("scheduled" | "enqueued") => RunNowOutcome::Running,
                Some(_) => RunNowOutcome::NotWaiting,
            }
        }
    };
    transaction.commit().await?;
    Ok(outcome)
}

async fn retry_locked(
    transaction: &mut Transaction<'_, Postgres>,
    schema: &Schema,
    id: JobId,
) -> Result<RetryOutcome> {
    let mut attempt = Connection::begin(&mut **transaction).await?;
    let retried = sqlx::query(AssertSqlSafe(format!(
        "WITH retried AS (
             UPDATE {job}
             SET state = 'enqueued', attempts = 0, run_at = statement_timestamp(),
                 updated_at = statement_timestamp()
             WHERE id = $1 AND state = 'failed'
             RETURNING id
         )
         INSERT INTO {job_history} (job_id, state, attempt, created_at)
         SELECT id, 'enqueued', 0, statement_timestamp() FROM retried",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
    )))
    .bind(id.0)
    .execute(&mut *attempt)
    .await;
    match retried {
        Ok(result) if result.rows_affected() == 1 => {
            attempt.commit().await?;
            Ok(RetryOutcome::Retried)
        }
        Ok(_) => {
            attempt.rollback().await?;
            Ok(RetryOutcome::NotFailed)
        }
        Err(e) if is_unique_violation(&e) => {
            attempt.rollback().await?;
            let holder: Option<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT other.id FROM {job} AS job
                 JOIN {job} AS other ON other.job_key = job.job_key AND other.id <> job.id
                 WHERE job.id = $1 AND other.state IN {JOB_KEY_STATES}
                 ORDER BY other.id LIMIT 1",
                job = schema.table("job"),
            )))
            .bind(id.0)
            .fetch_optional(&mut **transaction)
            .await?;
            holder
                .map(JobId)
                .map(RetryOutcome::KeyTaken)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::JobKeyContention,
                        format!("job {id}: its job_key was taken while retrying"),
                    )
                })
        }
        Err(e) => Err(e.into()),
    }
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == UNIQUE_VIOLATION)
}

async fn notify(connection: &mut PgConnection, schema: &Schema, queue: &str) -> Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(schema.notify_channel(JOB_CHANNEL))
        .bind(queue)
        .execute(connection)
        .await?;
    Ok(())
}
