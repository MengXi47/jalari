use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{AssertSqlSafe, Connection, PgConnection, Postgres, Transaction};
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, info_span, warn};

use super::middleware::{JobMeta, Next};
use super::runner::Shared;
use crate::job::context::{self, Captured};
use crate::job::registry::JobRegistration;
use crate::job::{JobState, run};
use crate::storage::interval;
use crate::{Error, ErrorKind, JobError, JobId, JobResult, JobRun, Result, Schema};

const CLAIM_LEASE: Duration = Duration::from_secs(2);
const CONNECTION_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const CONNECTION_CHECK_TIMEOUT: Duration = Duration::from_secs(2);
const TAKEOVER_DELAY: Duration = Duration::from_secs(6);
const IDLE_SESSION_TIMEOUT: Duration = Duration::from_secs(10);
const INTERRUPTED: &str = "the worker stopped during the last attempt";

const _: () = assert!(
    TAKEOVER_DELAY.as_millis()
        >= 2 * (CONNECTION_CHECK_INTERVAL.as_millis() + CONNECTION_CHECK_TIMEOUT.as_millis())
);
const _: () = assert!(
    IDLE_SESSION_TIMEOUT.as_millis()
        >= 2 * (CONNECTION_CHECK_INTERVAL.as_millis() + CONNECTION_CHECK_TIMEOUT.as_millis())
);

#[derive(sqlx::FromRow)]
struct ClaimedJob {
    id: i64,
    task: String,
    payload: Vec<u8>,
    attempts: i32,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    queue_key: Option<String>,
    updated_at: DateTime<Utc>,
    context: Option<Value>,
}

struct AbortOnDrop(JoinHandle<JobResult>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Shared {
    pub(super) async fn process_next(&self, queue: &str) -> Result<bool> {
        let schema = &self.config.schema;
        let mut connection = self.config.pool.pool().acquire().await?;
        let mut excluded_keys: Vec<String> = Vec::new();
        for _ in 0..self.settings.max_claim_tries {
            let mut transaction = connection.begin().await?;
            let Some(job) = self.claim(&mut transaction, queue, &excluded_keys).await? else {
                transaction.rollback().await?;
                return Ok(false);
            };
            if let Some(queue_key) = &job.queue_key
                && !try_lock_queue_key(&mut transaction, schema, queue_key).await?
            {
                transaction.rollback().await?;
                excluded_keys.push(queue_key.clone());
                continue;
            }
            if job.attempts > 0 && job.attempts >= job.max_attempts {
                job.fail_interrupted(&mut transaction, schema).await?;
                transaction.commit().await?;
                return Ok(true);
            }
            if job.attempts > 0 && job.was_interrupted(&mut transaction, schema).await? {
                job.delay_takeover(&mut transaction, schema).await?;
                transaction.commit().await?;
                return Ok(true);
            }
            if let Some(queue_key) = &job.queue_key {
                job.mark_interrupted_siblings(&mut transaction, schema, queue_key)
                    .await?;
                if let Some(until) = job
                    .queue_key_fence(&mut transaction, schema, queue_key)
                    .await?
                {
                    job.postpone(&mut transaction, schema, until, job.attempts)
                        .await?;
                    transaction.commit().await?;
                    return Ok(true);
                }
            }
            let attempt = job.start_attempt(&mut transaction, schema).await?;
            transaction.commit().await?;

            let mut transaction = connection.begin().await?;
            if !job.relock(&mut transaction, schema, attempt).await? {
                transaction.rollback().await?;
                return Ok(true);
            }
            limit_idle_time(&mut transaction).await?;
            if let Some(queue_key) = &job.queue_key {
                lock_queue_key(&mut transaction, schema, queue_key).await?;
                if let Some(until) = job
                    .queue_key_fence(&mut transaction, schema, queue_key)
                    .await?
                {
                    job.postpone(&mut transaction, schema, until, attempt - 1)
                        .await?;
                    transaction.commit().await?;
                    return Ok(true);
                }
            }
            self.execute(transaction, queue, job, attempt).await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn claim(
        &self,
        connection: &mut PgConnection,
        queue: &str,
        excluded_keys: &[String],
    ) -> Result<Option<ClaimedJob>> {
        let sql = format!(
            "SELECT id, task, payload, attempts, max_attempts, timeout_ms, queue_key, updated_at,
                 context
             FROM {job}
             WHERE queue = $1
                 AND state IN ('scheduled', 'enqueued')
                 AND run_at <= now()
                 AND task = ANY($2)
                 AND (queue_key IS NULL OR queue_key <> ALL($3))
             ORDER BY priority DESC, run_at, id
             LIMIT 1
             FOR UPDATE SKIP LOCKED",
            job = self.config.schema.table("job"),
        );
        let job = sqlx::query_as(AssertSqlSafe(sql))
            .bind(queue)
            .bind(&self.task_names)
            .bind(excluded_keys)
            .fetch_optional(connection)
            .await?;
        Ok(job)
    }

    async fn execute(
        &self,
        mut transaction: Transaction<'_, Postgres>,
        queue: &str,
        job: ClaimedJob,
        attempt: i32,
    ) -> Result<()> {
        let span = info_span!("job", job_id = job.id, task = %job.task, queue = %queue, attempt);
        async move {
            let started = Instant::now();
            let result = match self.jobs.get(job.task.as_str()) {
                Some(&registration) => {
                    let run = job.run(self, registration, queue, attempt);
                    tokio::pin!(run);
                    loop {
                        tokio::select! {
                            result = &mut run => break result,
                            () = tokio::time::sleep(CONNECTION_CHECK_INTERVAL) => {
                                if let Err(e) = check_connection(&mut transaction).await {
                                    warn!(error = %e, "lost the database connection, aborting the job");
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
                None => Err(JobError::new(format!(
                    "no job is registered as {:?}",
                    job.task
                ))),
            };
            self.record(&mut transaction, &job, attempt, result, started.elapsed())
                .await?;
            transaction.commit().await?;
            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn record(
        &self,
        connection: &mut PgConnection,
        job: &ClaimedJob,
        attempt: i32,
        result: JobResult,
        duration: Duration,
    ) -> Result<()> {
        let schema = &self.config.schema;
        let duration_ms = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        let (state, retry_delay, error) = match result {
            Ok(()) => {
                debug!(duration_ms, "job succeeded");
                (JobState::Succeeded, None, None)
            }
            Err(error) => match self
                .retry_policy
                .next_retry(attempt, job.max_attempts, &error)
            {
                Some(delay) => {
                    warn!(error = %error, ?delay, "job failed, retrying");
                    (JobState::Scheduled, Some(delay), Some(error))
                }
                None => {
                    warn!(error = %error, "job failed");
                    (JobState::Failed, None, Some(error))
                }
            },
        };

        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {job} SET state = $2, attempts = $3, last_error = $4,
                 run_at = coalesce(now() + $5, run_at), updated_at = now()
             WHERE id = $1",
            job = schema.table("job"),
        )))
        .bind(job.id)
        .bind(state.as_str())
        .bind(attempt)
        .bind(error.as_ref().map(JobError::msg))
        .bind(retry_delay.map(interval))
        .execute(&mut *connection)
        .await?;
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {job_history} (job_id, state, attempt, error, duration_ms)
             VALUES ($1, $2, $3, $4, $5)",
            job_history = schema.table("job_history"),
        )))
        .bind(job.id)
        .bind(state.as_str())
        .bind(attempt)
        .bind(error.as_ref().map(JobError::msg))
        .bind(duration_ms)
        .execute(connection)
        .await?;
        Ok(())
    }
}

impl ClaimedJob {
    async fn start_attempt(&self, connection: &mut PgConnection, schema: &Schema) -> Result<i32> {
        let attempt = sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE {job} SET attempts = attempts + 1, run_at = now() + $2
             WHERE id = $1
             RETURNING attempts",
            job = schema.table("job"),
        )))
        .bind(self.id)
        .bind(CLAIM_LEASE)
        .fetch_one(connection)
        .await?;
        Ok(attempt)
    }

    async fn relock(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
        attempt: i32,
    ) -> Result<bool> {
        let attempts: Option<i32> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT attempts FROM {job}
             WHERE id = $1 AND state IN ('scheduled', 'enqueued')
             FOR UPDATE",
            job = schema.table("job"),
        )))
        .bind(self.id)
        .fetch_optional(connection)
        .await?;
        Ok(attempts == Some(attempt))
    }

    async fn was_interrupted(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
    ) -> Result<bool> {
        let recorded: bool = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT EXISTS (
                 SELECT 1 FROM {job_history}
                 WHERE job_id = $1 AND attempt = $2 AND created_at >= $3
             )",
            job_history = schema.table("job_history"),
        )))
        .bind(self.id)
        .bind(self.attempts)
        .bind(self.updated_at)
        .fetch_one(connection)
        .await?;
        Ok(!recorded)
    }

    async fn delay_takeover(&self, connection: &mut PgConnection, schema: &Schema) -> Result<()> {
        let state = JobState::Scheduled.as_str();
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {job} SET state = $2, last_error = $3, run_at = now() + $4, updated_at = now()
             WHERE id = $1",
            job = schema.table("job"),
        )))
        .bind(self.id)
        .bind(state)
        .bind(INTERRUPTED)
        .bind(TAKEOVER_DELAY)
        .execute(&mut *connection)
        .await?;
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {job_history} (job_id, state, attempt, error) VALUES ($1, $2, $3, $4)",
            job_history = schema.table("job_history"),
        )))
        .bind(self.id)
        .bind(state)
        .bind(self.attempts)
        .bind(INTERRUPTED)
        .execute(connection)
        .await?;
        warn!(
            job_id = self.id,
            task = %self.task,
            attempts = self.attempts,
            delay = ?TAKEOVER_DELAY,
            "job was interrupted, running it again after a delay"
        );
        Ok(())
    }

    async fn mark_interrupted_siblings(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
        queue_key: &str,
    ) -> Result<()> {
        let siblings: Vec<ClaimedJob> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT id, task, payload, attempts, max_attempts, timeout_ms, queue_key, updated_at,
                 context
             FROM {job}
             WHERE queue_key = $1 AND id <> $2 AND state IN ('scheduled', 'enqueued')
                 AND attempts > 0
             FOR UPDATE SKIP LOCKED",
            job = schema.table("job"),
        )))
        .bind(queue_key)
        .bind(self.id)
        .fetch_all(&mut *connection)
        .await?;
        for sibling in siblings {
            if sibling.was_interrupted(connection, schema).await? {
                sibling.delay_takeover(connection, schema).await?;
            }
        }
        Ok(())
    }

    async fn queue_key_fence(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
        queue_key: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let until = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT max(until) FROM (
                 SELECT CASE WHEN state = 'failed' THEN updated_at + $3 ELSE run_at END AS until
                 FROM {job}
                 WHERE queue_key = $1 AND id <> $2 AND last_error = $4
                     AND ((state IN ('scheduled', 'enqueued') AND run_at > now())
                         OR (state = 'failed' AND updated_at > now() - $3))
                 UNION ALL
                 SELECT now() + $3 AS until
                 FROM {job} AS job
                 WHERE queue_key = $1 AND id <> $2 AND state IN ('scheduled', 'enqueued')
                     AND attempts > 0
                     AND NOT EXISTS (
                         SELECT 1 FROM {job_history} AS history
                         WHERE history.job_id = job.id AND history.attempt = job.attempts
                             AND history.created_at >= job.updated_at
                     )
             ) AS fences",
            job = schema.table("job"),
            job_history = schema.table("job_history"),
        )))
        .bind(queue_key)
        .bind(self.id)
        .bind(TAKEOVER_DELAY)
        .bind(INTERRUPTED)
        .fetch_one(connection)
        .await?;
        Ok(until)
    }

    async fn postpone(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
        until: DateTime<Utc>,
        attempts: i32,
    ) -> Result<()> {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {job} SET run_at = $2, attempts = $3 WHERE id = $1",
            job = schema.table("job"),
        )))
        .bind(self.id)
        .bind(until)
        .bind(attempts)
        .execute(connection)
        .await?;
        debug!(job_id = self.id, %until, "waiting for an interrupted job with the same queue_key");
        Ok(())
    }

    async fn fail_interrupted(&self, connection: &mut PgConnection, schema: &Schema) -> Result<()> {
        let state = JobState::Failed.as_str();
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {job} SET state = $2, last_error = $3, updated_at = now()
             WHERE id = $1",
            job = schema.table("job"),
        )))
        .bind(self.id)
        .bind(state)
        .bind(INTERRUPTED)
        .execute(&mut *connection)
        .await?;
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {job_history} (job_id, state, attempt, error) VALUES ($1, $2, $3, $4)",
            job_history = schema.table("job_history"),
        )))
        .bind(self.id)
        .bind(state)
        .bind(self.attempts)
        .bind(INTERRUPTED)
        .execute(connection)
        .await?;
        warn!(
            job_id = self.id,
            task = %self.task,
            attempts = self.attempts,
            "job failed because its last attempt was interrupted"
        );
        Ok(())
    }

    async fn run(
        &self,
        shared: &Shared,
        registration: &'static JobRegistration,
        queue: &str,
        attempt: i32,
    ) -> JobResult {
        let job_run = JobRun::new(
            JobId(self.id),
            registration.name,
            Arc::from(queue),
            attempt,
            self.max_attempts,
            shared.job_shutdown.child_token(),
        );
        let meta = JobMeta::new(job_run.clone(), self.context.clone());
        let captured = Captured::from_stored(self.context.clone());
        let middlewares = Arc::clone(&shared.middlewares);
        let payload = self.payload.clone();
        let attempt = async move {
            Next::start(
                &middlewares,
                &meta,
                (registration.context)(),
                registration.run,
                &payload,
            )
            .run()
            .await
        };
        let attempt = run::within(job_run, context::within(captured, attempt)).in_current_span();
        let mut handle = AbortOnDrop(tokio::spawn(attempt));
        let joined = match self.timeout_ms {
            Some(timeout_ms) => {
                let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
                match tokio::time::timeout(timeout, &mut handle.0).await {
                    Ok(joined) => joined,
                    Err(_) => return Err(JobError::new(format!("timed out after {timeout:?}"))),
                }
            }
            None => (&mut handle.0).await,
        };
        match joined {
            Ok(result) => result,
            Err(e) if e.is_panic() => Err(JobError::new(format!(
                "panicked: {}",
                panic_message(e.into_panic())
            ))),
            Err(e) => Err(JobError::new(format!("job task ended unexpectedly: {e}"))),
        }
    }
}

async fn limit_idle_time(connection: &mut PgConnection) -> Result<()> {
    sqlx::query("SELECT set_config('idle_in_transaction_session_timeout', $1, true)")
        .bind(IDLE_SESSION_TIMEOUT.as_millis().to_string())
        .execute(connection)
        .await?;
    Ok(())
}

async fn check_connection(connection: &mut PgConnection) -> Result<()> {
    let ping = sqlx::query("SELECT 1").execute(connection);
    match tokio::time::timeout(CONNECTION_CHECK_TIMEOUT, ping).await {
        Ok(result) => {
            result?;
            Ok(())
        }
        Err(_) => Err(Error::new(
            ErrorKind::ConnectionFailed,
            format!("the database did not answer within {CONNECTION_CHECK_TIMEOUT:?}"),
        )),
    }
}

async fn try_lock_queue_key(
    connection: &mut PgConnection,
    schema: &Schema,
    queue_key: &str,
) -> Result<bool> {
    let locked = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(queue_key_lock(schema, queue_key))
        .fetch_one(connection)
        .await?;
    Ok(locked)
}

async fn lock_queue_key(
    connection: &mut PgConnection,
    schema: &Schema,
    queue_key: &str,
) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(queue_key_lock(schema, queue_key))
        .execute(connection)
        .await?;
    Ok(())
}

fn queue_key_lock(schema: &Schema, queue_key: &str) -> String {
    format!("{}:{queue_key}", schema.lock_key("queue_key"))
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => payload
            .downcast_ref::<&str>()
            .map_or_else(|| "unknown panic payload".to_owned(), |s| (*s).to_owned()),
    }
}
