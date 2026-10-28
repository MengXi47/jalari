use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::job::{JOB_KEY_STATES, context, fingerprint, registry};
use crate::recurring::store::StoredJob;
use crate::storage::{JOB_CHANNEL, interval};
use crate::worker::INTERRUPTED;
use crate::{
    EnqueueOptions, EnqueueOutcome, Error, ErrorKind, Job, JobId, OnConflict, Result, Schema,
    current_job,
};

const DEFAULT_QUEUE: &str = "default";
const MAX_CONFLICT_RETRIES: usize = 3;
const LOCK_POLL: Duration = Duration::from_millis(25);
const REPLACE_PATIENCE: Duration = Duration::from_secs(1);

enum Replacement {
    Replaced(JobId),
    Running(JobId),
    Kept(JobId),
    Missing,
}

#[derive(sqlx::FromRow)]
struct ReplaceRow {
    id: i64,
    kept: bool,
    running: bool,
    replaced: bool,
}

pub(crate) struct EnqueueRequest {
    pub(super) task: String,
    pub(super) payload: Vec<u8>,
    pub(super) max_attempts: i32,
    pub(super) timeout_ms: Option<i32>,
    pub(super) queue: String,
    pub(super) run_at: Option<DateTime<Utc>>,
    pub(super) delay: Option<Duration>,
    pub(super) job_key: Option<String>,
    pub(super) queue_key: Option<String>,
    pub(super) on_conflict: OnConflict,
    pub(super) context: Option<Value>,
    pub(super) fingerprint: Option<&'static str>,
    pub(super) inherit_when_submitted: bool,
    pub(super) priority: i16,
    pub(super) parent_id: Option<i64>,
    pub(super) root_id: Option<i64>,
    pub(super) queue_key_limit: Option<i16>,
    pub(super) expires_at: Option<DateTime<Utc>>,
    pub(super) expire_after: Option<Duration>,
}

pub(crate) fn encode_payload<T: Job>(job: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(job).map_err(|err| {
        Error::new(
            ErrorKind::PayloadEncodeFailed,
            format!("{}: {err}", T::NAME),
        )
    })
}

pub(crate) fn timeout_ms(task: &str, timeout: Option<Duration>) -> Result<Option<i32>> {
    timeout
        .map(|timeout| {
            i32::try_from(timeout.as_millis()).map_err(|_| {
                Error::new(
                    ErrorKind::TimeoutTooLong,
                    format!("{task}: timeout {timeout:?} does not fit in i32 milliseconds"),
                )
            })
        })
        .transpose()
}

fn max_attempts(task: &str, max_attempts: Option<i32>, default: i32) -> Result<i32> {
    match max_attempts {
        Some(max_attempts) if max_attempts < 1 => Err(Error::new(
            ErrorKind::InvalidOptions,
            format!("{task}: max_attempts {max_attempts} must be at least 1"),
        )),
        Some(max_attempts) => Ok(max_attempts),
        None => Ok(default),
    }
}

fn queue_key_limit(task: &str, limit: Option<u16>, keyed: bool) -> Result<Option<i16>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let invalid = |reason: &str| {
        Err(Error::new(
            ErrorKind::InvalidOptions,
            format!("{task}: queue_key_limit {limit} {reason}"),
        ))
    };
    if !keyed {
        return invalid("needs a queue_key");
    }
    match i16::try_from(limit) {
        Ok(0) => invalid("must be at least 1"),
        Ok(1) => Ok(None),
        Ok(limit) => Ok(Some(limit)),
        Err(_) => invalid(&format!("must be at most {}", i16::MAX)),
    }
}

pub(crate) fn default_queue(task: &str) -> &'static str {
    registry::find(task).map_or(DEFAULT_QUEUE, |registration| registration.queue)
}

impl EnqueueRequest {
    pub(crate) fn build<T: Job>(job: &T, options: EnqueueOptions) -> Result<Self> {
        let payload = encode_payload(job)?;
        let timeout_ms = timeout_ms(T::NAME, options.timeout.or(T::TIMEOUT))?;
        let max_attempts = max_attempts(T::NAME, options.max_attempts, T::MAX_ATTEMPTS)?;
        let queue_key_limit = queue_key_limit(
            T::NAME,
            options.queue_key_limit,
            options.queue_key.is_some(),
        )?;
        let lineage = current_job().map(|run| (run.id().0, run.root_id().0));
        let queue = options
            .queue
            .unwrap_or_else(|| default_queue(T::NAME).to_owned());
        let inherits = options.context.inherits();
        let context = options.context.resolve().into_value(T::NAME)?;
        Ok(Self {
            inherit_when_submitted: inherits && context.is_none(),
            task: T::NAME.to_owned(),
            payload,
            max_attempts,
            timeout_ms,
            queue,
            run_at: options.run_at,
            delay: options.delay.map(interval),
            job_key: options.job_key,
            queue_key: options.queue_key,
            on_conflict: options.on_conflict,
            context,
            fingerprint: fingerprint::of_task(T::NAME),
            priority: options.priority.unwrap_or(T::PRIORITY).level(),
            parent_id: lineage.map(|(parent_id, _)| parent_id),
            root_id: lineage.map(|(_, root_id)| root_id),
            queue_key_limit,
            expires_at: options.expires_at,
            expire_after: options.expire_after.map(interval),
        })
    }

    pub(crate) fn recurring(name: &str, job: StoredJob) -> Self {
        Self {
            fingerprint: fingerprint::of_task(&job.task),
            task: job.task,
            payload: job.payload,
            max_attempts: job.max_attempts,
            timeout_ms: job.timeout_ms,
            queue: job.queue,
            run_at: None,
            delay: None,
            job_key: Some(format!("recurring:{name}")),
            queue_key: None,
            on_conflict: OnConflict::KeepExisting,
            context: job.context,
            inherit_when_submitted: false,
            priority: job.priority,
            parent_id: None,
            root_id: None,
            queue_key_limit: None,
            expires_at: None,
            expire_after: None,
        }
    }

    pub(crate) async fn submit(self) -> Result<EnqueueOutcome> {
        let request = self.inherit_context()?;
        let config = config()?;
        let mut transaction = config.pool.pool().begin().await?;
        let outcome = request.insert(&mut transaction, &config.schema).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub(crate) async fn submit_in(self, connection: &mut PgConnection) -> Result<EnqueueOutcome> {
        let request = self.inherit_context()?;
        let config = config()?;
        request.insert(connection, &config.schema).await
    }

    pub(super) fn inherit_context(mut self) -> Result<Self> {
        if self.inherit_when_submitted {
            self.context = context::current().into_value(&self.task)?;
        }
        Ok(self)
    }

    pub(crate) async fn insert(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
    ) -> Result<EnqueueOutcome> {
        let Some(job_key) = &self.job_key else {
            let id = self.try_insert(connection, schema).await?.ok_or_else(|| {
                Error::new(ErrorKind::QueryFailed, "insert returned no row".to_owned())
            })?;
            self.notify(connection, schema).await?;
            return Ok(EnqueueOutcome::Inserted(id));
        };
        if self.on_conflict == OnConflict::Replace {
            lock_job_key(connection, schema, job_key).await?;
        }

        for _ in 0..MAX_CONFLICT_RETRIES {
            if let Some(id) = self.try_insert(connection, schema).await? {
                self.notify(connection, schema).await?;
                return Ok(EnqueueOutcome::Inserted(id));
            }
            match self.on_conflict {
                OnConflict::KeepExisting => {
                    if let Some(id) = pending_id(connection, schema, job_key).await? {
                        return Ok(EnqueueOutcome::Kept(id));
                    }
                }
                OnConflict::Replace => match self.try_replace(connection, schema, job_key).await? {
                    Replacement::Replaced(id) => {
                        self.notify(connection, schema).await?;
                        return Ok(EnqueueOutcome::Replaced(id));
                    }
                    Replacement::Running(id) => return Ok(EnqueueOutcome::Running(id)),
                    Replacement::Kept(id) => return Ok(EnqueueOutcome::Kept(id)),
                    Replacement::Missing => {}
                },
            }
        }

        Err(Error::new(
            ErrorKind::JobKeyContention,
            format!("job_key {job_key:?} kept changing state while enqueuing"),
        ))
    }

    async fn try_insert(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
    ) -> Result<Option<JobId>> {
        let sql = format!(
            "INSERT INTO {job} (queue, task, payload, state, job_key, queue_key, max_attempts, \
                 run_at, timeout_ms, context, fingerprint, priority, parent_id, root_id, \
                 queue_key_limit, expires_at)
             SELECT $1, $2, $3, CASE WHEN input.run_at > now() THEN 'scheduled' ELSE 'enqueued' END,
                 $4, $5, $6, input.run_at, $7, $10, $11, $12, $13, $14, $15,
                 COALESCE($16::timestamptz, statement_timestamp() + $17::interval)
             FROM (SELECT COALESCE($8::timestamptz, now() + COALESCE($9::interval, interval '0'))
                 AS run_at) AS input
             ON CONFLICT (job_key) WHERE job_key IS NOT NULL AND state IN {JOB_KEY_STATES}
             DO NOTHING
             RETURNING id",
            job = schema.table("job"),
        );
        let id = sqlx::query_scalar(AssertSqlSafe(sql))
            .bind(&self.queue)
            .bind(&self.task)
            .bind(&self.payload)
            .bind(&self.job_key)
            .bind(&self.queue_key)
            .bind(self.max_attempts)
            .bind(self.timeout_ms)
            .bind(self.run_at)
            .bind(self.delay)
            .bind(&self.context)
            .bind(self.fingerprint)
            .bind(self.priority)
            .bind(self.parent_id)
            .bind(self.root_id)
            .bind(self.queue_key_limit)
            .bind(self.expires_at)
            .bind(self.expire_after)
            .fetch_optional(connection)
            .await?;
        Ok(id.map(JobId))
    }

    async fn try_replace(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
        job_key: &str,
    ) -> Result<Replacement> {
        let sql = format!(
            "WITH target AS (
                 SELECT job.id, job.state,
                     job.state = 'stopped' OR job.command IS NOT NULL AS kept,
                     CASE WHEN job.last_error = $11 AND job.run_at > statement_timestamp()
                         THEN job.run_at END AS takeover_at
                 FROM {job} AS job
                 WHERE job.job_key = $9 AND job.state IN {JOB_KEY_STATES}
                 FOR NO KEY UPDATE SKIP LOCKED
             ),
             replaced AS (
                 UPDATE {job} AS job
                 SET queue = $1, task = $2, payload = $3,
                     state = CASE WHEN input.run_at > now() THEN 'scheduled' ELSE 'enqueued' END,
                     queue_key = $4, attempts = 0, max_attempts = $5, run_at = input.run_at,
                     timeout_ms = $6,
                     last_error = CASE WHEN target.takeover_at IS NOT NULL THEN $11 END,
                     context = $10, fingerprint = $12, priority = $13, parent_id = $14,
                     root_id = $15, queue_key_limit = $16,
                     expires_at = COALESCE($17::timestamptz, statement_timestamp() + $18::interval),
                     updated_at = statement_timestamp()
                 FROM target, LATERAL (
                     SELECT greatest(
                         COALESCE($7::timestamptz, now() + COALESCE($8::interval, interval '0')),
                         target.takeover_at
                     ) AS run_at
                 ) AS input
                 WHERE job.id = target.id AND target.state IN ('scheduled', 'enqueued')
                     AND NOT target.kept
                 RETURNING job.id
             ),
             reset AS (
                 UPDATE {job_control}
                 SET progress = NULL, lease = lease + 1, updated_at = statement_timestamp()
                 WHERE job_id IN (SELECT id FROM replaced)
             ),
             forgotten AS (
                 DELETE FROM {job_step} WHERE job_id IN (SELECT id FROM replaced)
             )
             SELECT target.id, target.kept, target.state = 'running' AS running,
                 EXISTS (SELECT 1 FROM replaced) AS replaced
             FROM target",
            job = schema.table("job"),
            job_control = schema.table("job_control"),
            job_step = schema.table("job_step"),
        );
        let polling_since = Instant::now();
        loop {
            let row: Option<ReplaceRow> = sqlx::query_as(AssertSqlSafe(sql.as_str()))
                .bind(&self.queue)
                .bind(&self.task)
                .bind(&self.payload)
                .bind(&self.queue_key)
                .bind(self.max_attempts)
                .bind(self.timeout_ms)
                .bind(self.run_at)
                .bind(self.delay)
                .bind(job_key)
                .bind(&self.context)
                .bind(INTERRUPTED)
                .bind(self.fingerprint)
                .bind(self.priority)
                .bind(self.parent_id)
                .bind(self.root_id)
                .bind(self.queue_key_limit)
                .bind(self.expires_at)
                .bind(self.expire_after)
                .fetch_optional(&mut *connection)
                .await?;
            match row {
                Some(row) if row.kept => return Ok(Replacement::Kept(JobId(row.id))),
                Some(row) if row.replaced => return Ok(Replacement::Replaced(JobId(row.id))),
                Some(row) if row.running => return Ok(Replacement::Running(JobId(row.id))),
                Some(_) => return Ok(Replacement::Missing),
                None if pending_id(connection, schema, job_key).await?.is_none() => {
                    return Ok(Replacement::Missing);
                }
                None if polling_since.elapsed() >= REPLACE_PATIENCE => {
                    return Err(Error::new(
                        ErrorKind::JobKeyContention,
                        format!("job_key {job_key:?} stayed locked while enqueuing"),
                    ));
                }
                None => tokio::time::sleep(LOCK_POLL).await,
            }
        }
    }

    async fn notify(&self, connection: &mut PgConnection, schema: &Schema) -> Result<()> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(schema.notify_channel(JOB_CHANNEL))
            .bind(&self.queue)
            .execute(connection)
            .await?;
        Ok(())
    }
}

async fn lock_job_key(connection: &mut PgConnection, schema: &Schema, job_key: &str) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("{}:{job_key}", schema.lock_key("job_key")))
        .execute(connection)
        .await?;
    Ok(())
}

async fn pending_id(
    connection: &mut PgConnection,
    schema: &Schema,
    job_key: &str,
) -> Result<Option<JobId>> {
    let sql = format!(
        "SELECT id FROM {job} WHERE job_key = $1 AND state IN {JOB_KEY_STATES}",
        job = schema.table("job"),
    );
    let id = sqlx::query_scalar(AssertSqlSafe(sql))
        .bind(job_key)
        .fetch_optional(connection)
        .await?;
    Ok(id.map(JobId))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_attempts_rejects_values_below_one() {
        for value in [0, -1] {
            let error = max_attempts("task", Some(value), 10).unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidOptions);
            assert!(error.msg.contains("max_attempts"), "{}", error.msg);
        }
        assert_eq!(max_attempts("task", Some(1), 10).unwrap(), 1);
        assert_eq!(max_attempts("task", None, 10).unwrap(), 10);
    }

    #[test]
    fn test_queue_key_limit_stores_one_as_no_limit() {
        assert_eq!(queue_key_limit("task", None, false).unwrap(), None);
        assert_eq!(queue_key_limit("task", Some(1), true).unwrap(), None);
        assert_eq!(queue_key_limit("task", Some(4), true).unwrap(), Some(4));
        assert_eq!(
            queue_key_limit("task", Some(32_767), true).unwrap(),
            Some(i16::MAX)
        );
        for (limit, keyed) in [(0, true), (32_768, true), (40_000, true), (2, false)] {
            let error = queue_key_limit("task", Some(limit), keyed).unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidOptions, "{limit} {keyed}");
        }
    }
}
