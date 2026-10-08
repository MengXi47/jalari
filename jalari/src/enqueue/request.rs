use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::job::{context, registry};
use crate::storage::{JOB_CHANNEL, interval};
use crate::{
    EnqueueOptions, EnqueueOutcome, Error, ErrorKind, Job, JobId, OnConflict, Result, Schema,
};

const DEFAULT_QUEUE: &str = "default";
const MAX_CONFLICT_RETRIES: usize = 3;

pub(crate) struct EnqueueRequest {
    task: String,
    payload: Vec<u8>,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    queue: String,
    run_at: Option<DateTime<Utc>>,
    delay: Option<Duration>,
    job_key: Option<String>,
    queue_key: Option<String>,
    on_conflict: OnConflict,
    context: Option<Value>,
    inherit_when_submitted: bool,
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

pub(crate) fn default_queue(task: &str) -> &'static str {
    registry::find(task).map_or(DEFAULT_QUEUE, |registration| registration.queue)
}

impl EnqueueRequest {
    pub(crate) fn build<T: Job>(job: &T, options: EnqueueOptions) -> Result<Self> {
        let payload = encode_payload(job)?;
        let timeout_ms = timeout_ms(T::NAME, options.timeout.or(T::TIMEOUT))?;
        let queue = options
            .queue
            .unwrap_or_else(|| default_queue(T::NAME).to_owned());
        let inherits = options.context.inherits();
        let context = options.context.resolve().into_value(T::NAME)?;
        Ok(Self {
            inherit_when_submitted: inherits && context.is_none(),
            task: T::NAME.to_owned(),
            payload,
            max_attempts: T::MAX_ATTEMPTS,
            timeout_ms,
            queue,
            run_at: options.run_at,
            delay: options.delay.map(interval),
            job_key: options.job_key,
            queue_key: options.queue_key,
            on_conflict: options.on_conflict,
            context,
        })
    }

    pub(crate) fn recurring(
        name: &str,
        task: String,
        payload: Vec<u8>,
        queue: String,
        max_attempts: i32,
        timeout_ms: Option<i32>,
        context: Option<Value>,
    ) -> Self {
        Self {
            task,
            payload,
            max_attempts,
            timeout_ms,
            queue,
            run_at: None,
            delay: None,
            job_key: Some(format!("recurring:{name}")),
            queue_key: None,
            on_conflict: OnConflict::KeepExisting,
            context,
            inherit_when_submitted: false,
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

    fn inherit_context(mut self) -> Result<Self> {
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
                OnConflict::Replace => {
                    if let Some(id) = self.try_replace(connection, schema, job_key).await? {
                        self.notify(connection, schema).await?;
                        return Ok(EnqueueOutcome::Replaced(id));
                    }
                    if let Some(id) = pending_id(connection, schema, job_key).await? {
                        return Ok(EnqueueOutcome::Running(id));
                    }
                }
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
                 run_at, timeout_ms, context)
             SELECT $1, $2, $3, CASE WHEN input.run_at > now() THEN 'scheduled' ELSE 'enqueued' END,
                 $4, $5, $6, input.run_at, $7, $10
             FROM (SELECT COALESCE($8::timestamptz, now() + COALESCE($9::interval, interval '0'))
                 AS run_at) AS input
             ON CONFLICT (job_key) WHERE job_key IS NOT NULL AND state IN ('scheduled', 'enqueued')
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
            .fetch_optional(connection)
            .await?;
        Ok(id.map(JobId))
    }

    async fn try_replace(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
        job_key: &str,
    ) -> Result<Option<JobId>> {
        let sql = format!(
            "UPDATE {job} AS job
             SET queue = $1, task = $2, payload = $3,
                 state = CASE WHEN input.run_at > now() THEN 'scheduled' ELSE 'enqueued' END,
                 queue_key = $4, attempts = 0, max_attempts = $5, run_at = input.run_at,
                 timeout_ms = $6, last_error = NULL, context = $10, updated_at = now()
             FROM (SELECT COALESCE($7::timestamptz, now() + COALESCE($8::interval, interval '0'))
                 AS run_at) AS input
             WHERE job.id = (
                 SELECT id FROM {job}
                 WHERE job_key = $9 AND state IN ('scheduled', 'enqueued')
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING job.id",
            job = schema.table("job"),
        );
        let id = sqlx::query_scalar(AssertSqlSafe(sql))
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
            .fetch_optional(connection)
            .await?;
        Ok(id.map(JobId))
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
        "SELECT id FROM {job} WHERE job_key = $1 AND state IN ('scheduled', 'enqueued')",
        job = schema.table("job"),
    );
    let id = sqlx::query_scalar(AssertSqlSafe(sql))
        .bind(job_key)
        .fetch_optional(connection)
        .await?;
    Ok(id.map(JobId))
}
