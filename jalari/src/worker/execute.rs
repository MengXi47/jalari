use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Connection, PgConnection, Postgres, Transaction};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, info, info_span, warn};

use super::close::{Closing, Ending, Outcome, close, decide};
use super::middleware::{JobMeta, Next};
use super::runner::Shared;
use crate::job::context::{self, Captured};
use crate::job::registry::JobRegistration;
use crate::job::{Command, JobState, RunControl, run};
use crate::{Error, ErrorKind, JobError, JobId, JobResult, JobRun, Result, Schema};

const CLAIM_LEASE: Duration = Duration::from_secs(2);
const CONNECTION_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const CONNECTION_CHECK_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const TAKEOVER_DELAY: Duration = Duration::from_secs(6);
const IDLE_SESSION_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const INTERRUPTED: &str = "the worker stopped during the last attempt";

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
    context: Option<Value>,
}

#[derive(sqlx::FromRow)]
struct ClaimedControl {
    command: Option<String>,
    lease: i64,
    progress: Option<Value>,
    unfinished: bool,
}

struct QueueKeyLock(i64);

struct StartedAttempt {
    job_id: i64,
    number: i32,
    token: String,
}

#[derive(sqlx::FromRow)]
struct MarkedJob {
    id: i64,
    task: String,
    state: String,
    attempts: i32,
    commanded: bool,
}

struct AbortOnDrop(JoinHandle<JobResult>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct CloseUnlessFinished {
    connection: PoolConnection<Postgres>,
    finished: bool,
}

impl Drop for CloseUnlessFinished {
    fn drop(&mut self) {
        if !self.finished {
            self.connection.close_on_drop();
        }
    }
}

impl Shared {
    pub(super) async fn process_next(&self, queue: &str, waker: &Notify) -> Result<bool> {
        let mut pooled = CloseUnlessFinished {
            connection: self.config.pool.pool().acquire().await?,
            finished: false,
        };
        let processed = self
            .claim_and_run(&mut pooled.connection, queue, waker)
            .await?;
        pooled.finished = true;
        Ok(processed)
    }

    async fn claim_and_run(
        &self,
        connection: &mut PgConnection,
        queue: &str,
        waker: &Notify,
    ) -> Result<bool> {
        let schema = &self.config.schema;
        let mut excluded_keys: Vec<String> = Vec::new();
        for _ in 0..self.settings.max_claim_tries {
            let mut transaction = connection
                .begin_with(AssertSqlSafe(begin_with_idle_timeout()))
                .await?;
            let Some(job) = self.claim(&mut transaction, queue, &excluded_keys).await? else {
                transaction.rollback().await?;
                return Ok(false);
            };
            let control = job.load_control(&mut transaction, schema).await?;
            if let Some(command) = control.command() {
                if control.unfinished {
                    mark_interrupted(&mut transaction, schema, &[job.id], false).await?;
                } else {
                    let closing = Closing::idle(job.id, job.attempts, command);
                    close(&mut transaction, schema, &closing).await?;
                    log_outcome(job.id, &closing);
                }
                transaction.commit().await?;
                return Ok(true);
            }
            let key = match &job.queue_key {
                Some(queue_key) => {
                    match try_lock_queue_key(&mut transaction, schema, queue_key).await? {
                        Some(key) => Some(key),
                        None => {
                            transaction.rollback().await?;
                            excluded_keys.push(queue_key.clone());
                            continue;
                        }
                    }
                }
                None => None,
            };
            if job.attempts > 0
                && mark_interrupted(&mut transaction, schema, &[job.id], true).await?
            {
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
                    job.postpone(&mut transaction, schema, until).await?;
                    transaction.commit().await?;
                    return Ok(true);
                }
            }
            let attempt = job.start_attempt(&mut transaction, schema).await?;
            attempt.hand_over(&mut transaction, key.as_ref()).await?;
            let Some(command) = attempt.relock(&mut transaction, schema).await? else {
                transaction.rollback().await?;
                return Ok(true);
            };
            waker.notify_one();
            self.execute(transaction, queue, job, control, attempt.number, command)
                .await?;
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
        let schema = &self.config.schema;
        let sql = format!(
            "SELECT id, task, payload, attempts, max_attempts, timeout_ms, queue_key, context
             FROM {job}
             WHERE queue = $1
                 AND state IN ('scheduled', 'enqueued')
                 AND run_at <= now()
                 AND task = ANY($2)
                 AND (queue_key IS NULL OR queue_key <> ALL($3))
             ORDER BY priority DESC, run_at, id
             LIMIT 1
             FOR UPDATE SKIP LOCKED",
            job = schema.table("job"),
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
        claimed: ClaimedControl,
        attempt: i32,
        command: Option<Command>,
    ) -> Result<()> {
        let span = info_span!("job", job_id = job.id, task = %job.task, queue = %queue, attempt);
        async move {
            let schema = &self.config.schema;
            let started = Instant::now();
            let control = Arc::new(RunControl::new(
                job.id,
                claimed.lease,
                claimed.progress,
                self.job_shutdown.child_token(),
            ));
            let registration = self.runs.register(&control);
            let mut command = command;
            let ending = match (self.jobs.get(job.task.as_str()), command) {
                (_, Some(_)) => Ending::NotStarted,
                (None, None) => Ending::Returned(Err(JobError::new(format!(
                    "no job is registered as {:?}",
                    job.task
                )))),
                (Some(&registration), None) => {
                    let run = job.run(self, registration, queue, attempt, Arc::clone(&control));
                    tokio::pin!(run);
                    let mut deadline = None;
                    loop {
                        tokio::select! {
                            result = &mut run => break Ending::Returned(result),
                            () = sleep_until(deadline), if deadline.is_some() => {
                                break Ending::Aborted { after: self.settings.stop_timeout };
                            }
                            () = tokio::time::sleep(CONNECTION_CHECK_INTERVAL) => {
                                match poll_command(&mut transaction, schema, job.id).await {
                                    Ok(Some(polled)) => {
                                        control.request(polled);
                                        command = command.max(Some(polled));
                                        deadline.get_or_insert_with(|| {
                                            tokio::time::Instant::now()
                                                + self.settings.stop_timeout
                                        });
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        warn!(error = %e, "lost the database connection, aborting the job");
                                        return Err(e);
                                    }
                                }
                            }
                        }
                    }
                }
            };
            let command = command.max(lock_command(&mut transaction, schema, job.id).await?);
            let outcome = decide(
                ending,
                command,
                attempt,
                job.max_attempts,
                &*self.retry_policy,
            );
            let closing = Closing::after_run(
                job.id,
                attempt,
                outcome,
                started.elapsed(),
                command.is_some(),
                control.latest(),
            );
            close(&mut transaction, schema, &closing).await?;
            transaction.commit().await?;
            registration.close();
            log_outcome(job.id, &closing);
            Ok(())
        }
        .instrument(span)
        .await
    }
}

fn log_outcome(job_id: i64, closing: &Closing) {
    let duration_ms = closing.duration_ms();
    match closing.outcome() {
        Outcome::Succeeded => debug!(duration_ms, "job succeeded"),
        Outcome::Retry { delay, error } => {
            warn!(error = %error, ?delay, "job failed, retrying");
        }
        Outcome::Failed { error } => warn!(error = %error, "job failed"),
        Outcome::Released { note } => info!(job_id, note = %note, "job released"),
        Outcome::Stopped { note } => info!(job_id, note = %note, "job stopped"),
        Outcome::Cancelled { note } => info!(job_id, note = %note, "job cancelled"),
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

impl ClaimedControl {
    fn command(&self) -> Option<Command> {
        self.command.as_deref().and_then(Command::parse)
    }
}

impl ClaimedJob {
    async fn load_control(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
    ) -> Result<ClaimedControl> {
        let control = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT control.command, coalesce(control.lease, 0) AS lease, control.progress,
                 CASE WHEN control.command IS NULL THEN FALSE ELSE {unfinished} END
                     AS unfinished
             FROM {job} AS job
             LEFT JOIN {job_control} AS control ON control.job_id = job.id
             WHERE job.id = $1",
            job = schema.table("job"),
            job_control = schema.table("job_control"),
            unfinished = schema.unfinished_attempt("job"),
        )))
        .bind(self.id)
        .fetch_one(connection)
        .await?;
        Ok(control)
    }

    async fn start_attempt(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
    ) -> Result<StartedAttempt> {
        let (number, token) = sqlx::query_as(AssertSqlSafe(format!(
            "UPDATE {job} SET attempts = attempts + 1, run_at = now() + $2
             WHERE id = $1
             RETURNING attempts, xmin::text",
            job = schema.table("job"),
        )))
        .bind(self.id)
        .bind(CLAIM_LEASE)
        .fetch_one(connection)
        .await?;
        Ok(StartedAttempt {
            job_id: self.id,
            number,
            token,
        })
    }

    async fn mark_interrupted_siblings(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
        queue_key: &str,
    ) -> Result<()> {
        let siblings: Vec<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT id FROM {job} AS job
             WHERE queue_key = $1 AND id <> $2 AND state IN ('scheduled', 'enqueued')
                 AND {unfinished}
             FOR UPDATE SKIP LOCKED",
            job = schema.table("job"),
            unfinished = schema.unfinished_attempt("job"),
        )))
        .bind(queue_key)
        .bind(self.id)
        .fetch_all(&mut *connection)
        .await?;
        mark_interrupted(connection, schema, &siblings, true).await?;
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
                 SELECT CASE WHEN state = 'failed' THEN updated_at + $3
                         ELSE least(run_at, updated_at + $3) END AS until
                 FROM {job}
                 WHERE queue_key = $1 AND id <> $2 AND last_error = $4
                     AND ((state IN ('scheduled', 'enqueued') AND run_at > now()
                             AND updated_at > now() - $3)
                         OR (state = 'failed' AND updated_at > now() - $3))
                 UNION ALL
                 SELECT now() + $3 AS until
                 FROM {job} AS job
                 WHERE queue_key = $1 AND id <> $2 AND state IN ('scheduled', 'enqueued')
                     AND run_at <= now() AND {unfinished}
             ) AS fences",
            job = schema.table("job"),
            unfinished = schema.unfinished_attempt("job"),
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
    ) -> Result<()> {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {job} SET run_at = $2 WHERE id = $1",
            job = schema.table("job"),
        )))
        .bind(self.id)
        .bind(until)
        .execute(connection)
        .await?;
        debug!(job_id = self.id, %until, "waiting for an interrupted job with the same queue_key");
        Ok(())
    }

    async fn run(
        &self,
        shared: &Shared,
        registration: &'static JobRegistration,
        queue: &str,
        attempt: i32,
        control: Arc<RunControl>,
    ) -> JobResult {
        let job_run = JobRun::new(
            JobId(self.id),
            registration.name,
            Arc::from(queue),
            attempt,
            self.max_attempts,
            control.shutdown().clone(),
            control,
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

impl StartedAttempt {
    async fn hand_over(
        &self,
        connection: &mut PgConnection,
        key: Option<&QueueKeyLock>,
    ) -> Result<()> {
        let commit = format!("COMMIT; {}", begin_with_idle_timeout());
        let sql = match key {
            Some(QueueKeyLock(lock_id)) => format!(
                "SELECT pg_advisory_lock('{lock_id}'::bigint);
                 {commit};
                 SELECT pg_advisory_xact_lock('{lock_id}'::bigint);
                 SELECT pg_advisory_unlock('{lock_id}'::bigint)"
            ),
            None => commit,
        };
        sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(connection)
            .await?;
        Ok(())
    }

    async fn relock(
        &self,
        connection: &mut PgConnection,
        schema: &Schema,
    ) -> Result<Option<Option<Command>>> {
        let locked: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT (SELECT command FROM {job_control} WHERE job_id = $1)
             FROM {job} WHERE id = $1 AND xmin = $2::text::xid FOR UPDATE",
            job = schema.table("job"),
            job_control = schema.table("job_control"),
        )))
        .bind(self.job_id)
        .bind(&self.token)
        .fetch_optional(connection)
        .await?;
        Ok(locked.map(|command| command.as_deref().and_then(Command::parse)))
    }
}

pub(crate) async fn mark_interrupted(
    connection: &mut PgConnection,
    schema: &Schema,
    ids: &[i64],
    fail_exhausted: bool,
) -> Result<bool> {
    if ids.is_empty() {
        return Ok(false);
    }
    let commanded = format!(
        "EXISTS (SELECT 1 FROM {job_control} AS control
             WHERE control.job_id = job.id AND control.command IS NOT NULL)",
        job_control = schema.table("job_control"),
    );
    let exhausted = format!("($4 AND job.attempts >= job.max_attempts AND NOT {commanded})");
    let marked: Vec<MarkedJob> = sqlx::query_as(AssertSqlSafe(format!(
        "WITH marked AS (
             UPDATE {job} AS job
             SET state = CASE WHEN {exhausted} THEN 'failed' ELSE 'scheduled' END,
                 attempts = CASE WHEN {commanded} THEN job.attempts - 1 ELSE job.attempts END,
                 last_error = $2,
                 run_at = CASE WHEN {exhausted} THEN job.run_at
                     ELSE statement_timestamp() + $3 END,
                 updated_at = statement_timestamp()
             WHERE job.id = ANY($1) AND job.state IN ('scheduled', 'enqueued')
                 AND job.attempts > 0
                 AND ({exhausted} OR {unfinished})
             RETURNING job.id, job.task, job.state, job.attempts, {commanded} AS commanded
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, error, created_at)
             SELECT id, state, attempts, $2, statement_timestamp() FROM marked
         ),
         fenced AS (
             INSERT INTO {job_control} AS control (job_id, lease)
             SELECT id, 1 FROM marked ORDER BY id
             ON CONFLICT (job_id) DO UPDATE
             SET lease = control.lease + 1, updated_at = statement_timestamp()
         )
         SELECT id, task, state, attempts, commanded FROM marked",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
        job_control = schema.table("job_control"),
        unfinished = schema.unfinished_attempt("job"),
    )))
    .bind(ids)
    .bind(INTERRUPTED)
    .bind(TAKEOVER_DELAY)
    .bind(fail_exhausted)
    .fetch_all(connection)
    .await?;
    for job in &marked {
        if job.state == JobState::Failed.as_str() {
            warn!(
                job_id = job.id,
                task = %job.task,
                attempts = job.attempts,
                "job failed because its last attempt was interrupted"
            );
        } else if job.commanded {
            warn!(
                job_id = job.id,
                task = %job.task,
                attempts = job.attempts,
                delay = ?TAKEOVER_DELAY,
                "job was interrupted while asked to stop, settling it after a delay"
            );
        } else {
            warn!(
                job_id = job.id,
                task = %job.task,
                attempts = job.attempts,
                delay = ?TAKEOVER_DELAY,
                "job was interrupted, running it again after a delay"
            );
        }
    }
    Ok(!marked.is_empty())
}

fn begin_with_idle_timeout() -> String {
    format!(
        "BEGIN; SET LOCAL idle_in_transaction_session_timeout = {}",
        IDLE_SESSION_TIMEOUT.as_millis()
    )
}

async fn poll_command(
    connection: &mut PgConnection,
    schema: &Schema,
    job_id: i64,
) -> Result<Option<Command>> {
    let sql = format!(
        "SELECT command FROM {job_control} WHERE job_id = $1",
        job_control = schema.table("job_control"),
    );
    let poll = sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(sql))
        .bind(job_id)
        .fetch_optional(connection);
    match tokio::time::timeout(CONNECTION_CHECK_TIMEOUT, poll).await {
        Ok(command) => Ok(command?.flatten().as_deref().and_then(Command::parse)),
        Err(_) => Err(Error::new(
            ErrorKind::ConnectionFailed,
            format!("the database did not answer within {CONNECTION_CHECK_TIMEOUT:?}"),
        )),
    }
}

async fn lock_command(
    connection: &mut PgConnection,
    schema: &Schema,
    job_id: i64,
) -> Result<Option<Command>> {
    let command: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT command FROM {job_control} WHERE job_id = $1 FOR UPDATE",
        job_control = schema.table("job_control"),
    )))
    .bind(job_id)
    .fetch_optional(connection)
    .await?;
    Ok(command.flatten().as_deref().and_then(Command::parse))
}

async fn try_lock_queue_key(
    connection: &mut PgConnection,
    schema: &Schema,
    queue_key: &str,
) -> Result<Option<QueueKeyLock>> {
    let (lock_id, locked): (i64, bool) = sqlx::query_as(
        "SELECT lock_id, pg_try_advisory_xact_lock(lock_id)
         FROM (SELECT hashtextextended($1, 0) AS lock_id) AS queue_key",
    )
    .bind(queue_key_lock(schema, queue_key))
    .fetch_one(connection)
    .await?;
    Ok(locked.then_some(QueueKeyLock(lock_id)))
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
