use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sqlx::types::Json;
use sqlx::{AssertSqlSafe, PgPool};
use tracing::{info, warn};

use super::close::Closing;
use super::lifeline::{CLAIM_FENCE, Incarnation, TAKEOVER_GRACE};
use super::runner::Shared;
use super::runs::{Hold, PendingClaim};
use crate::job::Command;
use crate::job::fingerprint::compatible;
use crate::storage::{JOB_CHANNEL, interval};
use crate::{JobState, Result, Schema};

pub(crate) const INTERRUPTED: &str = "the worker stopped during the last attempt";
const UNIQUE_VIOLATION: &str = "23505";
const REAP_BATCH: i64 = 100;

#[derive(sqlx::FromRow)]
struct ClaimedRow {
    id: i64,
    task: String,
    payload: Vec<u8>,
    attempts: i32,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    context: Option<Value>,
    lease: i64,
    progress: Option<Value>,
    command: Option<String>,
}

pub(super) struct Claimed {
    pub(super) hold: Hold,
    pub(super) task: String,
    pub(super) payload: Vec<u8>,
    pub(super) attempt: i32,
    pub(super) max_attempts: i32,
    pub(super) timeout_ms: Option<i32>,
    pub(super) context: Option<Value>,
    pub(super) lease: i64,
    pub(super) progress: Option<Value>,
    pub(super) command: Option<Command>,
}

pub(super) enum Closed {
    Written,
    Recommanded(Option<Command>),
    Lost,
}

pub(super) enum Target<'a> {
    DeadOwners { sweep_orphans: bool },
    Orphans(&'a [Hold]),
    Shutdown(&'a [Hold]),
}

impl Target<'_> {
    fn delay(&self) -> Duration {
        match self {
            Self::DeadOwners { .. } => Duration::ZERO,
            Self::Orphans(_) | Self::Shutdown(_) => TAKEOVER_GRACE,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct Interrupted {
    id: i64,
    task: String,
    queue: String,
    state: String,
    attempts: i32,
    commanded: bool,
}

impl Shared {
    pub(super) async fn claim(
        &self,
        incarnation: &Incarnation,
        queue: &str,
    ) -> Result<Option<(Claimed, PendingClaim<'_>)>> {
        let schema = &self.config.schema;
        let sql = format!(
            "WITH alive AS MATERIALIZED (
                 SELECT NOT pg_try_advisory_xact_lock_shared(hashtextextended($5, 0))
                     AND EXISTS (
                         SELECT 1 FROM {worker}
                         WHERE id = $3::uuid AND heartbeat_at >= statement_timestamp() - $6
                     ) AS alive
             ),
             picked AS (
                 SELECT job.id FROM {job} AS job
                 WHERE (SELECT alive FROM alive)
                     AND job.queue = $1
                     AND job.state IN ('scheduled', 'enqueued')
                     AND job.run_at <= now()
                     AND {compatible}
                     AND (job.queue_key IS NULL OR NOT EXISTS (
                         SELECT 1 FROM {job} AS holder
                         WHERE holder.queue_key = job.queue_key AND holder.state = 'running'
                     ))
                 ORDER BY job.priority DESC, job.run_at, job.id
                 LIMIT 1
                 FOR NO KEY UPDATE SKIP LOCKED
             ),
             claimed AS (
                 UPDATE {job} AS claimed
                 SET state = 'running', attempts = claimed.attempts + 1,
                     locked_by = $3::uuid, locked_at = statement_timestamp(), lock_token = $4,
                     queue_key_slot = CASE WHEN claimed.queue_key IS NULL THEN NULL ELSE 0 END,
                     updated_at = statement_timestamp()
                 FROM picked
                 WHERE claimed.id = picked.id
                 RETURNING claimed.id, claimed.task, claimed.payload, claimed.attempts,
                     claimed.max_attempts, claimed.timeout_ms, claimed.context
             ),
             control AS (
                 SELECT control.job_id, control.lease, control.progress, control.command
                 FROM {job_control} AS control
                 WHERE control.job_id IN (SELECT id FROM claimed)
                 FOR SHARE
             )
             SELECT claimed.id, claimed.task, claimed.payload, claimed.attempts,
                 claimed.max_attempts, claimed.timeout_ms, claimed.context,
                 coalesce(control.lease, 0) AS lease, control.progress, control.command
             FROM claimed
             LEFT JOIN control ON control.job_id = claimed.id",
            job = schema.table("job"),
            worker = schema.table("worker"),
            job_control = schema.table("job_control"),
            compatible = compatible("$2::jsonb"),
        );
        let lock_key = incarnation.lock_key();
        for _ in 0..self.settings.max_claim_tries.max(1) {
            let pending = self.runs.pending(incarnation.worker_id());
            let claimed = sqlx::query_as::<_, ClaimedRow>(AssertSqlSafe(sql.as_str()))
                .bind(queue)
                .bind(Json(&self.served))
                .bind(&**incarnation.worker_id())
                .bind(pending.token())
                .bind(lock_key)
                .bind(interval(CLAIM_FENCE))
                .fetch_optional(&self.config.pool.pool())
                .await;
            let row = match claimed {
                Ok(row) => row,
                Err(e) if is_unique_violation(&e) => continue,
                Err(e) => return Err(e.into()),
            };
            return Ok(row.map(|row| {
                let claimed = Claimed {
                    hold: Hold {
                        job_id: row.id,
                        worker_id: Arc::clone(incarnation.worker_id()),
                        token: pending.token(),
                    },
                    task: row.task,
                    payload: row.payload,
                    attempt: row.attempts,
                    max_attempts: row.max_attempts,
                    timeout_ms: row.timeout_ms,
                    context: row.context,
                    lease: row.lease,
                    progress: row.progress,
                    command: row.command.as_deref().and_then(Command::parse),
                };
                (claimed, pending)
            }));
        }
        Ok(None)
    }
}

pub(super) async fn close(
    pool: &PgPool,
    schema: &Schema,
    hold: &Hold,
    closing: &Closing,
) -> Result<Closed> {
    let outcome = closing.outcome();
    let last_error = outcome.last_error();
    let terminal = outcome.is_terminal();
    let kept = closing.kept();
    let closed: bool = sqlx::query_scalar(AssertSqlSafe(format!(
        "WITH closed AS (
             UPDATE {job}
             SET state = $4, attempts = $5,
                 last_error = CASE WHEN $6 THEN $7 ELSE last_error END,
                 run_at = coalesce(statement_timestamp() + $8, run_at),
                 updated_at = statement_timestamp(),
                 locked_by = NULL, locked_at = NULL, lock_token = NULL, queue_key_slot = NULL
             WHERE id = $1 AND locked_by = $2::uuid AND lock_token = $3
                 AND (SELECT control.command FROM {job_control} AS control
                     WHERE control.job_id = $1) IS NOT DISTINCT FROM $9
             RETURNING id
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, error, duration_ms, created_at)
             SELECT id, $4, $5, $10, $11, statement_timestamp() FROM closed
         ),
         kept AS (
             INSERT INTO {job_control} AS control (job_id, lease, progress)
             SELECT id, 1, $13 FROM closed WHERE $14
             ON CONFLICT (job_id) DO UPDATE
             SET lease = control.lease + 1,
                 command = CASE WHEN control.command IS NOT DISTINCT FROM $9 THEN NULL
                     ELSE control.command END,
                 progress = EXCLUDED.progress, updated_at = statement_timestamp()
         ),
         bumped AS (
             UPDATE {job_control}
             SET lease = lease + 1,
                 command = CASE WHEN command IS NOT DISTINCT FROM $9 THEN NULL ELSE command END,
                 progress = CASE WHEN $12 THEN NULL ELSE progress END,
                 updated_at = statement_timestamp()
             WHERE job_id IN (SELECT id FROM closed) AND NOT $14
         )
         SELECT EXISTS (SELECT 1 FROM closed)",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
        job_control = schema.table("job_control"),
    )))
    .bind(hold.job_id)
    .bind(&*hold.worker_id)
    .bind(hold.token)
    .bind(outcome.state().as_str())
    .bind(closing.attempts())
    .bind(last_error.is_some())
    .bind(last_error.flatten())
    .bind(closing.run_at_delay().map(interval))
    .bind(closing.seen().map(Command::as_str))
    .bind(outcome.note())
    .bind(closing.duration_ms())
    .bind(terminal)
    .bind(kept.filter(|_| !terminal))
    .bind(kept.is_some())
    .fetch_one(pool)
    .await?;
    if closed {
        return Ok(Closed::Written);
    }
    let command: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT (SELECT control.command FROM {job_control} AS control WHERE control.job_id = $1)
         FROM {job} WHERE id = $1 AND locked_by = $2::uuid AND lock_token = $3",
        job = schema.table("job"),
        job_control = schema.table("job_control"),
    )))
    .bind(hold.job_id)
    .bind(&*hold.worker_id)
    .bind(hold.token)
    .fetch_optional(pool)
    .await?;
    Ok(match command {
        Some(command) => Closed::Recommanded(command.as_deref().and_then(Command::parse)),
        None => Closed::Lost,
    })
}

pub(super) async fn interrupt(
    pool: &PgPool,
    schema: &Schema,
    target: Target<'_>,
) -> Result<Vec<Interrupted>> {
    let job = schema.table("job");
    let worker = schema.table("worker");
    let departed = match target {
        Target::DeadOwners { .. } => format!(
            ",
             departed AS (
                 DELETE FROM {worker} AS worker
                 WHERE worker.id IN (SELECT id FROM dead)
                     AND NOT EXISTS (
                         SELECT 1 FROM {job} AS job
                         WHERE job.locked_by = worker.id AND job.state = 'running'
                             AND job.id NOT IN (SELECT id FROM marked)
                     )
             )"
        ),
        Target::Orphans(_) | Target::Shutdown(_) => String::new(),
    };
    let doomed = match target {
        Target::DeadOwners { .. } => format!(
            "suspects AS MATERIALIZED (
                 SELECT worker.id FROM {worker} AS worker
                 WHERE worker.heartbeat_at <= statement_timestamp() - $3
                     AND EXISTS (
                         SELECT 1 FROM {job} AS job
                         WHERE job.locked_by = worker.id AND job.state = 'running'
                     )
                 UNION
                 SELECT job.locked_by FROM {job} AS job
                 WHERE $5 AND job.state = 'running'
                     AND NOT EXISTS (SELECT 1 FROM {worker} AS worker WHERE worker.id = job.locked_by)
             ),
             dead AS MATERIALIZED (
                 SELECT suspects.id FROM suspects
                 WHERE pg_try_advisory_xact_lock(hashtextextended($4 || suspects.id::text, 0))
             ),
             doomed AS (
                 SELECT job.id FROM {job} AS job
                 WHERE job.state = 'running' AND job.locked_by IN (SELECT id FROM dead)
                 ORDER BY job.id
                 LIMIT $6
                 FOR NO KEY UPDATE SKIP LOCKED
             )"
        ),
        Target::Orphans(_) | Target::Shutdown(_) => format!(
            "doomed AS (
                 SELECT job.id FROM {job} AS job
                 JOIN unnest($3::bigint[], $4::text[], $5::bigint[]) AS hold (job_id, worker_id, token)
                     ON job.id = hold.job_id AND job.locked_by = hold.worker_id::uuid
                         AND job.lock_token = hold.token
                 WHERE job.state = 'running'
                 ORDER BY job.id
                 FOR NO KEY UPDATE OF job
             )"
        ),
    };
    let sql = format!(
        "WITH {doomed},
         commanded AS (
             SELECT doomed.id, control.command FROM doomed
             LEFT JOIN {job_control} AS control ON control.job_id = doomed.id
         ),
         marked AS (
             UPDATE {job} AS job
             SET state = CASE WHEN commanded.command = 'cancel' THEN 'deleted'
                     WHEN commanded.command = 'stop' THEN 'stopped'
                     WHEN job.attempts >= job.max_attempts THEN 'failed'
                     ELSE 'scheduled' END,
                 attempts = CASE WHEN commanded.command IS NULL THEN job.attempts
                     ELSE job.attempts - 1 END,
                 last_error = $1,
                 run_at = statement_timestamp() + $2,
                 locked_by = NULL, locked_at = NULL, lock_token = NULL, queue_key_slot = NULL,
                 updated_at = statement_timestamp()
             FROM commanded
             WHERE job.id = commanded.id
             RETURNING job.id, job.task, job.queue, job.state, job.attempts,
                 commanded.command IS NOT NULL AS commanded
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, error, created_at)
             SELECT id, state, attempts, $1, statement_timestamp() FROM marked
         ),
         fenced AS (
             INSERT INTO {job_control} AS control (job_id, lease)
             SELECT id, 1 FROM marked ORDER BY id
             ON CONFLICT (job_id) DO UPDATE
             SET lease = control.lease + 1, command = NULL,
                 progress = CASE WHEN control.job_id IN (
                         SELECT id FROM marked WHERE state IN ('failed', 'deleted'))
                     THEN NULL ELSE control.progress END,
                 updated_at = statement_timestamp()
         ){departed}
         SELECT id, task, queue, state, attempts, commanded FROM marked",
        job_history = schema.table("job_history"),
        job_control = schema.table("job_control"),
    );
    let delay = target.delay();
    let query = sqlx::query_as::<_, Interrupted>(AssertSqlSafe(sql))
        .bind(INTERRUPTED)
        .bind(interval(delay));
    let interrupted = match target {
        Target::DeadOwners { sweep_orphans } => {
            query
                .bind(interval(TAKEOVER_GRACE))
                .bind(lock_prefix(schema))
                .bind(sweep_orphans)
                .bind(REAP_BATCH)
                .fetch_all(pool)
                .await?
        }
        Target::Orphans(holds) | Target::Shutdown(holds) => {
            let job_ids: Vec<i64> = holds.iter().map(|hold| hold.job_id).collect();
            let worker_ids: Vec<&str> = holds.iter().map(|hold| &*hold.worker_id).collect();
            let tokens: Vec<i64> = holds.iter().map(|hold| hold.token).collect();
            query
                .bind(job_ids)
                .bind(worker_ids)
                .bind(tokens)
                .fetch_all(pool)
                .await?
        }
    };
    for job in &interrupted {
        log_interrupted(job, delay);
    }
    let mut queues: Vec<&str> = interrupted
        .iter()
        .filter(|job| job.state == JobState::Scheduled.as_str())
        .map(|job| job.queue.as_str())
        .collect();
    queues.sort_unstable();
    queues.dedup();
    if !queues.is_empty() {
        sqlx::query("SELECT pg_notify($1, queue) FROM unnest($2::text[]) AS queue")
            .bind(schema.notify_channel(JOB_CHANNEL))
            .bind(&queues)
            .execute(pool)
            .await?;
    }
    Ok(interrupted)
}

pub(super) fn lock_prefix(schema: &Schema) -> String {
    format!("{}:", schema.lock_key("worker"))
}

pub(super) fn reaped_full_batch(interrupted: &[Interrupted]) -> bool {
    i64::try_from(interrupted.len()).unwrap_or(i64::MAX) >= REAP_BATCH
}

fn log_interrupted(job: &Interrupted, delay: Duration) {
    if job.commanded {
        info!(
            job_id = job.id,
            task = %job.task,
            state = %job.state,
            "job settled its pending request after its worker stopped"
        );
    } else if job.state == JobState::Failed.as_str() {
        warn!(
            job_id = job.id,
            task = %job.task,
            attempts = job.attempts,
            "job failed because its last attempt was interrupted"
        );
    } else {
        warn!(
            job_id = job.id,
            task = %job.task,
            attempts = job.attempts,
            ?delay,
            "job was interrupted, running it again"
        );
    }
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == UNIQUE_VIOLATION)
}
