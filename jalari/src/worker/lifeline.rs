use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::types::Json;
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::ownership::{Target, interrupt, lock_prefix};
use super::runner::{QueueConfig, Shared};
use super::runs::{Hold, Owned};
use crate::Result;
use crate::job::Command;

const PING_INTERVAL: Duration = Duration::from_secs(1);
const PING_TIMEOUT: Duration = Duration::from_secs(2);
const SELF_FENCE: Duration = Duration::from_secs(3);
const FREEZE_TOLERANCE: Duration = Duration::from_secs(10);
const LIFELINE_IDLE_TIMEOUT: Duration = Duration::from_secs(11);
pub(super) const CLAIM_FENCE: Duration = SELF_FENCE;
pub(super) const TAKEOVER_GRACE: Duration = Duration::from_secs(7);
const REGISTER_BACKOFF: Duration = Duration::from_secs(1);
const APPLICATION_NAME: &str = "jalari lifeline";

const _: () =
    assert!(SELF_FENCE.as_millis() >= PING_INTERVAL.as_millis() + PING_TIMEOUT.as_millis());
const _: () = assert!(TAKEOVER_GRACE.as_millis() >= 2 * SELF_FENCE.as_millis());
const _: () = assert!(
    LIFELINE_IDLE_TIMEOUT.as_millis() >= FREEZE_TOLERANCE.as_millis() + PING_INTERVAL.as_millis()
);
const _: () = assert!(LIFELINE_IDLE_TIMEOUT.as_millis() > TAKEOVER_GRACE.as_millis());

pub(super) struct WorkerRegistration {
    hostname: String,
    pid: i32,
    queues: Value,
    pub(super) queue_names: Vec<String>,
    scheduler: bool,
}

impl WorkerRegistration {
    pub(super) fn new(queues: &[QueueConfig], scheduler: bool) -> Self {
        Self {
            hostname: gethostname::gethostname().to_string_lossy().into_owned(),
            pid: i32::try_from(std::process::id()).unwrap_or(i32::MAX),
            queues: queues
                .iter()
                .map(|queue| json!({ "name": queue.name, "concurrency": queue.concurrency }))
                .collect(),
            queue_names: queues.iter().map(|queue| queue.name.clone()).collect(),
            scheduler,
        }
    }
}

pub(super) struct Incarnation {
    worker_id: Arc<str>,
    lock_key: String,
    dead: CancellationToken,
}

impl Incarnation {
    pub(super) fn worker_id(&self) -> &Arc<str> {
        &self.worker_id
    }

    pub(super) fn lock_key(&self) -> &str {
        &self.lock_key
    }

    pub(super) fn dead(&self) -> &CancellationToken {
        &self.dead
    }
}

pub(super) type Current = watch::Receiver<Option<Arc<Incarnation>>>;

#[derive(sqlx::FromRow)]
struct Registered {
    id: String,
    collides: bool,
    locked: bool,
}

#[derive(sqlx::FromRow)]
struct OwnedRow {
    id: Option<i64>,
    lock_token: Option<i64>,
    command: Option<String>,
}

struct Lifeline {
    connection: PgConnection,
    incarnation: Arc<Incarnation>,
}

pub(super) async fn lifeline(
    shared: Arc<Shared>,
    registration: WorkerRegistration,
    current: watch::Sender<Option<Arc<Incarnation>>>,
    retire: CancellationToken,
) {
    let mut previous: Option<Arc<str>> = None;
    while !retire.is_cancelled() {
        let mut lifeline = match Lifeline::open(&shared, &registration).await {
            Ok(Some(lifeline)) => lifeline,
            Ok(None) => {
                pause(&retire, REGISTER_BACKOFF).await;
                continue;
            }
            Err(e) => {
                warn!(error = %e, "failed to open the worker lifeline");
                pause(&retire, REGISTER_BACKOFF).await;
                continue;
            }
        };
        if let Some(worker_id) = previous.take()
            && let Err(e) = unregister(&mut lifeline.connection, &shared, &worker_id).await
        {
            warn!(error = %e, worker_id = %worker_id, "failed to remove the previous worker entry");
        }
        let incarnation = Arc::clone(&lifeline.incarnation);
        current.send_replace(Some(Arc::clone(&incarnation)));
        let kept = lifeline.keep(&shared, &retire).await;
        current.send_replace(None);
        match kept {
            Some(lifeline) => {
                if let Err(e) = lifeline.retire(&shared).await {
                    warn!(error = %e, "failed to remove the worker from the worker list");
                }
                return;
            }
            None => {
                incarnation.dead.cancel();
                shared.flush_now.notify_one();
                previous = Some(Arc::clone(&incarnation.worker_id));
            }
        }
    }
}

async fn pause(retire: &CancellationToken, duration: Duration) {
    tokio::select! {
        () = retire.cancelled() => {}
        () = tokio::time::sleep(duration) => {}
    }
}

impl Lifeline {
    async fn open(shared: &Shared, registration: &WorkerRegistration) -> Result<Option<Self>> {
        let schema = &shared.config.schema;
        let mut connection = shared.config.pool.pool().acquire().await?.detach();
        sqlx::query(
            "SELECT set_config('idle_session_timeout', $1, false),
                 set_config('tcp_user_timeout', $1, false),
                 set_config('statement_timeout', $2, false),
                 (SELECT set_config('transaction_timeout', '0', false)
                  WHERE current_setting('transaction_timeout', true) IS NOT NULL)",
        )
        .bind(LIFELINE_IDLE_TIMEOUT.as_millis().to_string())
        .bind(PING_TIMEOUT.as_millis().to_string())
        .execute(&mut connection)
        .await?;
        let prefix = lock_prefix(schema);
        let registered: Registered = sqlx::query_as(AssertSqlSafe(format!(
            "WITH registered AS (
                 INSERT INTO {worker} (hostname, pid, queues, scheduler, tasks)
                 VALUES ($1, $2, $3, $4, $5)
                 RETURNING id
             )
             SELECT registered.id::text AS id,
                 EXISTS (
                     SELECT 1 FROM {worker} AS other
                     WHERE other.id <> registered.id
                         AND hashtextextended($6 || other.id::text, 0)
                             = hashtextextended($6 || registered.id::text, 0)
                         AND EXISTS (
                             SELECT 1 FROM {job} AS job
                             WHERE job.locked_by = other.id AND job.state = 'running'
                         )
                 ) AS collides,
                 pg_try_advisory_lock(hashtextextended($6 || registered.id::text, 0)) AS locked,
                 set_config('application_name', $7 || ' ' || registered.id::text, false)
             FROM registered",
            worker = schema.table("worker"),
            job = schema.table("job"),
        )))
        .bind(&registration.hostname)
        .bind(registration.pid)
        .bind(Json(&registration.queues))
        .bind(registration.scheduler)
        .bind(Json(&shared.served))
        .bind(&prefix)
        .bind(APPLICATION_NAME)
        .fetch_one(&mut connection)
        .await?;
        if registered.collides || !registered.locked {
            unregister(&mut connection, shared, &registered.id).await?;
            connection.close().await?;
            return Ok(None);
        }
        Ok(Some(Self {
            connection,
            incarnation: Arc::new(Incarnation {
                lock_key: format!("{prefix}{}", registered.id),
                worker_id: Arc::from(registered.id),
                dead: CancellationToken::new(),
            }),
        }))
    }

    async fn keep(mut self, shared: &Arc<Shared>, retire: &CancellationToken) -> Option<Self> {
        let worker_id = Arc::clone(&self.incarnation.worker_id);
        loop {
            tokio::select! {
                () = retire.cancelled() => return Some(self),
                () = tokio::time::sleep(PING_INTERVAL) => {}
            }
            let sent = shared.runs.running(&worker_id);
            let failure = match tokio::time::timeout(PING_TIMEOUT, self.beat(shared)).await {
                Ok(Ok(Some(owned))) => {
                    let orphans = shared.runs.deliver(&worker_id, &sent, &owned);
                    if !orphans.is_empty() {
                        tokio::spawn(release(Arc::clone(shared), orphans));
                    }
                    continue;
                }
                Ok(Ok(None)) => "the worker row was removed".to_owned(),
                Ok(Err(e)) => e.to_string(),
                Err(_) => format!("the database did not answer within {PING_TIMEOUT:?}"),
            };
            warn!(
                worker_id = %worker_id,
                error = %failure,
                "lost the worker lifeline, aborting its jobs and registering again"
            );
            return None;
        }
    }

    async fn beat(&mut self, shared: &Shared) -> Result<Option<Vec<Owned>>> {
        let schema = &shared.config.schema;
        let rows: Vec<OwnedRow> = sqlx::query_as(AssertSqlSafe(format!(
            "WITH beat AS (
                 UPDATE {worker} SET heartbeat_at = statement_timestamp()
                 WHERE id = $1::uuid
                 RETURNING id
             )
             SELECT job.id, job.lock_token, control.command
             FROM beat
             LEFT JOIN {job} AS job ON job.locked_by = beat.id AND job.state = 'running'
             LEFT JOIN {job_control} AS control ON control.job_id = job.id",
            worker = schema.table("worker"),
            job = schema.table("job"),
            job_control = schema.table("job_control"),
        )))
        .bind(&*self.incarnation.worker_id)
        .fetch_all(&mut self.connection)
        .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            rows.into_iter()
                .filter_map(|row| {
                    Some(Owned {
                        job_id: row.id?,
                        token: row.lock_token?,
                        command: row.command.as_deref().and_then(Command::parse),
                    })
                })
                .collect(),
        ))
    }

    async fn retire(mut self, shared: &Shared) -> Result<()> {
        unregister(&mut self.connection, shared, &self.incarnation.worker_id).await?;
        self.connection.close().await?;
        Ok(())
    }
}

async fn release(shared: Arc<Shared>, orphans: Vec<Hold>) {
    shared.flush_logged().await;
    if let Err(e) = interrupt(
        &shared.config.pool.pool(),
        &shared.config.schema,
        Target::Orphans(&orphans),
    )
    .await
    {
        warn!(error = %e, "failed to release jobs this worker no longer runs");
    }
}

async fn unregister(connection: &mut PgConnection, shared: &Shared, worker_id: &str) -> Result<()> {
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {worker} AS worker
         WHERE worker.id = $1::uuid
             AND NOT EXISTS (
                 SELECT 1 FROM {job} AS job
                 WHERE job.locked_by = worker.id AND job.state = 'running'
             )",
        worker = shared.config.schema.table("worker"),
        job = shared.config.schema.table("job"),
    )))
    .bind(worker_id)
    .execute(connection)
    .await?;
    Ok(())
}
