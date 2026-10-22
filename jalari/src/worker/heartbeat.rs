use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sqlx::AssertSqlSafe;
use sqlx::types::Json;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::runner::{QueueConfig, Shared};
use crate::Result;
use crate::admin::{StrandedJobs, stranded_in};

const STRANDED_CHECK: Duration = Duration::from_secs(300);

pub(super) struct WorkerRegistration {
    hostname: String,
    pid: i32,
    queues: Value,
    queue_names: Vec<String>,
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

pub(super) async fn heartbeat(
    shared: Arc<Shared>,
    registration: WorkerRegistration,
    shutdown: CancellationToken,
) {
    let mut worker_id: Option<String> = None;
    let mut checked_at: Option<Instant> = None;
    loop {
        match shared.beat(&registration, worker_id.as_deref()).await {
            Ok(id) => worker_id = Some(id),
            Err(e) => warn!(error = %e, "failed to record the worker heartbeat"),
        }
        if worker_id.is_some() && checked_at.is_none_or(|at| at.elapsed() >= STRANDED_CHECK) {
            checked_at = Some(Instant::now());
            shared.warn_stranded(&registration.queue_names).await;
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(shared.settings.heartbeat_interval) => {}
        }
    }
    if let Some(id) = worker_id
        && let Err(e) = shared.unregister(&id).await
    {
        warn!(error = %e, "failed to remove the worker from the worker list");
    }
}

impl Shared {
    async fn beat(
        &self,
        registration: &WorkerRegistration,
        worker_id: Option<&str>,
    ) -> Result<String> {
        let worker = self.config.schema.table("worker");
        let pool = self.config.pool.pool();

        let refreshed: Option<String> = match worker_id {
            Some(id) => sqlx::query_scalar(AssertSqlSafe(format!(
                "UPDATE {worker} SET heartbeat_at = now() WHERE id = $1::uuid RETURNING id::text"
            )))
            .bind(id)
            .fetch_optional(&pool)
            .await?,
            None => None,
        };
        let id = match refreshed {
            Some(id) => id,
            None => {
                sqlx::query_scalar(AssertSqlSafe(format!(
                    "INSERT INTO {worker} (hostname, pid, queues, scheduler, tasks)
                     VALUES ($1, $2, $3, $4, $5)
                     RETURNING id::text"
                )))
                .bind(&registration.hostname)
                .bind(registration.pid)
                .bind(Json(&registration.queues))
                .bind(registration.scheduler)
                .bind(Json(&self.served))
                .fetch_one(&pool)
                .await?
            }
        };

        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {worker}
             WHERE heartbeat_at < now() - (SELECT worker_timeout FROM {config})",
            config = self.config.schema.table("config"),
        )))
        .execute(&pool)
        .await?;
        Ok(id)
    }

    async fn warn_stranded(&self, queues: &[String]) {
        let groups = match self.stranded(queues).await {
            Ok(groups) => groups,
            Err(e) => {
                warn!(error = %e, "failed to check for jobs no worker can run");
                return;
            }
        };
        for group in groups {
            let message = if self.jobs.contains_key(group.task.as_str()) {
                "no live worker runs these jobs, enqueued by another build of the task"
            } else {
                "no live worker knows this task"
            };
            warn!(
                queue = %group.queue,
                task = %group.task,
                fingerprint = group.fingerprint.as_deref().unwrap_or("none"),
                count = group.count,
                since = %group.since,
                "{message}"
            );
        }
    }

    async fn stranded(&self, queues: &[String]) -> Result<Vec<StrandedJobs>> {
        let mut connection = self.config.pool.pool().acquire().await?;
        stranded_in(&mut connection, &self.config.schema, Some(queues)).await
    }

    async fn unregister(&self, worker_id: &str) -> Result<()> {
        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {worker} WHERE id = $1::uuid",
            worker = self.config.schema.table("worker"),
        )))
        .bind(worker_id)
        .execute(&self.config.pool.pool())
        .await?;
        Ok(())
    }
}
