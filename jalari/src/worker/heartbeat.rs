use std::sync::Arc;

use serde_json::{Value, json};
use sqlx::AssertSqlSafe;
use sqlx::types::Json;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::runner::{QueueConfig, Shared};
use crate::Result;

pub(super) struct WorkerRegistration {
    hostname: String,
    pid: i32,
    queues: Value,
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
    loop {
        match shared.beat(&registration, worker_id.as_deref()).await {
            Ok(id) => worker_id = Some(id),
            Err(e) => warn!(error = %e, "failed to record the worker heartbeat"),
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
                    "INSERT INTO {worker} (hostname, pid, queues, scheduler)
                     VALUES ($1, $2, $3, $4)
                     RETURNING id::text"
                )))
                .bind(&registration.hostname)
                .bind(registration.pid)
                .bind(Json(&registration.queues))
                .bind(registration.scheduler)
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
