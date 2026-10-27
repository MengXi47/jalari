use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sqlx::AssertSqlSafe;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::runner::Shared;
use crate::Result;

const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

pub(super) async fn flush_progress(shared: Arc<Shared>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = shared.flush_now.notified() => {}
            () = tokio::time::sleep(FLUSH_INTERVAL) => {}
        }
        shared.flush_logged().await;
    }
    shared.flush_logged().await;
}

impl Shared {
    pub(super) async fn flush_logged(&self) {
        if let Err(e) = self.flush().await {
            warn!(error = %e, "failed to save job progress");
        }
    }

    async fn flush(&self) -> Result<()> {
        let dirty = self.runs.dirty();
        if dirty.is_empty() {
            self.runs.prune();
            return Ok(());
        }
        let job_ids: Vec<i64> = dirty.iter().map(|(control, _)| control.job_id()).collect();
        let leases: Vec<i64> = dirty.iter().map(|(control, _)| control.lease()).collect();
        let progress: Vec<Value> = dirty.iter().map(|(_, progress)| progress.clone()).collect();
        let job_control = self.config.schema.table("job_control");
        let flushed = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {job_control} AS control (job_id, lease, progress)
             SELECT batch.job_id, batch.lease, batch.progress
             FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[])
                 AS batch (job_id, lease, progress)
             ORDER BY batch.job_id
             ON CONFLICT (job_id) DO UPDATE
             SET progress = EXCLUDED.progress, updated_at = statement_timestamp()
             WHERE control.lease = EXCLUDED.lease"
        )))
        .bind(&job_ids)
        .bind(&leases)
        .bind(&progress)
        .execute(&self.config.pool.pool())
        .await;
        if flushed.is_err() {
            for (control, _) in &dirty {
                control.mark_dirty();
            }
        }
        flushed?;
        self.runs.prune();
        Ok(())
    }
}
