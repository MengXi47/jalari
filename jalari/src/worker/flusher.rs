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
        let progress: Vec<&Value> = dirty.iter().map(|(_, copy)| &copy.value).collect();
        let seqs: Vec<i64> = dirty.iter().map(|(_, copy)| copy.seq).collect();
        let job_control = self.config.schema.table("job_control");
        let skipped: sqlx::Result<Vec<i64>> = sqlx::query_scalar(AssertSqlSafe(format!(
            "WITH batch AS (
                 SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[], $4::bigint[])
                     AS batch (job_id, lease, progress, progress_seq)
             ),
             locked AS (
                 SELECT control.job_id FROM {job_control} AS control
                 JOIN batch ON batch.job_id = control.job_id AND batch.lease = control.lease
                 ORDER BY control.job_id
                 FOR NO KEY UPDATE OF control SKIP LOCKED
             ),
             written AS (
                 UPDATE {job_control} AS control
                 SET progress = CASE WHEN batch.progress_seq > control.progress_seq
                         THEN batch.progress ELSE control.progress END,
                     progress_seq = greatest(control.progress_seq, batch.progress_seq),
                     updated_at = statement_timestamp()
                 FROM batch
                 WHERE control.job_id = batch.job_id
                     AND control.job_id IN (SELECT job_id FROM locked)
             )
             SELECT batch.job_id FROM batch
             WHERE batch.job_id NOT IN (SELECT job_id FROM locked)
                 AND EXISTS (
                     SELECT 1 FROM {job_control} AS control
                     WHERE control.job_id = batch.job_id AND control.lease = batch.lease
                 )"
        )))
        .bind(&job_ids)
        .bind(&leases)
        .bind(&progress)
        .bind(&seqs)
        .fetch_all(&self.config.pool.pool())
        .await;
        let skipped = match skipped {
            Ok(skipped) => skipped,
            Err(e) => {
                for (control, _) in &dirty {
                    control.mark_dirty();
                }
                return Err(e.into());
            }
        };
        for (control, _) in &dirty {
            if skipped.contains(&control.job_id()) {
                control.mark_dirty();
            }
        }
        self.runs.prune();
        Ok(())
    }
}
