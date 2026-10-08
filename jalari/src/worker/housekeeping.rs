use std::sync::Arc;
use std::time::Duration;

use sqlx::AssertSqlSafe;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::runner::{ERROR_BACKOFF, Shared, wait};
use crate::Result;

const BATCH_SIZE: i64 = 1000;
const MIN_DELAY: Duration = Duration::from_secs(1);
const DISABLED_RECHECK: Duration = Duration::from_secs(10 * 60);

#[derive(sqlx::FromRow)]
struct Schedule {
    housekeeping_enabled: bool,
    wait_ms: i64,
}

pub(super) async fn housekeep(
    shared: Arc<Shared>,
    waker: Arc<Notify>,
    shutdown: CancellationToken,
) {
    while !shutdown.is_cancelled() {
        let delay = match shared.sweep(&shutdown).await {
            Ok(delay) => delay,
            Err(e) => {
                warn!(error = %e, "housekeeping failed");
                ERROR_BACKOFF
            }
        };
        wait(&waker, &shutdown, delay).await;
    }
}

impl Shared {
    async fn sweep(&self, shutdown: &CancellationToken) -> Result<Duration> {
        let schema = &self.config.schema;
        let pool = self.config.pool.pool();
        let config = schema.table("config");

        let claimed: Option<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE {config} SET last_housekeeping_at = now()
             WHERE housekeeping_enabled
                 AND (last_housekeeping_at IS NULL
                     OR last_housekeeping_at + housekeeping_interval <= now())
             RETURNING TRUE"
        )))
        .fetch_optional(&pool)
        .await?;

        if claimed.is_some() {
            let mut deleted: u64 = 0;
            while !shutdown.is_cancelled() {
                let removed = sqlx::query(AssertSqlSafe(format!(
                    "DELETE FROM {job} WHERE id IN (
                         SELECT job.id FROM {job} AS job CROSS JOIN {config} AS config
                         WHERE (job.state = 'succeeded'
                                 AND job.updated_at < now() - config.succeeded_retention)
                             OR (job.state = 'deleted'
                                 AND job.updated_at < now() - config.deleted_retention)
                             OR (job.state = 'failed'
                                 AND job.updated_at < now() - config.failed_retention)
                         LIMIT $1
                         FOR UPDATE OF job SKIP LOCKED
                     )",
                    job = schema.table("job"),
                )))
                .bind(BATCH_SIZE)
                .execute(&pool)
                .await?
                .rows_affected();
                deleted = deleted.saturating_add(removed);
                if removed < BATCH_SIZE.unsigned_abs() {
                    break;
                }
            }
            if deleted > 0 {
                info!(deleted, "housekeeping removed finished jobs");
            }
        }

        let schedule: Schedule = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT housekeeping_enabled,
                 (EXTRACT(EPOCH FROM coalesce(last_housekeeping_at, now())
                     + housekeeping_interval - now()) * 1000)::bigint AS wait_ms
             FROM {config}"
        )))
        .fetch_one(&pool)
        .await?;
        if !schedule.housekeeping_enabled {
            return Ok(DISABLED_RECHECK);
        }
        let wait = Duration::from_millis(u64::try_from(schedule.wait_ms).unwrap_or(0));
        Ok(wait.max(MIN_DELAY))
    }
}
