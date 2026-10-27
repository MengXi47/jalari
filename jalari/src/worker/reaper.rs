use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::AssertSqlSafe;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::lifeline::Current;
use super::ownership::{Target, interrupt, reaped_full_batch};
use super::runner::Shared;
use crate::Result;
use crate::admin::{StrandedJobs, stranded_in};
use crate::storage::interval;

const REAP_INTERVAL: Duration = Duration::from_secs(1);
const ORPHAN_SWEEP: Duration = Duration::from_secs(60);
const STRANDED_CHECK: Duration = Duration::from_secs(300);
const ROSTER_FLOOR: Duration = Duration::from_secs(12);

pub(super) async fn reap(
    shared: Arc<Shared>,
    queues: Vec<String>,
    current: Current,
    shutdown: CancellationToken,
) {
    let mut swept_at = Instant::now();
    let mut checked_at: Option<Instant> = None;
    let mut forgotten = false;
    while !shutdown.is_cancelled() {
        let sweep_orphans = swept_at.elapsed() >= ORPHAN_SWEEP;
        if sweep_orphans {
            swept_at = Instant::now();
        }
        if let Err(e) = shared.reap_until_clean(sweep_orphans).await {
            warn!(error = %e, "failed to take over the jobs of dead workers");
        }
        if !forgotten || sweep_orphans {
            match shared.forget_departed().await {
                Ok(_) => forgotten = true,
                Err(e) => {
                    warn!(error = %e, "failed to remove departed workers from the worker list")
                }
            }
        }
        if current.borrow().is_some() && checked_at.is_none_or(|at| at.elapsed() >= STRANDED_CHECK)
        {
            checked_at = Some(Instant::now());
            shared.warn_stranded(&queues).await;
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(REAP_INTERVAL) => {}
        }
    }
}

impl Shared {
    pub(super) async fn reap_until_clean(&self, sweep_orphans: bool) -> Result<usize> {
        let pool = self.config.pool.pool();
        let mut reaped = 0;
        loop {
            let interrupted = interrupt(
                &pool,
                &self.config.schema,
                Target::DeadOwners { sweep_orphans },
            )
            .await?;
            reaped += interrupted.len();
            if !reaped_full_batch(&interrupted) {
                return Ok(reaped);
            }
        }
    }

    async fn forget_departed(&self) -> Result<u64> {
        let schema = &self.config.schema;
        let removed = sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {worker} AS worker
             WHERE worker.heartbeat_at < now() - greatest(
                     (SELECT worker_timeout FROM {config}), $1::interval)
                 AND NOT EXISTS (
                     SELECT 1 FROM {job} AS job
                     WHERE job.locked_by = worker.id AND job.state = 'running'
                 )",
            worker = schema.table("worker"),
            config = schema.table("config"),
            job = schema.table("job"),
        )))
        .bind(interval(ROSTER_FLOOR))
        .execute(&self.config.pool.pool())
        .await?;
        Ok(removed.rows_affected())
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
                "no live worker runs these jobs, enqueued by another build of the task; start a \
                 worker of that build, or release them with jalari::admin::clear_fingerprint"
            } else {
                "no live worker knows this task; start a worker that links it"
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
}
