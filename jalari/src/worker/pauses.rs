use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use sqlx::{AssertSqlSafe, PgPool};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::listener::wake;
use super::runner::{Shared, Wakers, wait};
use crate::{Result, Schema};

#[derive(Default)]
pub(super) struct Pauses(RwLock<HashSet<String>>);

impl Pauses {
    pub(super) fn contains(&self, queue: &str) -> bool {
        self.0.read().unwrap().contains(queue)
    }

    pub(super) async fn refresh(
        &self,
        pool: &PgPool,
        schema: &Schema,
        queues: &[String],
    ) -> Result<Vec<String>> {
        let paused: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT queue FROM {queue_pause} WHERE queue = ANY($1)",
            queue_pause = schema.table("queue_pause"),
        )))
        .bind(queues)
        .fetch_all(pool)
        .await?;
        let paused: HashSet<String> = paused.into_iter().collect();
        let mut current = self.0.write().unwrap();
        let resumed = current.difference(&paused).cloned().collect();
        *current = paused;
        Ok(resumed)
    }
}

pub(super) async fn watch_pauses(
    shared: Arc<Shared>,
    pauses: Arc<Pauses>,
    wakers: Arc<Wakers>,
    shutdown: CancellationToken,
) {
    let queues: Vec<String> = wakers.queues.keys().cloned().collect();
    while !shutdown.is_cancelled() {
        wait(&wakers.pauses, &shutdown, shared.settings.poll_interval).await;
        if shutdown.is_cancelled() {
            return;
        }
        match pauses
            .refresh(&shared.config.pool.pool(), &shared.config.schema, &queues)
            .await
        {
            Ok(resumed) => {
                for queue in resumed {
                    wake(&wakers.queues[&queue]);
                }
            }
            Err(e) => warn!(error = %e, "failed to read the paused queues"),
        }
    }
}
