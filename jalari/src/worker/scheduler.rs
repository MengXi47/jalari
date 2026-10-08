use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::AssertSqlSafe;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::runner::{ERROR_BACKOFF, Shared, wait};
use crate::enqueue::{EnqueueRequest, timeout_ms};
use crate::recurring::store::{lock_schedule, next_run};
use crate::{Cron, Error, ErrorKind, Result};

#[derive(sqlx::FromRow)]
struct DueRecurring {
    name: String,
    cron: String,
    timezone: String,
    task: String,
    payload: Vec<u8>,
    queue: String,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    scheduled_at: DateTime<Utc>,
    now: DateTime<Utc>,
}

pub(super) async fn schedule(shared: Arc<Shared>, waker: Arc<Notify>, shutdown: CancellationToken) {
    while !shutdown.is_cancelled() {
        match shared.sync_declared().await {
            Ok(()) => break,
            Err(e) => {
                warn!(error = %e, "failed to sync declared recurring jobs");
                wait(&waker, &shutdown, ERROR_BACKOFF).await;
            }
        }
    }

    while !shutdown.is_cancelled() {
        let delay = match shared.fire_due().await {
            Ok(delay) => delay,
            Err(e) => {
                warn!(error = %e, "failed to fire recurring jobs");
                ERROR_BACKOFF
            }
        };
        wait(&waker, &shutdown, delay).await;
    }
}

impl Shared {
    async fn sync_declared(&self) -> Result<()> {
        let schema = &self.config.schema;
        let mut transaction = self.config.pool.pool().begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(schema.lock_key("recurring_sync"))
            .execute(&mut *transaction)
            .await?;

        let mut declared_names = Vec::with_capacity(self.crons.len());
        for declared in &self.crons {
            let registration = declared.registration;
            let payload = (declared.declaration.payload)().map_err(|err| {
                Error::new(
                    ErrorKind::PayloadEncodeFailed,
                    format!("{}: {err}", registration.name),
                )
            })?;

            let existing = lock_schedule(&mut transaction, schema, registration.name).await?;
            let unchanged = existing.as_ref().filter(|schedule| {
                schedule.managed
                    && schedule.enabled
                    && schedule.cron == declared.cron.expression()
                    && schedule.timezone == declared.cron.timezone_name()
            });
            let next_run_at = match unchanged {
                Some(schedule) => schedule.next_run_at,
                None => {
                    let last_run_at = existing.and_then(|schedule| schedule.last_run_at);
                    next_run(&mut transaction, &declared.cron, last_run_at).await?
                }
            };

            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {recurring} (name, cron, timezone, task, payload, queue,
                     max_attempts, timeout_ms, managed, next_run_at)
                 VALUES ($1, $2, $3, $1, $4, $5, $6, $7, TRUE, $8)
                 ON CONFLICT (name) DO UPDATE SET
                     task = EXCLUDED.task, payload = EXCLUDED.payload, queue = EXCLUDED.queue,
                     max_attempts = EXCLUDED.max_attempts, timeout_ms = EXCLUDED.timeout_ms,
                     cron = EXCLUDED.cron, timezone = EXCLUDED.timezone,
                     next_run_at = EXCLUDED.next_run_at,
                     enabled = TRUE,
                     managed = TRUE,
                     updated_at = now()",
                recurring = schema.table("recurring"),
            )))
            .bind(registration.name)
            .bind(declared.cron.expression())
            .bind(declared.cron.timezone_name())
            .bind(payload)
            .bind(registration.queue)
            .bind(registration.max_attempts)
            .bind(timeout_ms(registration.name, registration.timeout)?)
            .bind(next_run_at)
            .execute(&mut *transaction)
            .await?;
            declared_names.push(registration.name.to_owned());
        }

        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {recurring}
             WHERE managed AND task = ANY($1) AND NOT (name = ANY($2))",
            recurring = schema.table("recurring"),
        )))
        .bind(&self.task_names)
        .bind(&declared_names)
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        Ok(())
    }

    async fn fire_due(&self) -> Result<Duration> {
        let schema = &self.config.schema;
        let pool = self.config.pool.pool();
        loop {
            let mut transaction = pool.begin().await?;
            let due: Option<DueRecurring> = sqlx::query_as(AssertSqlSafe(format!(
                "SELECT name, cron, timezone, task, payload, queue, max_attempts, timeout_ms,
                     next_run_at AS scheduled_at, now() AS now
                 FROM {recurring}
                 WHERE enabled AND next_run_at <= now() AND task = ANY($1)
                 ORDER BY next_run_at
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED",
                recurring = schema.table("recurring"),
            )))
            .bind(&self.task_names)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(due) = due else {
                transaction.rollback().await?;
                break;
            };

            let after = due.now.max(due.scheduled_at);
            match Cron::parse(&due.cron, &due.timezone).and_then(|cron| cron.next_after(after)) {
                Ok(next_run_at) => {
                    let outcome = EnqueueRequest::recurring(
                        &due.name,
                        due.task,
                        due.payload,
                        due.queue,
                        due.max_attempts,
                        due.timeout_ms,
                    )
                    .insert(&mut transaction, schema)
                    .await?;
                    sqlx::query(AssertSqlSafe(format!(
                        "UPDATE {recurring} SET next_run_at = $2, last_run_at = $3
                         WHERE name = $1",
                        recurring = schema.table("recurring"),
                    )))
                    .bind(&due.name)
                    .bind(next_run_at)
                    .bind(due.scheduled_at)
                    .execute(&mut *transaction)
                    .await?;
                    debug!(recurring = %due.name, ?outcome, %next_run_at, "fired recurring job");
                }
                Err(e) => {
                    sqlx::query(AssertSqlSafe(format!(
                        "UPDATE {recurring} SET enabled = FALSE, updated_at = now()
                         WHERE name = $1",
                        recurring = schema.table("recurring"),
                    )))
                    .bind(&due.name)
                    .execute(&mut *transaction)
                    .await?;
                    warn!(
                        recurring = %due.name,
                        error = %e,
                        "disabled recurring job with an invalid schedule"
                    );
                }
            }
            transaction.commit().await?;
        }

        let seconds: Option<f64> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT EXTRACT(EPOCH FROM min(next_run_at) - now())::float8
             FROM {recurring}
             WHERE enabled AND task = ANY($1)",
            recurring = schema.table("recurring"),
        )))
        .bind(&self.task_names)
        .fetch_one(&pool)
        .await?;
        let poll_interval = self.settings.poll_interval;
        Ok(seconds.map_or(poll_interval, |seconds| {
            Duration::try_from_secs_f64(seconds.max(0.0))
                .unwrap_or(poll_interval)
                .min(poll_interval)
        }))
    }
}
