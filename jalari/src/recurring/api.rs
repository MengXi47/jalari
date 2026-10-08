use serde_json::Value;
use sqlx::AssertSqlSafe;

use super::store::{lock_schedule, managed_by_code, next_run, not_found, notify_change};
use crate::enqueue::{EnqueueRequest, default_queue, encode_payload, timeout_ms};
use crate::init::config;
use crate::{Cron, EnqueueOutcome, Job, RecurringInfo, Result};

#[derive(sqlx::FromRow)]
struct TriggerRow {
    task: String,
    payload: Vec<u8>,
    queue: String,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    context: Option<Value>,
}

/// Creates a runtime schedule that enqueues `job` on every `cron` run, or updates the one named
/// `name`.
///
/// The job goes to `T`'s default queue. Every run carries the context of the surrounding
/// [`scope`](crate::scope), if any. Updating keeps a paused schedule paused and never repeats a
/// run that already fired. Runs are enqueued by workers with the scheduler enabled;
/// they all wake up as soon as this commits.
///
/// # Errors
///
/// Returns an error if:
/// - `name` belongs to a schedule declared with `#[jalari::job(cron = ...)]`
///   ([`RecurringManagedByCode`](crate::ErrorKind::RecurringManagedByCode))
/// - `job` cannot be serialized ([`PayloadEncodeFailed`](crate::ErrorKind::PayloadEncodeFailed))
/// - [`init`](crate::init) has not completed or the database query fails
///
/// # Examples
///
/// ```rust,no_run
/// # use jalari::{Job, JobResult};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct Digest { team: String }
/// # #[jalari::job]
/// # impl Job for Digest {
/// #     const NAME: &'static str = "digest";
/// #     async fn run(self) -> JobResult { Ok(()) }
/// # }
/// # async fn example() -> jalari::Result<()> {
/// let cron = jalari::Cron::new("0 0 9 * * MON-FRI")?.timezone("Asia/Taipei")?;
/// let digest = Digest { team: "sales".to_owned() };
/// jalari::recurring::add_or_update("digest:sales", cron, &digest).await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_or_update<T: Job>(name: &str, cron: Cron, job: &T) -> Result<()> {
    let config = config()?;
    let schema = &config.schema;
    let payload = encode_payload(job)?;
    let timeout_ms = timeout_ms(T::NAME, T::TIMEOUT)?;
    let context = crate::job::context::current().into_value(T::NAME)?;

    let mut transaction = config.pool.pool().begin().await?;
    let existing = lock_schedule(&mut transaction, schema, name).await?;
    if existing.as_ref().is_some_and(|schedule| schedule.managed) {
        return Err(managed_by_code(name));
    }
    let last_run_at = existing.and_then(|schedule| schedule.last_run_at);
    let next_run_at = next_run(&mut transaction, &cron, last_run_at).await?;
    let saved: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {recurring} AS existing (name, cron, timezone, task, payload, queue,
             max_attempts, timeout_ms, managed, next_run_at, context)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, $9, $10)
         ON CONFLICT (name) DO UPDATE SET
             cron = EXCLUDED.cron, timezone = EXCLUDED.timezone, task = EXCLUDED.task,
             payload = EXCLUDED.payload, queue = EXCLUDED.queue,
             max_attempts = EXCLUDED.max_attempts, timeout_ms = EXCLUDED.timeout_ms,
             next_run_at = EXCLUDED.next_run_at, context = EXCLUDED.context,
             updated_at = now()
         WHERE NOT existing.managed
         RETURNING name",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .bind(cron.expression())
    .bind(cron.timezone_name())
    .bind(T::NAME)
    .bind(payload)
    .bind(default_queue(T::NAME))
    .bind(T::MAX_ATTEMPTS)
    .bind(timeout_ms)
    .bind(next_run_at)
    .bind(&context)
    .fetch_optional(&mut *transaction)
    .await?;
    if saved.is_none() {
        return Err(managed_by_code(name));
    }
    notify_change(&mut transaction, schema, name).await?;
    transaction.commit().await?;
    Ok(())
}

/// Changes only the cron expression and time zone of a runtime schedule.
///
/// The next run is recomputed from the new expression and is always later than the last run.
///
/// # Errors
///
/// Returns an error if:
/// - No schedule is named `name` ([`RecurringNotFound`](crate::ErrorKind::RecurringNotFound))
/// - The schedule is declared in code
///   ([`RecurringManagedByCode`](crate::ErrorKind::RecurringManagedByCode))
/// - [`init`](crate::init) has not completed or the database query fails
pub async fn update_schedule(name: &str, cron: Cron) -> Result<()> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let existing = lock_schedule(&mut transaction, schema, name)
        .await?
        .ok_or_else(|| not_found(name))?;
    if existing.managed {
        return Err(managed_by_code(name));
    }
    let next_run_at = next_run(&mut transaction, &cron, existing.last_run_at).await?;
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {recurring}
         SET cron = $2, timezone = $3, next_run_at = $4, updated_at = now()
         WHERE name = $1",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .bind(cron.expression())
    .bind(cron.timezone_name())
    .bind(next_run_at)
    .execute(&mut *transaction)
    .await?;
    notify_change(&mut transaction, schema, name).await?;
    transaction.commit().await?;
    Ok(())
}

/// Enqueues the schedule's job now, outside its timetable.
///
/// Works for runtime and code-declared schedules, and for paused ones. It does not move the
/// next scheduled run. The job shares the schedule's `job_key`, so while an earlier run is still
/// waiting or running the existing job is kept and returned as [`EnqueueOutcome::Kept`].
///
/// # Errors
///
/// Returns an error if:
/// - No schedule is named `name` ([`RecurringNotFound`](crate::ErrorKind::RecurringNotFound))
/// - [`init`](crate::init) has not completed or the database query fails
pub async fn trigger(name: &str) -> Result<EnqueueOutcome> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let row: Option<TriggerRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT task, payload, queue, max_attempts, timeout_ms, context FROM {recurring}
         WHERE name = $1",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .fetch_optional(&mut *transaction)
    .await?;
    let row = row.ok_or_else(|| not_found(name))?;
    let outcome = EnqueueRequest::recurring(
        name,
        row.task,
        row.payload,
        row.queue,
        row.max_attempts,
        row.timeout_ms,
        row.context,
    )
    .insert(&mut transaction, schema)
    .await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Stops a runtime schedule from firing until [`resume`] is called.
///
/// Jobs it already enqueued are not affected. Pausing a paused schedule does nothing.
///
/// # Errors
///
/// Returns an error if:
/// - No schedule is named `name` ([`RecurringNotFound`](crate::ErrorKind::RecurringNotFound))
/// - The schedule is declared in code
///   ([`RecurringManagedByCode`](crate::ErrorKind::RecurringManagedByCode))
/// - [`init`](crate::init) has not completed or the database query fails
pub async fn pause(name: &str) -> Result<()> {
    set_enabled(name, false).await
}

/// Lets a paused runtime schedule fire again.
///
/// Runs missed while paused are skipped; the next run is the first one after now.
///
/// # Errors
///
/// Returns an error under the same conditions as [`pause`].
pub async fn resume(name: &str) -> Result<()> {
    set_enabled(name, true).await
}

/// Deletes a runtime schedule.
///
/// Jobs it already enqueued still run.
///
/// # Errors
///
/// Returns an error under the same conditions as [`pause`].
pub async fn remove(name: &str) -> Result<()> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let existing = lock_schedule(&mut transaction, schema, name)
        .await?
        .ok_or_else(|| not_found(name))?;
    if existing.managed {
        return Err(managed_by_code(name));
    }
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {recurring} WHERE name = $1",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .execute(&mut *transaction)
    .await?;
    notify_change(&mut transaction, schema, name).await?;
    transaction.commit().await?;
    Ok(())
}

/// Lists every schedule, runtime and code-declared, ordered by name.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
pub async fn list() -> Result<Vec<RecurringInfo>> {
    let config = config()?;
    let rows = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT name, cron, timezone, task, queue, enabled, managed, next_run_at,
             last_run_at, context
         FROM {recurring}
         ORDER BY name",
        recurring = config.schema.table("recurring"),
    )))
    .fetch_all(&config.pool.pool())
    .await?;
    Ok(rows)
}

async fn set_enabled(name: &str, enabled: bool) -> Result<()> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let existing = lock_schedule(&mut transaction, schema, name)
        .await?
        .ok_or_else(|| not_found(name))?;
    if existing.managed {
        return Err(managed_by_code(name));
    }
    let next_run_at = if enabled {
        let cron = Cron::parse(&existing.cron, &existing.timezone)?;
        Some(next_run(&mut transaction, &cron, existing.last_run_at).await?)
    } else {
        None
    };

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {recurring}
         SET enabled = $2,
             next_run_at = CASE WHEN $2 AND NOT enabled THEN $3 ELSE next_run_at END,
             updated_at = now()
         WHERE name = $1",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .bind(enabled)
    .bind(next_run_at)
    .execute(&mut *transaction)
    .await?;
    notify_change(&mut transaction, schema, name).await?;
    transaction.commit().await?;
    Ok(())
}
