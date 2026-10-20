use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::AssertSqlSafe;

use super::store::{
    Plan, SCHEDULE_COLUMNS, StoredSchedule, lock_schedule, managed_by_code, not_found,
    notify_change, plan,
};
use crate::enqueue::{EnqueueRequest, default_queue, encode_payload, timeout_ms};
use crate::init::config;
use crate::{EnqueueOutcome, Job, RecurringInfo, Result, Schedule};

#[derive(sqlx::FromRow)]
struct TriggerRow {
    task: String,
    payload: Vec<u8>,
    queue: String,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    context: Option<Value>,
}

#[derive(sqlx::FromRow)]
struct InfoRow {
    name: String,
    #[sqlx(flatten)]
    stored: StoredSchedule,
    task: String,
    queue: String,
    enabled: bool,
    managed: bool,
    next_run_at: Option<DateTime<Utc>>,
    last_run_at: Option<DateTime<Utc>>,
    run_count: i32,
    finished_at: Option<DateTime<Utc>>,
    context: Option<Value>,
}

/// Creates a runtime schedule that enqueues `job` on every run of `schedule`, or updates the
/// one named `name`.
///
/// `schedule` is a [`Cron`](crate::Cron), an [`Interval`](crate::Interval) or a [`Schedule`]
/// with end conditions. The job goes to `T`'s default queue. Every run carries the context of
/// the surrounding [`scope`](crate::scope), if any. Updating keeps a paused schedule paused and
/// never repeats a run that already fired. An unchanged schedule keeps its run count, its next
/// run and its finished state, so calling this at every start of your application is safe. A
/// schedule whose cron or interval changed starts counting again from zero; one where only
/// `until` or `max_runs` changed keeps its count and is finished or revived accordingly. Runs
/// are enqueued by workers with the scheduler enabled; they all wake up as soon as this
/// commits.
///
/// # Errors
///
/// Returns an error if:
/// - `name` belongs to a schedule declared with `#[jalari::job(cron = ...)]`
///   ([`RecurringManagedByCode`](crate::ErrorKind::RecurringManagedByCode))
/// - `job` cannot be serialized ([`PayloadEncodeFailed`](crate::ErrorKind::PayloadEncodeFailed))
/// - The schedule has no run after the last one, such as a cron expression for February 30th
///   ([`InvalidCronExpression`](crate::ErrorKind::InvalidCronExpression) or
///   [`InvalidInterval`](crate::ErrorKind::InvalidInterval))
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
///
/// let anchor = chrono::NaiveDate::from_ymd_opt(2026, 10, 3)
///     .unwrap()
///     .and_hms_opt(9, 0, 0)
///     .unwrap();
/// let every_five_days = jalari::Interval::new(5, jalari::IntervalUnit::Days, anchor)?
///     .timezone("Asia/Taipei")?;
/// let ten_times = jalari::Schedule::from(every_five_days).max_runs(10)?;
/// jalari::recurring::add_or_update("digest:sales:trial", ten_times, &digest).await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_or_update<T: Job>(
    name: &str,
    schedule: impl Into<Schedule>,
    job: &T,
) -> Result<()> {
    let schedule = schedule.into();
    let config = config()?;
    let schema = &config.schema;
    let payload = encode_payload(job)?;
    let timeout_ms = timeout_ms(T::NAME, T::TIMEOUT)?;
    let context = crate::job::context::current().into_value(T::NAME)?;

    let mut transaction = config.pool.pool().begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("{}:{name}", schema.lock_key("recurring")))
        .execute(&mut *transaction)
        .await?;
    let existing = lock_schedule(&mut transaction, schema, name).await?;
    if existing.as_ref().is_some_and(|schedule| schedule.managed) {
        return Err(managed_by_code(name));
    }
    let plan = plan(&mut transaction, existing.as_ref(), &schedule).await?;
    let stored = schedule.stored();
    let saved: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {recurring} AS existing (name, {SCHEDULE_COLUMNS}, task, payload, queue,
             max_attempts, timeout_ms, managed, next_run_at, run_count, finished_at, context)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, FALSE, $14, $15, $16,
             $17)
         ON CONFLICT (name) DO UPDATE SET
             cron = EXCLUDED.cron, timezone = EXCLUDED.timezone,
             interval_count = EXCLUDED.interval_count, interval_unit = EXCLUDED.interval_unit,
             anchor = EXCLUDED.anchor, until_at = EXCLUDED.until_at,
             max_runs = EXCLUDED.max_runs, task = EXCLUDED.task, payload = EXCLUDED.payload,
             queue = EXCLUDED.queue, max_attempts = EXCLUDED.max_attempts,
             timeout_ms = EXCLUDED.timeout_ms, next_run_at = EXCLUDED.next_run_at,
             run_count = EXCLUDED.run_count, finished_at = EXCLUDED.finished_at,
             context = EXCLUDED.context, updated_at = now()
         WHERE NOT existing.managed
         RETURNING name",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .bind(&stored.cron)
    .bind(&stored.timezone)
    .bind(stored.interval_count)
    .bind(&stored.interval_unit)
    .bind(stored.anchor)
    .bind(stored.until_at)
    .bind(stored.max_runs)
    .bind(T::NAME)
    .bind(payload)
    .bind(default_queue(T::NAME))
    .bind(T::MAX_ATTEMPTS)
    .bind(timeout_ms)
    .bind(plan.next_run_at)
    .bind(plan.run_count)
    .bind(plan.finished_at)
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

/// Changes only when a runtime schedule runs, keeping its job and payload.
///
/// The next run is recomputed from the new schedule and is always later than the last run. A
/// new cron or interval starts counting its runs again from zero; new end conditions alone
/// keep the count and finish or revive the schedule accordingly; an unchanged schedule is
/// left as it is.
///
/// # Errors
///
/// Returns an error if:
/// - No schedule is named `name` ([`RecurringNotFound`](crate::ErrorKind::RecurringNotFound))
/// - The schedule is declared in code
///   ([`RecurringManagedByCode`](crate::ErrorKind::RecurringManagedByCode))
/// - The schedule has no run after the last one
///   ([`InvalidCronExpression`](crate::ErrorKind::InvalidCronExpression) or
///   [`InvalidInterval`](crate::ErrorKind::InvalidInterval))
/// - [`init`](crate::init) has not completed or the database query fails
pub async fn update_schedule(name: &str, schedule: impl Into<Schedule>) -> Result<()> {
    let schedule = schedule.into();
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let existing = lock_schedule(&mut transaction, schema, name)
        .await?
        .ok_or_else(|| not_found(name))?;
    if existing.managed {
        return Err(managed_by_code(name));
    }
    let plan = plan(&mut transaction, Some(&existing), &schedule).await?;
    let stored = schedule.stored();
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {recurring}
         SET cron = $2, timezone = $3, interval_count = $4, interval_unit = $5, anchor = $6,
             until_at = $7, max_runs = $8, next_run_at = $9, run_count = $10,
             finished_at = $11, updated_at = now()
         WHERE name = $1",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .bind(&stored.cron)
    .bind(&stored.timezone)
    .bind(stored.interval_count)
    .bind(&stored.interval_unit)
    .bind(stored.anchor)
    .bind(stored.until_at)
    .bind(stored.max_runs)
    .bind(plan.next_run_at)
    .bind(plan.run_count)
    .bind(plan.finished_at)
    .execute(&mut *transaction)
    .await?;
    notify_change(&mut transaction, schema, name).await?;
    transaction.commit().await?;
    Ok(())
}

/// Enqueues the schedule's job now, outside its timetable.
///
/// Works for runtime and code-declared schedules, and for paused or finished ones. It does not
/// move the next scheduled run and does not count towards the schedule's run limit. The job
/// shares the schedule's `job_key`, so while an earlier run is still waiting or running the
/// existing job is kept and returned as [`EnqueueOutcome::Kept`].
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
/// Runs missed while paused are skipped; the next run is the first one after now. A schedule
/// that reaches its end while paused, or already finished, stays finished.
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
    let rows: Vec<InfoRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT name, {SCHEDULE_COLUMNS}, task, queue, enabled, managed, next_run_at,
             last_run_at, run_count, finished_at, context
         FROM {recurring}
         ORDER BY name",
        recurring = config.schema.table("recurring"),
    )))
    .fetch_all(&config.pool.pool())
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| RecurringInfo {
            name: row.name,
            schedule: row.stored.parse().ok(),
            task: row.task,
            queue: row.queue,
            enabled: row.enabled,
            managed: row.managed,
            next_run_at: row.next_run_at,
            last_run_at: row.last_run_at,
            run_count: row.run_count,
            finished_at: row.finished_at,
            context: row.context,
        })
        .collect())
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
    let plan = if enabled && !existing.enabled && existing.finished_at.is_none() {
        let schedule = existing.stored.parse()?;
        Plan::fresh(
            &mut transaction,
            &schedule,
            existing.last_run_at,
            existing.run_count,
        )
        .await?
    } else {
        Plan::kept(&existing)
    };

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {recurring}
         SET enabled = $2, next_run_at = $3, finished_at = $4, updated_at = now()
         WHERE name = $1",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .bind(enabled)
    .bind(plan.next_run_at)
    .bind(plan.finished_at)
    .execute(&mut *transaction)
    .await?;
    notify_change(&mut transaction, schema, name).await?;
    transaction.commit().await?;
    Ok(())
}
