use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgConnection};

use crate::storage::RECURRING_CHANNEL;
use crate::{Cron, Error, ErrorKind, Result, Schema};

#[derive(sqlx::FromRow)]
pub(crate) struct LockedSchedule {
    pub(crate) cron: String,
    pub(crate) timezone: String,
    pub(crate) enabled: bool,
    pub(crate) managed: bool,
    pub(crate) next_run_at: DateTime<Utc>,
    pub(crate) last_run_at: Option<DateTime<Utc>>,
}

pub(crate) async fn database_now(connection: &mut PgConnection) -> Result<DateTime<Utc>> {
    let now = sqlx::query_scalar("SELECT now()")
        .fetch_one(connection)
        .await?;
    Ok(now)
}

pub(crate) async fn lock_schedule(
    connection: &mut PgConnection,
    schema: &Schema,
    name: &str,
) -> Result<Option<LockedSchedule>> {
    let schedule = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT cron, timezone, enabled, managed, next_run_at, last_run_at
         FROM {recurring}
         WHERE name = $1
         FOR UPDATE",
        recurring = schema.table("recurring"),
    )))
    .bind(name)
    .fetch_optional(connection)
    .await?;
    Ok(schedule)
}

pub(crate) async fn next_run(
    connection: &mut PgConnection,
    cron: &Cron,
    last_run_at: Option<DateTime<Utc>>,
) -> Result<DateTime<Utc>> {
    let now = database_now(connection).await?;
    let after = last_run_at.map_or(now, |last_run_at| last_run_at.max(now));
    cron.next_after(after)
}

pub(crate) async fn notify_change(
    connection: &mut PgConnection,
    schema: &Schema,
    name: &str,
) -> Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(schema.notify_channel(RECURRING_CHANNEL))
        .bind(name)
        .execute(connection)
        .await?;
    Ok(())
}

pub(crate) fn not_found(name: &str) -> Error {
    Error::new(
        ErrorKind::RecurringNotFound,
        format!("no recurring job is named {name:?}"),
    )
}

pub(crate) fn managed_by_code(name: &str) -> Error {
    Error::new(
        ErrorKind::RecurringManagedByCode,
        format!(
            "recurring job {name:?} is declared with #[jalari::job(cron = ...)] and cannot be \
             changed at runtime; add a separately named schedule with recurring::add_or_update"
        ),
    )
}
