use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgConnection};

use super::schedule::IntervalUnit;
use crate::storage::RECURRING_CHANNEL;
use crate::{Cron, Error, ErrorKind, Interval, Result, Schedule, Schema};

pub(crate) const SCHEDULE_COLUMNS: &str =
    "cron, timezone, interval_count, interval_unit, anchor, until_at, max_runs";

pub(crate) const JOB_COLUMNS: &str =
    "task, payload, queue, max_attempts, timeout_ms, priority, context";

#[derive(sqlx::FromRow)]
pub(crate) struct StoredJob {
    pub(crate) task: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) queue: String,
    pub(crate) max_attempts: i32,
    pub(crate) timeout_ms: Option<i32>,
    pub(crate) priority: i16,
    pub(crate) context: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct StoredSchedule {
    pub(crate) cron: Option<String>,
    pub(crate) timezone: String,
    pub(crate) interval_count: Option<i32>,
    pub(crate) interval_unit: Option<String>,
    pub(crate) anchor: Option<NaiveDateTime>,
    pub(crate) until_at: Option<DateTime<Utc>>,
    pub(crate) max_runs: Option<i32>,
}

impl StoredSchedule {
    fn rule(
        &self,
    ) -> (
        &Option<String>,
        &str,
        Option<i32>,
        &Option<String>,
        Option<NaiveDateTime>,
    ) {
        (
            &self.cron,
            &self.timezone,
            self.interval_count,
            &self.interval_unit,
            self.anchor,
        )
    }

    pub(crate) fn parse(&self) -> Result<Schedule> {
        let mut schedule = match (
            &self.cron,
            self.interval_count,
            &self.interval_unit,
            self.anchor,
        ) {
            (Some(cron), None, None, None) => Schedule::from(Cron::parse(cron, &self.timezone)?),
            (None, Some(count), Some(unit), Some(anchor)) => {
                let unit = IntervalUnit::parse(unit).ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInterval,
                        format!("{unit:?} is not an interval unit"),
                    )
                })?;
                let count = u32::try_from(count).map_err(|_| {
                    Error::new(
                        ErrorKind::InvalidInterval,
                        format!("{count} is not a valid interval count"),
                    )
                })?;
                Schedule::from(Interval::new(count, unit, anchor)?.timezone(&self.timezone)?)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidSchedule,
                    "the stored schedule is neither a cron expression nor an interval".to_owned(),
                ));
            }
        };
        if let Some(until) = self.until_at {
            schedule = schedule.until(until);
        }
        if let Some(max_runs) = self.max_runs {
            schedule = schedule.max_runs(u32::try_from(max_runs).unwrap_or(0))?;
        }
        Ok(schedule)
    }
}

impl Schedule {
    pub(crate) fn stored(&self) -> StoredSchedule {
        let interval = self.interval();
        StoredSchedule {
            cron: self.cron().map(|cron| cron.expression().to_owned()),
            timezone: self.timezone_name().to_owned(),
            interval_count: interval
                .map(|interval| i32::try_from(interval.count()).unwrap_or(i32::MAX)),
            interval_unit: interval.map(|interval| interval.unit().as_str().to_owned()),
            anchor: interval.map(Interval::anchor),
            until_at: self.ends_at(),
            max_runs: self
                .run_limit()
                .map(|max_runs| i32::try_from(max_runs).unwrap_or(i32::MAX)),
        }
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct LockedSchedule {
    #[sqlx(flatten)]
    pub(crate) stored: StoredSchedule,
    pub(crate) enabled: bool,
    pub(crate) managed: bool,
    pub(crate) next_run_at: Option<DateTime<Utc>>,
    pub(crate) last_run_at: Option<DateTime<Utc>>,
    pub(crate) run_count: i32,
    pub(crate) finished_at: Option<DateTime<Utc>>,
}

pub(crate) struct Plan {
    pub(crate) next_run_at: Option<DateTime<Utc>>,
    pub(crate) run_count: i32,
    pub(crate) finished_at: Option<DateTime<Utc>>,
}

impl Plan {
    pub(crate) fn kept(existing: &LockedSchedule) -> Self {
        Self {
            next_run_at: existing.next_run_at,
            run_count: existing.run_count,
            finished_at: existing.finished_at,
        }
    }

    pub(crate) async fn fresh(
        connection: &mut PgConnection,
        schedule: &Schedule,
        last_run_at: Option<DateTime<Utc>>,
        run_count: i32,
    ) -> Result<Self> {
        let now = database_now(connection).await?;
        let after = last_run_at.map_or(now, |last_run_at| last_run_at.max(now));
        let next_run_at = schedule.next_run(after, run_count)?;
        Ok(Self {
            next_run_at,
            run_count,
            finished_at: next_run_at.is_none().then_some(now),
        })
    }
}

pub(crate) async fn plan(
    connection: &mut PgConnection,
    existing: Option<&LockedSchedule>,
    schedule: &Schedule,
) -> Result<Plan> {
    let stored = schedule.stored();
    match existing {
        Some(existing) if existing.stored == stored => Ok(Plan::kept(existing)),
        Some(existing) if existing.stored.rule() == stored.rule() => {
            Plan::fresh(
                connection,
                schedule,
                existing.last_run_at,
                existing.run_count,
            )
            .await
        }
        _ => {
            Plan::fresh(
                connection,
                schedule,
                existing.and_then(|e| e.last_run_at),
                0,
            )
            .await
        }
    }
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
        "SELECT {SCHEDULE_COLUMNS}, enabled, managed, next_run_at, last_run_at, run_count,
             finished_at
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
