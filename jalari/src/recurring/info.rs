use chrono::{DateTime, Utc};

/// Snapshot of one recurring schedule, as returned by [`list`](crate::recurring::list).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RecurringInfo {
    /// Unique name of the schedule; for code-declared schedules it equals the job's `NAME`.
    pub name: String,
    /// Cron expression.
    pub cron: String,
    /// IANA time zone the expression is evaluated in.
    pub timezone: String,
    /// [`Job::NAME`](crate::Job::NAME) of the job enqueued on each run.
    pub task: String,
    /// Queue each run's job goes to.
    pub queue: String,
    /// `false` while paused.
    pub enabled: bool,
    /// `true` when declared with `#[jalari::job(cron = ...)]`; such schedules follow the
    /// deployed code and cannot be changed at runtime.
    pub managed: bool,
    /// When the next run is due.
    pub next_run_at: DateTime<Utc>,
    /// Scheduled time of the last run, not the moment it was enqueued; `None` before the first.
    pub last_run_at: Option<DateTime<Utc>>,
}
