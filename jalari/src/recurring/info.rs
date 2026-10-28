use chrono::{DateTime, Utc};

use super::Missed;
use crate::{Priority, Schedule};

/// Snapshot of one recurring schedule, as returned by [`list`](crate::recurring::list).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecurringInfo {
    /// Unique name of the schedule; for code-declared schedules it equals the job's `NAME`.
    pub name: String,
    /// When the job runs, with any end conditions.
    ///
    /// `None` when the stored schedule cannot be read back, for example a cron expression that
    /// this version does not accept. The scheduler disables such a schedule; fix it with
    /// [`update_schedule`](crate::recurring::update_schedule) or
    /// [`remove`](crate::recurring::remove) it.
    pub schedule: Option<Schedule>,
    /// [`Job::NAME`](crate::Job::NAME) of the job enqueued on each run.
    pub task: String,
    /// Queue each run's job goes to.
    pub queue: String,
    /// Priority of each run's job, taken from the job type when the schedule was written.
    ///
    /// A code-declared schedule follows the job type at every worker start; a runtime schedule
    /// keeps it until [`add_or_update`](crate::recurring::add_or_update) writes it again.
    pub priority: Priority,
    /// What the scheduler does with a run it finds more than a minute overdue.
    ///
    /// A code-declared schedule follows the `missed` argument at every worker start; a runtime
    /// schedule takes it from the [`Schedule`] given to
    /// [`add_or_update`](crate::recurring::add_or_update) or
    /// [`update_schedule`](crate::recurring::update_schedule). Schedules created before 0.8.0
    /// use [`Missed::Once`].
    pub missed: Missed,
    /// `false` while paused.
    pub enabled: bool,
    /// `true` when declared with `#[jalari::job(cron = ...)]`; such schedules follow the
    /// deployed code and cannot be changed at runtime.
    pub managed: bool,
    /// When the next run is due; `None` once the schedule is finished.
    pub next_run_at: Option<DateTime<Utc>>,
    /// Scheduled time of the last run, not the moment it was enqueued; `None` before the first.
    pub last_run_at: Option<DateTime<Utc>>,
    /// Runs the scheduler has enqueued since the schedule was created or last changed;
    /// [`trigger`](crate::recurring::trigger) does not count.
    pub run_count: i32,
    /// When the schedule reached its end, after which it never fires again.
    pub finished_at: Option<DateTime<Utc>>,
    /// Context stored with every run, captured from the [`scope`](crate::scope) that created
    /// the schedule; `None` for code-declared schedules.
    pub context: Option<serde_json::Value>,
}
