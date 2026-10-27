use std::fmt;

use crate::{Error, ErrorKind};

pub(crate) const JOB_KEY_STATES: &str = "('scheduled', 'enqueued', 'running', 'stopped')";

/// Where a job is in its life, as stored in the `state` column.
///
/// New states may be added in minor releases, so a `match` needs a `_` arm.
///
/// # Examples
///
/// ```rust
/// assert_eq!(jalari::JobState::Running.as_str(), "running");
/// assert!(jalari::JobState::Discarded.is_finished());
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobState {
    /// Waiting for its `run_at` time: a delayed job, a retry waiting for its backoff, or a job
    /// whose worker died, waiting to run again.
    Scheduled,
    /// Due now and waiting for a worker.
    Enqueued,
    /// Being run by the worker named in
    /// [`JobInfo::worker_id`](crate::admin::JobInfo::worker_id).
    Running,
    /// Finished successfully.
    Succeeded,
    /// Gave up after its last attempt or a permanent error.
    Failed,
    /// Gave up on purpose, without retrying; never runs again.
    Discarded,
    /// Held by [`stop`](crate::stop) until [`restart`](crate::restart).
    Stopped,
    /// Cancelled, or marked for deletion; housekeeping removes it later.
    Deleted,
}

impl JobState {
    pub(crate) const ALL: [Self; 8] = [
        Self::Scheduled,
        Self::Enqueued,
        Self::Running,
        Self::Succeeded,
        Self::Failed,
        Self::Discarded,
        Self::Stopped,
        Self::Deleted,
    ];

    /// The value stored in the `state` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Enqueued => "enqueued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Discarded => "discarded",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }

    /// Whether the job will never run again: succeeded, failed, discarded or deleted.
    pub fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Discarded | Self::Deleted
        )
    }

    pub(crate) fn parse(value: &str) -> crate::Result<Self> {
        Self::ALL
            .into_iter()
            .find(|state| state.as_str() == value)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::RowDecodeFailed,
                    format!("{value:?} is not a job state"),
                )
            })
    }

    pub(crate) fn finished_sql() -> String {
        let finished: Vec<String> = Self::ALL
            .into_iter()
            .filter(|state| state.is_finished())
            .map(|state| format!("'{state}'"))
            .collect();
        format!("({})", finished.join(", "))
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_state_parses_back_from_its_column_value() {
        for state in JobState::ALL {
            assert_eq!(JobState::parse(state.as_str()).unwrap(), state);
        }
        assert!(JobState::parse("unknown").is_err());
    }

    #[test]
    fn test_finished_sql_lists_exactly_the_finished_states() {
        assert_eq!(
            JobState::finished_sql(),
            "('succeeded', 'failed', 'discarded', 'deleted')"
        );
    }
}
