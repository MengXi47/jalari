use std::fmt;

use crate::{Error, ErrorKind};

pub(crate) const JOB_KEY_STATES: &str = "('scheduled', 'enqueued', 'stopped')";

/// Where a job is in its life, as stored in the `state` column.
///
/// New states may be added in minor releases, so a `match` needs a `_` arm.
///
/// # Examples
///
/// ```rust
/// assert_eq!(jalari::JobState::Failed.as_str(), "failed");
/// assert!(jalari::JobState::Succeeded.is_finished());
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobState {
    /// Waiting for its `run_at` time; also the state of a retry that waits for its backoff.
    Scheduled,
    /// Due now, waiting for a worker or being run by one.
    Enqueued,
    /// Finished successfully.
    Succeeded,
    /// Gave up after its last attempt or a permanent error.
    Failed,
    /// Held by [`stop`](crate::stop) until [`restart`](crate::restart).
    Stopped,
    /// Cancelled, or marked for deletion; housekeeping removes it later.
    Deleted,
}

impl JobState {
    const ALL: [Self; 6] = [
        Self::Scheduled,
        Self::Enqueued,
        Self::Succeeded,
        Self::Failed,
        Self::Stopped,
        Self::Deleted,
    ];

    /// The value stored in the `state` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Enqueued => "enqueued",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }

    /// Whether the job will never run again: succeeded, failed or deleted.
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Deleted)
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
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
