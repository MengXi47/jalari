pub(crate) const JOB_KEY_STATES: &str = "('scheduled', 'enqueued', 'stopped')";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobState {
    Scheduled,
    Enqueued,
    Succeeded,
    Failed,
    Stopped,
    Deleted,
}

impl JobState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Enqueued => "enqueued",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }
}
