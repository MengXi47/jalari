#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobState {
    Scheduled,
    Succeeded,
    Failed,
}

impl JobState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}
