use std::time::Duration;

use serde_json::Value;
use tracing::{debug, info, warn};

use crate::job::{Command, Halt, JobState, SNOOZED};
use crate::{JobError, JobResult, RetryPolicy};

const LONGEST_DELAY: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

pub(super) enum Ending {
    Returned(JobResult),
    Aborted { after: Duration },
    NotStarted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Succeeded,
    Retry { delay: Duration, error: String },
    Failed { error: String },
    Discarded { error: String },
    Snoozed { delay: Duration },
    Released { note: String },
    Stopped { note: String },
    Cancelled { note: String },
}

impl Outcome {
    fn commanded(command: Command, note: String) -> Self {
        match command {
            Command::Stop => Self::Stopped { note },
            Command::Cancel => Self::Cancelled { note },
        }
    }

    pub(super) fn state(&self) -> JobState {
        match self {
            Self::Succeeded => JobState::Succeeded,
            Self::Retry { .. } => JobState::Scheduled,
            Self::Failed { .. } => JobState::Failed,
            Self::Discarded { .. } => JobState::Discarded,
            Self::Snoozed { .. } => JobState::Scheduled,
            Self::Released { .. } => JobState::Enqueued,
            Self::Stopped { .. } => JobState::Stopped,
            Self::Cancelled { .. } => JobState::Deleted,
        }
    }

    pub(super) fn is_terminal(&self) -> bool {
        match self {
            Self::Succeeded
            | Self::Failed { .. }
            | Self::Discarded { .. }
            | Self::Cancelled { .. } => true,
            Self::Retry { .. }
            | Self::Snoozed { .. }
            | Self::Released { .. }
            | Self::Stopped { .. } => false,
        }
    }

    pub(super) fn last_error(&self) -> Option<Option<&str>> {
        match self {
            Self::Succeeded => Some(None),
            Self::Retry { error, .. } | Self::Failed { error } | Self::Discarded { error } => {
                Some(Some(error))
            }
            Self::Snoozed { .. }
            | Self::Released { .. }
            | Self::Stopped { .. }
            | Self::Cancelled { .. } => None,
        }
    }

    pub(super) fn note(&self) -> Option<&str> {
        match self {
            Self::Succeeded => None,
            Self::Retry { error, .. } | Self::Failed { error } | Self::Discarded { error } => {
                Some(error)
            }
            Self::Snoozed { .. } => Some(SNOOZED),
            Self::Released { note } | Self::Stopped { note } | Self::Cancelled { note } => {
                Some(note)
            }
        }
    }
}

pub(super) fn decide(
    ending: &Ending,
    command: Option<Command>,
    attempt: i32,
    max_attempts: i32,
    retry_policy: &dyn RetryPolicy,
) -> Outcome {
    let created;
    let error = match ending {
        Ending::Returned(Ok(())) => return Outcome::Succeeded,
        Ending::Returned(Err(error)) => error,
        Ending::Aborted { after } => {
            created = JobError::new(format!(
                "aborted because it reached no checkpoint within {after:?}"
            ));
            &created
        }
        Ending::NotStarted => {
            created = JobError::new("the request arrived before the job started".to_owned());
            &created
        }
    };
    if let Some(command) = command {
        return Outcome::commanded(command, error.msg().to_owned());
    }
    if error.halt() == Some(Halt::Shutdown) {
        return Outcome::Released {
            note: error.msg().to_owned(),
        };
    }
    if let Some(delay) = error.snoozed_for() {
        return Outcome::Snoozed {
            delay: delay.min(LONGEST_DELAY),
        };
    }
    if error.is_discarded() {
        return Outcome::Discarded {
            error: error.msg().to_owned(),
        };
    }
    match retry_policy.next_retry(attempt, max_attempts, error) {
        Some(delay) => Outcome::Retry {
            delay: error.retry_delay().unwrap_or(delay).min(LONGEST_DELAY),
            error: error.msg().to_owned(),
        },
        None => Outcome::Failed {
            error: error.msg().to_owned(),
        },
    }
}

pub(super) struct Closing {
    outcome: Outcome,
    seen: Option<Command>,
    attempts: i32,
    run_at_delay: Option<Duration>,
    duration_ms: Option<i64>,
    kept: Option<Value>,
}

impl Closing {
    pub(super) fn after_run(
        attempt: i32,
        outcome: Outcome,
        seen: Option<Command>,
        duration: Duration,
        kept: Option<Value>,
    ) -> Self {
        let (attempts, run_at_delay) = match &outcome {
            Outcome::Retry { delay, .. } => (attempt, Some(*delay)),
            Outcome::Snoozed { delay } => (attempt - 1, Some(*delay)),
            Outcome::Succeeded | Outcome::Failed { .. } | Outcome::Discarded { .. } => {
                (attempt, None)
            }
            Outcome::Released { .. } | Outcome::Stopped { .. } | Outcome::Cancelled { .. } => {
                (attempt - 1, Some(Duration::ZERO))
            }
        };
        Self {
            outcome,
            seen,
            attempts,
            run_at_delay,
            duration_ms: Some(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)),
            kept,
        }
    }

    pub(super) fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub(super) fn duration_ms(&self) -> Option<i64> {
        self.duration_ms
    }

    pub(super) fn seen(&self) -> Option<Command> {
        self.seen
    }

    pub(super) fn attempts(&self) -> i32 {
        self.attempts
    }

    pub(super) fn run_at_delay(&self) -> Option<Duration> {
        self.run_at_delay
    }

    pub(super) fn kept(&self) -> Option<&Value> {
        self.kept.as_ref()
    }

    pub(super) fn log(&self, job_id: i64) {
        let duration_ms = self.duration_ms;
        match &self.outcome {
            Outcome::Succeeded => debug!(duration_ms, "job succeeded"),
            Outcome::Retry { delay, error } => {
                warn!(error = %error, ?delay, "job failed, retrying");
            }
            Outcome::Failed { error } => warn!(error = %error, "job failed"),
            Outcome::Discarded { error } => info!(job_id, error = %error, "job discarded"),
            Outcome::Snoozed { delay } => info!(job_id, ?delay, "job snoozed"),
            Outcome::Released { note } => info!(job_id, note = %note, "job released"),
            Outcome::Stopped { note } => info!(job_id, note = %note, "job stopped"),
            Outcome::Cancelled { note } => info!(job_id, note = %note, "job cancelled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExponentialBackoff, snooze};

    fn policy() -> ExponentialBackoff {
        ExponentialBackoff {
            base: Duration::from_secs(1),
            max: Duration::from_secs(1),
            jitter: 0.0,
        }
    }

    fn error(msg: &str) -> Ending {
        Ending::Returned(Err(JobError::new(msg.to_owned())))
    }

    #[test]
    fn test_ok_succeeds() {
        assert_eq!(
            decide(&Ending::Returned(Ok(())), None, 1, 3, &policy()),
            Outcome::Succeeded
        );
    }

    #[test]
    fn test_ok_wins_over_a_pending_command() {
        for command in [Command::Stop, Command::Cancel] {
            assert_eq!(
                decide(&Ending::Returned(Ok(())), Some(command), 1, 3, &policy()),
                Outcome::Succeeded
            );
        }
    }

    #[test]
    fn test_error_with_attempts_left_retries() {
        assert_eq!(
            decide(&error("boom"), None, 1, 3, &policy()),
            Outcome::Retry {
                delay: Duration::from_secs(1),
                error: "boom".to_owned()
            }
        );
    }

    #[test]
    fn test_error_on_last_attempt_fails() {
        assert_eq!(
            decide(&error("boom"), None, 3, 3, &policy()),
            Outcome::Failed {
                error: "boom".to_owned()
            }
        );
    }

    #[test]
    fn test_any_other_ending_follows_the_pending_command() {
        let endings = || {
            [
                error("boom"),
                Ending::Returned(Err(JobError::permanent(std::fmt::Error))),
                Ending::Aborted {
                    after: Duration::from_secs(1),
                },
                Ending::NotStarted,
                Ending::Returned(snooze(Duration::from_secs(30))),
                Ending::Returned(Err(JobError::discard(std::fmt::Error))),
            ]
        };
        for ending in endings() {
            assert!(matches!(
                decide(&ending, Some(Command::Stop), 3, 3, &policy()),
                Outcome::Stopped { .. }
            ));
        }
        for ending in endings() {
            assert!(matches!(
                decide(&ending, Some(Command::Cancel), 3, 3, &policy()),
                Outcome::Cancelled { .. }
            ));
        }
    }

    #[test]
    fn test_stopped_run_gives_its_attempt_back() {
        let outcome = decide(&error("boom"), Some(Command::Stop), 2, 3, &policy());
        let closing = Closing::after_run(2, outcome, Some(Command::Stop), Duration::ZERO, None);
        assert_eq!(closing.attempts, 1);
        let retried = decide(&error("boom"), None, 2, 3, &policy());
        assert_eq!(
            Closing::after_run(2, retried, None, Duration::ZERO, None).attempts,
            2
        );
    }

    #[test]
    fn test_shutdown_halt_releases_the_job_without_counting_the_attempt() {
        let halted = Ending::Returned(Err(JobError::halted(Halt::Shutdown)));
        let outcome = decide(&halted, None, 3, 3, &policy());
        assert!(matches!(outcome, Outcome::Released { .. }));
        let closing = Closing::after_run(3, outcome, None, Duration::ZERO, None);
        assert_eq!(closing.attempts, 2);
    }

    #[test]
    fn test_shutdown_halt_with_a_pending_stop_stops_the_job() {
        let halted = Ending::Returned(Err(JobError::halted(Halt::Shutdown)));
        assert!(matches!(
            decide(&halted, Some(Command::Stop), 1, 3, &policy()),
            Outcome::Stopped { .. }
        ));
    }

    #[test]
    fn test_ordinary_error_during_shutdown_is_retried() {
        let ending = Ending::Returned(Err(JobError::new(Halt::Shutdown.message().to_owned())));
        assert!(matches!(
            decide(&ending, None, 1, 3, &policy()),
            Outcome::Retry { .. }
        ));
    }

    #[test]
    fn test_retry_after_replaces_the_policy_delay() {
        let ending = Ending::Returned(Err(
            JobError::new("slow down".to_owned()).retry_after(Duration::from_secs(7))
        ));
        assert_eq!(
            decide(&ending, None, 1, 3, &policy()),
            Outcome::Retry {
                delay: Duration::from_secs(7),
                error: "slow down".to_owned()
            }
        );
    }

    #[test]
    fn test_retry_after_is_ignored_on_the_last_attempt() {
        let ending = Ending::Returned(Err(
            JobError::new("slow down".to_owned()).retry_after(Duration::from_secs(7))
        ));
        assert_eq!(
            decide(&ending, None, 3, 3, &policy()),
            Outcome::Failed {
                error: "slow down".to_owned()
            }
        );
    }

    #[test]
    fn test_retry_after_is_ignored_for_a_permanent_error() {
        let ending = Ending::Returned(Err(
            JobError::permanent(std::fmt::Error).retry_after(Duration::from_secs(7))
        ));
        assert!(matches!(
            decide(&ending, None, 1, 3, &policy()),
            Outcome::Failed { .. }
        ));
    }

    struct Forever;

    impl RetryPolicy for Forever {
        fn next_retry(&self, _: i32, _: i32, _: &JobError) -> Option<Duration> {
            Some(Duration::MAX)
        }
    }

    #[test]
    fn test_delays_are_capped() {
        let longest = Outcome::Retry {
            delay: LONGEST_DELAY,
            error: "boom".to_owned(),
        };
        assert_eq!(decide(&error("boom"), None, 1, 3, &Forever), longest);
        let ending = Ending::Returned(Err(
            JobError::new("boom".to_owned()).retry_after(Duration::MAX)
        ));
        assert_eq!(decide(&ending, None, 1, 3, &policy()), longest);
        assert_eq!(
            decide(
                &Ending::Returned(snooze(Duration::MAX)),
                None,
                1,
                3,
                &policy()
            ),
            Outcome::Snoozed {
                delay: LONGEST_DELAY
            }
        );
    }

    #[test]
    fn test_snooze_schedules_without_counting_the_attempt() {
        let ending = Ending::Returned(snooze(Duration::from_secs(30)));
        let outcome = decide(&ending, None, 2, 3, &policy());
        assert_eq!(
            outcome,
            Outcome::Snoozed {
                delay: Duration::from_secs(30)
            }
        );
        assert_eq!(outcome.state(), JobState::Scheduled);
        assert_eq!(outcome.last_error(), None);
        assert!(!outcome.is_terminal());
        assert_eq!(outcome.note(), Some(SNOOZED));
        let closing = Closing::after_run(2, outcome, None, Duration::ZERO, None);
        assert_eq!(closing.attempts, 1);
        assert_eq!(closing.run_at_delay, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_snooze_on_the_last_attempt_still_snoozes() {
        let ending = Ending::Returned(snooze(Duration::from_secs(30)));
        let outcome = decide(&ending, None, 3, 3, &policy());
        assert!(matches!(outcome, Outcome::Snoozed { .. }));
        assert_eq!(
            Closing::after_run(3, outcome, None, Duration::ZERO, None).attempts,
            2
        );
    }

    #[test]
    fn test_discard_ends_the_job_with_attempts_left() {
        let ending = Ending::Returned(Err(JobError::discard(std::io::Error::other("gone"))));
        let outcome = decide(&ending, None, 1, 10, &policy());
        assert_eq!(
            outcome,
            Outcome::Discarded {
                error: "gone".to_owned()
            }
        );
        assert_eq!(outcome.state(), JobState::Discarded);
        assert!(outcome.is_terminal());
        assert_eq!(outcome.last_error(), Some(Some("gone")));
        let closing = Closing::after_run(1, outcome, None, Duration::ZERO, None);
        assert_eq!(closing.attempts, 1);
        assert_eq!(closing.run_at_delay, None);
    }

    #[test]
    fn test_error_reading_like_a_snooze_is_retried() {
        assert!(matches!(
            decide(&error(SNOOZED), None, 1, 3, &policy()),
            Outcome::Retry { .. }
        ));
    }
}
