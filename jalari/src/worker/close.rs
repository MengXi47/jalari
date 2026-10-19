use std::time::Duration;

use serde_json::Value;
use sqlx::{AssertSqlSafe, PgConnection};

use crate::job::{Command, Halt, JobState};
use crate::storage::interval;
use crate::{JobError, JobResult, Result, RetryPolicy, Schema};

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

    fn state(&self) -> JobState {
        match self {
            Self::Succeeded => JobState::Succeeded,
            Self::Retry { .. } => JobState::Scheduled,
            Self::Failed { .. } => JobState::Failed,
            Self::Released { .. } => JobState::Enqueued,
            Self::Stopped { .. } => JobState::Stopped,
            Self::Cancelled { .. } => JobState::Deleted,
        }
    }

    fn is_terminal(&self) -> bool {
        match self {
            Self::Succeeded | Self::Failed { .. } | Self::Cancelled { .. } => true,
            Self::Retry { .. } | Self::Released { .. } | Self::Stopped { .. } => false,
        }
    }

    fn last_error(&self) -> Option<Option<&str>> {
        match self {
            Self::Succeeded => Some(None),
            Self::Retry { error, .. } | Self::Failed { error } => Some(Some(error)),
            Self::Released { .. } | Self::Stopped { .. } | Self::Cancelled { .. } => None,
        }
    }

    fn note(&self) -> Option<&str> {
        match self {
            Self::Succeeded => None,
            Self::Retry { error, .. } | Self::Failed { error } => Some(error),
            Self::Released { note } | Self::Stopped { note } | Self::Cancelled { note } => {
                Some(note)
            }
        }
    }
}

pub(super) fn decide(
    ending: Ending,
    command: Option<Command>,
    attempt: i32,
    max_attempts: i32,
    retry_policy: &dyn RetryPolicy,
) -> Outcome {
    let error = match ending {
        Ending::Returned(Ok(())) => return Outcome::Succeeded,
        Ending::Returned(Err(error)) => error,
        Ending::Aborted { after } => JobError::new(format!(
            "aborted because it reached no checkpoint within {after:?}"
        )),
        Ending::NotStarted => {
            JobError::new("the request arrived before the job started".to_owned())
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
    match retry_policy.next_retry(attempt, max_attempts, &error) {
        Some(delay) => Outcome::Retry {
            delay,
            error: error.msg().to_owned(),
        },
        None => Outcome::Failed {
            error: error.msg().to_owned(),
        },
    }
}

pub(crate) struct Closing {
    job_id: i64,
    outcome: Outcome,
    attempts: i32,
    run_at_delay: Option<Duration>,
    duration_ms: Option<i64>,
    clears_command: bool,
    kept: Option<Value>,
}

impl Closing {
    pub(super) fn after_run(
        job_id: i64,
        attempt: i32,
        outcome: Outcome,
        duration: Duration,
        clears_command: bool,
        kept: Option<Value>,
    ) -> Self {
        let (attempts, run_at_delay) = match &outcome {
            Outcome::Retry { delay, .. } => (attempt, Some(*delay)),
            Outcome::Succeeded | Outcome::Failed { .. } => (attempt, None),
            Outcome::Released { .. } | Outcome::Stopped { .. } | Outcome::Cancelled { .. } => {
                (attempt - 1, Some(Duration::ZERO))
            }
        };
        Self {
            job_id,
            outcome,
            attempts,
            run_at_delay,
            duration_ms: Some(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)),
            clears_command,
            kept,
        }
    }

    pub(crate) fn idle(job_id: i64, attempts: i32, command: Command) -> Self {
        Self {
            job_id,
            outcome: Outcome::commanded(command, command.message().to_owned()),
            attempts,
            run_at_delay: None,
            duration_ms: None,
            clears_command: true,
            kept: None,
        }
    }

    pub(super) fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub(super) fn duration_ms(&self) -> Option<i64> {
        self.duration_ms
    }
}

pub(crate) async fn close(
    connection: &mut PgConnection,
    schema: &Schema,
    closing: &Closing,
) -> Result<()> {
    let outcome = &closing.outcome;
    let last_error = outcome.last_error();
    sqlx::query(AssertSqlSafe(format!(
        "WITH closed AS (
             UPDATE {job}
             SET state = $2, attempts = $3,
                 last_error = CASE WHEN $4 THEN $5 ELSE last_error END,
                 run_at = coalesce(now() + $6, run_at), updated_at = now()
             WHERE id = $1
             RETURNING id
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, error, duration_ms, created_at)
             SELECT id, $2, $3, $7, $8, now() FROM closed
         ),
         kept AS (
             INSERT INTO {job_control} AS control (job_id, lease, progress)
             SELECT id, 1, $11 FROM closed WHERE $12
             ON CONFLICT (job_id) DO UPDATE
             SET lease = control.lease + 1,
                 command = CASE WHEN $9 THEN NULL ELSE control.command END,
                 progress = EXCLUDED.progress,
                 updated_at = now()
         )
         UPDATE {job_control}
         SET lease = lease + 1,
             command = CASE WHEN $9 THEN NULL ELSE command END,
             progress = CASE WHEN $10 THEN NULL ELSE progress END,
             updated_at = now()
         WHERE job_id = $1 AND NOT $12",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
        job_control = schema.table("job_control"),
    )))
    .bind(closing.job_id)
    .bind(outcome.state().as_str())
    .bind(closing.attempts)
    .bind(last_error.is_some())
    .bind(last_error.flatten())
    .bind(closing.run_at_delay.map(interval))
    .bind(outcome.note())
    .bind(closing.duration_ms)
    .bind(closing.clears_command)
    .bind(outcome.is_terminal())
    .bind(closing.kept.as_ref().filter(|_| !outcome.is_terminal()))
    .bind(closing.kept.is_some())
    .execute(connection)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExponentialBackoff;

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
            decide(Ending::Returned(Ok(())), None, 1, 3, &policy()),
            Outcome::Succeeded
        );
    }

    #[test]
    fn test_ok_wins_over_a_pending_command() {
        for command in [Command::Stop, Command::Cancel] {
            assert_eq!(
                decide(Ending::Returned(Ok(())), Some(command), 1, 3, &policy()),
                Outcome::Succeeded
            );
        }
    }

    #[test]
    fn test_error_with_attempts_left_retries() {
        assert_eq!(
            decide(error("boom"), None, 1, 3, &policy()),
            Outcome::Retry {
                delay: Duration::from_secs(1),
                error: "boom".to_owned()
            }
        );
    }

    #[test]
    fn test_error_on_last_attempt_fails() {
        assert_eq!(
            decide(error("boom"), None, 3, 3, &policy()),
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
            ]
        };
        for ending in endings() {
            assert!(matches!(
                decide(ending, Some(Command::Stop), 3, 3, &policy()),
                Outcome::Stopped { .. }
            ));
        }
        for ending in endings() {
            assert!(matches!(
                decide(ending, Some(Command::Cancel), 3, 3, &policy()),
                Outcome::Cancelled { .. }
            ));
        }
    }

    #[test]
    fn test_stopped_run_gives_its_attempt_back() {
        let outcome = decide(error("boom"), Some(Command::Stop), 2, 3, &policy());
        let closing = Closing::after_run(1, 2, outcome, Duration::ZERO, true, None);
        assert_eq!(closing.attempts, 1);
        let retried = decide(error("boom"), None, 2, 3, &policy());
        assert_eq!(
            Closing::after_run(1, 2, retried, Duration::ZERO, false, None).attempts,
            2
        );
    }

    #[test]
    fn test_shutdown_halt_releases_the_job_without_counting_the_attempt() {
        let halted = Ending::Returned(Err(JobError::halted(Halt::Shutdown)));
        let outcome = decide(halted, None, 3, 3, &policy());
        assert!(matches!(outcome, Outcome::Released { .. }));
        let closing = Closing::after_run(1, 3, outcome, Duration::ZERO, false, None);
        assert_eq!(closing.attempts, 2);
    }

    #[test]
    fn test_shutdown_halt_with_a_pending_stop_stops_the_job() {
        let halted = Ending::Returned(Err(JobError::halted(Halt::Shutdown)));
        assert!(matches!(
            decide(halted, Some(Command::Stop), 1, 3, &policy()),
            Outcome::Stopped { .. }
        ));
    }

    #[test]
    fn test_ordinary_error_during_shutdown_is_retried() {
        let ending = Ending::Returned(Err(JobError::new(Halt::Shutdown.message().to_owned())));
        assert!(matches!(
            decide(ending, None, 1, 3, &policy()),
            Outcome::Retry { .. }
        ));
    }
}
