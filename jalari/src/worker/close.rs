use std::time::Duration;

use sqlx::{AssertSqlSafe, PgConnection};

use crate::job::JobState;
use crate::storage::interval;
use crate::{JobResult, Result, RetryPolicy, Schema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Outcome {
    Succeeded,
    Retry { delay: Duration, error: String },
    Failed { error: String },
}

impl Outcome {
    fn state(&self) -> JobState {
        match self {
            Self::Succeeded => JobState::Succeeded,
            Self::Retry { .. } => JobState::Scheduled,
            Self::Failed { .. } => JobState::Failed,
        }
    }

    fn error(&self) -> Option<&str> {
        match self {
            Self::Succeeded => None,
            Self::Retry { error, .. } | Self::Failed { error } => Some(error),
        }
    }

    fn retry_delay(&self) -> Option<Duration> {
        match self {
            Self::Retry { delay, .. } => Some(*delay),
            Self::Succeeded | Self::Failed { .. } => None,
        }
    }
}

pub(super) fn decide(
    result: JobResult,
    attempt: i32,
    max_attempts: i32,
    retry_policy: &dyn RetryPolicy,
) -> Outcome {
    match result {
        Ok(()) => Outcome::Succeeded,
        Err(error) => match retry_policy.next_retry(attempt, max_attempts, &error) {
            Some(delay) => Outcome::Retry {
                delay,
                error: error.msg().to_owned(),
            },
            None => Outcome::Failed {
                error: error.msg().to_owned(),
            },
        },
    }
}

pub(super) struct Closing {
    pub(super) job_id: i64,
    pub(super) outcome: Outcome,
    pub(super) attempts: i32,
    pub(super) duration_ms: Option<i64>,
}

pub(super) async fn close(
    connection: &mut PgConnection,
    schema: &Schema,
    closing: &Closing,
) -> Result<()> {
    let outcome = &closing.outcome;
    sqlx::query(AssertSqlSafe(format!(
        "WITH closed AS (
             UPDATE {job} SET state = $2, attempts = $3, last_error = $4,
                 run_at = coalesce(now() + $5, run_at), updated_at = now()
             WHERE id = $1
             RETURNING id
         )
         INSERT INTO {job_history} (job_id, state, attempt, error, duration_ms, created_at)
         SELECT id, $2, $3, $4, $6, now() FROM closed",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
    )))
    .bind(closing.job_id)
    .bind(outcome.state().as_str())
    .bind(closing.attempts)
    .bind(outcome.error())
    .bind(outcome.retry_delay().map(interval))
    .bind(closing.duration_ms)
    .execute(connection)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExponentialBackoff, JobError};

    fn policy() -> ExponentialBackoff {
        ExponentialBackoff {
            base: Duration::from_secs(1),
            max: Duration::from_secs(1),
            jitter: 0.0,
        }
    }

    #[test]
    fn test_ok_succeeds() {
        assert_eq!(decide(Ok(()), 1, 3, &policy()), Outcome::Succeeded);
    }

    #[test]
    fn test_error_with_attempts_left_retries() {
        let outcome = decide(Err(JobError::new("boom".to_owned())), 1, 3, &policy());
        assert_eq!(
            outcome,
            Outcome::Retry {
                delay: Duration::from_secs(1),
                error: "boom".to_owned()
            }
        );
    }

    #[test]
    fn test_error_on_last_attempt_fails() {
        let outcome = decide(Err(JobError::new("boom".to_owned())), 3, 3, &policy());
        assert_eq!(
            outcome,
            Outcome::Failed {
                error: "boom".to_owned()
            }
        );
    }
}
