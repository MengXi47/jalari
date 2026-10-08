use std::hash::{BuildHasher, RandomState};
use std::time::Duration;

use crate::JobError;

const DEFAULT_BASE: Duration = Duration::from_secs(5);
const DEFAULT_MAX: Duration = Duration::from_secs(3600);
const DEFAULT_JITTER: f64 = 0.2;
const MAX_EXPONENT: u32 = 31;

/// Decides whether and when a failed attempt is retried.
///
/// Set one per worker with [`WorkerBuilder::retry_policy`](crate::WorkerBuilder::retry_policy);
/// the default is [`ExponentialBackoff`].
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use jalari::{JobError, RetryPolicy};
///
/// struct FixedDelay(Duration);
///
/// impl RetryPolicy for FixedDelay {
///     fn next_retry(
///         &self,
///         attempt: i32,
///         max_attempts: i32,
///         error: &JobError,
///     ) -> Option<Duration> {
///         (!error.is_permanent() && attempt < max_attempts).then_some(self.0)
///     }
/// }
/// ```
pub trait RetryPolicy: Send + Sync + 'static {
    /// Returns how long to wait before the next attempt, or `None` to mark the job failed.
    ///
    /// `attempt` is the number of the attempt that just failed, starting at 1, and
    /// `max_attempts` is the job's [`MAX_ATTEMPTS`](crate::Job::MAX_ATTEMPTS).
    fn next_retry(&self, attempt: i32, max_attempts: i32, error: &JobError) -> Option<Duration>;
}

/// Retry delay that doubles after each failed attempt, up to a maximum, with random jitter.
///
/// The delay after attempt `n` is `base * 2^(n-1)`, capped at `max`, then scaled by a random
/// factor in `1 ± jitter`. Permanent errors and exhausted attempts are not retried. The default
/// waits 5 s, 10 s, 20 s... up to 1 hour, with 20% jitter.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// let policy = jalari::ExponentialBackoff {
///     base: Duration::from_secs(10),
///     max: Duration::from_secs(600),
///     jitter: 0.1,
/// };
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ExponentialBackoff {
    /// Delay after the first failed attempt.
    pub base: Duration,
    /// Longest delay.
    pub max: Duration,
    /// Random spread as a fraction of the delay, clamped to `0.0..=1.0`; spreads out retries of
    /// jobs that failed together.
    pub jitter: f64,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            base: DEFAULT_BASE,
            max: DEFAULT_MAX,
            jitter: DEFAULT_JITTER,
        }
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn next_retry(&self, attempt: i32, max_attempts: i32, error: &JobError) -> Option<Duration> {
        if error.is_permanent() || attempt >= max_attempts {
            return None;
        }
        let exponent = u32::try_from(attempt.saturating_sub(1))
            .unwrap_or(0)
            .min(MAX_EXPONENT);
        let delay = self.base.saturating_mul(1 << exponent).min(self.max);
        Some(apply_jitter(delay, self.jitter))
    }
}

fn apply_jitter(delay: Duration, jitter: f64) -> Duration {
    let jitter = jitter.clamp(0.0, 1.0);
    if jitter == 0.0 {
        return delay;
    }
    let random = u32::try_from(RandomState::new().hash_one(()) >> 32).unwrap_or(0);
    let unit = f64::from(random) / f64::from(u32::MAX);
    delay.mul_f64(1.0 + jitter * (2.0 * unit - 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ExponentialBackoff {
        ExponentialBackoff {
            base: Duration::from_secs(5),
            max: Duration::from_secs(60),
            jitter: 0.0,
        }
    }

    #[test]
    fn test_delay_doubles_until_max() {
        let error = JobError::new("boom".to_owned());
        let delays: Vec<Option<Duration>> = (1..=6)
            .map(|attempt| policy().next_retry(attempt, 10, &error))
            .collect();
        let expected: Vec<Option<Duration>> = [5, 10, 20, 40, 60, 60]
            .into_iter()
            .map(|secs| Some(Duration::from_secs(secs)))
            .collect();
        assert_eq!(delays, expected);
    }

    #[test]
    fn test_stops_when_attempts_run_out_or_error_is_permanent() {
        let error = JobError::new("boom".to_owned());
        assert_eq!(policy().next_retry(3, 3, &error), None);

        let permanent = JobError::permanent(std::io::Error::other("bad"));
        assert_eq!(policy().next_retry(1, 10, &permanent), None);
    }

    #[test]
    fn test_jitter_stays_within_range() {
        let jittered = ExponentialBackoff {
            jitter: 0.2,
            ..policy()
        };
        let error = JobError::new("boom".to_owned());
        for _ in 0..100 {
            let delay = jittered.next_retry(1, 10, &error).unwrap();
            assert!(delay >= Duration::from_secs(4) && delay <= Duration::from_secs(6));
        }
    }

    #[test]
    fn test_large_attempt_does_not_overflow() {
        let error = JobError::new("boom".to_owned());
        assert_eq!(
            policy().next_retry(i32::MAX - 1, i32::MAX, &error),
            Some(Duration::from_secs(60))
        );
    }
}
