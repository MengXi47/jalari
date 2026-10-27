use std::fmt;
use std::time::Duration;

use super::checkpoint::Halt;
use crate::JobResult;

pub(crate) const SNOOZED: &str = "the job asked to run again later";

/// Failure of one attempt of [`Job::run`](crate::Job::run).
///
/// Any `std::error::Error` converts into a retryable `JobError`, so `?` works inside `run`. Use
/// [`JobError::permanent`] for failures that retrying cannot fix, such as invalid input, and
/// [`JobError::discard`] when the job no longer needs to run at all.
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// let retryable: jalari::JobError = io::Error::from(io::ErrorKind::TimedOut).into();
/// assert!(!retryable.is_permanent());
///
/// let permanent = jalari::JobError::permanent(io::Error::from(io::ErrorKind::InvalidInput));
/// assert!(permanent.is_permanent());
///
/// let discarded = jalari::JobError::discard(io::Error::other("order cancelled"));
/// assert!(discarded.is_discarded());
/// assert!(!discarded.is_permanent());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobError {
    msg: String,
    disposition: Disposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Retry { delay: Option<Duration> },
    Permanent,
    Discard,
    Snooze { delay: Duration },
    Halt(Halt),
}

impl JobError {
    /// Creates a retryable error with a message.
    pub fn new(msg: String) -> Self {
        Self {
            msg,
            disposition: Disposition::Retry { delay: None },
        }
    }

    /// Creates an error that marks the job failed right away, without further retries.
    pub fn permanent<E: std::error::Error>(e: E) -> Self {
        Self {
            msg: e.to_string(),
            disposition: Disposition::Permanent,
        }
    }

    /// Creates an error that ends the job as [`Discarded`](crate::JobState::Discarded), without
    /// further retries.
    ///
    /// The attempt counts and `last_error` keeps the message. Unlike
    /// [`permanent`](Self::permanent), the job is not `failed`: [`retry`](crate::admin::retry)
    /// and [`retry_failed`](crate::admin::retry_failed) leave it alone, and housekeeping
    /// removes it after
    /// [`deleted_retention`](crate::ClusterConfig::deleted_retention). A pending
    /// [`stop`](crate::stop) or [`cancel`](crate::cancel) still wins.
    ///
    /// `JobError` itself is not a `std::error::Error`, so middleware that turns a `JobError`
    /// into a discard wraps its text, for example in `io::Error::other(error.to_string())`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::io;
    ///
    /// use jalari::{JobError, JobResult};
    ///
    /// fn ship(order_cancelled: bool) -> JobResult {
    ///     if order_cancelled {
    ///         return Err(JobError::discard(io::Error::other("order cancelled before shipping")));
    ///     }
    ///     Ok(())
    /// }
    ///
    /// assert!(ship(true).unwrap_err().is_discarded());
    /// ```
    pub fn discard<E: std::error::Error>(e: E) -> Self {
        Self {
            msg: e.to_string(),
            disposition: Disposition::Discard,
        }
    }

    pub(crate) fn halted(halt: Halt) -> Self {
        Self {
            msg: halt.message().to_owned(),
            disposition: Disposition::Halt(halt),
        }
    }

    pub(crate) fn halt(&self) -> Option<Halt> {
        match self.disposition {
            Disposition::Halt(halt) => Some(halt),
            Disposition::Retry { .. }
            | Disposition::Permanent
            | Disposition::Discard
            | Disposition::Snooze { .. } => None,
        }
    }

    /// Retries the job after `delay` instead of the delay the retry policy picks.
    ///
    /// The attempt still counts and `last_error` keeps the message. The retry policy still
    /// decides whether there is a retry at all, so on the last attempt, or on a
    /// [`permanent`](Self::permanent) error, it has no effect. Use [`snooze`](crate::snooze) to
    /// wait without using an attempt. `delay` is measured by the database clock and capped at
    /// 100 years.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use jalari::{JobError, JobResult};
    ///
    /// fn check(status: u16, retry_after: Option<Duration>) -> JobResult {
    ///     if status == 429 {
    ///         let wait = retry_after.unwrap_or(Duration::from_secs(60));
    ///         return Err(JobError::new("rate limited".to_owned()).retry_after(wait));
    ///     }
    ///     Ok(())
    /// }
    ///
    /// let error = check(429, Some(Duration::from_secs(5))).unwrap_err();
    /// assert_eq!(error.retry_delay(), Some(Duration::from_secs(5)));
    /// ```
    #[must_use]
    pub fn retry_after(mut self, delay: Duration) -> Self {
        if let Disposition::Retry { .. } = self.disposition {
            self.disposition = Disposition::Retry { delay: Some(delay) };
        }
        self
    }

    /// The delay set with [`retry_after`](Self::retry_after), if any.
    pub fn retry_delay(&self) -> Option<Duration> {
        match self.disposition {
            Disposition::Retry { delay } => delay,
            Disposition::Permanent
            | Disposition::Discard
            | Disposition::Snooze { .. }
            | Disposition::Halt(_) => None,
        }
    }

    /// The delay passed to [`snooze`], or `None` for any other error.
    pub fn snoozed_for(&self) -> Option<Duration> {
        match self.disposition {
            Disposition::Snooze { delay } => Some(delay),
            Disposition::Retry { .. }
            | Disposition::Permanent
            | Disposition::Discard
            | Disposition::Halt(_) => None,
        }
    }

    /// The message stored in `last_error` and in the job history.
    pub fn msg(&self) -> &str {
        &self.msg
    }

    /// Whether the error ends the job as discarded.
    pub fn is_discarded(&self) -> bool {
        matches!(self.disposition, Disposition::Discard)
    }

    /// Whether retrying is pointless.
    pub fn is_permanent(&self) -> bool {
        matches!(self.disposition, Disposition::Permanent)
    }

    /// Whether the error came from a [`checkpoint`](crate::checkpoint) or
    /// [`interrupted`](crate::interrupted); middleware must pass such errors through unchanged.
    ///
    /// Returning one releases or stops the job without counting the attempt. Any other error
    /// counts as a failed attempt.
    pub fn is_interruption(&self) -> bool {
        matches!(self.disposition, Disposition::Halt(_))
    }
}

/// Ends the current attempt and runs the job again after `delay`, without counting the attempt.
///
/// Return it from [`Job::run`](crate::Job::run) or from a
/// [`JobMiddleware`](crate::JobMiddleware). The job keeps its saved progress, its `job_key` and
/// its attempt number, and its `queue_key` is free while it waits. A pending
/// [`stop`](crate::stop) or [`cancel`](crate::cancel) wins over the snooze. There is no limit on
/// how often a job snoozes, so a job that always snoozes never ends. `delay` is measured by the
/// database clock and capped at 100 years.
///
/// # Examples
///
/// ```rust,no_run
/// use std::time::Duration;
///
/// # async fn export_ready() -> Result<bool, std::io::Error> { Ok(true) }
/// # async fn download() -> jalari::JobResult { Ok(()) }
/// async fn run() -> jalari::JobResult {
///     if !export_ready().await? {
///         return jalari::snooze(Duration::from_secs(30));
///     }
///     download().await
/// }
/// ```
pub fn snooze(delay: Duration) -> JobResult {
    Err(JobError {
        msg: format!("snoozed for {delay:?}"),
        disposition: Disposition::Snooze { delay },
    })
}

impl<E: std::error::Error> From<E> for JobError {
    fn from(e: E) -> Self {
        Self::new(e.to_string())
    }
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_after_only_applies_to_retryable_errors() {
        let delay = Duration::from_secs(7);
        assert_eq!(
            JobError::new("boom".to_owned())
                .retry_after(delay)
                .retry_delay(),
            Some(delay)
        );
        assert_eq!(
            JobError::from(std::fmt::Error)
                .retry_after(delay)
                .retry_delay(),
            Some(delay)
        );
        assert_eq!(JobError::new("boom".to_owned()).retry_delay(), None);
        assert_eq!(
            JobError::permanent(std::fmt::Error)
                .retry_after(delay)
                .retry_delay(),
            None
        );
        assert_eq!(
            JobError::halted(Halt::Shutdown)
                .retry_after(delay)
                .retry_delay(),
            None
        );
        assert_eq!(
            snooze(delay).unwrap_err().retry_after(delay).retry_delay(),
            None
        );
        assert_eq!(
            JobError::discard(std::fmt::Error)
                .retry_after(delay)
                .retry_delay(),
            None
        );
    }

    #[test]
    fn test_dispositions_do_not_overlap() {
        let snoozed = snooze(Duration::from_secs(30)).unwrap_err();
        assert_eq!(snoozed.snoozed_for(), Some(Duration::from_secs(30)));
        assert!(!snoozed.is_permanent());
        assert!(!snoozed.is_interruption());
        assert_eq!(snoozed.halt(), None);
        assert_eq!(snoozed.msg(), "snoozed for 30s");
        assert!(!snoozed.is_discarded());

        let discarded = JobError::discard(std::fmt::Error);
        assert!(discarded.is_discarded());
        assert!(!discarded.is_permanent());
        assert!(!discarded.is_interruption());
        assert_eq!(discarded.snoozed_for(), None);
        assert_eq!(discarded.halt(), None);

        let permanent = JobError::permanent(std::fmt::Error);
        assert_eq!(permanent.snoozed_for(), None);
        assert!(!permanent.is_discarded());
        assert!(!permanent.is_interruption());

        let halted = JobError::halted(Halt::Shutdown);
        assert_eq!(halted.snoozed_for(), None);
        assert!(!halted.is_permanent());

        assert_eq!(JobError::new("boom".to_owned()).snoozed_for(), None);
    }

    #[test]
    fn test_retry_after_keeps_the_kind_of_error() {
        let delay = Duration::from_secs(7);
        assert!(
            JobError::permanent(std::fmt::Error)
                .retry_after(delay)
                .is_permanent()
        );
        assert_eq!(
            JobError::halted(Halt::Shutdown).retry_after(delay).halt(),
            Some(Halt::Shutdown)
        );
    }
}
