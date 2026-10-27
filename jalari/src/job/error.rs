use std::fmt;
use std::time::Duration;

use super::checkpoint::Halt;
use crate::JobResult;

pub(crate) const SNOOZED: &str = "the job asked to run again later";

/// Failure of one attempt of [`Job::run`](crate::Job::run).
///
/// Any `std::error::Error` converts into a retryable `JobError`, so `?` works inside `run`. Use
/// [`JobError::permanent`] for failures that retrying cannot fix, such as invalid input.
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
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobError {
    msg: String,
    disposition: Disposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Retry,
    Permanent,
    Halt(Halt),
}

impl JobError {
    /// Creates a retryable error with a message.
    pub fn new(msg: String) -> Self {
        Self {
            msg,
            disposition: Disposition::Retry,
        }
    }

    /// Creates an error that marks the job failed right away, without further retries.
    pub fn permanent<E: std::error::Error>(e: E) -> Self {
        Self {
            msg: e.to_string(),
            disposition: Disposition::Permanent,
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
            Disposition::Retry | Disposition::Permanent => None,
        }
    }

    /// The message stored in `last_error` and in the job history.
    pub fn msg(&self) -> &str {
        &self.msg
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
#[expect(unused_variables, reason = "filled in by W1")]
pub fn snooze(delay: Duration) -> JobResult {
    todo!("W1")
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
