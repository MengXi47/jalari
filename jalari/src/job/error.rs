use std::fmt;

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
    permanent: bool,
}

impl JobError {
    /// Creates a retryable error with a message.
    pub fn new(msg: String) -> Self {
        Self {
            msg,
            permanent: false,
        }
    }

    /// Creates an error that marks the job failed right away, without further retries.
    pub fn permanent<E: std::error::Error>(e: E) -> Self {
        Self {
            msg: e.to_string(),
            permanent: true,
        }
    }

    /// The message stored in `last_error` and in the job history.
    pub fn msg(&self) -> &str {
        &self.msg
    }

    /// Whether retrying is pointless.
    pub fn is_permanent(&self) -> bool {
        self.permanent
    }
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
