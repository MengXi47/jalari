use std::sync::atomic::{AtomicU8, Ordering};

use super::run::current_job;
use crate::{JobError, JobResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Command {
    Stop,
    Cancel,
}

impl Command {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "stop" => Some(Self::Stop),
            "cancel" => Some(Self::Cancel),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Cancel => "cancel",
        }
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Stop => "the job was asked to stop",
            Self::Cancel => "the job was asked to cancel",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Stop => 1,
            Self::Cancel => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Stop),
            2 => Some(Self::Cancel),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RunControl {
    command: AtomicU8,
}

impl RunControl {
    pub(crate) fn request(&self, command: Command) {
        self.command.fetch_max(command.code(), Ordering::SeqCst);
    }

    fn command(&self) -> Option<Command> {
        Command::from_code(self.command.load(Ordering::SeqCst))
    }
}

/// Returns an error once the running job is asked to stop, so it can unwind with `?`.
///
/// Call it between units of work. It never touches the database: it checks a flag the worker
/// sets within about a second of [`stop`](crate::stop) or [`cancel`](crate::cancel), and yields
/// once so a job that loops without other `.await`s can still be aborted. A job that never
/// calls it is aborted [`WorkerConfig::stop_timeout`](crate::WorkerConfig::stop_timeout) after
/// the request. Outside a job it does nothing and returns `Ok`.
///
/// # Errors
///
/// Returns an error if:
/// - The job was asked to stop or to cancel
///
/// # Examples
///
/// ```rust,no_run
/// # async fn export(id: i64) -> jalari::JobResult { Ok(()) }
/// # async fn example(ids: Vec<i64>) -> jalari::JobResult {
/// for id in ids {
///     export(id).await?;
///     jalari::checkpoint().await?;
/// }
/// # Ok(())
/// # }
/// ```
pub async fn checkpoint() -> JobResult {
    if let Some(command) = current_job().and_then(|job| job.control().command()) {
        return Err(JobError::new(command.message().to_owned()));
    }
    tokio::task::consume_budget().await;
    Ok(())
}
