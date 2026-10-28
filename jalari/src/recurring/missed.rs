use std::fmt;
use std::str::FromStr;

use crate::{Error, ErrorKind};

/// What the scheduler does with a run it finds more than a minute overdue, such as after an
/// outage or while no worker ran the scheduler.
///
/// A run less than 60 seconds late always fires, whatever the policy and however short the
/// schedule's period, so ordinary scheduler lateness never skips a run.
///
/// # Examples
///
/// ```rust
/// let missed: jalari::Missed = "skip".parse()?;
/// assert_eq!(missed, jalari::Missed::Skip);
/// assert_eq!(missed.to_string(), "skip");
/// assert_eq!(jalari::Missed::default(), jalari::Missed::Once);
/// # Ok::<(), jalari::Error>(())
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Missed {
    /// Enqueues one catch-up run for all the missed ones, then continues with the first run
    /// after now.
    #[default]
    Once,
    /// Enqueues nothing for the missed runs and continues with the first run after now. A
    /// skipped run does not count toward [`max_runs`](crate::Schedule::max_runs) and does not
    /// change [`last_run_at`](crate::RecurringInfo::last_run_at); a schedule whose
    /// [`until`](crate::Schedule::until) has passed finishes.
    Skip,
}

impl Missed {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Once => "once",
            Self::Skip => "skip",
        }
    }
}

impl fmt::Display for Missed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Missed {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        [Self::Once, Self::Skip]
            .into_iter()
            .find(|missed| missed.as_str() == text)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidSchedule,
                    format!("{text:?} is not a missed-run policy; expected \"once\" or \"skip\""),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_names_round_trip_and_unknown_names_fail() {
        for missed in [Missed::Once, Missed::Skip] {
            assert_eq!(missed.to_string().parse::<Missed>().unwrap(), missed);
        }
        for text in ["all", "Skip", ""] {
            let err = text.parse::<Missed>().unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidSchedule);
        }
    }
}
