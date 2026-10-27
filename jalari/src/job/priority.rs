/// How soon a due job runs compared with other due jobs of its queue.
///
/// Workers take the highest priority first, and the earliest `run_at` within a level. Priority
/// never makes a job run before its `run_at`, never interrupts a running job and is kept across
/// retries. The `priority` column stores the levels as -1, 0, 1 and 2, from [`Low`](Self::Low)
/// to [`Urgent`](Self::Urgent), for people who query the table directly.
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::{Job, JobResult, Priority};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct PasswordReset {
///     user_id: i64,
/// }
///
/// #[jalari::job(queue = "emails")]
/// impl Job for PasswordReset {
///     const NAME: &'static str = "password_reset";
///     const PRIORITY: Priority = Priority::Urgent;
///
///     async fn run(self) -> JobResult {
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Priority {
    /// Runs after every due job of a higher priority in its queue.
    Low,
    /// The default for every job that sets no priority.
    #[default]
    Normal,
    /// Runs before due `Normal` and `Low` jobs.
    High,
    /// Runs before every other due job of its queue; keep such jobs few.
    Urgent,
}

impl Priority {
    /// Every priority, from the highest to the lowest.
    pub const ALL: [Priority; 4] = [
        Priority::Urgent,
        Priority::High,
        Priority::Normal,
        Priority::Low,
    ];

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by enqueue and admin in the next commit")
    )]
    pub(crate) const fn level(self) -> i16 {
        match self {
            Self::Low => -1,
            Self::Normal => 0,
            Self::High => 1,
            Self::Urgent => 2,
        }
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by admin in the next commit")
    )]
    pub(crate) const fn from_level(level: i16) -> Self {
        match level {
            ..=-1 => Self::Low,
            0 => Self::Normal,
            1 => Self::High,
            _ => Self::Urgent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levels_round_trip_and_clamp() {
        for priority in Priority::ALL {
            assert_eq!(Priority::from_level(priority.level()), priority);
        }
        assert_eq!(Priority::from_level(-7), Priority::Low);
        assert_eq!(Priority::from_level(9), Priority::Urgent);
        assert_eq!(Priority::default().level(), 0);
    }

    #[test]
    fn test_all_runs_from_the_highest_level_down() {
        let levels: Vec<i16> = Priority::ALL
            .iter()
            .map(|priority| priority.level())
            .collect();
        assert_eq!(levels, [2, 1, 0, -1]);
        assert!(Priority::ALL.is_sorted_by(|higher, lower| higher > lower));
    }

    #[test]
    fn test_priority_levels_match_the_check() {
        let migration = include_str!("../../migrations/0009_job_options.sql");
        let low = Priority::ALL.iter().map(|priority| priority.level()).min();
        let high = Priority::ALL.iter().map(|priority| priority.level()).max();
        let (Some(low), Some(high)) = (low, high) else {
            panic!("Priority::ALL is empty");
        };
        assert_eq!(
            usize::try_from(high - low + 1).unwrap(),
            Priority::ALL.len()
        );
        for constraint in ["recurring_priority_check", "job_priority_check"] {
            let check = format!("{constraint} CHECK (priority BETWEEN {low} AND {high})");
            assert!(migration.contains(&check), "0009 defines {check}");
        }
        assert!(migration.contains(&format!("greatest({low}, least({high}, priority))")));
    }
}
