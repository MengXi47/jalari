use std::str::FromStr;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::parser::{CronParser, Seconds, Year};

use crate::{Error, ErrorKind, Result};

/// Validated cron expression with the time zone it is evaluated in.
///
/// Accepts five fields (minute to weekday) or six with a leading seconds field. Times are
/// evaluated in the time zone, so daylight saving changes are handled: a repeated local hour
/// fires once and a skipped one still fires.
///
/// # Examples
///
/// ```rust
/// let every_five_minutes = jalari::Cron::new("*/5 * * * *")?;
/// let daily_at_three = jalari::Cron::new("0 0 3 * * *")?.timezone("Asia/Taipei")?;
/// assert_eq!(daily_at_three.timezone_name(), "Asia/Taipei");
/// # Ok::<(), jalari::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct Cron {
    expression: String,
    schedule: croner::Cron,
    timezone: Tz,
}

impl Cron {
    /// Parses an expression evaluated in UTC.
    ///
    /// # Errors
    ///
    /// Returns an error if the expression cannot be parsed
    /// ([`InvalidCronExpression`](ErrorKind::InvalidCronExpression)).
    pub fn new(expression: &str) -> Result<Self> {
        let schedule = CronParser::builder()
            .seconds(Seconds::Optional)
            .year(Year::Disallowed)
            .build()
            .parse(expression)
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidCronExpression,
                    format!("{expression:?}: {err}"),
                )
            })?;
        Ok(Self {
            expression: expression.to_owned(),
            schedule,
            timezone: Tz::UTC,
        })
    }

    /// Evaluates the expression in an IANA time zone such as `Asia/Taipei`.
    ///
    /// # Errors
    ///
    /// Returns an error if the name is not a known time zone
    /// ([`UnknownTimezone`](ErrorKind::UnknownTimezone)).
    pub fn timezone(mut self, timezone: &str) -> Result<Self> {
        self.timezone = Tz::from_str(timezone).map_err(|_| {
            Error::new(
                ErrorKind::UnknownTimezone,
                format!("{timezone:?} is not an IANA timezone"),
            )
        })?;
        Ok(self)
    }

    /// Expression as given to [`new`](Self::new).
    pub fn expression(&self) -> &str {
        &self.expression
    }

    /// IANA name of the time zone; `UTC` unless [`timezone`](Self::timezone) was called.
    pub fn timezone_name(&self) -> &str {
        self.timezone.name()
    }

    /// Returns the first run strictly after `after`.
    ///
    /// # Errors
    ///
    /// Returns an error if the expression never matches again, such as February 30th
    /// ([`InvalidCronExpression`](ErrorKind::InvalidCronExpression)).
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
        self.schedule
            .find_next_occurrence(&after.with_timezone(&self.timezone), false)
            .map(|next| next.with_timezone(&Utc))
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidCronExpression,
                    format!("{:?} has no next run: {err}", self.expression),
                )
            })
    }

    pub(crate) fn parse(expression: &str, timezone: &str) -> Result<Self> {
        Self::new(expression)?.timezone(timezone)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
    }

    #[test]
    fn test_accepts_five_and_six_fields() {
        let after = utc(2026, 10, 7, 0, 0, 0);
        let five = Cron::new("0 3 * * *").unwrap();
        let six = Cron::new("0 0 3 * * *").unwrap();
        assert_eq!(five.next_after(after).unwrap(), utc(2026, 10, 7, 3, 0, 0));
        assert_eq!(six.next_after(after).unwrap(), utc(2026, 10, 7, 3, 0, 0));

        let every_ten_seconds = Cron::new("*/10 * * * * *").unwrap();
        assert_eq!(
            every_ten_seconds
                .next_after(utc(2026, 10, 7, 0, 0, 1))
                .unwrap(),
            utc(2026, 10, 7, 0, 0, 10)
        );
    }

    #[test]
    fn test_rejects_invalid_expressions() {
        for expression in ["", "* * *", "61 * * * *", "0 0 3 * * * 2026", "not cron"] {
            let err = Cron::new(expression).unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidCronExpression, "{expression:?}");
        }
    }

    #[test]
    fn test_timezone_shifts_the_schedule() {
        let cron = Cron::new("0 0 3 * * *")
            .unwrap()
            .timezone("Asia/Taipei")
            .unwrap();
        assert_eq!(cron.timezone_name(), "Asia/Taipei");
        assert_eq!(
            cron.next_after(utc(2026, 10, 7, 0, 0, 0)).unwrap(),
            utc(2026, 10, 7, 19, 0, 0)
        );
    }

    #[test]
    fn test_daylight_saving_time_is_respected() {
        let cron = Cron::new("0 0 9 * * *")
            .unwrap()
            .timezone("America/New_York")
            .unwrap();
        assert_eq!(
            cron.next_after(utc(2026, 7, 1, 0, 0, 0)).unwrap(),
            utc(2026, 7, 1, 13, 0, 0)
        );
        assert_eq!(
            cron.next_after(utc(2026, 12, 1, 0, 0, 0)).unwrap(),
            utc(2026, 12, 1, 14, 0, 0)
        );
    }

    #[test]
    fn test_repeated_local_hour_runs_once() {
        let cron = Cron::new("0 30 1 * * *")
            .unwrap()
            .timezone("America/New_York")
            .unwrap();
        let mut runs = Vec::new();
        let mut after = utc(2026, 11, 1, 0, 0, 0);
        while after < utc(2026, 11, 2, 12, 0, 0) {
            after = cron.next_after(after).unwrap();
            runs.push(after);
        }
        let on_fall_back_day: Vec<_> = runs
            .iter()
            .filter(|run| run.date_naive() == utc(2026, 11, 1, 0, 0, 0).date_naive())
            .collect();
        assert_eq!(on_fall_back_day.len(), 1, "{runs:?}");
    }

    #[test]
    fn test_skipped_local_hour_still_runs() {
        let cron = Cron::new("0 30 2 * * *")
            .unwrap()
            .timezone("America/New_York")
            .unwrap();
        let next = cron.next_after(utc(2026, 3, 8, 0, 0, 0)).unwrap();
        assert!(next < utc(2026, 3, 9, 0, 0, 0), "{next}");
    }

    #[test]
    fn test_rejects_unknown_timezone() {
        let err = Cron::new("0 3 * * *")
            .unwrap()
            .timezone("Mars/Olympus")
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::UnknownTimezone);
    }
}
