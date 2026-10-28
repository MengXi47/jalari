use std::fmt;
use std::str::FromStr;

use chrono::{
    DateTime, Datelike, LocalResult, Months, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone,
    Timelike, Utc,
};
use chrono_tz::Tz;

use super::Missed;
use crate::{Cron, Error, ErrorKind, Result};

/// Unit of an [`Interval`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalUnit {
    /// Counted in absolute time.
    Seconds,
    /// Counted in absolute time.
    Minutes,
    /// Counted in absolute time.
    Hours,
    /// Counted on the local calendar; the run keeps the anchor's wall-clock time.
    Days,
    /// Counted on the local calendar; the run keeps the anchor's wall-clock time.
    Weeks,
    /// Counted on the local calendar; a day past the end of a shorter month is moved to its
    /// last day.
    Months,
    /// Counted on the local calendar; February 29th moves to the 28th in a common year.
    Years,
}

impl IntervalUnit {
    const ALL: [Self; 7] = [
        Self::Seconds,
        Self::Minutes,
        Self::Hours,
        Self::Days,
        Self::Weeks,
        Self::Months,
        Self::Years,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Seconds => "seconds",
            Self::Minutes => "minutes",
            Self::Hours => "hours",
            Self::Days => "days",
            Self::Weeks => "weeks",
            Self::Months => "months",
            Self::Years => "years",
        }
    }

    pub(crate) fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|unit| unit.as_str() == name)
    }

    fn seconds(self) -> Option<i64> {
        match self {
            Self::Seconds => Some(1),
            Self::Minutes => Some(60),
            Self::Hours => Some(3600),
            Self::Days | Self::Weeks | Self::Months | Self::Years => None,
        }
    }

    fn days(self) -> Option<i64> {
        match self {
            Self::Days => Some(1),
            Self::Weeks => Some(7),
            Self::Seconds | Self::Minutes | Self::Hours | Self::Months | Self::Years => None,
        }
    }

    fn months(self) -> i64 {
        match self {
            Self::Years => 12,
            _ => 1,
        }
    }
}

/// Runs every `count` units from an anchor, such as every 5 days from October 3rd at 09:00.
///
/// The anchor is a local time in the interval's time zone, `UTC` unless
/// [`timezone`](Self::timezone) is called. Seconds, minutes and hours count absolute time from
/// the anchor. Days, weeks, months and years count on the local calendar, so a run keeps the
/// anchor's wall-clock time across daylight saving changes: a repeated local hour runs once, at
/// its first occurrence, and a skipped one runs at the first valid time after the gap, as a
/// [`Cron`] does. A monthly or yearly run whose day does not exist in the target month moves to
/// that month's last day, and later months use the anchor's day again, so every month from
/// January 31st runs on February 28th or 29th and then on March 31st.
///
/// Each next run is computed from the anchor, never by stepping through the runs an outage
/// missed, so a long outage costs nothing. A run found overdue after an outage follows the
/// schedule's [`Missed`](crate::Missed) policy: one catch-up run under the default
/// [`Missed::Once`](crate::Missed::Once), none under [`Missed::Skip`](crate::Missed::Skip).
/// Either way the run after it is the first one after now that is aligned with the anchor.
///
/// # Examples
///
/// ```rust
/// use chrono::NaiveDate;
///
/// let anchor = NaiveDate::from_ymd_opt(2026, 10, 3).unwrap().and_hms_opt(9, 0, 0).unwrap();
/// let interval = jalari::Interval::new(5, jalari::IntervalUnit::Days, anchor)?
///     .timezone("Asia/Taipei")?;
/// assert_eq!(interval.count(), 5);
/// # Ok::<(), jalari::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval {
    count: u32,
    unit: IntervalUnit,
    anchor: NaiveDateTime,
    timezone: Tz,
}

impl Interval {
    /// Creates an interval of `count` units from `anchor`, a local time in UTC.
    ///
    /// # Errors
    ///
    /// Returns an error if `count` is zero ([`InvalidInterval`](ErrorKind::InvalidInterval)).
    pub fn new(count: u32, unit: IntervalUnit, anchor: NaiveDateTime) -> Result<Self> {
        if count == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInterval,
                "an interval must be at least one unit long".to_owned(),
            ));
        }
        if i32::try_from(count).is_err() {
            return Err(Error::new(
                ErrorKind::InvalidInterval,
                format!("an interval of {count} units is longer than the database can store"),
            ));
        }
        let anchor = truncate_to_microseconds(anchor);
        Ok(Self {
            count,
            unit,
            anchor,
            timezone: Tz::UTC,
        })
    }

    /// Reads the anchor as a local time in an IANA time zone such as `Asia/Taipei`.
    ///
    /// # Errors
    ///
    /// Returns an error if the name is not a known time zone
    /// ([`UnknownTimezone`](ErrorKind::UnknownTimezone)).
    pub fn timezone(mut self, timezone: &str) -> Result<Self> {
        self.timezone = parse_timezone(timezone)?;
        Ok(self)
    }

    /// Number of units between runs.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Unit the runs are counted in.
    pub fn unit(&self) -> IntervalUnit {
        self.unit
    }

    /// Local time the runs are aligned with, as given to [`new`](Self::new) and truncated to
    /// whole microseconds, the precision the database keeps.
    pub fn anchor(&self) -> NaiveDateTime {
        self.anchor
    }

    /// IANA name of the time zone; `UTC` unless [`timezone`](Self::timezone) was called.
    pub fn timezone_name(&self) -> &str {
        self.timezone.name()
    }

    /// Returns the first run strictly after `after`.
    ///
    /// # Errors
    ///
    /// Returns an error if the run falls outside the range of representable dates
    /// ([`InvalidInterval`](ErrorKind::InvalidInterval)).
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
        let count = i64::from(self.count);
        if let Some(seconds) = self.unit.seconds() {
            let step = count * seconds;
            let start = resolve(self.timezone, self.anchor)?;
            if after < start {
                return Ok(start);
            }
            let rounds = (after - start).num_seconds() / step + 1;
            return rounds
                .checked_mul(step)
                .and_then(|offset| start.checked_add_signed(TimeDelta::seconds(offset)))
                .ok_or_else(|| self.out_of_range());
        }
        let after_local = after.with_timezone(&self.timezone).naive_local();
        let elapsed = match self.unit.days() {
            Some(days) => (after_local.date() - self.anchor.date()).num_days() / (count * days),
            None => {
                months_between(self.anchor.date(), after_local.date())
                    / (count * self.unit.months())
            }
        };
        let first = elapsed.max(0);
        for round in first..first + 3 {
            let candidate = resolve(self.timezone, self.occurrence(round)?)?;
            if candidate > after {
                return Ok(candidate);
            }
        }
        Err(self.out_of_range())
    }

    fn occurrence(&self, round: i64) -> Result<NaiveDateTime> {
        let count = i64::from(self.count);
        let date = match self.unit.days() {
            Some(days) => round.checked_mul(count * days).and_then(|offset| {
                self.anchor
                    .date()
                    .checked_add_signed(TimeDelta::days(offset))
            }),
            None => round
                .checked_mul(count * self.unit.months())
                .and_then(|months| u32::try_from(months).ok())
                .and_then(|months| self.anchor.date().checked_add_months(Months::new(months))),
        };
        date.map(|date| date.and_time(self.anchor.time()))
            .ok_or_else(|| self.out_of_range())
    }

    fn out_of_range(&self) -> Error {
        Error::new(
            ErrorKind::InvalidInterval,
            format!("{self} has no next run within the representable dates"),
        )
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "every {} {} from {} {}",
            self.count,
            self.unit.as_str(),
            self.anchor.format("%Y-%m-%dT%H:%M:%S"),
            self.timezone.name()
        )
    }
}

/// When a recurring job runs: a [`Cron`] or an [`Interval`], optionally ending at a time or
/// after a number of runs.
///
/// Both end conditions count only runs the scheduler enqueued;
/// [`trigger`](crate::recurring::trigger) does not count. A schedule that reaches its end is
/// finished: it never fires again, [`RecurringInfo`](crate::RecurringInfo) reports when, and
/// [`resume`](crate::recurring::resume) does not revive it. Changing the cron or interval with
/// [`add_or_update`](crate::recurring::add_or_update) or
/// [`update_schedule`](crate::recurring::update_schedule) starts counting again from zero;
/// changing only the end conditions keeps the count, so raising `max_runs` revives a finished
/// schedule for the difference. Times are kept to whole microseconds, as the database stores
/// them.
///
/// # Examples
///
/// ```rust
/// use chrono::{NaiveDate, TimeZone, Utc};
///
/// let anchor = NaiveDate::from_ymd_opt(2026, 10, 3).unwrap().and_hms_opt(9, 0, 0).unwrap();
/// let interval = jalari::Interval::new(5, jalari::IntervalUnit::Days, anchor)?;
/// let schedule = jalari::Schedule::from(interval)
///     .until(Utc.with_ymd_and_hms(2026, 12, 31, 0, 0, 0).unwrap())
///     .max_runs(10)?;
/// assert_eq!(schedule.run_limit(), Some(10));
///
/// let nightly = jalari::Schedule::from(jalari::Cron::new("0 0 3 * * *")?);
/// assert!(nightly.cron().is_some());
/// # Ok::<(), jalari::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    rule: Rule,
    until: Option<DateTime<Utc>>,
    max_runs: Option<u32>,
    missed: Missed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Rule {
    Cron(Box<Cron>),
    Interval(Interval),
}

impl Schedule {
    /// Stops the schedule after the last run at or before `until`.
    pub fn until(mut self, until: DateTime<Utc>) -> Self {
        let truncated = truncate_to_microseconds(until.naive_utc()).and_utc();
        self.until = Some(truncated);
        self
    }

    /// Stops the schedule once it has enqueued `max_runs` runs.
    ///
    /// # Errors
    ///
    /// Returns an error if `max_runs` is zero ([`InvalidSchedule`](ErrorKind::InvalidSchedule)).
    pub fn max_runs(mut self, max_runs: u32) -> Result<Self> {
        if max_runs == 0 {
            return Err(Error::new(
                ErrorKind::InvalidSchedule,
                "a schedule must allow at least one run".to_owned(),
            ));
        }
        if i32::try_from(max_runs).is_err() {
            return Err(Error::new(
                ErrorKind::InvalidSchedule,
                format!("a run limit of {max_runs} is higher than the database can store"),
            ));
        }
        self.max_runs = Some(max_runs);
        Ok(self)
    }

    /// Sets what the scheduler does with a run it finds more than a minute overdue;
    /// [`Missed::Once`] unless called.
    ///
    /// Changing only this keeps the schedule's run count, next run and finished state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let hourly = jalari::Schedule::from(jalari::Cron::new("0 0 * * * *")?)
    ///     .missed(jalari::Missed::Skip);
    /// assert_ne!(hourly, jalari::Schedule::from(jalari::Cron::new("0 0 * * * *")?));
    /// # Ok::<(), jalari::Error>(())
    /// ```
    pub fn missed(mut self, missed: Missed) -> Self {
        self.missed = missed;
        self
    }

    pub(crate) fn missed_policy(&self) -> Missed {
        self.missed
    }

    /// The cron expression, if the schedule is one.
    pub fn cron(&self) -> Option<&Cron> {
        match &self.rule {
            Rule::Cron(cron) => Some(cron),
            Rule::Interval(_) => None,
        }
    }

    /// The interval, if the schedule is one.
    pub fn interval(&self) -> Option<&Interval> {
        match &self.rule {
            Rule::Interval(interval) => Some(interval),
            Rule::Cron(_) => None,
        }
    }

    /// Time of the last possible run, if [`until`](Self::until) was set.
    pub fn ends_at(&self) -> Option<DateTime<Utc>> {
        self.until
    }

    /// Number of runs after which the schedule finishes, if [`max_runs`](Self::max_runs) was
    /// set.
    pub fn run_limit(&self) -> Option<u32> {
        self.max_runs
    }

    /// IANA name of the time zone the schedule is evaluated in.
    pub fn timezone_name(&self) -> &str {
        match &self.rule {
            Rule::Cron(cron) => cron.timezone_name(),
            Rule::Interval(interval) => interval.timezone_name(),
        }
    }

    /// Returns the first run strictly after `after`, ignoring the end conditions.
    ///
    /// # Errors
    ///
    /// Returns an error under the conditions of [`Cron::next_after`] or
    /// [`Interval::next_after`].
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
        match &self.rule {
            Rule::Cron(cron) => cron.next_after(after),
            Rule::Interval(interval) => interval.next_after(after),
        }
    }

    pub(crate) fn next_run(
        &self,
        after: DateTime<Utc>,
        run_count: i32,
    ) -> Result<Option<DateTime<Utc>>> {
        let next = self.next_after(after)?;
        Ok(self.unless_ended(next, run_count))
    }

    pub(crate) fn unless_ended(
        &self,
        next: DateTime<Utc>,
        run_count: i32,
    ) -> Option<DateTime<Utc>> {
        let runs_left = self
            .max_runs
            .is_none_or(|max_runs| u32::try_from(run_count).unwrap_or(0) < max_runs);
        let in_time = self.until.is_none_or(|until| next <= until);
        (runs_left && in_time).then_some(next)
    }
}

impl From<Cron> for Schedule {
    fn from(cron: Cron) -> Self {
        Self {
            rule: Rule::Cron(Box::new(cron)),
            until: None,
            max_runs: None,
            missed: Missed::Once,
        }
    }
}

impl From<Interval> for Schedule {
    fn from(interval: Interval) -> Self {
        Self {
            rule: Rule::Interval(interval),
            until: None,
            max_runs: None,
            missed: Missed::Once,
        }
    }
}

impl fmt::Display for Schedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.rule {
            Rule::Cron(cron) => write!(f, "{} {}", cron.expression(), cron.timezone_name())?,
            Rule::Interval(interval) => write!(f, "{interval}")?,
        }
        if let Some(until) = self.until {
            write!(f, " until {}", until.format("%Y-%m-%dT%H:%M:%SZ"))?;
        }
        if let Some(max_runs) = self.max_runs {
            write!(f, " for {max_runs} runs")?;
        }
        Ok(())
    }
}

pub(crate) fn parse_timezone(timezone: &str) -> Result<Tz> {
    Tz::from_str(timezone).map_err(|_| {
        Error::new(
            ErrorKind::UnknownTimezone,
            format!("{timezone:?} is not an IANA timezone"),
        )
    })
}

fn truncate_to_microseconds(time: NaiveDateTime) -> NaiveDateTime {
    time.with_nanosecond(time.nanosecond() / 1000 * 1000)
        .unwrap_or(time)
}

fn months_between(from: NaiveDate, to: NaiveDate) -> i64 {
    (i64::from(to.year()) - i64::from(from.year())) * 12 + i64::from(to.month())
        - i64::from(from.month())
}

fn resolve(timezone: Tz, local: NaiveDateTime) -> Result<DateTime<Utc>> {
    match timezone.from_local_datetime(&local) {
        LocalResult::Single(instant) => Ok(instant.to_utc()),
        LocalResult::Ambiguous(earliest, _) => Ok(earliest.to_utc()),
        LocalResult::None => Ok(after_gap(timezone, local)),
    }
}

fn after_gap(timezone: Tz, local: NaiveDateTime) -> DateTime<Utc> {
    let as_utc = local.and_utc();
    let first = offset_seconds(timezone, as_utc);
    let second = offset_seconds(timezone, as_utc - TimeDelta::seconds(first));
    let (before, after) = (first.min(second), first.max(second));
    let mut low = as_utc - TimeDelta::seconds(after);
    let mut high = as_utc - TimeDelta::seconds(before);
    while high - low > TimeDelta::seconds(1) {
        let middle = low + (high - low) / 2;
        if offset_seconds(timezone, middle) == after {
            high = middle;
        } else {
            low = middle;
        }
    }
    high
}

fn offset_seconds(timezone: Tz, instant: DateTime<Utc>) -> i64 {
    i64::from(
        timezone
            .offset_from_utc_datetime(&instant.naive_utc())
            .fix()
            .local_minus_utc(),
    )
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Timelike};

    use super::*;

    fn local(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
    }

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
    }

    fn interval(count: u32, unit: IntervalUnit, anchor: NaiveDateTime, timezone: &str) -> Interval {
        Interval::new(count, unit, anchor)
            .unwrap()
            .timezone(timezone)
            .unwrap()
    }

    fn new_york(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        chrono_tz::America::New_York
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .to_utc()
    }

    #[test]
    fn test_rejects_zero_count_and_unknown_timezone() {
        let anchor = local(2026, 1, 1, 0, 0);
        let err = Interval::new(0, IntervalUnit::Days, anchor).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidInterval);
        let err = Interval::new(1, IntervalUnit::Days, anchor)
            .unwrap()
            .timezone("Mars/Olympus")
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::UnknownTimezone);
        let err = Schedule::from(Interval::new(1, IntervalUnit::Days, anchor).unwrap())
            .max_runs(0)
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidSchedule);
        let err = Interval::new(u32::MAX, IntervalUnit::Days, anchor).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidInterval);
        let err = Schedule::from(Interval::new(1, IntervalUnit::Days, anchor).unwrap())
            .max_runs(u32::MAX)
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidSchedule);
    }

    #[test]
    fn test_times_are_kept_to_whole_microseconds() {
        let anchor = local(2026, 1, 1, 0, 0)
            .with_nanosecond(123_456_789)
            .unwrap();
        let interval = Interval::new(1, IntervalUnit::Days, anchor).unwrap();
        assert_eq!(interval.anchor().nanosecond(), 123_456_000);
        let until = utc(2026, 1, 2, 0, 0).with_nanosecond(999_999_999).unwrap();
        let schedule = Schedule::from(interval).until(until);
        assert_eq!(schedule.ends_at().unwrap().nanosecond(), 999_999_000);
    }

    #[test]
    fn test_seconds_count_absolute_time_from_the_anchor() {
        let every = interval(
            90,
            IntervalUnit::Seconds,
            local(2026, 10, 3, 9, 0),
            "Asia/Taipei",
        );
        let anchor = utc(2026, 10, 3, 1, 0);
        assert_eq!(every.next_after(utc(2026, 1, 1, 0, 0)).unwrap(), anchor);
        assert_eq!(
            every.next_after(anchor).unwrap(),
            anchor + TimeDelta::seconds(90)
        );
        assert_eq!(
            every.next_after(anchor + TimeDelta::seconds(89)).unwrap(),
            anchor + TimeDelta::seconds(90)
        );
        assert_eq!(
            every.next_after(utc(2026, 10, 4, 1, 0)).unwrap(),
            utc(2026, 10, 4, 1, 1) + TimeDelta::seconds(30)
        );
    }

    #[test]
    fn test_hours_keep_absolute_spacing_across_daylight_saving() {
        let every = interval(
            6,
            IntervalUnit::Hours,
            local(2026, 3, 7, 20, 0),
            "America/New_York",
        );
        let anchor = new_york(2026, 3, 7, 20, 0);
        let next = every.next_after(anchor).unwrap();
        assert_eq!(next, anchor + TimeDelta::hours(6));
        assert_eq!(next.with_timezone(&chrono_tz::America::New_York).hour(), 3);
    }

    #[test]
    fn test_days_align_with_the_anchor_not_the_month() {
        let every = interval(
            5,
            IntervalUnit::Days,
            local(2026, 10, 3, 9, 0),
            "Asia/Taipei",
        );
        assert_eq!(
            every.next_after(utc(2026, 10, 29, 0, 0)).unwrap(),
            utc(2026, 11, 2, 1, 0)
        );
        assert_eq!(
            every.next_after(utc(2026, 11, 2, 1, 0)).unwrap(),
            utc(2026, 11, 7, 1, 0)
        );
        assert_eq!(
            every.next_after(utc(2026, 11, 2, 0, 59)).unwrap(),
            utc(2026, 11, 2, 1, 0)
        );
    }

    #[test]
    fn test_weeks_count_seven_days() {
        let every = interval(2, IntervalUnit::Weeks, local(2026, 1, 5, 8, 0), "UTC");
        assert_eq!(
            every.next_after(utc(2026, 1, 5, 8, 0)).unwrap(),
            utc(2026, 1, 19, 8, 0)
        );
        assert_eq!(
            every.next_after(utc(2026, 6, 1, 0, 0)).unwrap(),
            utc(2026, 6, 8, 8, 0)
        );
        assert_eq!(
            every.next_after(utc(2026, 6, 8, 7, 59)).unwrap(),
            utc(2026, 6, 8, 8, 0)
        );
    }

    #[test]
    fn test_months_clamp_to_the_month_end_and_recover() {
        let every = interval(1, IntervalUnit::Months, local(2026, 1, 31, 12, 0), "UTC");
        let mut after = utc(2026, 1, 31, 12, 0);
        let mut runs = Vec::new();
        for _ in 0..4 {
            after = every.next_after(after).unwrap();
            runs.push(after);
        }
        assert_eq!(
            runs,
            [
                utc(2026, 2, 28, 12, 0),
                utc(2026, 3, 31, 12, 0),
                utc(2026, 4, 30, 12, 0),
                utc(2026, 5, 31, 12, 0),
            ]
        );
        let leap = interval(1, IntervalUnit::Months, local(2028, 1, 31, 12, 0), "UTC");
        assert_eq!(
            leap.next_after(utc(2028, 1, 31, 12, 0)).unwrap(),
            utc(2028, 2, 29, 12, 0)
        );
    }

    #[test]
    fn test_years_handle_leap_day() {
        let every = interval(1, IntervalUnit::Years, local(2028, 2, 29, 6, 0), "UTC");
        assert_eq!(
            every.next_after(utc(2028, 2, 29, 6, 0)).unwrap(),
            utc(2029, 2, 28, 6, 0)
        );
        assert_eq!(
            every.next_after(utc(2031, 3, 1, 0, 0)).unwrap(),
            utc(2032, 2, 29, 6, 0)
        );
        let every_four = interval(4, IntervalUnit::Years, local(2028, 2, 29, 6, 0), "UTC");
        assert_eq!(
            every_four.next_after(utc(2028, 3, 1, 0, 0)).unwrap(),
            utc(2032, 2, 29, 6, 0)
        );
    }

    #[test]
    fn test_days_keep_wall_clock_time_across_daylight_saving() {
        let every = interval(
            1,
            IntervalUnit::Days,
            local(2026, 3, 7, 9, 0),
            "America/New_York",
        );
        let saturday = new_york(2026, 3, 7, 9, 0);
        let sunday = every.next_after(saturday).unwrap();
        assert_eq!(sunday, new_york(2026, 3, 8, 9, 0));
        assert_eq!(sunday - saturday, TimeDelta::hours(23));
    }

    #[test]
    fn test_skipped_local_time_runs_after_the_gap() {
        let every = interval(
            1,
            IntervalUnit::Days,
            local(2026, 3, 7, 2, 30),
            "America/New_York",
        );
        let next = every.next_after(new_york(2026, 3, 7, 2, 30)).unwrap();
        assert_eq!(next, new_york(2026, 3, 8, 3, 0));
        assert_eq!(every.next_after(next).unwrap(), new_york(2026, 3, 9, 2, 30));
        let berlin = interval(
            1,
            IntervalUnit::Days,
            local(2026, 3, 28, 2, 30),
            "Europe/Berlin",
        );
        let next = berlin.next_after(utc(2026, 3, 28, 1, 30)).unwrap();
        assert_eq!(next, utc(2026, 3, 29, 1, 0));
    }

    #[test]
    fn test_repeated_local_time_runs_once() {
        let every = interval(
            1,
            IntervalUnit::Days,
            local(2026, 10, 31, 1, 30),
            "America/New_York",
        );
        let saturday = new_york(2026, 10, 31, 1, 30);
        let sunday = every.next_after(saturday).unwrap();
        assert_eq!(sunday, utc(2026, 11, 1, 5, 30));
        assert_eq!(
            every.next_after(sunday).unwrap(),
            new_york(2026, 11, 2, 1, 30)
        );
    }

    #[test]
    fn test_anchor_in_a_gap_or_overlap_resolves_like_later_runs() {
        let gap = interval(
            1,
            IntervalUnit::Days,
            local(2026, 3, 8, 2, 30),
            "America/New_York",
        );
        assert_eq!(
            gap.next_after(utc(2026, 1, 1, 0, 0)).unwrap(),
            new_york(2026, 3, 8, 3, 0)
        );
        let overlap = interval(
            1,
            IntervalUnit::Days,
            local(2026, 11, 1, 1, 30),
            "America/New_York",
        );
        assert_eq!(
            overlap.next_after(utc(2026, 1, 1, 0, 0)).unwrap(),
            utc(2026, 11, 1, 5, 30)
        );
        assert_eq!(
            overlap.next_after(utc(2026, 11, 1, 5, 30)).unwrap(),
            new_york(2026, 11, 2, 1, 30)
        );
    }

    #[test]
    fn test_missed_rounds_are_skipped_and_stay_aligned() {
        let every = interval(3, IntervalUnit::Days, local(2026, 1, 1, 0, 0), "UTC");
        assert_eq!(
            every.next_after(utc(2026, 2, 20, 12, 0)).unwrap(),
            utc(2026, 2, 21, 0, 0)
        );
        let hourly = interval(7, IntervalUnit::Hours, local(2026, 1, 1, 0, 0), "UTC");
        assert_eq!(
            hourly.next_after(utc(2026, 3, 1, 0, 0)).unwrap(),
            utc(2026, 3, 1, 5, 0)
        );
        let monthly = interval(5, IntervalUnit::Months, local(2026, 1, 15, 0, 0), "UTC");
        assert_eq!(
            monthly.next_after(utc(2027, 12, 1, 0, 0)).unwrap(),
            utc(2028, 2, 15, 0, 0)
        );
    }

    #[test]
    fn test_runs_before_the_anchor_start_at_the_anchor() {
        let every = interval(1, IntervalUnit::Months, local(2026, 6, 15, 0, 0), "UTC");
        assert_eq!(
            every.next_after(utc(2020, 1, 1, 0, 0)).unwrap(),
            utc(2026, 6, 15, 0, 0)
        );
        assert_eq!(
            every.next_after(utc(2026, 6, 14, 23, 59)).unwrap(),
            utc(2026, 6, 15, 0, 0)
        );
    }

    #[test]
    fn test_far_future_runs_report_an_error() {
        let every = interval(
            u32::try_from(i32::MAX).unwrap(),
            IntervalUnit::Years,
            local(2026, 1, 1, 0, 0),
            "UTC",
        );
        let err = every.next_after(utc(2026, 1, 1, 0, 0)).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidInterval);
    }

    #[test]
    fn test_until_and_max_runs_end_the_schedule() {
        let every = interval(1, IntervalUnit::Days, local(2026, 1, 1, 0, 0), "UTC");
        let schedule = Schedule::from(every.clone())
            .until(utc(2026, 1, 3, 0, 0))
            .max_runs(5)
            .unwrap();
        assert_eq!(
            schedule.next_run(utc(2026, 1, 1, 12, 0), 0).unwrap(),
            Some(utc(2026, 1, 2, 0, 0))
        );
        assert_eq!(
            schedule.next_run(utc(2026, 1, 2, 0, 0), 1).unwrap(),
            Some(utc(2026, 1, 3, 0, 0))
        );
        assert_eq!(schedule.next_run(utc(2026, 1, 3, 0, 0), 2).unwrap(), None);
        assert_eq!(schedule.next_run(utc(2026, 1, 1, 12, 0), 5).unwrap(), None);
        assert_eq!(
            schedule.next_run(utc(2026, 1, 1, 12, 0), 4).unwrap(),
            Some(utc(2026, 1, 2, 0, 0))
        );
        let unbounded = Schedule::from(every);
        assert_eq!(
            unbounded.next_run(utc(2030, 1, 1, 0, 0), i32::MAX).unwrap(),
            Some(utc(2030, 1, 2, 0, 0))
        );
    }

    #[test]
    fn test_display_names_the_rule_and_its_end() {
        let anchor = local(2026, 10, 3, 9, 0);
        let schedule = Schedule::from(interval(5, IntervalUnit::Days, anchor, "Asia/Taipei"))
            .until(utc(2026, 12, 31, 0, 0))
            .max_runs(10)
            .unwrap();
        assert_eq!(
            schedule.to_string(),
            "every 5 days from 2026-10-03T09:00:00 Asia/Taipei until 2026-12-31T00:00:00Z \
             for 10 runs"
        );
        let cron = Schedule::from(Cron::new("0 3 * * *").unwrap());
        assert_eq!(cron.to_string(), "0 3 * * * UTC");
    }

    #[test]
    fn test_unit_names_round_trip() {
        for unit in IntervalUnit::ALL {
            assert_eq!(IntervalUnit::parse(unit.as_str()), Some(unit));
        }
        assert_eq!(IntervalUnit::parse("fortnights"), None);
    }
}
