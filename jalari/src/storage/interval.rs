use std::time::Duration;

pub(crate) fn interval(duration: Duration) -> Duration {
    Duration::from_micros(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
}
