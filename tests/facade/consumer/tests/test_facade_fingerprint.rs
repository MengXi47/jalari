//! The jobs of `facade_consumer`, written against jalari directly.

use std::sync::Mutex;

use jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

/// Words that [`Echo`] jobs have run with, in order.
pub static ECHOED: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Appends its word to [`ECHOED`].
#[derive(Serialize, Deserialize)]
pub struct Echo {
    /// The word to append.
    pub word: String,
}

#[jalari::job(name = "direct_echo")]
impl Job for Echo {
    async fn run(self) -> JobResult {
        ECHOED.lock().unwrap().push(self.word);
        Ok(())
    }
}

/// A recurring job that sets every argument of the attribute.
#[derive(Serialize, Deserialize, Default)]
pub struct Nightly;

#[jalari::job(
    name = "direct_nightly",
    queue = "facade_nightly",
    max_attempts = 3,
    timeout = 30,
    priority = "high",
    aliases = ["direct_nightly_old"],
    cron = "0 0 3 * * *",
    timezone = "Asia/Taipei",
    missed = "skip",
    fingerprint = true,
    checkpoints = true
)]
impl Job for Nightly {
    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct WrappedEcho {
    pub word: String,
}

#[jalari::job(name = "direct_wrapped_echo", fingerprint = true)]
impl Job for WrappedEcho {
    async fn run(self) -> JobResult {
        ECHOED.lock().unwrap().push(self.word);
        Ok(())
    }
}

#[test]
fn test_fingerprint_equals_the_same_job_written_against_jalari() {
    let fingerprint = |name: &str| {
        jalari::__private::find(name)
            .unwrap_or_else(|| panic!("{name} is registered"))
            .fingerprint
    };
    let through_facade = fingerprint(<facade_consumer::Nightly as Job>::NAME);
    assert_eq!(through_facade.map(str::len), Some(32));
    assert_eq!(through_facade, fingerprint(<Nightly as Job>::NAME));
}

#[test]
fn test_wrapper_job_fingerprint_equals_the_same_job_written_against_jalari() {
    let fingerprint = |name: &str| {
        jalari::__private::find(name)
            .unwrap_or_else(|| panic!("{name} is registered"))
            .fingerprint
    };
    let through_wrapper = fingerprint(<facade_consumer::WrappedEcho as Job>::NAME);
    assert_eq!(through_wrapper.map(str::len), Some(32));
    assert_eq!(through_wrapper, fingerprint(<WrappedEcho as Job>::NAME));
}

#[derive(Serialize, Deserialize)]
pub struct AttrEcho {
    pub word: String,
}

#[jalari::job(name = "direct_attr_echo", fingerprint = true)]
impl Job for AttrEcho {
    async fn run(self) -> JobResult {
        ECHOED.lock().unwrap().push(self.word);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct AttrNightly;

#[jalari::job(
    name = "direct_attr_nightly",
    queue = "facade_attr_nightly",
    max_attempts = 3,
    timeout = 30,
    priority = "high",
    aliases = ["direct_attr_nightly_old"],
    cron = "0 0 3 * * *",
    timezone = "Asia/Taipei",
    missed = "skip",
    fingerprint = true,
    checkpoints = true
)]
impl Job for AttrNightly {
    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[test]
fn test_attribute_wrapper_job_fingerprint_equals_the_same_job_written_against_jalari() {
    let fingerprint = |name: &str| {
        jalari::__private::find(name)
            .unwrap_or_else(|| panic!("{name} is registered"))
            .fingerprint
    };
    for (through_attribute, direct) in [
        (
            <facade_consumer::AttrEcho as Job>::NAME,
            <AttrEcho as Job>::NAME,
        ),
        (
            <facade_consumer::AttrNightly as Job>::NAME,
            <AttrNightly as Job>::NAME,
        ),
    ] {
        let value = fingerprint(through_attribute);
        assert_eq!(value.map(str::len), Some(32), "{through_attribute}");
        assert_eq!(value, fingerprint(direct), "{through_attribute}");
    }
}
