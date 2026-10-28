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
