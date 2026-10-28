//! Jobs defined by a crate that reaches jalari only through `facade`.

use std::sync::Mutex;

use facade::__jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

/// Words that [`Echo`] jobs have run with, in order.
pub static ECHOED: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Appends its word to [`ECHOED`].
#[derive(Serialize, Deserialize)]
pub struct Echo {
    /// The word to append.
    pub word: String,
}

#[facade::__jalari::job(name = "facade_echo", crate = "::facade::__jalari")]
impl Job for Echo {
    async fn run(self) -> JobResult {
        ECHOED.lock().unwrap().push(self.word);
        Ok(())
    }
}

/// Appends its word to [`ECHOED`], declared through the `facade::job!` wrapper.
#[derive(Serialize, Deserialize)]
pub struct WrappedEcho {
    /// The word to append.
    pub word: String,
}

facade::job! {
    (name = "facade_wrapped_echo", fingerprint = true)
    impl Job for WrappedEcho {
        async fn run(self) -> JobResult {
            ECHOED.lock().unwrap().push(self.word);
            Ok(())
        }
    }
}

/// A recurring job that sets every argument of the attribute.
#[derive(Serialize, Deserialize, Default)]
pub struct Nightly;

#[facade::__jalari::job(
    name = "facade_nightly",
    queue = "facade_nightly",
    max_attempts = 3,
    timeout = 30,
    priority = "high",
    aliases = ["facade_nightly_old"],
    cron = "0 0 3 * * *",
    timezone = "Asia/Taipei",
    missed = "skip",
    fingerprint = true,
    checkpoints = true,
    crate = "::facade::__jalari"
)]
impl Job for Nightly {
    async fn run(self) -> JobResult {
        Ok(())
    }
}

/// Appends its word to [`ECHOED`], declared through the `#[facade::job_attr]` attribute.
#[derive(Serialize, Deserialize)]
pub struct AttrEcho {
    /// The word to append.
    pub word: String,
}

#[facade::job_attr(name = "facade_attr_echo", fingerprint = true)]
impl Job for AttrEcho {
    async fn run(self) -> JobResult {
        ECHOED.lock().unwrap().push(self.word);
        Ok(())
    }
}

/// A recurring job that sets every argument of the attribute, through `#[facade::job_attr]`.
#[derive(Serialize, Deserialize, Default)]
pub struct AttrNightly;

#[facade::job_attr(
    name = "facade_attr_nightly",
    queue = "facade_attr_nightly",
    max_attempts = 3,
    timeout = 30,
    priority = "high",
    aliases = ["facade_attr_nightly_old"],
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
