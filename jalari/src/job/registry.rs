use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::{Job, JobError, JobResult};

#[doc(hidden)]
pub type JobFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;

#[doc(hidden)]
pub struct JobRegistration {
    pub name: &'static str,
    pub queue: &'static str,
    pub max_attempts: i32,
    pub timeout: Option<Duration>,
    pub cron: Option<CronRegistration>,
    pub run: fn(&[u8]) -> JobFuture,
}

#[doc(hidden)]
pub struct CronRegistration {
    pub expression: &'static str,
    pub timezone: &'static str,
    pub payload: fn() -> serde_json::Result<Vec<u8>>,
}

impl JobRegistration {
    pub const fn new<T: Job>(queue: &'static str, cron: Option<CronRegistration>) -> Self {
        Self {
            name: T::NAME,
            queue,
            max_attempts: T::MAX_ATTEMPTS,
            timeout: T::TIMEOUT,
            cron,
            run: run_job::<T>,
        }
    }
}

impl CronRegistration {
    pub const fn new<T: Job + Default>(expression: &'static str, timezone: &'static str) -> Self {
        Self {
            expression,
            timezone,
            payload: default_payload::<T>,
        }
    }
}

inventory::collect!(JobRegistration);

#[doc(hidden)]
pub fn registrations() -> impl Iterator<Item = &'static JobRegistration> {
    inventory::iter::<JobRegistration>.into_iter()
}

#[doc(hidden)]
pub fn find(name: &str) -> Option<&'static JobRegistration> {
    registrations().find(|registration| registration.name == name)
}

fn run_job<T: Job>(payload: &[u8]) -> JobFuture {
    match serde_json::from_slice::<T>(payload) {
        Ok(job) => Box::pin(job.run()),
        Err(e) => Box::pin(std::future::ready(Err(JobError::permanent(e)))),
    }
}

fn default_payload<T: Job + Default>() -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&T::default())
}
