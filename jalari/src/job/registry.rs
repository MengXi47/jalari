use std::any::{TypeId, type_name};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::worker::Provided;
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
    pub context: fn() -> ContextKey,
    pub run: fn(Provided, &[u8]) -> JobFuture,
}

#[doc(hidden)]
pub struct CronRegistration {
    pub expression: &'static str,
    pub timezone: &'static str,
    pub payload: fn() -> serde_json::Result<Vec<u8>>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct ContextKey {
    pub id: TypeId,
    pub name: &'static str,
}

impl ContextKey {
    pub fn of<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: type_name::<T>(),
        }
    }

    pub fn is_unit(&self) -> bool {
        self.id == TypeId::of::<()>()
    }
}

impl JobRegistration {
    pub const fn new<T: Job>(queue: &'static str, cron: Option<CronRegistration>) -> Self {
        Self {
            name: T::NAME,
            queue,
            max_attempts: T::MAX_ATTEMPTS,
            timeout: T::TIMEOUT,
            cron,
            context: ContextKey::of::<T::Context>,
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

fn run_job<T: Job>(provided: Provided, payload: &[u8]) -> JobFuture {
    match serde_json::from_slice::<T>(payload) {
        Ok(job) => Box::pin(async move {
            let Some(context) = provided.get::<T::Context>() else {
                return Err(JobError::new(format!(
                    "no middleware provided {} for {}",
                    type_name::<T::Context>(),
                    T::NAME
                )));
            };
            job.run(context).await
        }),
        Err(e) => Box::pin(std::future::ready(Err(JobError::permanent(e)))),
    }
}

fn default_payload<T: Job + Default>() -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&T::default())
}
