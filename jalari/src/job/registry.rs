use std::any::{TypeId, type_name};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::recurring::Missed;
use crate::worker::Provided;
use crate::{Job, JobError, JobResult, Priority, RetryPolicy};

#[doc(hidden)]
pub type JobFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;

#[doc(hidden)]
pub struct JobRegistration {
    pub name: &'static str,
    pub queue: &'static str,
    pub max_attempts: i32,
    pub timeout: Option<Duration>,
    pub priority: Priority,
    pub aliases: &'static [&'static str],
    pub retry_policy: Option<&'static dyn RetryPolicy>,
    pub checkpoints: bool,
    pub cron: Option<CronRegistration>,
    pub fingerprint: Option<&'static str>,
    pub context: fn() -> ContextKey,
    pub run: fn(Provided, &[u8]) -> JobFuture,
}

#[doc(hidden)]
pub struct CronRegistration {
    pub expression: &'static str,
    pub timezone: &'static str,
    pub missed: Missed,
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
    pub const fn new<T: Job>(
        queue: &'static str,
        cron: Option<CronRegistration>,
        fingerprint: Option<&'static str>,
        checkpoints: bool,
    ) -> Self {
        Self {
            name: T::NAME,
            queue,
            max_attempts: T::MAX_ATTEMPTS,
            timeout: T::TIMEOUT,
            priority: T::PRIORITY,
            aliases: T::ALIASES,
            retry_policy: T::RETRY_POLICY,
            checkpoints,
            cron,
            fingerprint,
            context: ContextKey::of::<T::Context>,
            run: run_job::<T>,
        }
    }
}

impl CronRegistration {
    pub const fn new<T: Job + Default>(
        expression: &'static str,
        timezone: &'static str,
        missed: Missed,
    ) -> Self {
        Self {
            expression,
            timezone,
            missed,
            payload: default_payload::<T>,
        }
    }
}

inventory::collect!(JobRegistration);

#[doc(hidden)]
pub fn registrations() -> impl Iterator<Item = &'static JobRegistration> {
    inventory::iter::<JobRegistration>.into_iter()
}

pub(crate) fn entries() -> impl Iterator<Item = (&'static str, &'static JobRegistration)> {
    registrations().flat_map(|registration| {
        std::iter::once(registration.name)
            .chain(registration.aliases.iter().copied())
            .map(move |name| (name, registration))
    })
}

#[doc(hidden)]
pub fn find(name: &str) -> Option<&'static JobRegistration> {
    entries()
        .find(|(entry, _)| *entry == name)
        .map(|(_, registration)| registration)
}

#[doc(hidden)]
pub const fn aliases_are_distinct(name: &str, aliases: &[&str]) -> bool {
    let mut index = 0;
    while index < aliases.len() {
        let alias = aliases[index];
        if alias.is_empty() || same_text(alias, name) {
            return false;
        }
        let mut earlier = 0;
        while earlier < index {
            if same_text(aliases[earlier], alias) {
                return false;
            }
            earlier += 1;
        }
        index += 1;
    }
    true
}

const fn same_text(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
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

#[cfg(test)]
mod tests {
    use super::aliases_are_distinct;

    #[test]
    fn test_aliases_are_distinct_rejects_name_empty_and_repeats() {
        assert!(aliases_are_distinct("send_email", &[]));
        assert!(aliases_are_distinct("send_email", &["mail", "send_mail"]));
        assert!(aliases_are_distinct(
            "send_email",
            &["send_emai", "send_emails"]
        ));
        assert!(!aliases_are_distinct("send_email", &[""]));
        assert!(!aliases_are_distinct("send_email", &["mail", "send_email"]));
        assert!(!aliases_are_distinct(
            "send_email",
            &["mail", "old", "mail"]
        ));
        assert!(!aliases_are_distinct("send_email", &["old", "old"]));
    }
}
