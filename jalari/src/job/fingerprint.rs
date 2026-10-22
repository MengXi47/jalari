use serde_json::Value;

use super::registry::{self, JobRegistration};

pub(crate) fn of_task(task: &str) -> Option<&'static str> {
    registry::find(task).and_then(|registration| registration.fingerprint)
}

pub(crate) fn served(registrations: impl Iterator<Item = &'static JobRegistration>) -> Value {
    Value::Object(
        registrations
            .map(|registration| {
                let fingerprint = registration.fingerprint.map_or(Value::Null, |fingerprint| {
                    Value::String(fingerprint.to_owned())
                });
                (registration.name.to_owned(), fingerprint)
            })
            .collect(),
    )
}

pub(crate) fn compatible(served: &str) -> String {
    format!(
        "(jsonb_exists({served}, job.task) \
         AND COALESCE(({served} ->> job.task) = job.fingerprint, TRUE))"
    )
}
