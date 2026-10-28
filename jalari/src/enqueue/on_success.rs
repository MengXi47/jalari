use super::request::EnqueueRequest;
use crate::job::in_step;
use crate::{EnqueueOptions, Error, ErrorKind, Job, OnConflict, Result, current_job};

/// Enqueues a job when the running job succeeds, in the same statement that records the success.
///
/// The child is serialized now and inherits the running job's context. It is inserted only if
/// this attempt ends as succeeded; a retry, failure, stop, cancel, shutdown or crash drops it,
/// and the next attempt enqueues again. Its delay counts from the success. A child with the
/// running job's own `job_key` is inserted, since that key is released by the same statement.
/// The call returns no id; use [`enqueue`](crate::enqueue) when you need one.
///
/// It is refused inside a [`step`](crate::step) closure: the step would be recorded as done
/// while the child exists only in memory, so a failed attempt would lose the child for good.
/// Enqueue it with [`enqueue_in`](crate::enqueue_in) in a transaction that
/// [`Checkpoint::save_in`](crate::Checkpoint::save_in) also writes to, or call
/// `enqueue_on_success` after the step returns.
///
/// # Errors
///
/// Returns an error if:
/// - It is called outside a running job's own task, or inside a [`step`](crate::step) closure
///   ([`NotInJob`](crate::ErrorKind::NotInJob))
/// - The job or its context cannot be serialized
///   ([`PayloadEncodeFailed`](crate::ErrorKind::PayloadEncodeFailed))
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct EmailReport { id: i64 }
/// # #[jalari::job(name = "email_report")]
/// # impl jalari::Job for EmailReport {
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn build_report() -> Result<i64, std::io::Error> { Ok(1) }
/// # async fn run() -> jalari::JobResult {
/// let id = build_report().await?;
/// jalari::enqueue_on_success(&EmailReport { id })?;
/// Ok(())
/// # }
/// ```
pub fn enqueue_on_success<T: Job>(job: &T) -> Result<()> {
    enqueue_on_success_with(job, EnqueueOptions::new())
}

/// Like [`enqueue_on_success`], with options such as a delay, a queue or a `job_key`.
///
/// Only [`OnConflict::KeepExisting`](crate::OnConflict::KeepExisting) is accepted: a child whose
/// `job_key` is taken when the running job succeeds is dropped.
///
/// # Errors
///
/// Returns an error if:
/// - Any condition listed for [`enqueue_on_success`] occurs
/// - The options ask for [`OnConflict::Replace`](crate::OnConflict::Replace) or hold a value out
///   of range ([`InvalidOptions`](crate::ErrorKind::InvalidOptions))
/// - The timeout exceeds about 24 days
///   ([`TimeoutTooLong`](crate::ErrorKind::TimeoutTooLong))
///
/// # Examples
///
/// ```rust,no_run
/// # use std::time::Duration;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct PollFeed { feed: String }
/// # #[jalari::job(name = "poll_feed")]
/// # impl jalari::Job for PollFeed {
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # fn run(feed: String) -> jalari::JobResult {
/// let options = jalari::EnqueueOptions::new()
///     .job_key(&format!("poll:{feed}"))
///     .delay(Duration::from_secs(60));
/// jalari::enqueue_on_success_with(&PollFeed { feed }, options)?;
/// # Ok(())
/// # }
/// ```
pub fn enqueue_on_success_with<T: Job>(job: &T, options: EnqueueOptions) -> Result<()> {
    let Some(run) = current_job() else {
        return Err(Error::new(
            ErrorKind::NotInJob,
            format!(
                "{}: enqueue_on_success works only in a running job's own task",
                T::NAME
            ),
        ));
    };
    if in_step() {
        return Err(Error::new(
            ErrorKind::NotInJob,
            format!(
                "{}: enqueue_on_success does not work inside jalari::step; call it after the \
                 step returns",
                T::NAME
            ),
        ));
    }
    if options.on_conflict == OnConflict::Replace {
        return Err(Error::new(
            ErrorKind::InvalidOptions,
            format!(
                "{}: enqueue_on_success_with accepts only OnConflict::KeepExisting",
                T::NAME
            ),
        ));
    }
    run.control().buffer(buffered(job, options)?);
    Ok(())
}

fn buffered<T: Job>(job: &T, options: EnqueueOptions) -> Result<EnqueueRequest> {
    let mut request = EnqueueRequest::build(job, options)?;
    request.inherit_when_submitted = false;
    Ok(request)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::job::context::{self, Captured};
    use crate::job::{RunControl, run};
    use crate::{JobResult, JobRun};

    #[derive(Serialize, Deserialize)]
    struct Child {
        n: u32,
    }

    impl Job for Child {
        const NAME: &'static str = "on_success_child";
        type Context = ();

        async fn run(self, _context: &()) -> JobResult {
            Ok(())
        }
    }

    fn parent(id: i64) -> (JobRun, Arc<RunControl>) {
        let control = Arc::new(RunControl::new(
            id,
            1,
            None,
            0,
            CancellationToken::new(),
            CancellationToken::new(),
            false,
        ));
        (JobRun::for_test(id, Arc::clone(&control)), control)
    }

    #[tokio::test]
    async fn test_on_success_outside_a_job_is_not_in_job() {
        let error = enqueue_on_success(&Child { n: 1 }).unwrap_err();
        assert_eq!(error.kind, ErrorKind::NotInJob);
        let (job, control) = parent(5);
        let spawned = run::within(job, async {
            tokio::spawn(async { enqueue_on_success(&Child { n: 2 }) })
                .await
                .unwrap()
        })
        .await;
        assert_eq!(spawned.unwrap_err().kind, ErrorKind::NotInJob);
        assert!(control.take_children().is_empty());
    }

    #[tokio::test]
    async fn test_on_success_rejects_replace() {
        let (job, control) = parent(5);
        let result = run::within(job, async {
            enqueue_on_success_with(
                &Child { n: 1 },
                EnqueueOptions::new()
                    .job_key("child")
                    .on_conflict(OnConflict::Replace),
            )
        })
        .await;
        assert_eq!(result.unwrap_err().kind, ErrorKind::InvalidOptions);
        assert!(control.take_children().is_empty());
    }

    #[tokio::test]
    async fn test_on_success_inherits_the_job_context() {
        let (job, control) = parent(5);
        let tenant = json!({ "tenant": "acme" });
        run::within(
            job,
            context::within(Captured::Value(tenant.clone()), async {
                enqueue_on_success(&Child { n: 1 }).unwrap();
                enqueue_on_success_with(&Child { n: 2 }, EnqueueOptions::new().no_context())
                    .unwrap();
                enqueue_on_success_with(
                    &Child { n: 3 },
                    EnqueueOptions::new().context(&json!({ "tenant": "other" })),
                )
                .unwrap();
            }),
        )
        .await;
        let children = control.take_children();
        let seen: Vec<(Value, Option<Value>, bool, Option<i64>)> = children
            .into_iter()
            .map(|child| {
                (
                    serde_json::from_slice(&child.payload).unwrap(),
                    child.context,
                    child.inherit_when_submitted,
                    child.parent_id,
                )
            })
            .collect();
        assert_eq!(
            seen,
            [
                (json!({ "n": 1 }), Some(tenant), false, Some(5)),
                (json!({ "n": 2 }), None, false, Some(5)),
                (
                    json!({ "n": 3 }),
                    Some(json!({ "tenant": "other" })),
                    false,
                    Some(5)
                ),
            ]
        );
        assert!(control.take_children().is_empty());

        let (job, control) = parent(6);
        run::within(job, async { enqueue_on_success(&Child { n: 4 }).unwrap() }).await;
        let [child] = control.take_children().try_into().ok().unwrap();
        assert_eq!(child.context, None);
        assert!(!child.inherit_when_submitted);
        assert_eq!(child.parent_id, Some(6));
    }
}
