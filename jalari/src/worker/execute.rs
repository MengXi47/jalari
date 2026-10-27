use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, info, info_span, warn};

use super::close::{Closing, Ending, Outcome, decide};
use super::lifeline::Incarnation;
use super::middleware::{JobMeta, Next};
use super::ownership::{Claimed, Closed, close};
use super::runner::Shared;
use super::runs::{PendingClaim, Registration};
use crate::job::context::{self, Captured};
use crate::job::registry::JobRegistration;
use crate::job::{RunControl, run};
use crate::{JobError, JobId, JobResult, JobRun, Result};

const MAX_CLOSE_TRIES: u32 = 5;
const CLOSE_RETRY: Duration = Duration::from_secs(1);

struct AbortOnDrop(JoinHandle<JobResult>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Shared {
    pub(super) async fn process_next(
        &self,
        incarnation: &Incarnation,
        queue: &str,
        waker: &Notify,
    ) -> Result<bool> {
        let Some((claimed, pending)) = self.claim(incarnation, queue).await? else {
            return Ok(false);
        };
        waker.notify_one();
        self.execute(incarnation, claimed, pending, queue).await;
        Ok(true)
    }

    async fn execute(
        &self,
        incarnation: &Incarnation,
        claimed: Claimed,
        pending: PendingClaim<'_>,
        queue: &str,
    ) {
        let span = info_span!(
            "job",
            job_id = claimed.hold.job_id,
            task = %claimed.task,
            queue = %queue,
            attempt = claimed.attempt
        );
        async move {
            let started = Instant::now();
            let control = Arc::new(RunControl::new(
                claimed.hold.job_id,
                claimed.lease,
                claimed.progress.clone(),
                self.job_shutdown.child_token(),
                incarnation.dead().child_token(),
            ));
            let registration = pending.register(claimed.hold.clone(), Arc::clone(&control));
            if control.lost().is_cancelled() {
                return;
            }
            let ending = match (self.jobs.get(claimed.task.as_str()), control.command()) {
                (_, Some(_)) => Ending::NotStarted,
                (None, None) => Ending::Returned(Err(JobError::new(format!(
                    "no job is registered as {:?}",
                    claimed.task
                )))),
                (Some(&job), None) => {
                    let run = claimed.run(self, job, queue, Arc::clone(&control));
                    tokio::pin!(run);
                    let mut deadline = None;
                    loop {
                        tokio::select! {
                            biased;
                            () = control.lost().cancelled() => {
                                warn!("the worker lost the job, aborting it");
                                return;
                            }
                            () = sleep_until(deadline), if deadline.is_some() => {
                                break Ending::Aborted { after: self.settings.stop_timeout };
                            }
                            () = control.commanded(), if deadline.is_none() => {
                                deadline = Some(tokio::time::Instant::now() + self.settings.stop_timeout);
                            }
                            result = &mut run => break Ending::Returned(result),
                        }
                    }
                }
            };
            registration.closing();
            self.settle(&claimed, &control, registration, &ending, started.elapsed())
                .await;
        }
        .instrument(span)
        .await;
    }

    async fn settle(
        &self,
        claimed: &Claimed,
        control: &RunControl,
        registration: Registration<'_>,
        ending: &Ending,
        duration: Duration,
    ) {
        let pool = self.config.pool.pool();
        let mut seen = control.command();
        for tries in 1..=MAX_CLOSE_TRIES {
            let outcome = decide(
                ending,
                seen,
                claimed.attempt,
                claimed.max_attempts,
                &*self.retry_policy,
            );
            let closing =
                Closing::after_run(claimed.attempt, outcome, seen, duration, control.latest());
            match close(&pool, &self.config.schema, &claimed.hold, &closing).await {
                Ok(Closed::Written) => {
                    registration.close();
                    log_outcome(claimed.hold.job_id, &closing);
                    return;
                }
                Ok(Closed::Recommanded(command)) => seen = command,
                Ok(Closed::Lost) => {
                    warn!("another worker took the job over, dropping the outcome of this run");
                    return;
                }
                Err(e) if tries < MAX_CLOSE_TRIES && !control.lost().is_cancelled() => {
                    warn!(error = %e, "failed to record the job outcome, retrying");
                    tokio::select! {
                        () = tokio::time::sleep(CLOSE_RETRY) => {}
                        () = control.lost().cancelled() => {}
                    }
                }
                Err(e) => {
                    warn!(error = %e, "failed to record the job outcome, giving up");
                    registration.abandon();
                    return;
                }
            }
        }
        warn!("the job outcome kept changing, giving up");
        registration.abandon();
    }
}

fn log_outcome(job_id: i64, closing: &Closing) {
    let duration_ms = closing.duration_ms();
    match closing.outcome() {
        Outcome::Succeeded => debug!(duration_ms, "job succeeded"),
        Outcome::Retry { delay, error } => {
            warn!(error = %error, ?delay, "job failed, retrying");
        }
        Outcome::Failed { error } => warn!(error = %error, "job failed"),
        Outcome::Released { note } => info!(job_id, note = %note, "job released"),
        Outcome::Stopped { note } => info!(job_id, note = %note, "job stopped"),
        Outcome::Cancelled { note } => info!(job_id, note = %note, "job cancelled"),
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

impl Claimed {
    async fn run(
        &self,
        shared: &Shared,
        registration: &'static JobRegistration,
        queue: &str,
        control: Arc<RunControl>,
    ) -> JobResult {
        let job_run = JobRun::new(
            JobId(self.hold.job_id),
            registration.name,
            Arc::from(queue),
            self.attempt,
            self.max_attempts,
            control.shutdown().clone(),
            control,
        );
        let meta = JobMeta::new(job_run.clone(), self.context.clone());
        let captured = Captured::from_stored(self.context.clone());
        let middlewares = Arc::clone(&shared.middlewares);
        let payload = self.payload.clone();
        let attempt = async move {
            Next::start(
                &middlewares,
                &meta,
                (registration.context)(),
                registration.run,
                &payload,
            )
            .run()
            .await
        };
        let attempt = run::within(job_run, context::within(captured, attempt)).in_current_span();
        let mut handle = AbortOnDrop(tokio::spawn(attempt));
        let joined = match self.timeout_ms {
            Some(timeout_ms) => {
                let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
                match tokio::time::timeout(timeout, &mut handle.0).await {
                    Ok(joined) => joined,
                    Err(_) => return Err(JobError::new(format!("timed out after {timeout:?}"))),
                }
            }
            None => (&mut handle.0).await,
        };
        match joined {
            Ok(result) => result,
            Err(e) if e.is_panic() => Err(JobError::new(format!(
                "panicked: {}",
                panic_message(e.into_panic())
            ))),
            Err(e) => Err(JobError::new(format!("job task ended unexpectedly: {e}"))),
        }
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => payload
            .downcast_ref::<&str>()
            .map_or_else(|| "unknown panic payload".to_owned(), |s| (*s).to_owned()),
    }
}
