use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, error, info_span, warn};

use super::close::{Closing, Ending, answered, decide, refused};
use super::lifeline::Incarnation;
use super::middleware::{Finished, JobMeta, Next, report_finished};
use super::ownership::{Claimed, Closed, close};
use super::runner::Shared;
use super::runs::{Hold, PendingClaim, Registration};
use crate::enqueue::Columns;
use crate::job::context::{self, Captured};
use crate::job::registry::{ContextKey, JobRegistration};
use crate::job::{RunControl, run};
use crate::{JobError, JobId, JobResult, JobRun, Priority, Result, Schema};

const MAX_CLOSE_TRIES: u32 = 5;
const CLOSE_RETRY: Duration = Duration::from_secs(1);
const CLOSE_WINDOW: Duration = Duration::from_secs(30);
const CLOSE_BACKOFF: Duration = Duration::from_millis(100);
const CLOSE_BACKOFF_LIMIT: Duration = Duration::from_millis(500);
const ABORT_WAIT: Duration = Duration::from_secs(1);

struct AbortOnDrop(JoinHandle<JobResult>);

impl AbortOnDrop {
    async fn abort_and_wait(&mut self) {
        self.0.abort();
        if tokio::time::timeout(ABORT_WAIT, &mut self.0).await.is_err() {
            warn!(
                "the aborted job did not stop within {ABORT_WAIT:?}, recording its outcome anyway"
            );
        }
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Report {
    meta: Arc<JobMeta>,
    needs: ContextKey,
    captured: Captured,
}

impl Shared {
    pub(super) async fn process_next(
        &self,
        incarnation: &Incarnation,
        queue: &str,
        waker: &Notify,
    ) -> Result<bool> {
        if self.job_shutdown.is_cancelled() {
            return Ok(false);
        }
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
            let registration = self.jobs.get(claimed.task.as_str()).copied();
            let control = Arc::new(RunControl::new(
                claimed.hold.job_id,
                claimed.lease,
                claimed.progress.clone(),
                claimed.progress_seq,
                self.job_shutdown.child_token(),
                incarnation.dead().child_token(),
                registration.is_some_and(|registration| registration.checkpoints),
            ));
            let registered = pending.register(claimed.hold.clone(), Arc::clone(&control));
            if control.lost().is_cancelled() {
                return;
            }
            let mut report = None;
            let ending = match (registration, control.command()) {
                (_, Some(_)) => Ending::NotStarted,
                (None, None) => Ending::Returned(Err(JobError::new(format!(
                    "no job is registered as {:?}",
                    claimed.task
                )))),
                (Some(job), None) => {
                    let job_run = claimed.job_run(job, queue, Arc::clone(&control));
                    let meta = Arc::new(JobMeta::new(job_run.clone(), claimed.context.clone()));
                    report = Some(Report {
                        meta: Arc::clone(&meta),
                        needs: (job.context)(),
                        captured: Captured::from_stored(claimed.context.clone()),
                    });
                    let mut attempt = claimed.spawn(self, job, job_run, meta);
                    let shutdown_abort = ShutdownAbort {
                        control: &control,
                        window: Some(self.settings.shutdown_timeout),
                    };
                    let ending = {
                        let run = claimed.join(&mut attempt, &control);
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
                                    report_missed_checkpoints(&control, self.settings.stop_timeout);
                                    break Ending::Aborted { after: self.settings.stop_timeout };
                                }
                                () = control.commanded(), if deadline.is_none() => {
                                    deadline = Some(tokio::time::Instant::now() + self.settings.stop_timeout);
                                }
                                result = &mut run => break Ending::Returned(result),
                            }
                        }
                    };
                    if let Ending::Aborted { .. } = ending {
                        attempt.abort_and_wait().await;
                    }
                    shutdown_abort.finished();
                    ending
                }
            };
            registered.closing();
            self.settle(
                &claimed,
                &control,
                registered,
                &ending,
                started.elapsed(),
                report,
            )
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
        report: Option<Report>,
    ) {
        let pool = self.config.pool.pool();
        let retry_policy = self
            .jobs
            .get(claimed.task.as_str())
            .and_then(|job| job.retry_policy)
            .unwrap_or(&*self.retry_policy);
        let mut seen = control.command();
        let buffered = control.take_children();
        let mut children = (!buffered.is_empty()).then(|| Arc::new(Columns::from(buffered)));
        let mut refusal = None;
        let deadline = Instant::now() + CLOSE_WINDOW;
        for tries in 1..=MAX_CLOSE_TRIES {
            let outcome = decide(
                refusal.as_ref().unwrap_or(ending),
                seen,
                claimed.attempt,
                claimed.max_attempts,
                retry_policy,
            );
            let closing = Closing::after_run(
                claimed.attempt,
                outcome,
                seen,
                duration,
                control.latest(),
                children.clone(),
            );
            match close_until_answered(
                &pool,
                &self.config.schema,
                &claimed.hold,
                &closing,
                deadline,
            )
            .await
            {
                Ok(Closed::Written { children_inserted }) => {
                    registration.close();
                    closing.log(claimed.hold.job_id);
                    if let Some(children) = closing.children() {
                        let buffered = i64::try_from(children.len()).unwrap_or(i64::MAX);
                        debug!(
                            children_inserted,
                            children_kept = buffered - children_inserted,
                            "enqueued the jobs buffered for success"
                        );
                    }
                    if let Some(report) = report {
                        report_finished(
                            &self.middlewares,
                            report.meta,
                            report.needs,
                            report.captured,
                            Finished::from(closing.outcome()),
                        )
                        .await;
                    }
                    return;
                }
                Ok(Closed::Recommanded(command)) => seen = command,
                Ok(Closed::Lost) => {
                    warn!("another worker took the job over, dropping the outcome of this run");
                    return;
                }
                Err(e) if !answered(&e) => {
                    warn!(
                        error = %e,
                        "the database did not answer for {CLOSE_WINDOW:?}, giving up on the job \
                         outcome"
                    );
                    registration.abandon();
                    return;
                }
                Err(e) if closing.children().is_some() => {
                    warn!(error = %e, "could not enqueue the jobs buffered for success");
                    refusal = Some(refused(&e));
                    children = None;
                }
                Err(e) if tries < MAX_CLOSE_TRIES => {
                    warn!(error = %e, "failed to record the job outcome, retrying");
                    tokio::time::sleep(CLOSE_RETRY).await;
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

async fn close_until_answered(
    pool: &PgPool,
    schema: &Schema,
    hold: &Hold,
    closing: &Closing,
    deadline: Instant,
) -> Result<Closed> {
    let mut backoff = CLOSE_BACKOFF;
    loop {
        match close(pool, schema, hold, closing).await {
            Err(e) if !answered(&e) && Instant::now() + backoff < deadline => {
                if backoff == CLOSE_BACKOFF {
                    warn!(error = %e, "the database did not answer the job outcome, retrying");
                } else {
                    debug!(error = %e, "the database did not answer the job outcome, retrying");
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(CLOSE_BACKOFF_LIMIT);
            }
            closed => return closed,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Missed {
    Silent { gap: Duration },
    IgnoredHalt,
}

fn missed_checkpoints(
    declared: bool,
    since_checkpoint: Duration,
    window: Duration,
) -> Option<Missed> {
    if !declared {
        return None;
    }
    if since_checkpoint < window {
        Some(Missed::IgnoredHalt)
    } else {
        Some(Missed::Silent {
            gap: since_checkpoint,
        })
    }
}

fn report_missed_checkpoints(control: &RunControl, window: Duration) {
    match missed_checkpoints(
        control.declares_checkpoints(),
        control.since_checkpoint(),
        window,
    ) {
        Some(Missed::Silent { gap }) => error!(
            "job declared checkpoints but went {}s without one",
            gap.as_secs()
        ),
        Some(Missed::IgnoredHalt) => {
            error!("job declared checkpoints but kept running after a checkpoint asked it to stop")
        }
        None => {}
    }
}

struct ShutdownAbort<'a> {
    control: &'a RunControl,
    window: Option<Duration>,
}

impl ShutdownAbort<'_> {
    fn finished(mut self) {
        self.window = None;
    }
}

impl Drop for ShutdownAbort<'_> {
    fn drop(&mut self) {
        if let Some(window) = self.window
            && self.control.shutdown().is_cancelled()
            && !self.control.lost().is_cancelled()
        {
            report_missed_checkpoints(self.control, window);
        }
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

impl Claimed {
    fn job_run(
        &self,
        registration: &'static JobRegistration,
        queue: &str,
        control: Arc<RunControl>,
    ) -> JobRun {
        JobRun {
            id: JobId(self.hold.job_id),
            name: registration.name,
            queue: Arc::from(queue),
            attempt: self.attempt,
            max_attempts: self.max_attempts,
            created_at: self.created_at,
            scheduled_at: self.scheduled_at,
            priority: Priority::from_level(self.priority),
            last_error: self.last_error.as_deref().map(Arc::from),
            parent_id: self.parent_id.map(JobId),
            root_id: JobId(self.root_id.unwrap_or(self.hold.job_id)),
            cancellation: control.shutdown().clone(),
            control,
        }
    }

    fn spawn(
        &self,
        shared: &Shared,
        registration: &'static JobRegistration,
        job_run: JobRun,
        meta: Arc<JobMeta>,
    ) -> AbortOnDrop {
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
        AbortOnDrop(tokio::spawn(attempt))
    }

    async fn join(&self, attempt: &mut AbortOnDrop, control: &RunControl) -> JobResult {
        let joined = match self.timeout_ms {
            Some(timeout_ms) => {
                let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
                match tokio::time::timeout(timeout, &mut attempt.0).await {
                    Ok(joined) => joined,
                    Err(_) => {
                        attempt.abort_and_wait().await;
                        control.shutdown().cancel();
                        return Err(JobError::new(format!("timed out after {timeout:?}")));
                    }
                }
            }
            None => (&mut attempt.0).await,
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

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(30);

    #[test]
    fn test_undeclared_job_reports_nothing() {
        for since in [Duration::ZERO, WINDOW, Duration::from_secs(600)] {
            assert_eq!(missed_checkpoints(false, since, WINDOW), None);
        }
    }

    #[test]
    fn test_long_gap_reports_silence() {
        for since in [WINDOW, Duration::from_secs(41)] {
            assert_eq!(
                missed_checkpoints(true, since, WINDOW),
                Some(Missed::Silent { gap: since })
            );
        }
    }

    #[test]
    fn test_recent_checkpoint_reports_an_ignored_halt() {
        for since in [Duration::ZERO, WINDOW - Duration::from_millis(1)] {
            assert_eq!(
                missed_checkpoints(true, since, WINDOW),
                Some(Missed::IgnoredHalt)
            );
        }
    }
}
