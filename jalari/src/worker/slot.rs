use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{JobError, JobResult};

pub(super) const ABORT_WAIT: Duration = Duration::from_secs(1);
const HELD_REPORT: Duration = Duration::from_secs(10);

pub(super) type AttemptFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Cause {
    TimedOut,
    StopTimeout,
    Lost,
}

impl Cause {
    fn describe(self) -> &'static str {
        match self {
            Cause::TimedOut => "timed out",
            Cause::StopTimeout => "reached stop_timeout",
            Cause::Lost => "was lost",
        }
    }
}

#[derive(Debug)]
pub(super) enum Returned {
    Finished(JobResult),
    Panicked(String),
    Aborted,
    Vanished(String),
}

impl Returned {
    pub(super) fn into_result(self) -> JobResult {
        match self {
            Returned::Finished(result) => result,
            Returned::Panicked(message) => Err(JobError::new(format!("panicked: {message}"))),
            Returned::Aborted => Err(JobError::new(
                "job task ended unexpectedly: the attempt was aborted".to_owned(),
            )),
            Returned::Vanished(detail) => Err(JobError::new(format!(
                "job task ended unexpectedly: {detail}"
            ))),
        }
    }
}

pub(super) struct Slot {
    queue: Arc<str>,
    index: usize,
    body: Body,
}

enum Body {
    Idle,
    Running {
        job_id: i64,
        done: Done,
    },
    Held {
        job_id: i64,
        done: Done,
        since: Instant,
        cause: Cause,
        warned: bool,
    },
}

enum Wake {
    Shutdown,
    Returned,
    Report,
}

impl Slot {
    pub(super) fn new(queue: Arc<str>, index: usize) -> Self {
        Self {
            queue,
            index,
            body: Body::Idle,
        }
    }

    pub(super) fn queue(&self) -> &Arc<str> {
        &self.queue
    }

    pub(super) async fn vacate(&mut self, shutdown: &CancellationToken) -> Option<Vacant<'_>> {
        loop {
            if shutdown.is_cancelled() {
                return None;
            }
            match self.body {
                Body::Idle => return Some(Vacant { slot: self }),
                Body::Running { .. } => self.hold(Cause::Lost),
                Body::Held { .. } => {
                    if !self.wait_held(shutdown).await {
                        return None;
                    }
                }
            }
        }
    }

    async fn wait_held(&mut self, shutdown: &CancellationToken) -> bool {
        let Body::Held {
            job_id,
            done,
            since,
            cause,
            warned,
        } = &mut self.body
        else {
            return true;
        };
        let (job_id, since, cause, warned) = (*job_id, *since, *cause, *warned);
        let next = if warned {
            let reports = since.elapsed().as_secs() / HELD_REPORT.as_secs() + 1;
            since + HELD_REPORT * u32::try_from(reports).unwrap_or(u32::MAX)
        } else {
            since + ABORT_WAIT
        };
        let woke = tokio::select! {
            () = shutdown.cancelled() => Wake::Shutdown,
            _ = done => Wake::Returned,
            () = tokio::time::sleep_until(next) => Wake::Report,
        };
        match woke {
            Wake::Shutdown => return false,
            Wake::Returned => self.release(),
            Wake::Report if warned => {
                let held_secs = since.elapsed().as_secs();
                error!(
                    job_id,
                    queue = %self.queue,
                    slot = self.index,
                    held_secs,
                    "job {job_id} still runs {held_secs} s after it {}; slot {}:{} takes no job \
                     until it returns",
                    cause.describe(),
                    self.queue,
                    self.index
                );
            }
            Wake::Report => self.held_warning(),
        }
        true
    }

    fn launch(&mut self, job_id: i64, attempt: AttemptFuture) {
        let (launch, done) = Launch::new(attempt);
        tokio::spawn(launch.run());
        debug!(queue = %self.queue, slot = self.index, job_id, "slot running job {job_id}");
        self.body = Body::Running { job_id, done };
    }

    fn hold(&mut self, cause: Cause) {
        let body = std::mem::replace(&mut self.body, Body::Idle);
        self.body = match body {
            Body::Running { job_id, done } => {
                done.cancel.cancel();
                debug!(
                    queue = %self.queue,
                    slot = self.index,
                    job_id,
                    cause = cause.describe(),
                    "slot held: job {job_id} {} and has not returned",
                    cause.describe()
                );
                Body::Held {
                    job_id,
                    done,
                    since: Instant::now(),
                    cause,
                    warned: false,
                }
            }
            other => other,
        };
    }

    fn release(&mut self) {
        match std::mem::replace(&mut self.body, Body::Idle) {
            Body::Idle => {}
            Body::Running { job_id, .. } => {
                debug!(queue = %self.queue, slot = self.index, job_id, "slot idle");
            }
            Body::Held {
                job_id,
                since,
                cause,
                warned,
                ..
            } => {
                let held_secs = since.elapsed().as_secs();
                if warned {
                    info!(
                        job_id,
                        queue = %self.queue,
                        slot = self.index,
                        held_secs,
                        "job {job_id} returned {held_secs} s after it {}; slot {}:{} takes jobs \
                         again",
                        cause.describe(),
                        self.queue,
                        self.index
                    );
                }
                debug!(
                    queue = %self.queue,
                    slot = self.index,
                    job_id,
                    held_secs,
                    "slot returned: job {job_id} returned {held_secs} s after it {}",
                    cause.describe()
                );
            }
        }
    }

    fn held_warning(&mut self) {
        if let Body::Held {
            job_id,
            cause,
            warned,
            ..
        } = &mut self.body
        {
            *warned = true;
            warn!(
                job_id = *job_id,
                queue = %self.queue,
                slot = self.index,
                cause = cause.describe(),
                "job {job_id} did not stop within {ABORT_WAIT:?} after it {}; slot {}:{} takes no \
                 job until it returns",
                cause.describe(),
                self.queue,
                self.index
            );
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.hold(Cause::Lost);
    }
}

pub(super) struct Vacant<'s> {
    slot: &'s mut Slot,
}

impl<'s> Vacant<'s> {
    pub(super) fn queue(&self) -> &Arc<str> {
        &self.slot.queue
    }

    pub(super) fn launch(self, job_id: i64, attempt: AttemptFuture) -> Attempt<'s> {
        self.slot.launch(job_id, attempt);
        Attempt { slot: self.slot }
    }
}

pub(super) struct Attempt<'s> {
    slot: &'s mut Slot,
}

impl Attempt<'_> {
    pub(super) async fn returned(&mut self) -> Returned {
        let returned = match &mut self.slot.body {
            Body::Running { done, .. } | Body::Held { done, .. } => done.await,
            Body::Idle => return Returned::Vanished("the attempt had already returned".to_owned()),
        };
        self.slot.release();
        returned
    }

    pub(super) async fn abort_and_wait(&mut self, cause: Cause) {
        self.slot.hold(cause);
        let Body::Held { done, .. } = &mut self.slot.body else {
            return;
        };
        if tokio::time::timeout(ABORT_WAIT, done).await.is_ok() {
            self.slot.release();
        } else {
            self.slot.held_warning();
        }
    }
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        self.slot.hold(Cause::Lost);
    }
}

struct Done {
    cancel: CancellationToken,
    returned: oneshot::Receiver<Returned>,
}

impl Future for Done {
    type Output = Returned;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Returned> {
        Pin::new(&mut self.get_mut().returned)
            .poll(cx)
            .map(|received| {
                received.unwrap_or_else(|_| {
                    Returned::Vanished("the attempt ended without a result".to_owned())
                })
            })
    }
}

struct Launch {
    attempt: AttemptFuture,
    cancel: CancellationToken,
    returned: oneshot::Sender<Returned>,
}

impl Launch {
    fn new(attempt: AttemptFuture) -> (Self, Done) {
        let cancel = CancellationToken::new();
        let (returned, receiver) = oneshot::channel();
        let done = Done {
            cancel: cancel.clone(),
            returned: receiver,
        };
        let launch = Self {
            attempt,
            cancel,
            returned,
        };
        (launch, done)
    }

    async fn run(self) {
        let Self {
            attempt,
            cancel,
            returned,
        } = self;
        let mut guarded = CatchUnwind(Box::pin(async move {
            tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                result = attempt => Some(result),
            }
        }));
        let caught = (&mut guarded).await;
        let dropped = catch_unwind(AssertUnwindSafe(move || drop(guarded)));
        let _ = returned.send(match (caught, dropped) {
            (Err(message), _) => Returned::Panicked(message),
            (Ok(_), Err(payload)) => Returned::Panicked(panic_message(payload)),
            (Ok(Some(result)), Ok(())) => Returned::Finished(result),
            (Ok(None), Ok(())) => Returned::Aborted,
        });
    }
}

struct CatchUnwind<F>(F);

impl<F: Future + Unpin> Future for CatchUnwind<F> {
    type Output = Result<F::Output, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match catch_unwind(AssertUnwindSafe(|| Pin::new(&mut self.0).poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(panic_message(payload))),
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
#[path = "slot_tests.rs"]
mod tests;
