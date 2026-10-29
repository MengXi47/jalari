use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, info, warn};

use super::runner::ERROR_BACKOFF;
use crate::{JobError, JobResult};

pub(super) const ABORT_WAIT: Duration = Duration::from_secs(1);
const HELD_REPORT: Duration = Duration::from_secs(10);
const THREAD_NAME_LIMIT: usize = 15;
const EXIT_POLL: Duration = Duration::from_millis(1);

pub(super) type AttemptFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Placement {
    Thread,
    Task,
}

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

pub(super) struct SlotThreads {
    handle: Handle,
    retired: mpsc::UnboundedSender<Retired>,
    retirees: mpsc::UnboundedReceiver<Retired>,
}

impl SlotThreads {
    pub(super) fn new() -> Self {
        let (retired, retirees) = mpsc::unbounded_channel();
        Self {
            handle: Handle::current(),
            retired,
            retirees,
        }
    }

    pub(super) fn slot(&self, queue: &Arc<str>, index: usize, placement: Placement) -> Slot {
        let executor = match placement {
            Placement::Task => Executor::Task,
            Placement::Thread => Executor::Thread {
                handle: self.handle.clone(),
                thread: None,
                retired: self.retired.clone(),
            },
        };
        Slot {
            queue: Arc::clone(queue),
            index,
            executor,
            body: Body::Idle,
        }
    }

    pub(super) async fn retire(self) -> Retirement {
        let Self {
            retired,
            mut retirees,
            ..
        } = self;
        drop(retired);
        let deadline = Instant::now() + ABORT_WAIT;
        let mut threads = Vec::new();
        while let Ok(Some(thread)) = tokio::time::timeout_at(deadline, retirees.recv()).await {
            threads.push(thread);
        }
        let mut retirement = Retirement { joined: 0, left: 0 };
        for thread in threads {
            if thread.exit_by(deadline).await {
                let _ = thread.thread.join();
                retirement.joined += 1;
            } else {
                thread.leave();
                retirement.left += 1;
            }
        }
        debug!(
            joined = retirement.joined,
            left = retirement.left,
            "retired slot threads"
        );
        retirement
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Retirement {
    joined: usize,
    left: usize,
}

pub(super) struct Slot {
    queue: Arc<str>,
    index: usize,
    executor: Executor,
    body: Body,
}

enum Executor {
    Task,
    Thread {
        handle: Handle,
        thread: Option<SlotThread>,
        retired: mpsc::UnboundedSender<Retired>,
    },
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
    pub(super) fn queue(&self) -> &Arc<str> {
        &self.queue
    }

    pub(super) async fn vacate(&mut self, shutdown: &CancellationToken) -> Option<Vacant<'_>> {
        loop {
            if shutdown.is_cancelled() {
                return None;
            }
            match self.body {
                Body::Idle => {
                    if self.ensure_thread() {
                        return Some(Vacant { slot: self });
                    }
                    tokio::select! {
                        () = shutdown.cancelled() => return None,
                        () = tokio::time::sleep(ERROR_BACKOFF) => {}
                    }
                }
                Body::Running { .. } => self.hold(Cause::Lost),
                Body::Held { .. } => {
                    if !self.wait_held(shutdown).await {
                        return None;
                    }
                }
            }
        }
    }

    fn ensure_thread(&mut self) -> bool {
        let Executor::Thread { handle, thread, .. } = &mut self.executor else {
            return true;
        };
        if thread.is_some() {
            return true;
        }
        let name = thread_name(&self.queue, self.index);
        match SlotThread::start(name, handle.clone()) {
            Ok(started) => {
                debug!(
                    queue = %self.queue,
                    slot = self.index,
                    thread = %started.name,
                    "slot thread started"
                );
                *thread = Some(started);
                true
            }
            Err(e) => {
                warn!(
                    queue = %self.queue,
                    slot = self.index,
                    error = %e,
                    "failed to start a slot thread, retrying"
                );
                false
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
        match &mut self.executor {
            Executor::Task => drop(tokio::spawn(launch.run())),
            Executor::Thread {
                thread, retired, ..
            } => {
                if let Some(running) = thread.as_ref()
                    && running.assignments.try_send(launch).is_err()
                    && let Some(gone) = thread.take()
                {
                    warn!(
                        queue = %self.queue,
                        slot = self.index,
                        thread = %gone.name,
                        "the slot thread is gone, starting a new one"
                    );
                    let _ = retired.send(gone.retire(None));
                }
            }
        }
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
        let job_id = self.body.job_id();
        if let Executor::Thread {
            thread, retired, ..
        } = &mut self.executor
            && let Some(thread) = thread.take()
        {
            let _ = retired.send(thread.retire(job_id));
        }
    }
}

impl Body {
    fn job_id(&self) -> Option<i64> {
        match self {
            Body::Idle => None,
            Body::Running { job_id, .. } | Body::Held { job_id, .. } => Some(*job_id),
        }
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

struct SlotThread {
    name: String,
    assignments: SyncSender<Launch>,
    thread: std::thread::JoinHandle<()>,
    exited: CancellationToken,
}

impl SlotThread {
    fn start(name: String, handle: Handle) -> std::io::Result<Self> {
        let (assignments, received) = sync_channel(1);
        let exited = CancellationToken::new();
        let guard = exited.clone().drop_guard();
        let thread = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || serve(&handle, &received, guard))?;
        Ok(Self {
            name,
            assignments,
            thread,
            exited,
        })
    }

    fn retire(self, job_id: Option<i64>) -> Retired {
        Retired {
            name: self.name,
            thread: self.thread,
            exited: self.exited,
            job_id,
        }
    }
}

fn serve(handle: &Handle, assignments: &Receiver<Launch>, exited: DropGuard) {
    let _exited = exited;
    while let Ok(launch) = assignments.recv() {
        handle.block_on(launch.run());
    }
}

pub(super) struct Retired {
    name: String,
    thread: std::thread::JoinHandle<()>,
    exited: CancellationToken,
    job_id: Option<i64>,
}

impl Retired {
    async fn exit_by(&self, deadline: Instant) -> bool {
        if tokio::time::timeout_at(deadline, self.exited.cancelled())
            .await
            .is_err()
        {
            return false;
        }
        while !self.thread.is_finished() {
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(EXIT_POLL).await;
        }
        true
    }

    fn leave(self) {
        match self.job_id {
            Some(job_id) => warn!(
                thread = %self.name,
                job_id,
                "left thread {} running job {job_id} at shutdown",
                self.name
            ),
            None => warn!(thread = %self.name, "left thread {} running at shutdown", self.name),
        }
    }
}

fn thread_name(queue: &str, index: usize) -> String {
    let suffix = format!(":{index}");
    let mut end = THREAD_NAME_LIMIT
        .saturating_sub(suffix.len())
        .min(queue.len());
    while !queue.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{suffix}", &queue[..end])
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
