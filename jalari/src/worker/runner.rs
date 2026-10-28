use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Notify, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::builder::WorkerBuilder;
use super::flusher;
use super::lifeline::{Current, LifelineThread, WorkerRegistration};
use super::middleware::ErasedMiddleware;
use super::ownership::{Target, interrupt};
use super::pauses::{Pauses, watch_pauses};
use super::runs::RunBook;
use super::{housekeeping, listener, reaper, scheduler};
use crate::init::Config;
use crate::job::registry::{CronRegistration, JobRegistration};
use crate::{Cron, RetryPolicy, WorkerConfig};

pub(super) const ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Process-local job runner that claims and executes jobs from the configured queues.
///
/// Start as many workers as you like, in one process or many; they coordinate only through
/// PostgreSQL. A job is claimed by exactly one worker at a time, and when a worker dies another
/// worker runs its jobs again from their saved progress within seconds. A worker that loses its
/// database connection aborts the jobs it was running before anyone else can start them; one
/// frozen for 10 seconds or more is taken over while frozen and aborts its jobs as soon as it
/// wakes up, because it stops trusting its lifeline 11 seconds after the last acknowledged
/// heartbeat, before PostgreSQL can have ended that session and released it. Aborting is cooperative: a job stops at its next `.await`, and code that never
/// yields, such as a blocking loop or a thread the job started, keeps running. The guide's
/// "Crashes and lost connections" gives the timings.
///
/// # Examples
///
/// ```rust,no_run
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = jalari::WorkerConfig::default();
/// config.poll_interval = Duration::from_secs(2);
///
/// let worker = jalari::Worker::builder()
///     .queue("default", 4)
///     .queue("emails", 8)
///     .config(config)
///     .build()
///     .await?;
///
/// let shutdown = jalari::CancellationToken::new();
/// let signal = shutdown.clone();
/// tokio::spawn(async move {
///     tokio::signal::ctrl_c().await.ok();
///     signal.cancel();
/// });
/// worker.run(shutdown).await;
/// # Ok(())
/// # }
/// ```
pub struct Worker {
    pub(super) shared: Arc<Shared>,
    pub(super) queues: Vec<QueueConfig>,
    pub(super) scheduler: bool,
}

pub(super) struct Shared {
    pub(super) config: &'static Config,
    pub(super) jobs: HashMap<&'static str, &'static JobRegistration>,
    pub(super) task_names: Vec<String>,
    pub(super) served: Value,
    pub(super) crons: Vec<DeclaredCron>,
    pub(super) settings: WorkerConfig,
    pub(super) retry_policy: Arc<dyn RetryPolicy>,
    pub(super) middlewares: Arc<[Arc<dyn ErasedMiddleware>]>,
    pub(super) job_shutdown: CancellationToken,
    pub(super) runs: RunBook,
    pub(super) flush_now: Notify,
}

pub(super) struct DeclaredCron {
    pub(super) registration: &'static JobRegistration,
    pub(super) declaration: &'static CronRegistration,
    pub(super) cron: Cron,
}

pub(super) struct QueueConfig {
    pub(super) name: String,
    pub(super) concurrency: usize,
}

pub(super) struct Wakers {
    pub(super) queues: HashMap<String, Arc<Notify>>,
    pub(super) scheduler: Arc<Notify>,
    pub(super) housekeeping: Arc<Notify>,
    pub(super) pauses: Arc<Notify>,
}

impl Worker {
    /// Starts configuring a worker.
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    /// Runs the worker until `shutdown` is cancelled.
    ///
    /// Besides executing jobs it listens for notifications, keeps its lifeline session, takes over
    /// the jobs of dead workers, runs the recurring scheduler when enabled and performs
    /// housekeeping when the cluster turns it on. Database outages are logged with `tracing` and
    /// retried; they never end the loop.
    ///
    /// After cancellation it stops claiming jobs, signals running ones through
    /// [`interrupted`](crate::interrupted) and [`shutdown_requested`](crate::shutdown_requested)
    /// and waits for them to finish. A job that returns the error of a
    /// [`checkpoint`](crate::checkpoint), a [`Checkpoint`](crate::Checkpoint) or `interrupted`
    /// then is put back in its queue without counting the attempt, keeping its saved progress.
    /// Jobs still running after [`WorkerConfig::shutdown_timeout`] are aborted, which counts as
    /// an attempt, and another worker runs them again after a few seconds.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let worker = jalari::Worker::builder().queue("default", 4).build().await?;
    /// let shutdown = jalari::CancellationToken::new();
    /// let running = tokio::spawn(worker.run(shutdown.child_token()));
    ///
    /// tokio::signal::ctrl_c().await?;
    /// shutdown.cancel();
    /// running.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run(self, shutdown: CancellationToken) {
        let wakers = Arc::new(Wakers {
            queues: self
                .queues
                .iter()
                .map(|queue| (queue.name.clone(), Arc::new(Notify::new())))
                .collect(),
            scheduler: Arc::new(Notify::new()),
            housekeeping: Arc::new(Notify::new()),
            pauses: Arc::new(Notify::new()),
        });

        let mut tasks = JoinSet::new();
        tasks.spawn(listener::listen(
            Arc::clone(&self.shared),
            Arc::clone(&wakers),
            shutdown.clone(),
        ));
        if self.scheduler {
            tasks.spawn(scheduler::schedule(
                Arc::clone(&self.shared),
                Arc::clone(&wakers.scheduler),
                shutdown.clone(),
            ));
        }
        tasks.spawn(housekeeping::housekeep(
            Arc::clone(&self.shared),
            Arc::clone(&wakers.housekeeping),
            shutdown.clone(),
        ));
        let registration = WorkerRegistration::new(&self.queues, self.scheduler);
        let queue_names = registration.queue_names.clone();
        let (current, registered) = watch::channel(None);
        let lifeline = LifelineThread::start(Arc::clone(&self.shared), registration, current);
        let flusher_shutdown = CancellationToken::new();
        let mut flusher = AbortOnDrop(tokio::spawn(flusher::flush_progress(
            Arc::clone(&self.shared),
            flusher_shutdown.clone(),
        )));
        while !shutdown.is_cancelled() {
            match self.shared.reap_until_clean(true).await {
                Ok(_) => break,
                Err(e) => {
                    warn!(error = %e, "failed to take over the jobs of dead workers");
                    tokio::select! {
                        () = shutdown.cancelled() => {}
                        () = tokio::time::sleep(ERROR_BACKOFF) => {}
                    }
                }
            }
        }
        let pauses = Arc::new(Pauses::default());
        if let Err(e) = pauses
            .refresh(
                &self.shared.config.pool.pool(),
                &self.shared.config.schema,
                &queue_names,
            )
            .await
        {
            warn!(error = %e, "failed to read the paused queues");
        }
        tasks.spawn(watch_pauses(
            Arc::clone(&self.shared),
            Arc::clone(&pauses),
            Arc::clone(&wakers),
            shutdown.clone(),
        ));
        tasks.spawn(reaper::reap(
            Arc::clone(&self.shared),
            queue_names,
            registered.clone(),
            shutdown.clone(),
        ));
        for queue in &self.queues {
            for _ in 0..queue.concurrency {
                tasks.spawn(work_loop(
                    Arc::clone(&self.shared),
                    Arc::clone(&pauses),
                    queue.name.clone(),
                    Arc::clone(&wakers.queues[&queue.name]),
                    registered.clone(),
                    shutdown.clone(),
                ));
            }
        }

        shutdown.cancelled().await;
        self.shared.job_shutdown.cancel();
        let timeout = self.shared.settings.shutdown_timeout;
        if tokio::time::timeout(timeout, drain(&mut tasks))
            .await
            .is_err()
        {
            warn!(
                ?timeout,
                "running jobs did not finish before the shutdown timeout, aborting them"
            );
            let abandoned = self.shared.runs.live();
            tasks.abort_all();
            drain(&mut tasks).await;
            self.shared.flush_logged().await;
            if let Err(e) = interrupt(
                &self.shared.config.pool.pool(),
                &self.shared.config.schema,
                Target::Shutdown(&abandoned),
            )
            .await
            {
                warn!(error = %e, "failed to hand the aborted jobs back");
            }
        }
        flusher_shutdown.cancel();
        if let Err(e) = (&mut flusher.0).await {
            warn!(error = %e, "the progress flusher ended unexpectedly");
        }
        lifeline.retire().await;
    }
}

async fn work_loop(
    shared: Arc<Shared>,
    pauses: Arc<Pauses>,
    queue: String,
    waker: Arc<Notify>,
    mut current: Current,
    shutdown: CancellationToken,
) {
    while !shutdown.is_cancelled() {
        let alive = current.borrow_and_update().clone();
        let Some(incarnation) = alive.filter(|incarnation| !incarnation.dead().is_cancelled())
        else {
            tokio::select! {
                () = shutdown.cancelled() => return,
                changed = current.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
            continue;
        };
        if pauses.contains(&queue) {
            wait(&waker, &shutdown, shared.settings.poll_interval).await;
            continue;
        }
        match shared.process_next(&incarnation, &queue, &waker).await {
            Ok(true) => {}
            Ok(false) if incarnation.dead().is_cancelled() => {}
            Ok(false) => wait(&waker, &shutdown, shared.settings.poll_interval).await,
            Err(e) => {
                warn!(queue = %queue, error = %e, "failed to process a job");
                wait(&waker, &shutdown, ERROR_BACKOFF).await;
            }
        }
    }
}

pub(super) async fn wait(waker: &Notify, shutdown: &CancellationToken, duration: Duration) {
    tokio::select! {
        () = waker.notified() => {}
        () = tokio::time::sleep(duration) => {}
        () = shutdown.cancelled() => {}
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn drain(tasks: &mut JoinSet<()>) {
    while tasks.join_next().await.is_some() {}
}
