use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::builder::WorkerBuilder;
use super::heartbeat::{self, WorkerRegistration};
use super::middleware::ErasedMiddleware;
use super::{housekeeping, listener, scheduler};
use crate::init::Config;
use crate::job::registry::{CronRegistration, JobRegistration};
use crate::{Cron, RetryPolicy, WorkerConfig};

pub(super) const ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Process-local job runner that claims and executes jobs from the configured queues.
///
/// Start as many workers as you like, in one process or many; they coordinate only through
/// PostgreSQL. A job is claimed by exactly one worker at a time, and a worker that dies releases
/// its jobs so another worker can pick them up within seconds. A worker that loses its database
/// connection aborts the jobs it was running before anyone else can start them.
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
    pub(super) crons: Vec<DeclaredCron>,
    pub(super) settings: WorkerConfig,
    pub(super) retry_policy: Arc<dyn RetryPolicy>,
    pub(super) middlewares: Arc<[Arc<dyn ErasedMiddleware>]>,
    pub(super) job_shutdown: CancellationToken,
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
}

impl Worker {
    /// Starts configuring a worker.
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    /// Runs the worker until `shutdown` is cancelled.
    ///
    /// Besides executing jobs it listens for notifications, sends heartbeats, runs the recurring
    /// scheduler when enabled and performs housekeeping when the cluster turns it on. Database
    /// outages are logged with `tracing` and retried; they never end the loop.
    ///
    /// After cancellation it stops claiming jobs, signals running ones through
    /// [`shutdown_requested`](crate::shutdown_requested) and waits for them to finish. Jobs still
    /// running after [`WorkerConfig::shutdown_timeout`] are aborted, which counts as an attempt,
    /// and another worker runs them again.
    pub async fn run(self, shutdown: CancellationToken) {
        let wakers = Arc::new(Wakers {
            queues: self
                .queues
                .iter()
                .map(|queue| (queue.name.clone(), Arc::new(Notify::new())))
                .collect(),
            scheduler: Arc::new(Notify::new()),
            housekeeping: Arc::new(Notify::new()),
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
        let heartbeat_shutdown = CancellationToken::new();
        let heartbeat = tokio::spawn(heartbeat::heartbeat(
            Arc::clone(&self.shared),
            WorkerRegistration::new(&self.queues, self.scheduler),
            heartbeat_shutdown.clone(),
        ));
        for queue in &self.queues {
            for _ in 0..queue.concurrency {
                tasks.spawn(work_loop(
                    Arc::clone(&self.shared),
                    queue.name.clone(),
                    Arc::clone(&wakers.queues[&queue.name]),
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
            tasks.abort_all();
            drain(&mut tasks).await;
        }
        heartbeat_shutdown.cancel();
        if let Err(e) = heartbeat.await {
            warn!(error = %e, "the heartbeat task ended unexpectedly");
        }
    }
}

async fn work_loop(
    shared: Arc<Shared>,
    queue: String,
    waker: Arc<Notify>,
    shutdown: CancellationToken,
) {
    while !shutdown.is_cancelled() {
        match shared.process_next(&queue).await {
            Ok(true) => {}
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

async fn drain(tasks: &mut JoinSet<()>) {
    while tasks.join_next().await.is_some() {}
}
