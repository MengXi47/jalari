use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sqlx::Connection;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::middleware::ErasedMiddleware;
use super::runner::{DeclaredCron, QueueConfig, Shared, Worker};
use super::runs::RunBook;
use crate::init::{Config, config};
use crate::job::fingerprint;
use crate::job::registry::{JobRegistration, entries};
use crate::{
    Cron, Error, ErrorKind, ExponentialBackoff, JobMiddleware, Result, RetryPolicy, WorkerConfig,
};

const MIN_CONNECTIONS: u32 = 2;

/// Configuration for a [`Worker`], created by [`Worker::builder`].
#[must_use]
pub struct WorkerBuilder {
    queues: Vec<QueueConfig>,
    settings: WorkerConfig,
    retry_policy: Arc<dyn RetryPolicy>,
    scheduler: bool,
    middlewares: Vec<Arc<dyn ErasedMiddleware>>,
}

impl WorkerBuilder {
    pub(super) fn new() -> Self {
        Self {
            queues: Vec::new(),
            settings: WorkerConfig::default(),
            retry_policy: Arc::new(ExponentialBackoff::default()),
            scheduler: true,
            middlewares: Vec::new(),
        }
    }

    /// Wraps every job attempt in `middleware`; the first one added is the outermost.
    ///
    /// Jobs whose [`Job::Context`](crate::Job::Context) is the middleware's
    /// [`Provides`](JobMiddleware::Provides) receive the value it provides.
    pub fn middleware<M: JobMiddleware>(mut self, middleware: M) -> Self {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    /// Turns the recurring scheduler on or off for this worker; on by default.
    ///
    /// The scheduler enqueues due cron runs and syncs the schedules declared in code. Running it
    /// on several workers is safe and keeps schedules firing if one of them stops; turn it off on
    /// workers that should only process jobs. At least one running worker needs it on.
    pub fn scheduler(mut self, enabled: bool) -> Self {
        self.scheduler = enabled;
        self
    }

    /// Processes jobs from `name`, running up to `concurrency` of them at once.
    ///
    /// Call it once per queue. A running job holds no connection: workers use the pool only for
    /// the short statements that claim and finish jobs, so `concurrency` is independent of the
    /// pool size.
    pub fn queue(mut self, name: &str, concurrency: usize) -> Self {
        self.queues.push(QueueConfig {
            name: name.to_owned(),
            concurrency,
        });
        self
    }

    /// Replaces the default timings such as the poll interval.
    pub fn config(mut self, settings: WorkerConfig) -> Self {
        self.settings = settings;
        self
    }

    /// Replaces the default [`ExponentialBackoff`] retry policy.
    pub fn retry_policy<R: RetryPolicy>(mut self, retry_policy: R) -> Self {
        self.retry_policy = Arc::new(retry_policy);
        self
    }

    /// Validates the configuration and creates the worker.
    ///
    /// The worker also opens one session outside the pool, its lifeline, which must be a direct
    /// or session-pooled connection to PostgreSQL.
    ///
    /// The worker runs every `#[jalari::job]` linked into the binary and skips jobs whose name
    /// it does not know, leaving them to other workers. It also skips jobs whose
    /// [`Job::Context`](crate::Job::Context) no registered middleware provides, so workers with
    /// different middleware can share a database.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A job whose default queue this worker serves needs a context no middleware provides
    ///   ([`MissingContext`](ErrorKind::MissingContext))
    /// - Two middleware provide the same type
    ///   ([`DuplicateContextProvider`](ErrorKind::DuplicateContextProvider))
    /// - A middleware's [`check`](JobMiddleware::check) fails
    ///   ([`MiddlewareCheckFailed`](ErrorKind::MiddlewareCheckFailed))
    /// - No queue is configured, one is configured twice or has a concurrency of zero
    ///   ([`NoQueueConfigured`](ErrorKind::NoQueueConfigured),
    ///   [`DuplicateQueue`](ErrorKind::DuplicateQueue),
    ///   [`InvalidConcurrency`](ErrorKind::InvalidConcurrency))
    /// - The binary contains no job, or two jobs share a name or an alias
    ///   ([`NoJobRegistered`](ErrorKind::NoJobRegistered),
    ///   [`DuplicateJobName`](ErrorKind::DuplicateJobName))
    /// - A declared cron expression or time zone is invalid
    ///   ([`InvalidCronExpression`](ErrorKind::InvalidCronExpression),
    ///   [`UnknownTimezone`](ErrorKind::UnknownTimezone))
    /// - [`init`](crate::init) has not completed ([`NotInitialized`](ErrorKind::NotInitialized))
    /// - The pool's `max_connections` is below 2
    ///   ([`InsufficientConnections`](ErrorKind::InsufficientConnections))
    /// - The pool does not keep a session across statements, as behind a transaction pooler
    ///   ([`NoSessionConnection`](ErrorKind::NoSessionConnection))
    pub async fn build(self) -> Result<Worker> {
        validate_queues(&self.queues)?;
        let jobs = collect_jobs()?;
        let crons = parse_crons(&jobs)?;
        let provided = collect_providers(&self.middlewares)?;
        let jobs = select_jobs(jobs, &provided, &self.queues)?;
        let config = config()?;
        check_connections(config)?;
        check_session(config).await?;
        check_middlewares(&self.middlewares).await?;
        let task_names = jobs.keys().map(|name| (*name).to_owned()).collect();
        let served = fingerprint::served(jobs.iter().map(|(name, job)| (*name, *job)));
        Ok(Worker {
            shared: Arc::new(Shared {
                config,
                jobs,
                task_names,
                served,
                crons,
                settings: self.settings,
                retry_policy: self.retry_policy,
                middlewares: self.middlewares.into(),
                job_shutdown: CancellationToken::new(),
                runs: RunBook::default(),
                flush_now: Notify::new(),
            }),
            queues: self.queues,
            scheduler: self.scheduler,
        })
    }
}

async fn check_session(config: &Config) -> Result<()> {
    let mut connection = config.pool.pool().acquire().await?.detach();
    let key = config.schema.lock_key("session_check");
    let (pid, locked): (i32, bool) = sqlx::query_as(
        "SELECT pg_backend_pid(),
             pg_try_advisory_lock(hashtextextended($1 || pg_backend_pid()::text, 0))",
    )
    .bind(&key)
    .fetch_one(&mut connection)
    .await?;
    let kept: bool = sqlx::query_scalar(
        "SELECT pg_backend_pid() = $1 AND EXISTS (
             SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
         )",
    )
    .bind(pid)
    .fetch_one(&mut connection)
    .await?;
    connection.close().await?;
    if !locked || !kept {
        return Err(Error::new(
            ErrorKind::NoSessionConnection,
            "a session advisory lock taken on a pooled connection was gone at the next \
             statement; point the pool at PostgreSQL or a session-mode pooler, not a \
             transaction pooler"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_queues(queues: &[QueueConfig]) -> Result<()> {
    if queues.is_empty() {
        return Err(Error::new(
            ErrorKind::NoQueueConfigured,
            "call WorkerBuilder::queue at least once".to_owned(),
        ));
    }
    let mut names = HashSet::new();
    for queue in queues {
        if queue.concurrency == 0 {
            return Err(Error::new(
                ErrorKind::InvalidConcurrency,
                format!("queue {:?} needs a concurrency of at least 1", queue.name),
            ));
        }
        if !names.insert(queue.name.as_str()) {
            return Err(Error::new(
                ErrorKind::DuplicateQueue,
                format!("queue {:?} is configured more than once", queue.name),
            ));
        }
    }
    Ok(())
}

fn collect_jobs() -> Result<HashMap<&'static str, &'static JobRegistration>> {
    let mut jobs = HashMap::new();
    let mut collisions = Vec::new();
    for (name, registration) in entries() {
        if let Some(owner) = jobs.insert(name, registration) {
            let mut owners = [owner.name, registration.name];
            owners.sort_unstable();
            collisions.push(format!(
                "{name:?} is the NAME or an alias of both {:?} and {:?}",
                owners[0], owners[1]
            ));
        }
    }
    if !collisions.is_empty() {
        collisions.sort();
        return Err(Error::new(
            ErrorKind::DuplicateJobName,
            collisions.join("; "),
        ));
    }
    if jobs.is_empty() {
        return Err(Error::new(
            ErrorKind::NoJobRegistered,
            "no #[jalari::job] is linked into this binary".to_owned(),
        ));
    }
    Ok(jobs)
}

fn collect_providers(middlewares: &[Arc<dyn ErasedMiddleware>]) -> Result<HashSet<TypeId>> {
    let mut provided = HashSet::new();
    for middleware in middlewares {
        let key = middleware.provides();
        if !key.is_unit() && !provided.insert(key.id) {
            return Err(Error::new(
                ErrorKind::DuplicateContextProvider,
                format!(
                    "more than one middleware provides {}; wrap one of them in its own type",
                    key.name
                ),
            ));
        }
    }
    Ok(provided)
}

fn select_jobs(
    jobs: HashMap<&'static str, &'static JobRegistration>,
    provided: &HashSet<TypeId>,
    queues: &[QueueConfig],
) -> Result<HashMap<&'static str, &'static JobRegistration>> {
    let mut selected = HashMap::new();
    let mut missing = Vec::new();
    for (name, registration) in jobs {
        let key = (registration.context)();
        if key.is_unit() || provided.contains(&key.id) {
            selected.insert(name, registration);
        } else if name == registration.name
            && queues.iter().any(|queue| queue.name == registration.queue)
        {
            missing.push(format!(
                "{name} (queue {:?}) needs {}",
                registration.queue, key.name
            ));
        }
    }
    if !missing.is_empty() {
        missing.sort();
        return Err(Error::new(
            ErrorKind::MissingContext,
            format!(
                "no middleware provides the context of: {}; register one with \
                 WorkerBuilder::middleware",
                missing.join(", ")
            ),
        ));
    }
    Ok(selected)
}

async fn check_middlewares(middlewares: &[Arc<dyn ErasedMiddleware>]) -> Result<()> {
    for middleware in middlewares {
        if let Err(e) = middleware.check().await {
            return Err(Error::new(
                ErrorKind::MiddlewareCheckFailed,
                format!("{}: {}", middleware.name(), e.msg()),
            ));
        }
    }
    Ok(())
}

fn parse_crons(
    jobs: &HashMap<&'static str, &'static JobRegistration>,
) -> Result<Vec<DeclaredCron>> {
    let mut crons = Vec::new();
    for (name, registration) in jobs {
        if *name != registration.name {
            continue;
        }
        let Some(declaration) = &registration.cron else {
            continue;
        };
        let cron = Cron::parse(declaration.expression, declaration.timezone).map_err(|err| {
            Error::new(
                err.kind,
                format!("#[jalari::job] {:?}: {}", registration.name, err.msg),
            )
        })?;
        crons.push(DeclaredCron {
            registration,
            declaration,
            cron,
        });
    }
    Ok(crons)
}

fn check_connections(config: &Config) -> Result<()> {
    let max_connections = config.pool.pool().options().get_max_connections();
    if max_connections < MIN_CONNECTIONS {
        return Err(Error::new(
            ErrorKind::InsufficientConnections,
            format!(
                "pool max_connections is {max_connections} but a worker needs \
                 {MIN_CONNECTIONS}: 1 for the notification listener and 1 for claiming and \
                 finishing jobs"
            ),
        ));
    }
    Ok(())
}
