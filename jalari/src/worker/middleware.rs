use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::{Instrument, warn};

use crate::job::context::{self, Captured};
use crate::job::registry::{ContextKey, JobFuture};
use crate::{JobError, JobId, JobResult, JobRun, Priority};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Code that wraps every job attempt on a worker, typically to rebuild the context a job runs in.
///
/// Register middleware with [`WorkerBuilder::middleware`](crate::WorkerBuilder::middleware); the
/// first one registered is the outermost. A middleware whose [`Provides`](Self::Provides) is
/// `()` wraps every job; any other middleware only wraps jobs whose
/// [`Job::Context`](crate::Job::Context) is its `Provides`, so jobs that do not need it are never
/// rejected by it. A middleware reads what the enqueuer stored with
/// [`scope`](crate::scope) through [`JobMeta::context`], prepares whatever the job needs, hands
/// it over with [`Next::provide`] and runs the rest of the chain with [`Next::run`]. Returning
/// an error instead of calling `run` rejects the attempt; a [`JobError::permanent`] fails the
/// job right away.
///
/// jalari attaches no meaning to the context: tenants, users, databases or tracing are all up to
/// the middleware.
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::{Job, JobError, JobMeta, JobMiddleware, JobResult, Next};
/// use jalari::sqlx::PgPool;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Caller {
///     user_id: i64,
/// }
///
/// struct CurrentUser {
///     id: i64,
///     db: PgPool,
/// }
///
/// struct UserMiddleware {
///     db: PgPool,
/// }
///
/// impl JobMiddleware for UserMiddleware {
///     type Provides = CurrentUser;
///
///     async fn check(&self) -> Result<(), JobError> {
///         jalari::sqlx::query("SELECT 1").execute(&self.db).await?;
///         Ok(())
///     }
///
///     async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
///         let Some(caller) = job.context::<Caller>()? else {
///             return Err(JobError::permanent(std::io::Error::other("no caller")));
///         };
///         next.provide(CurrentUser { id: caller.user_id, db: self.db.clone() })
///             .run()
///             .await
///     }
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct SendDigest;
///
/// #[jalari::job(name = "send_digest", queue = "mail")]
/// impl Job for SendDigest {
///     type Context = CurrentUser;
///
///     async fn run(self, user: &CurrentUser) -> JobResult {
///         println!("sending a digest to user {}", user.id);
///         Ok(())
///     }
/// }
///
/// # async fn example(db: PgPool) -> jalari::Result<()> {
/// let worker = jalari::Worker::builder()
///     .middleware(UserMiddleware { db })
///     .queue("mail", 4)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait JobMiddleware: Send + Sync + 'static {
    /// Type handed to jobs through [`Next::provide`]; `()` when the middleware provides nothing.
    ///
    /// A job whose [`Job::Context`](crate::Job::Context) is this type can only run on workers
    /// that register this middleware, and two middleware on one worker cannot provide the same
    /// type.
    type Provides: Send + Sync + 'static;

    /// Runs once in [`WorkerBuilder::build`](crate::WorkerBuilder::build), for example to make
    /// sure a database is reachable; does nothing by default.
    ///
    /// # Errors
    ///
    /// Returning an error makes `build` fail with
    /// [`MiddlewareCheckFailed`](crate::ErrorKind::MiddlewareCheckFailed).
    fn check(&self) -> impl Future<Output = Result<(), JobError>> + Send {
        async { Ok(()) }
    }

    /// Wraps one attempt; call `next.run()` to continue, or return an error to reject it.
    fn call(&self, job: &JobMeta, next: Next<'_>) -> impl Future<Output = JobResult> + Send;

    /// Runs after an attempt's result is saved; does nothing by default.
    ///
    /// The hook only observes; change the result in [`call`](Self::call) instead. Hooks run
    /// innermost first, and only on middleware that wrapped the attempt. Each one runs in its own
    /// task with the job's context attached, so jobs it enqueues inherit that context, while
    /// [`current_job`](crate::current_job) is `None`. A panic is logged and the next hook still
    /// runs. The worker waits for the hooks before it claims another job, so a slow hook holds
    /// the slot.
    ///
    /// Only the worker that ran the attempt calls it, and only once that worker saved the
    /// outcome. A lost run, an attempt aborted at shutdown, a job the reaper settles, and
    /// commands or expiry acting on a job that is not running do not call it.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use jalari::{Finished, JobMeta, JobMiddleware, JobResult, Next};
    ///
    /// struct Alerts;
    ///
    /// impl JobMiddleware for Alerts {
    ///     type Provides = ();
    ///
    ///     async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
    ///         next.run().await
    ///     }
    ///
    ///     async fn finished(&self, job: &JobMeta, outcome: &Finished) {
    ///         match outcome {
    ///             Finished::Failed { error } => eprintln!("job {} failed: {error}", job.id()),
    ///             Finished::Retrying { .. } if job.attempt() + 1 == job.max_attempts() => {
    ///                 eprintln!("job {} has one attempt left", job.id());
    ///             }
    ///             _ => {}
    ///         }
    ///     }
    /// }
    /// ```
    #[expect(unused_variables, reason = "the default hook ignores the attempt")]
    fn finished(&self, job: &JobMeta, outcome: &Finished) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// How an attempt ended, as saved by the worker that ran it.
///
/// New outcomes may be added in minor releases, so a `match` needs a `_` arm.
///
/// # Examples
///
/// ```rust
/// use jalari::Finished;
///
/// fn summary(outcome: &Finished) -> String {
///     match outcome {
///         Finished::Succeeded => "succeeded".to_owned(),
///         Finished::Retrying { delay, error } => format!("retrying in {delay:?} after {error}"),
///         Finished::Failed { error } | Finished::Discarded { error } => error.clone(),
///         _ => format!("{outcome:?}"),
///     }
/// }
///
/// assert_eq!(summary(&Finished::Succeeded), "succeeded");
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finished {
    /// The job succeeded.
    Succeeded,
    /// The attempt failed and the job runs again after `delay`.
    Retrying {
        /// Wait before the next attempt.
        delay: Duration,
        /// Message of the error the attempt returned.
        error: String,
    },
    /// The attempt failed and the job will not run again.
    Failed {
        /// Message of the error the attempt returned.
        error: String,
    },
    /// The job discarded itself and will not run again.
    Discarded {
        /// Message of the error the attempt returned.
        error: String,
    },
    /// The job asked to run again after `delay`, without counting the attempt.
    Snoozed {
        /// Wait before the next attempt.
        delay: Duration,
    },
    /// The worker shut down and gave the job back to its queue without counting the attempt.
    Released,
    /// The job was stopped and waits for a restart.
    Stopped,
    /// The job was cancelled.
    Cancelled,
}

/// What a [`JobMiddleware`] knows about the attempt it wraps.
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::sqlx::types::chrono::Utc;
/// use jalari::{JobMeta, JobMiddleware, JobResult, Next};
///
/// struct QueueDelay;
///
/// impl JobMiddleware for QueueDelay {
///     type Provides = ();
///
///     async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
///         let waited = Utc::now() - job.scheduled_at();
///         println!(
///             "{} #{} at {:?} waited {} ms for a worker",
///             job.name(),
///             job.id(),
///             job.priority(),
///             waited.num_milliseconds()
///         );
///         next.run().await
///     }
/// }
/// ```
#[derive(Debug)]
pub struct JobMeta {
    run: JobRun,
    context: Option<Value>,
}

impl JobMeta {
    pub(crate) fn new(run: JobRun, context: Option<Value>) -> Self {
        Self { run, context }
    }

    /// Id of the job; it stays the same across retries.
    pub fn id(&self) -> JobId {
        self.run.id()
    }

    /// [`Job::NAME`](crate::Job::NAME) of the job.
    pub fn name(&self) -> &'static str {
        self.run.name()
    }

    /// Queue the job was taken from.
    pub fn queue(&self) -> &str {
        self.run.queue()
    }

    /// Number of this attempt, starting at 1.
    pub fn attempt(&self) -> i32 {
        self.run.attempt()
    }

    /// Attempts allowed before the job is marked failed.
    pub fn max_attempts(&self) -> i32 {
        self.run.max_attempts()
    }

    /// Whether this attempt is the last one [`max_attempts`](Self::max_attempts) allows; a
    /// permanent error can end the job earlier.
    pub fn is_last_attempt(&self) -> bool {
        self.attempt() >= self.max_attempts()
    }

    /// When the job was first enqueued; see [`JobRun::created_at`].
    pub fn created_at(&self) -> DateTime<Utc> {
        self.run.created_at()
    }

    /// When this attempt became due; see [`JobRun::scheduled_at`].
    pub fn scheduled_at(&self) -> DateTime<Utc> {
        self.run.scheduled_at()
    }

    /// [`Priority`] the job was claimed with.
    pub fn priority(&self) -> Priority {
        self.run.priority()
    }

    /// Error of the previous attempt; see [`JobRun::last_error`].
    pub fn last_error(&self) -> Option<&str> {
        self.run.last_error()
    }

    /// Id of the job that was running when this one was enqueued, if any.
    pub fn parent_id(&self) -> Option<JobId> {
        self.run.parent_id()
    }

    /// Id of the first job in this chain; see [`JobRun::root_id`].
    pub fn root_id(&self) -> JobId {
        self.run.root_id()
    }

    /// The context stored with the job, decoded as `T`; `None` when the job has none.
    ///
    /// # Errors
    ///
    /// Returns a [`JobError::permanent`] if the stored context does not decode as `T`.
    pub fn context<T: DeserializeOwned>(&self) -> Result<Option<T>, JobError> {
        self.context
            .as_ref()
            .map(|value| T::deserialize(value).map_err(JobError::permanent))
            .transpose()
    }

    /// The context stored with the job, as JSON.
    pub fn raw_context(&self) -> Option<&Value> {
        self.context.as_ref()
    }
}

#[doc(hidden)]
pub struct Provided(HashMap<TypeId, Box<dyn Any + Send + Sync>>);

impl Provided {
    pub fn new() -> Self {
        let mut provided = Self(HashMap::new());
        provided.insert(());
        provided
    }

    fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.0.insert(TypeId::of::<T>(), Box::new(value));
    }

    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref())
    }
}

impl Default for Provided {
    fn default() -> Self {
        Self::new()
    }
}

/// The rest of the middleware chain, ending with the job itself.
pub struct Next<'a> {
    chain: &'a [Arc<dyn ErasedMiddleware>],
    job: &'a JobMeta,
    needs: TypeId,
    provided: Provided,
    run: fn(Provided, &[u8]) -> JobFuture,
    payload: &'a [u8],
}

impl<'a> Next<'a> {
    pub(crate) fn start(
        chain: &'a [Arc<dyn ErasedMiddleware>],
        job: &'a JobMeta,
        needs: ContextKey,
        run: fn(Provided, &[u8]) -> JobFuture,
        payload: &'a [u8],
    ) -> Self {
        Self {
            chain,
            job,
            needs: needs.id,
            provided: Provided::new(),
            run,
            payload,
        }
    }

    /// Hands `value` to the job, which receives it when its
    /// [`Job::Context`](crate::Job::Context) is `T`.
    ///
    /// Providing the same type again replaces the earlier value.
    #[must_use]
    pub fn provide<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.provided.insert(value);
        self
    }

    /// Runs the remaining middleware and then the job.
    ///
    /// # Errors
    ///
    /// Returns the error of the job or of a middleware further in; pass it on unchanged unless
    /// the middleware means to change the outcome.
    pub async fn run(self) -> JobResult {
        let mut chain = self.chain;
        while let Some((first, rest)) = chain.split_first() {
            if wraps(first.as_ref(), self.needs) {
                let job = self.job;
                let next = Next {
                    chain: rest,
                    job,
                    needs: self.needs,
                    provided: self.provided,
                    run: self.run,
                    payload: self.payload,
                };
                return first.call(job, next).await;
            }
            chain = rest;
        }
        (self.run)(self.provided, self.payload).await
    }
}

fn wraps(middleware: &dyn ErasedMiddleware, needs: TypeId) -> bool {
    let provides = middleware.provides();
    provides.is_unit() || provides.id == needs
}

pub(crate) async fn report_finished(
    chain: &[Arc<dyn ErasedMiddleware>],
    meta: Arc<JobMeta>,
    needs: ContextKey,
    captured: Captured,
    outcome: Finished,
) {
    let outcome = Arc::new(outcome);
    let mut hooks = JoinSet::new();
    for middleware in chain.iter().rev() {
        if !wraps(middleware.as_ref(), needs.id) {
            continue;
        }
        let hook = {
            let middleware = Arc::clone(middleware);
            let meta = Arc::clone(&meta);
            let outcome = Arc::clone(&outcome);
            async move { middleware.finished(&meta, &outcome).await }
        };
        hooks.spawn(context::within(captured.clone(), hook).in_current_span());
        if let Some(Err(e)) = hooks.join_next().await {
            warn!(middleware = middleware.name(), error = %e, "middleware finished hook failed");
        }
    }
}

pub(crate) trait ErasedMiddleware: Send + Sync {
    fn name(&self) -> &'static str;
    fn provides(&self) -> ContextKey;
    fn check(&self) -> BoxFuture<'_, Result<(), JobError>>;
    fn call<'a>(&'a self, job: &'a JobMeta, next: Next<'a>) -> BoxFuture<'a, JobResult>;
    fn finished<'a>(&'a self, job: &'a JobMeta, outcome: &'a Finished) -> BoxFuture<'a, ()>;
}

impl<M: JobMiddleware> ErasedMiddleware for M {
    fn name(&self) -> &'static str {
        type_name::<M>()
    }

    fn provides(&self) -> ContextKey {
        ContextKey::of::<M::Provides>()
    }

    fn check(&self) -> BoxFuture<'_, Result<(), JobError>> {
        Box::pin(JobMiddleware::check(self))
    }

    fn call<'a>(&'a self, job: &'a JobMeta, next: Next<'a>) -> BoxFuture<'a, JobResult> {
        Box::pin(JobMiddleware::call(self, job, next))
    }

    fn finished<'a>(&'a self, job: &'a JobMeta, outcome: &'a Finished) -> BoxFuture<'a, ()> {
        Box::pin(JobMiddleware::finished(self, job, outcome))
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;
    use std::sync::Mutex;

    use serde_json::json;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::current_job;
    use crate::job::{RunControl, run};

    struct Needed;

    struct Other;

    #[derive(Debug, PartialEq)]
    struct Call {
        label: &'static str,
        job: JobId,
        outcome: Finished,
        context: Option<Value>,
        in_job: bool,
    }

    type Log = Arc<Mutex<Vec<Call>>>;

    struct Recorder<P> {
        label: &'static str,
        log: Log,
        provides: PhantomData<fn() -> P>,
    }

    impl<P: Send + Sync + 'static> JobMiddleware for Recorder<P> {
        type Provides = P;

        async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
            next.run().await
        }

        async fn finished(&self, job: &JobMeta, outcome: &Finished) {
            let call = Call {
                label: self.label,
                job: job.id(),
                outcome: outcome.clone(),
                context: context::current().into_value("hook").unwrap(),
                in_job: current_job().is_some(),
            };
            self.log.lock().unwrap().push(call);
        }
    }

    fn recorder<P: Send + Sync + 'static>(
        label: &'static str,
        log: &Log,
    ) -> Arc<dyn ErasedMiddleware> {
        Arc::new(Recorder::<P> {
            label,
            log: Arc::clone(log),
            provides: PhantomData,
        })
    }

    struct Panics;

    impl JobMiddleware for Panics {
        type Provides = ();

        async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
            next.run().await
        }

        async fn finished(&self, _job: &JobMeta, _outcome: &Finished) {
            panic!("hook exploded");
        }
    }

    struct Hangs(Mutex<Option<oneshot::Sender<()>>>);

    impl JobMiddleware for Hangs {
        type Provides = ();

        async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
            next.run().await
        }

        async fn finished(&self, _job: &JobMeta, _outcome: &Finished) {
            let _held = self.0.lock().unwrap().take();
            std::future::pending::<()>().await;
        }
    }

    fn job_run(attempt: i32, max_attempts: i32) -> JobRun {
        let control = Arc::new(RunControl::new(
            7,
            1,
            None,
            0,
            CancellationToken::new(),
            CancellationToken::new(),
            false,
        ));
        JobRun {
            attempt,
            max_attempts,
            ..JobRun::for_test(7, control)
        }
    }

    fn retrying() -> Finished {
        Finished::Retrying {
            delay: Duration::from_secs(4),
            error: "boom".to_owned(),
        }
    }

    async fn report<T: 'static>(chain: &[Arc<dyn ErasedMiddleware>]) {
        let context = json!({ "tenant": "acme" });
        let meta = Arc::new(JobMeta::new(job_run(1, 3), Some(context.clone())));
        let captured = Captured::from_stored(Some(context));
        let report = report_finished(chain, meta, ContextKey::of::<T>(), captured, retrying());
        run::within(job_run(1, 3), report).await;
    }

    fn labels(log: &Log) -> Vec<&'static str> {
        log.lock().unwrap().iter().map(|call| call.label).collect()
    }

    #[test]
    fn test_is_last_attempt() {
        for (attempt, max_attempts, last) in [
            (1, 3, false),
            (2, 3, false),
            (3, 3, true),
            (4, 3, true),
            (1, 1, true),
        ] {
            let meta = JobMeta::new(job_run(attempt, max_attempts), None);
            assert_eq!(
                meta.is_last_attempt(),
                last,
                "attempt {attempt} of {max_attempts}"
            );
        }
    }

    #[tokio::test]
    async fn test_finished_runs_innermost_first_with_the_job_context() {
        let log = Log::default();
        report::<()>(&[recorder::<()>("outer", &log), recorder::<()>("inner", &log)]).await;
        let expected = ["inner", "outer"].map(|label| Call {
            label,
            job: JobId(7),
            outcome: retrying(),
            context: Some(json!({ "tenant": "acme" })),
            in_job: false,
        });
        assert_eq!(*log.lock().unwrap(), expected);
    }

    #[tokio::test]
    async fn test_finished_skips_middleware_that_did_not_wrap() {
        let log = Log::default();
        let chain = [
            recorder::<()>("every", &log),
            recorder::<Other>("other", &log),
            recorder::<Needed>("needed", &log),
        ];
        report::<Needed>(&chain).await;
        assert_eq!(labels(&log), ["needed", "every"]);
        log.lock().unwrap().clear();
        report::<()>(&chain).await;
        assert_eq!(labels(&log), ["every"]);
    }

    #[tokio::test]
    async fn test_finished_panic_is_contained() {
        let log = Log::default();
        let chain: [Arc<dyn ErasedMiddleware>; 3] = [
            recorder::<()>("outer", &log),
            Arc::new(Panics),
            recorder::<()>("inner", &log),
        ];
        report::<()>(&chain).await;
        assert_eq!(labels(&log), ["inner", "outer"]);
    }

    #[tokio::test]
    async fn test_finished_hook_ends_with_its_report() {
        let (held, released) = oneshot::channel();
        let chain: [Arc<dyn ErasedMiddleware>; 1] = [Arc::new(Hangs(Mutex::new(Some(held))))];
        let report = tokio::time::timeout(Duration::from_millis(50), report::<()>(&chain));
        assert!(report.await.is_err(), "the hook never returns");
        let released = tokio::time::timeout(Duration::from_secs(5), released).await;
        assert!(
            matches!(released, Ok(Err(_))),
            "dropping the report aborts the running hook"
        );
    }
}
