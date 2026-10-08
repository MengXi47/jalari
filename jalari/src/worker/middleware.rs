use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::job::registry::{ContextKey, JobFuture};
use crate::{JobError, JobId, JobResult, JobRun};

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
/// #[jalari::job(queue = "mail")]
/// impl Job for SendDigest {
///     const NAME: &'static str = "send_digest";
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
}

/// What a [`JobMiddleware`] knows about the attempt it wraps.
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
    pub async fn run(self) -> JobResult {
        let mut chain = self.chain;
        while let Some((first, rest)) = chain.split_first() {
            let provides = first.provides();
            if provides.is_unit() || provides.id == self.needs {
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

pub(crate) trait ErasedMiddleware: Send + Sync {
    fn name(&self) -> &'static str;
    fn provides(&self) -> ContextKey;
    fn check(&self) -> BoxFuture<'_, Result<(), JobError>>;
    fn call<'a>(&'a self, job: &'a JobMeta, next: Next<'a>) -> BoxFuture<'a, JobResult>;
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
}
