use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, Cron, EnqueueOptions, ErrorKind, ExponentialBackoff, Job, JobError, JobId,
    JobMeta, JobMiddleware, JobResult, Next, OnConflict, Schema, Worker, WorkerConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    tenant: String,
    attempt: i32,
    job_id: JobId,
}

static RUNS: LazyLock<Mutex<HashMap<String, Vec<Seen>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static EVENTS: LazyLock<Mutex<HashMap<i64, Vec<String>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static SHUTDOWN_SEEN: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_STARTED: AtomicBool = AtomicBool::new(false);

fn record_event(job_id: JobId, event: String) {
    EVENTS
        .lock()
        .unwrap()
        .entry(job_id.0)
        .or_default()
        .push(event);
}

fn events(job_id: JobId) -> Vec<String> {
    EVENTS
        .lock()
        .unwrap()
        .get(&job_id.0)
        .cloned()
        .unwrap_or_default()
}

fn runs(label: &str) -> Vec<Seen> {
    RUNS.lock().unwrap().get(label).cloned().unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Caller {
    tenant: String,
}

impl Caller {
    fn new(tenant: &str) -> Self {
        Self {
            tenant: tenant.to_owned(),
        }
    }
}

struct Tenant(String);

struct TenantMiddleware;

impl JobMiddleware for TenantMiddleware {
    type Provides = Tenant;

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        record_event(job.id(), "tenant".to_owned());
        let Some(caller) = job.context::<Caller>()? else {
            return Err(JobError::permanent(io::Error::other("missing caller")));
        };
        if caller.tenant == "disabled" {
            return Err(JobError::permanent(io::Error::other("tenant disabled")));
        }
        next.provide(Tenant(caller.tenant)).run().await
    }
}

struct TraceMiddleware;

impl JobMiddleware for TraceMiddleware {
    type Provides = ();

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        record_event(job.id(), "trace".to_owned());
        next.run().await
    }
}

struct Unreachable;

struct FailingCheck;

impl JobMiddleware for FailingCheck {
    type Provides = Unreachable;

    async fn check(&self) -> Result<(), JobError> {
        Err(JobError::new("database unreachable".to_owned()))
    }

    async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
        next.run().await
    }
}

#[derive(Serialize, Deserialize, Default)]
struct TenantJob {
    label: String,
    fail_first: bool,
    spawn_child: bool,
}

impl TenantJob {
    fn labelled(label: &str) -> Self {
        Self {
            label: label.to_owned(),
            ..Self::default()
        }
    }
}

#[jalari::job(queue = "tenant")]
impl Job for TenantJob {
    const NAME: &'static str = "c_tenant";
    const MAX_ATTEMPTS: i32 = 3;
    type Context = Tenant;

    async fn run(self, tenant: &Tenant) -> JobResult {
        let job = jalari::current_job().expect("inside a job");
        record_event(job.id(), format!("job:{}", tenant.0));
        RUNS.lock()
            .unwrap()
            .entry(self.label.clone())
            .or_default()
            .push(Seen {
                tenant: tenant.0.clone(),
                attempt: job.attempt(),
                job_id: job.id(),
            });
        if self.fail_first && job.attempt() == 1 {
            return Err(JobError::new("first attempt fails".to_owned()));
        }
        if self.spawn_child {
            jalari::enqueue(&TenantJob::labelled(&format!("{}-child", self.label)))
                .await
                .map_err(|e| JobError::new(e.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct PlainJob {
    label: String,
}

#[jalari::job(queue = "tenant")]
impl Job for PlainJob {
    const NAME: &'static str = "c_plain";

    async fn run(self) -> JobResult {
        let job = jalari::current_job().expect("inside a job");
        record_event(job.id(), "job:plain".to_owned());
        RUNS.lock()
            .unwrap()
            .entry(self.label)
            .or_default()
            .push(Seen {
                tenant: String::new(),
                attempt: job.attempt(),
                job_id: job.id(),
            });
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct WaitsForShutdown;

#[jalari::job(queue = "shutdown")]
impl Job for WaitsForShutdown {
    const NAME: &'static str = "c_waits_for_shutdown";
    const MAX_ATTEMPTS: i32 = 1;

    async fn run(self) -> JobResult {
        SHUTDOWN_STARTED.store(true, Ordering::SeqCst);
        jalari::shutdown_requested().await;
        SHUTDOWN_SEEN.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct Fixture {
    pool: PgPool,
    schema_name: String,
}

impl Fixture {
    fn table(&self, name: &str) -> String {
        format!("\"{}\".{name}", self.schema_name)
    }

    async fn context(&self, id: JobId) -> Option<Value> {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT context FROM {} WHERE id = $1",
            self.table("job")
        )))
        .bind(id.0)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn state(&self, id: JobId) -> (String, Option<String>) {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state, last_error FROM {} WHERE id = $1",
            self.table("job")
        )))
        .bind(id.0)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn wait_for_state(&self, id: JobId, state: &str) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let (current, last_error) = self.state(id).await;
            if current == state {
                return last_error;
            }
            assert!(
                Instant::now() < deadline,
                "job {id} stuck in {current}: {last_error:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn job_by_label(&self, label: &str) -> JobId {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let id: Option<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT id FROM {} WHERE convert_from(payload, 'UTF8')::jsonb ->> 'label' = $1",
                self.table("job")
            )))
            .bind(label)
            .fetch_optional(&self.pool)
            .await
            .unwrap();
            if let Some(id) = id {
                return JobId(id);
            }
            assert!(Instant::now() < deadline, "no job labelled {label}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_ctx_{}_{nanos}", std::process::id())
}

fn worker_config() -> WorkerConfig {
    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn fast_retry() -> ExponentialBackoff {
    ExponentialBackoff {
        base: Duration::from_millis(10),
        max: Duration::from_millis(50),
        jitter: 0.5,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_context() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let schema_name = unique_schema_name();
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&url)
        .await
        .unwrap();
    jalari::init(pool.clone())
        .schema(Schema::named(&schema_name).unwrap())
        .migrate()
        .await
        .unwrap();
    let fixture = Fixture {
        pool: pool.clone(),
        schema_name: schema_name.clone(),
    };

    check_enqueue_stores_context(&fixture).await;
    check_build_validates_middleware().await;
    check_current_job_is_none_outside_jobs();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .middleware(TraceMiddleware)
        .middleware(TenantMiddleware)
        .queue("tenant", 4)
        .config(worker_config())
        .retry_policy(fast_retry())
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.child_token()));

    check_middleware_provides_context_in_order(&fixture).await;
    check_job_without_context_runs_through_middleware(&fixture).await;
    check_middleware_rejection_fails_job(&fixture).await;
    check_undecodable_context_fails_job(&fixture).await;
    check_retry_keeps_context(&fixture).await;
    check_nested_enqueue_inherits_context(&fixture).await;
    check_recurring_keeps_scope_context(&fixture).await;
    check_shutdown_reaches_running_job(&fixture).await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .unwrap()
        .unwrap();

    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}

async fn check_enqueue_stores_context(fixture: &Fixture) {
    let plain = || PlainJob {
        label: "stored".to_owned(),
    };
    let none = jalari::enqueue_with(&plain(), EnqueueOptions::new().queue("idle"))
        .await
        .unwrap()
        .id();
    assert_eq!(fixture.context(none).await, None);

    let explicit = jalari::enqueue_with(
        &plain(),
        EnqueueOptions::new()
            .queue("idle")
            .context(&Caller::new("acme")),
    )
    .await
    .unwrap()
    .id();
    assert_eq!(
        fixture.context(explicit).await,
        Some(json!({"tenant": "acme"}))
    );

    let (scoped, overridden, cleared) = jalari::scope(&Caller::new("globex"), async {
        let scoped = jalari::enqueue_with(&plain(), EnqueueOptions::new().queue("idle"))
            .await
            .unwrap()
            .id();
        let overridden = jalari::enqueue_with(
            &plain(),
            EnqueueOptions::new()
                .queue("idle")
                .context(&Caller::new("initech")),
        )
        .await
        .unwrap()
        .id();
        let cleared =
            jalari::enqueue_with(&plain(), EnqueueOptions::new().queue("idle").no_context())
                .await
                .unwrap()
                .id();
        (scoped, overridden, cleared)
    })
    .await;
    assert_eq!(
        fixture.context(scoped).await,
        Some(json!({"tenant": "globex"}))
    );
    assert_eq!(
        fixture.context(overridden).await,
        Some(json!({"tenant": "initech"}))
    );
    assert_eq!(fixture.context(cleared).await, None);

    let mut transaction = fixture.pool.begin().await.unwrap();
    let in_transaction = jalari::scope(
        &Caller::new("umbrella"),
        jalari::enqueue_in(
            &mut transaction,
            &plain(),
            EnqueueOptions::new().queue("idle"),
        ),
    )
    .await
    .unwrap()
    .id();
    transaction.commit().await.unwrap();
    assert_eq!(
        fixture.context(in_transaction).await,
        Some(json!({"tenant": "umbrella"}))
    );

    let keyed = |tenant: &str| {
        EnqueueOptions::new()
            .queue("idle")
            .job_key("replace-context")
            .on_conflict(OnConflict::Replace)
            .context(&Caller::new(tenant))
    };
    let first = jalari::enqueue_with(&plain(), keyed("before"))
        .await
        .unwrap()
        .id();
    let replaced = jalari::enqueue_with(&plain(), keyed("after"))
        .await
        .unwrap();
    assert_eq!(replaced.id(), first);
    assert_eq!(
        fixture.context(first).await,
        Some(json!({"tenant": "after"}))
    );

    let mut unencodable = HashMap::new();
    unencodable.insert(vec![1_u8], 1);
    let err = jalari::enqueue_with(&plain(), EnqueueOptions::new().context(&unencodable))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::PayloadEncodeFailed);
}

async fn check_build_validates_middleware() {
    let err = Worker::builder()
        .queue("tenant", 1)
        .config(worker_config())
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind, ErrorKind::MissingContext);
    assert!(err.msg.contains("c_tenant"), "{}", err.msg);
    assert!(err.msg.contains("Tenant"), "{}", err.msg);

    let err = Worker::builder()
        .middleware(TenantMiddleware)
        .middleware(TenantMiddleware)
        .queue("tenant", 1)
        .config(worker_config())
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind, ErrorKind::DuplicateContextProvider);

    let err = Worker::builder()
        .middleware(TenantMiddleware)
        .middleware(FailingCheck)
        .queue("tenant", 1)
        .config(worker_config())
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind, ErrorKind::MiddlewareCheckFailed);
    assert!(err.msg.contains("database unreachable"), "{}", err.msg);

    Worker::builder()
        .middleware(TraceMiddleware)
        .middleware(TraceMiddleware)
        .queue("shutdown", 1)
        .config(worker_config())
        .build()
        .await
        .unwrap();
}

fn check_current_job_is_none_outside_jobs() {
    assert!(jalari::current_job().is_none());
}

async fn check_middleware_provides_context_in_order(fixture: &Fixture) {
    let id = jalari::scope(
        &Caller::new("acme"),
        jalari::enqueue(&TenantJob::labelled("ordered")),
    )
    .await
    .unwrap()
    .id();
    fixture.wait_for_state(id, "succeeded").await;
    assert_eq!(events(id), ["trace", "tenant", "job:acme"]);
    assert_eq!(
        runs("ordered"),
        [Seen {
            tenant: "acme".to_owned(),
            attempt: 1,
            job_id: id,
        }]
    );
}

async fn check_job_without_context_runs_through_middleware(fixture: &Fixture) {
    let id = jalari::enqueue(&PlainJob {
        label: "plain".to_owned(),
    })
    .await
    .unwrap()
    .id();
    fixture.wait_for_state(id, "succeeded").await;
    assert_eq!(events(id), ["trace", "job:plain"]);
}

async fn check_middleware_rejection_fails_job(fixture: &Fixture) {
    let missing = jalari::enqueue(&TenantJob::labelled("missing"))
        .await
        .unwrap()
        .id();
    let error = fixture.wait_for_state(missing, "failed").await;
    assert_eq!(error.as_deref(), Some("missing caller"));

    let disabled = jalari::enqueue_with(
        &TenantJob::labelled("disabled"),
        EnqueueOptions::new().context(&Caller::new("disabled")),
    )
    .await
    .unwrap()
    .id();
    let error = fixture.wait_for_state(disabled, "failed").await;
    assert_eq!(error.as_deref(), Some("tenant disabled"));
    assert!(runs("missing").is_empty());
    assert!(runs("disabled").is_empty());
    assert_eq!(events(disabled), ["trace", "tenant"]);
}

async fn check_undecodable_context_fails_job(fixture: &Fixture) {
    let id = jalari::enqueue_with(
        &TenantJob::labelled("undecodable"),
        EnqueueOptions::new().context(&json!({"tenant": 42})),
    )
    .await
    .unwrap()
    .id();
    let error = fixture.wait_for_state(id, "failed").await.unwrap();
    assert!(error.contains("invalid type"), "{error}");
    assert!(runs("undecodable").is_empty());
}

async fn check_retry_keeps_context(fixture: &Fixture) {
    let id = jalari::scope(
        &Caller::new("initech"),
        jalari::enqueue(&TenantJob {
            label: "retried".to_owned(),
            fail_first: true,
            spawn_child: false,
        }),
    )
    .await
    .unwrap()
    .id();
    fixture.wait_for_state(id, "succeeded").await;
    let seen = runs("retried");
    assert_eq!(seen.len(), 2);
    assert!(seen.iter().all(|run| run.tenant == "initech"));
    assert_eq!(
        seen.iter().map(|run| run.attempt).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        fixture.context(id).await,
        Some(json!({"tenant": "initech"}))
    );
}

async fn check_nested_enqueue_inherits_context(fixture: &Fixture) {
    let id = jalari::scope(
        &Caller::new("globex"),
        jalari::enqueue(&TenantJob {
            label: "parent".to_owned(),
            fail_first: false,
            spawn_child: true,
        }),
    )
    .await
    .unwrap()
    .id();
    fixture.wait_for_state(id, "succeeded").await;
    let child = fixture.job_by_label("parent-child").await;
    fixture.wait_for_state(child, "succeeded").await;
    assert_eq!(
        fixture.context(child).await,
        Some(json!({"tenant": "globex"}))
    );
    assert_eq!(runs("parent-child")[0].tenant, "globex");
}

async fn check_recurring_keeps_scope_context(fixture: &Fixture) {
    let cron = Cron::new("0 0 0 1 1 *").unwrap();
    jalari::scope(
        &Caller::new("umbrella"),
        jalari::recurring::add_or_update("tenant-yearly", cron, &TenantJob::labelled("yearly")),
    )
    .await
    .unwrap();
    let listed = jalari::recurring::list().await.unwrap();
    let schedule = listed
        .iter()
        .find(|schedule| schedule.name == "tenant-yearly")
        .unwrap();
    assert_eq!(schedule.context, Some(json!({"tenant": "umbrella"})));

    let id = jalari::recurring::trigger("tenant-yearly")
        .await
        .unwrap()
        .id();
    fixture.wait_for_state(id, "succeeded").await;
    assert_eq!(runs("yearly")[0].tenant, "umbrella");
    jalari::recurring::remove("tenant-yearly").await.unwrap();
}

async fn check_shutdown_reaches_running_job(fixture: &Fixture) {
    let mut config = worker_config();
    config.shutdown_timeout = Duration::from_secs(30);
    let worker = Worker::builder()
        .queue("shutdown", 1)
        .config(config)
        .build()
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(worker.run(shutdown.clone()));
    let id = jalari::enqueue(&WaitsForShutdown).await.unwrap().id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !SHUTDOWN_STARTED.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "job never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let started = Instant::now();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .unwrap()
        .unwrap();
    assert!(SHUTDOWN_SEEN.load(Ordering::SeqCst));
    assert!(started.elapsed() < Duration::from_secs(5));
    fixture.wait_for_state(id, "succeeded").await;
}
