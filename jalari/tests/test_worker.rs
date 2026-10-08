use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jalari::admin::{self, WorkerInfo};
use jalari::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, EnqueueOptions, ExponentialBackoff, Job, JobError, JobId, JobResult, Schema,
    Worker, WorkerConfig,
};
use serde::{Deserialize, Serialize};

static EXECUTIONS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static BLOCKING_STARTED: AtomicBool = AtomicBool::new(false);
static BLOCKING_FINISHED: AtomicBool = AtomicBool::new(false);
static GRACEFUL_STARTED: AtomicBool = AtomicBool::new(false);
static GRACEFUL_FINISHED: AtomicBool = AtomicBool::new(false);
static KEYED_RUNNING: AtomicUsize = AtomicUsize::new(0);
static KEYED_OVERLAPPED: AtomicBool = AtomicBool::new(false);

struct KeyedRun;

impl KeyedRun {
    fn enter() -> Self {
        if KEYED_RUNNING.fetch_add(1, Ordering::SeqCst) > 0 {
            KEYED_OVERLAPPED.store(true, Ordering::SeqCst);
        }
        Self
    }
}

impl Drop for KeyedRun {
    fn drop(&mut self) {
        KEYED_RUNNING.fetch_sub(1, Ordering::SeqCst);
    }
}

fn record_execution(label: &str) -> u32 {
    let mut executions = EXECUTIONS.lock().unwrap();
    let count = executions.entry(label.to_owned()).or_default();
    *count += 1;
    *count
}

fn executions(label: &str) -> u32 {
    EXECUTIONS.lock().unwrap().get(label).copied().unwrap_or(0)
}

#[derive(Serialize, Deserialize)]
struct Counted {
    n: u32,
}

#[jalari::job(queue = "counted")]
impl Job for Counted {
    const NAME: &'static str = "w_counted";

    async fn run(self) -> JobResult {
        record_execution(&format!("counted:{}", self.n));
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Flaky {
    label: String,
    failures: u32,
}

#[jalari::job(queue = "retry")]
impl Job for Flaky {
    const NAME: &'static str = "w_flaky";
    const MAX_ATTEMPTS: i32 = 5;

    async fn run(self) -> JobResult {
        if record_execution(&self.label) <= self.failures {
            return Err(JobError::new(format!("{} failed", self.label)));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct AlwaysFails;

#[jalari::job(queue = "retry")]
impl Job for AlwaysFails {
    const NAME: &'static str = "w_always_fails";
    const MAX_ATTEMPTS: i32 = 2;

    async fn run(self) -> JobResult {
        Err(JobError::new("always fails".to_owned()))
    }
}

#[derive(Serialize, Deserialize)]
struct Rejects;

#[jalari::job(queue = "retry")]
impl Job for Rejects {
    const NAME: &'static str = "w_rejects";

    async fn run(self) -> JobResult {
        Err(JobError::permanent(std::io::Error::other("bad input")))
    }
}

#[derive(Serialize, Deserialize)]
struct Slow;

#[jalari::job(queue = "isolation")]
impl Job for Slow {
    const NAME: &'static str = "w_slow";
    const MAX_ATTEMPTS: i32 = 1;
    const TIMEOUT: Option<Duration> = Some(Duration::from_millis(100));

    async fn run(self) -> JobResult {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Panics;

#[jalari::job(queue = "isolation")]
impl Job for Panics {
    const NAME: &'static str = "w_panics";
    const MAX_ATTEMPTS: i32 = 1;

    async fn run(self) -> JobResult {
        panic!("job exploded");
    }
}

#[derive(Serialize, Deserialize)]
struct Blocking;

#[jalari::job(queue = "crash")]
impl Job for Blocking {
    const NAME: &'static str = "w_blocking";

    async fn run(self) -> JobResult {
        if record_execution("blocking") == 1 {
            BLOCKING_STARTED.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(2)).await;
            BLOCKING_FINISHED.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Keyed {
    label: String,
}

#[jalari::job(queue = "keyed")]
impl Job for Keyed {
    const NAME: &'static str = "w_keyed";

    async fn run(self) -> JobResult {
        let _running = KeyedRun::enter();
        if record_execution(&self.label) == 1 && self.label == "keyed_first" {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Interrupted;

#[jalari::job(queue = "crash")]
impl Job for Interrupted {
    const NAME: &'static str = "w_interrupted";
    const MAX_ATTEMPTS: i32 = 2;

    async fn run(self) -> JobResult {
        record_execution("interrupted");
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Graceful;

#[jalari::job(queue = "graceful")]
impl Job for Graceful {
    const NAME: &'static str = "w_graceful";

    async fn run(self) -> JobResult {
        GRACEFUL_STARTED.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        GRACEFUL_FINISHED.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Notified;

#[jalari::job(queue = "notify")]
impl Job for Notified {
    const NAME: &'static str = "w_notified";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Debug, sqlx::FromRow)]
struct JobRow {
    state: String,
    attempts: i32,
    last_error: Option<String>,
}

struct Fixture {
    pool: PgPool,
    schema_name: String,
}

impl Fixture {
    fn table(&self, name: &str) -> String {
        format!("\"{}\".{name}", self.schema_name)
    }

    async fn row(&self, id: JobId) -> JobRow {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state, attempts, last_error FROM {} WHERE id = $1",
            self.table("job")
        )))
        .bind(id.0)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn wait_for_state(&self, id: JobId, state: &str) -> JobRow {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let row = self.row(id).await;
            if row.state == state {
                return row;
            }
            assert!(Instant::now() < deadline, "job {id} stuck in {row:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn terminate_running_job(&self) {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let terminated: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = current_database()
                 AND state = 'idle in transaction'
                 AND state_change < now() - interval '200 milliseconds'
                 AND application_name = $1",
        )
        .bind(&self.schema_name)
        .fetch_all(&self.pool)
        .await
        .unwrap();
        assert_eq!(terminated, [true]);
    }

    async fn history_states(&self, id: JobId) -> Vec<String> {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state FROM {} WHERE job_id = $1 ORDER BY id",
            self.table("job_history")
        )))
        .bind(id.0)
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_test_{}_{nanos}", std::process::id())
}

fn fast_retry() -> ExponentialBackoff {
    ExponentialBackoff {
        base: Duration::from_nanos(10_000_001),
        max: Duration::from_millis(50),
        jitter: 0.5,
    }
}

fn worker_config(poll_interval: Duration) -> WorkerConfig {
    let mut config = WorkerConfig::default();
    config.poll_interval = poll_interval;
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

async fn wait_for_workers<F: Fn(&[WorkerInfo]) -> bool>(description: &str, condition: F) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let workers = admin::workers().await.unwrap();
        if condition(&workers) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}: {workers:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_worker() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let schema_name = unique_schema_name();
    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(30)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options.application_name(&schema_name))
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

    let shutdown = CancellationToken::new();
    let main_worker = Worker::builder()
        .queue("counted", 4)
        .queue("retry", 2)
        .queue("isolation", 2)
        .queue("crash", 2)
        .queue("keyed", 2)
        .config(worker_config(Duration::from_millis(100)))
        .retry_policy(fast_retry())
        .build()
        .await
        .unwrap();
    let second_worker = Worker::builder()
        .queue("counted", 4)
        .config(worker_config(Duration::from_millis(100)))
        .build()
        .await
        .unwrap();
    let notify_worker = Worker::builder()
        .queue("notify", 1)
        .config(worker_config(Duration::from_secs(60)))
        .build()
        .await
        .unwrap();
    let running = [
        tokio::spawn(main_worker.run(shutdown.child_token())),
        tokio::spawn(second_worker.run(shutdown.child_token())),
        tokio::spawn(notify_worker.run(shutdown.child_token())),
    ];

    check_each_job_runs_once(&fixture).await;
    check_unknown_task_is_left_alone(&fixture).await;
    check_retry_then_succeed(&fixture).await;
    check_retries_run_out(&fixture).await;
    check_permanent_error_skips_retry(&fixture).await;
    check_timeout_and_panic_only_fail_the_job(&fixture).await;
    check_job_is_taken_over_after_connection_loss(&fixture).await;
    check_queue_key_waits_for_interrupted_job(&fixture).await;
    check_interrupted_job_stops_after_max_attempts(&fixture).await;
    check_notification_wakes_idle_worker(&fixture).await;
    check_workers_are_listed(&fixture).await;
    check_graceful_shutdown_waits_for_running_job(&fixture).await;

    shutdown.cancel();
    for handle in running {
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(admin::workers().await.unwrap(), []);

    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}

async fn check_each_job_runs_once(fixture: &Fixture) {
    let mut ids = Vec::new();
    for n in 0..60 {
        ids.push(jalari::enqueue(&Counted { n }).await.unwrap().id());
    }
    for id in &ids {
        fixture.wait_for_state(*id, "succeeded").await;
    }
    for n in 0..60 {
        assert_eq!(executions(&format!("counted:{n}")), 1, "job {n}");
    }
}

async fn check_unknown_task_is_left_alone(fixture: &Fixture) {
    let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {} (queue, task, payload, state, max_attempts)
         VALUES ('counted', 'not_registered', '{{}}', 'enqueued', 1)
         RETURNING id",
        fixture.table("job")
    )))
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    let marker = jalari::enqueue(&Counted { n: 1000 }).await.unwrap().id();
    fixture.wait_for_state(marker, "succeeded").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(fixture.row(JobId(id)).await.state, "enqueued");
}

async fn check_retry_then_succeed(fixture: &Fixture) {
    let id = jalari::enqueue(&Flaky {
        label: "flaky".to_owned(),
        failures: 2,
    })
    .await
    .unwrap()
    .id();
    let row = fixture.wait_for_state(id, "succeeded").await;
    assert_eq!(row.attempts, 3);
    assert_eq!(row.last_error, None);
    assert_eq!(
        fixture.history_states(id).await,
        ["scheduled", "scheduled", "succeeded"]
    );
}

async fn check_retries_run_out(fixture: &Fixture) {
    let id = jalari::enqueue(&AlwaysFails).await.unwrap().id();
    let row = fixture.wait_for_state(id, "failed").await;
    assert_eq!(row.attempts, 2);
    assert_eq!(row.last_error.as_deref(), Some("always fails"));
}

async fn check_permanent_error_skips_retry(fixture: &Fixture) {
    let id = jalari::enqueue(&Rejects).await.unwrap().id();
    let row = fixture.wait_for_state(id, "failed").await;
    assert_eq!(row.attempts, 1);
    assert_eq!(row.last_error.as_deref(), Some("bad input"));
}

async fn check_timeout_and_panic_only_fail_the_job(fixture: &Fixture) {
    let slow = jalari::enqueue(&Slow).await.unwrap().id();
    let panics = jalari::enqueue(&Panics).await.unwrap().id();

    let row = fixture.wait_for_state(slow, "failed").await;
    assert!(row.last_error.unwrap().starts_with("timed out"));
    let row = fixture.wait_for_state(panics, "failed").await;
    assert_eq!(row.last_error.as_deref(), Some("panicked: job exploded"));

    let after = jalari::enqueue(&Counted { n: 2000 }).await.unwrap().id();
    fixture.wait_for_state(after, "succeeded").await;
}

async fn check_job_is_taken_over_after_connection_loss(fixture: &Fixture) {
    let id = jalari::enqueue(&Blocking).await.unwrap().id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !BLOCKING_STARTED.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "blocking job never started");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    fixture.terminate_running_job().await;

    let row = fixture.wait_for_state(id, "succeeded").await;
    assert_eq!(executions("blocking"), 2);
    assert_eq!(row.attempts, 2);
    assert!(!BLOCKING_FINISHED.load(Ordering::SeqCst));
    assert_eq!(fixture.history_states(id).await, ["scheduled", "succeeded"]);
}

async fn check_queue_key_waits_for_interrupted_job(fixture: &Fixture) {
    let keyed = |label: &str| {
        jalari::enqueue_with(
            &Keyed {
                label: label.to_owned(),
            },
            EnqueueOptions::new().queue_key("account:1"),
        )
    };
    let first = keyed("keyed_first").await.unwrap().id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while executions("keyed_first") < 1 {
        assert!(Instant::now() < deadline, "first keyed job never started");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let second = keyed("keyed_second").await.unwrap().id();

    fixture.terminate_running_job().await;

    fixture.wait_for_state(second, "succeeded").await;
    fixture.wait_for_state(first, "succeeded").await;
    assert!(!KEYED_OVERLAPPED.load(Ordering::SeqCst));
    assert_eq!(executions("keyed_first"), 2);
    assert_eq!(executions("keyed_second"), 1);
}

async fn check_interrupted_job_stops_after_max_attempts(fixture: &Fixture) {
    let id = jalari::enqueue(&Interrupted).await.unwrap().id();
    for attempt in 1..=2 {
        let deadline = Instant::now() + Duration::from_secs(10);
        while executions("interrupted") < attempt {
            assert!(Instant::now() < deadline, "attempt {attempt} never started");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        fixture.terminate_running_job().await;
    }

    let row = fixture.wait_for_state(id, "failed").await;
    assert_eq!(row.attempts, 2);
    assert_eq!(
        row.last_error.as_deref(),
        Some("the worker stopped during the last attempt")
    );
    assert_eq!(fixture.history_states(id).await, ["scheduled", "failed"]);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(executions("interrupted"), 2);
}

async fn check_notification_wakes_idle_worker(fixture: &Fixture) {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = Instant::now();
    let id = jalari::enqueue(&Notified).await.unwrap().id();
    fixture.wait_for_state(id, "succeeded").await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}, the 60s poll interval should not be needed",
        started.elapsed()
    );
}

async fn check_workers_are_listed(fixture: &Fixture) {
    wait_for_workers("three running workers", |workers| workers.len() == 3).await;
    let workers = admin::workers().await.unwrap();
    let pid = i32::try_from(std::process::id()).unwrap();
    for worker in &workers {
        assert!(!worker.hostname.is_empty());
        assert_eq!(worker.pid, pid);
        assert!(worker.scheduler);
        assert!(worker.heartbeat_at >= worker.started_at);
    }
    let notify = workers
        .iter()
        .find(|worker| worker.queues.len() == 1 && worker.queues[0].name == "notify")
        .unwrap();
    assert_eq!(notify.queues[0].concurrency, 1);

    let first_beat = notify.heartbeat_at;
    let notify_id = notify.id.clone();
    wait_for_workers("the heartbeat to advance", |workers| {
        workers
            .iter()
            .any(|worker| worker.id == notify_id && worker.heartbeat_at > first_beat)
    })
    .await;

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (hostname, pid, queues, scheduler, started_at, heartbeat_at)
         VALUES ('gone', 1, '[]', TRUE, now() - interval '1 hour', now() - interval '10 minutes')",
        fixture.table("worker")
    )))
    .execute(&fixture.pool)
    .await
    .unwrap();
    wait_for_workers("the stale worker to be removed", |workers| {
        workers.len() == 3 && workers.iter().all(|worker| worker.hostname != "gone")
    })
    .await;

    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .queue("idle", 1)
        .scheduler(false)
        .config(worker_config(Duration::from_millis(100)))
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.clone()));
    wait_for_workers("the scheduler-less worker", |workers| {
        workers
            .iter()
            .any(|worker| !worker.scheduler && worker.queues[0].name == "idle")
    })
    .await;
    shutdown.cancel();
    running.await.unwrap();
    wait_for_workers("the stopped worker to unregister", |workers| {
        workers.len() == 3
    })
    .await;
}

async fn check_graceful_shutdown_waits_for_running_job(fixture: &Fixture) {
    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .queue("graceful", 1)
        .config(worker_config(Duration::from_millis(100)))
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.clone()));

    let id = jalari::enqueue(&Graceful).await.unwrap().id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !GRACEFUL_STARTED.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "graceful job never started");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .unwrap()
        .unwrap();
    assert!(GRACEFUL_FINISHED.load(Ordering::SeqCst));
    assert_eq!(fixture.row(id).await.state, "succeeded");
}
