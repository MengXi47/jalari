use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, Cron, EnqueueOutcome, ErrorKind, Job, JobResult, RecurringInfo, Schema,
    Worker, WorkerConfig, recurring,
};
use serde::{Deserialize, Serialize};

static EXECUTIONS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn record_execution(label: &str) {
    *EXECUTIONS
        .lock()
        .unwrap()
        .entry(label.to_owned())
        .or_default() += 1;
}

fn executions(label: &str) -> u32 {
    EXECUTIONS.lock().unwrap().get(label).copied().unwrap_or(0)
}

#[derive(Serialize, Deserialize, Default)]
struct Tick;

#[jalari::job(cron = "* * * * * *", queue = "ticks")]
impl Job for Tick {
    const NAME: &'static str = "r_tick";

    async fn run(self) -> JobResult {
        record_execution("tick");
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Daily {
    label: String,
}

#[jalari::job(cron = "0 0 3 * * *", timezone = "Asia/Taipei", queue = "daily")]
impl Job for Daily {
    const NAME: &'static str = "r_daily";

    async fn run(self) -> JobResult {
        record_execution(&format!("daily:{}", self.label));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Report {
    title: String,
}

#[jalari::job(queue = "runtime")]
impl Job for Report {
    const NAME: &'static str = "r_report";

    async fn run(self) -> JobResult {
        record_execution(&format!("report:{}", self.title));
        Ok(())
    }
}

struct Fixture {
    pool: PgPool,
    schema_name: String,
}

impl Fixture {
    fn recurring_table(&self) -> String {
        format!("\"{}\".recurring", self.schema_name)
    }

    async fn insert_row(&self, name: &str, cron: &str, task: &str, managed: bool) {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {} (name, cron, timezone, task, payload, queue, max_attempts, managed,
                 next_run_at)
             VALUES ($1, $2, 'UTC', $3, 'null', 'runtime', 1, $4, now() - interval '1 second')",
            self.recurring_table()
        )))
        .bind(name)
        .bind(cron)
        .bind(task)
        .bind(managed)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn disable_directly(&self, name: &str) {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {} SET enabled = FALSE WHERE name = $1",
            self.recurring_table()
        )))
        .bind(name)
        .execute(&self.pool)
        .await
        .unwrap();
    }
}

async fn info(name: &str) -> Option<RecurringInfo> {
    recurring::list()
        .await
        .unwrap()
        .into_iter()
        .find(|info| info.name == name)
}

async fn wait_until<F: AsyncFn() -> bool>(description: &str, condition: F) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition().await {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_stops(label: &str) {
    tokio::time::sleep(Duration::from_millis(500)).await;
    let settled = executions(label);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(executions(label), settled, "{label} kept running");
}

fn every_second() -> Cron {
    Cron::new("* * * * * *").unwrap()
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_test_{}_{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_recurring() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await
        .unwrap();
    let schema_name = unique_schema_name();
    jalari::init(pool.clone())
        .schema(Schema::named(&schema_name).unwrap())
        .migrate()
        .await
        .unwrap();
    let fixture = Fixture {
        pool: pool.clone(),
        schema_name: schema_name.clone(),
    };

    fixture
        .insert_row("removed_from_code", "* * * * * *", "r_report", true)
        .await;
    fixture
        .insert_row("other_binary", "* * * * * *", "ghost_task", true)
        .await;
    fixture
        .insert_row("broken", "not a cron", "r_report", false)
        .await;
    fixture
        .insert_row("r_daily", "0 0 1 * * *", "r_daily", true)
        .await;

    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(200);
    check_scheduler_can_be_disabled(config.clone()).await;

    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .queue("ticks", 1)
        .queue("daily", 1)
        .queue("runtime", 1)
        .config(config)
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.child_token()));

    check_declared_jobs_are_synced().await;
    check_code_declared_cron_fires().await;
    check_code_declared_cron_cannot_change_at_runtime(&fixture).await;
    check_runtime_schedule_is_independent().await;
    check_runtime_schedule_can_change_pause_and_cancel().await;
    check_trigger_runs_now().await;
    check_recomputed_schedule_never_repeats_a_fired_run(&fixture).await;
    check_each_run_is_enqueued_once_under_contention(&fixture).await;

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

async fn check_scheduler_can_be_disabled(config: WorkerConfig) {
    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .queue("ticks", 1)
        .queue("runtime", 1)
        .scheduler(false)
        .config(config)
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.clone()));
    tokio::time::sleep(Duration::from_millis(800)).await;
    shutdown.cancel();
    running.await.unwrap();

    assert!(info("r_tick").await.is_none());
    assert!(info("removed_from_code").await.is_some());
    assert!(info("broken").await.unwrap().enabled);
    assert_eq!(executions("tick"), 0);
}

async fn check_declared_jobs_are_synced() {
    wait_until("declared jobs to be synced", async || {
        info("r_tick").await.is_some()
    })
    .await;

    let tick = info("r_tick").await.unwrap();
    assert_eq!(tick.cron, "* * * * * *");
    assert_eq!(tick.queue, "ticks");
    assert!(tick.managed && tick.enabled);

    let daily = info("r_daily").await.unwrap();
    assert_eq!(daily.cron, "0 0 3 * * *");
    assert_eq!(daily.timezone, "Asia/Taipei");
    assert_eq!(daily.queue, "daily");
    assert!(daily.next_run_at > chrono::Utc::now());

    assert!(info("removed_from_code").await.is_none());
    assert!(info("other_binary").await.is_some());

    wait_until("the broken schedule to be disabled", async || {
        !info("broken").await.unwrap().enabled
    })
    .await;
}

async fn check_code_declared_cron_fires() {
    wait_until("the every-second job to run twice", async || {
        executions("tick") >= 2
    })
    .await;
    assert!(info("r_tick").await.unwrap().last_run_at.is_some());
}

async fn check_code_declared_cron_cannot_change_at_runtime(fixture: &Fixture) {
    let far_future = Cron::new("0 0 0 1 1 *").unwrap();
    let errors = [
        recurring::update_schedule("r_tick", far_future).await,
        recurring::pause("r_tick").await,
        recurring::resume("r_tick").await,
        recurring::remove("r_tick").await,
        recurring::add_or_update("r_tick", every_second(), &Tick).await,
    ];
    for error in errors {
        assert_eq!(error.unwrap_err().kind, ErrorKind::RecurringManagedByCode);
    }
    let tick = info("r_tick").await.unwrap();
    assert_eq!(tick.cron, "* * * * * *");
    assert!(tick.enabled);

    fixture.disable_directly("r_tick").await;
    let restarted = Worker::builder().queue("ticks", 1).build().await.unwrap();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(restarted.run(shutdown.clone()));
    wait_until(
        "a restarted worker to enable the declared job",
        async || info("r_tick").await.unwrap().enabled,
    )
    .await;
    shutdown.cancel();
    running.await.unwrap();
}

async fn check_runtime_schedule_is_independent() {
    recurring::add_or_update(
        "daily_every_second",
        every_second(),
        &Daily {
            label: "runtime".to_owned(),
        },
    )
    .await
    .unwrap();
    let runtime = info("daily_every_second").await.unwrap();
    assert!(!runtime.managed);
    assert_eq!(runtime.task, "r_daily");
    assert_eq!(runtime.queue, "daily");

    wait_until("the runtime schedule to run twice", async || {
        executions("daily:runtime") >= 2
    })
    .await;

    let declared = info("r_daily").await.unwrap();
    assert_eq!(declared.cron, "0 0 3 * * *");
    assert_eq!(declared.timezone, "Asia/Taipei");
    assert!(declared.last_run_at.is_none());
    assert_eq!(executions("daily:"), 0);

    recurring::remove("daily_every_second").await.unwrap();
    assert_stops("daily:runtime").await;
    assert!(info("r_daily").await.is_some());
}

async fn check_runtime_schedule_can_change_pause_and_cancel() {
    recurring::add_or_update(
        "report",
        every_second(),
        &Report {
            title: "first".to_owned(),
        },
    )
    .await
    .unwrap();
    wait_until("the runtime schedule to run", async || {
        executions("report:first") >= 1
    })
    .await;

    recurring::add_or_update(
        "report",
        every_second(),
        &Report {
            title: "second".to_owned(),
        },
    )
    .await
    .unwrap();
    wait_until("the updated payload to run", async || {
        executions("report:second") >= 1
    })
    .await;

    recurring::update_schedule(
        "report",
        Cron::new("0 0 0 1 1 *")
            .unwrap()
            .timezone("Asia/Taipei")
            .unwrap(),
    )
    .await
    .unwrap();
    let report = info("report").await.unwrap();
    assert_eq!(report.cron, "0 0 0 1 1 *");
    assert_eq!(report.timezone, "Asia/Taipei");
    assert_stops("report:second").await;

    recurring::update_schedule("report", every_second())
        .await
        .unwrap();
    let before = executions("report:second");
    wait_until("the rescheduled job to run", async || {
        executions("report:second") > before
    })
    .await;

    recurring::pause("report").await.unwrap();
    assert!(!info("report").await.unwrap().enabled);
    assert_stops("report:second").await;

    let paused = executions("report:second");
    recurring::resume("report").await.unwrap();
    wait_until("the resumed job to run", async || {
        executions("report:second") > paused
    })
    .await;

    recurring::remove("report").await.unwrap();
    assert!(info("report").await.is_none());
    assert_stops("report:second").await;

    let error = recurring::remove("report").await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::RecurringNotFound);
    let error = recurring::update_schedule("report", every_second())
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::RecurringNotFound);
}

async fn check_recomputed_schedule_never_repeats_a_fired_run(fixture: &Fixture) {
    recurring::add_or_update(
        "guarded",
        Cron::new("0 0 0 1 1 *").unwrap(),
        &Report {
            title: "guarded".to_owned(),
        },
    )
    .await
    .unwrap();
    let fired_run: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(AssertSqlSafe(format!(
        "UPDATE {} SET last_run_at = date_trunc('second', now()) + interval '1 hour'
         WHERE name = 'guarded'
         RETURNING last_run_at",
        fixture.recurring_table()
    )))
    .fetch_one(&fixture.pool)
    .await
    .unwrap();

    recurring::update_schedule("guarded", every_second())
        .await
        .unwrap();
    assert!(info("guarded").await.unwrap().next_run_at > fired_run);

    recurring::pause("guarded").await.unwrap();
    recurring::resume("guarded").await.unwrap();
    assert!(info("guarded").await.unwrap().next_run_at > fired_run);

    recurring::add_or_update(
        "guarded",
        every_second(),
        &Report {
            title: "guarded".to_owned(),
        },
    )
    .await
    .unwrap();
    assert!(info("guarded").await.unwrap().next_run_at > fired_run);

    recurring::remove("guarded").await.unwrap();
}

async fn check_each_run_is_enqueued_once_under_contention(fixture: &Fixture) {
    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    let shutdown = CancellationToken::new();
    let mut extra_schedulers = Vec::new();
    for _ in 0..2 {
        let worker = Worker::builder()
            .queue("runtime", 1)
            .config(config.clone())
            .build()
            .await
            .unwrap();
        extra_schedulers.push(tokio::spawn(worker.run(shutdown.clone())));
    }

    recurring::add_or_update(
        "contended",
        every_second(),
        &Report {
            title: "contended".to_owned(),
        },
    )
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        recurring::update_schedule("contended", every_second())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    recurring::remove("contended").await.unwrap();
    shutdown.cancel();
    for handle in extra_schedulers {
        handle.await.unwrap();
    }

    let (jobs, distinct_seconds, span): (i64, i64, f64) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT count(*), count(DISTINCT date_trunc('second', created_at)),
             EXTRACT(EPOCH FROM max(created_at) - min(created_at))::float8
         FROM \"{}\".job
         WHERE job_key = 'recurring:contended'",
        fixture.schema_name
    )))
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert!(jobs >= 3, "only {jobs} runs were enqueued");
    assert_eq!(jobs, distinct_seconds, "a run was enqueued twice");
    assert!(
        f64::from(u32::try_from(jobs).unwrap()) <= span.ceil() + 1.0,
        "{jobs} jobs within {span}s"
    );
}

async fn check_trigger_runs_now() {
    let outcome = recurring::trigger("r_daily").await.unwrap();
    assert!(matches!(outcome, EnqueueOutcome::Inserted(_)));
    wait_until("the triggered job to run", async || {
        executions("daily:") >= 1
    })
    .await;
    assert_eq!(info("r_daily").await.unwrap().cron, "0 0 3 * * *");

    jalari::enqueue(&Daily {
        label: "manual".to_owned(),
    })
    .await
    .unwrap();
    wait_until("the enqueued cron job to run", async || {
        executions("daily:manual") >= 1
    })
    .await;

    let error = recurring::trigger("missing").await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::RecurringNotFound);
}
