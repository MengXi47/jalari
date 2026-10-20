use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{TimeDelta, Utc};
use jalari::admin::{self, JobFilter, RetryOutcome, RunNowOutcome};
use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, Command, EnqueueOptions, ErrorKind, Job, JobError, JobId, JobResult,
    JobState, OnConflict, Schema, StopOutcome, Worker, WorkerConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

static EXECUTIONS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static GATE_OPEN: AtomicBool = AtomicBool::new(false);

fn record_execution(label: &str) -> u32 {
    let mut executions = EXECUTIONS.lock().unwrap();
    let count = executions.entry(label.to_owned()).or_default();
    *count += 1;
    *count
}

fn executions(label: &str) -> u32 {
    EXECUTIONS.lock().unwrap().get(label).copied().unwrap_or(0)
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Ping {
    label: String,
    fail_times: u32,
}

#[jalari::job(queue = "pings")]
impl Job for Ping {
    const NAME: &'static str = "a_ping";
    const MAX_ATTEMPTS: i32 = 2;

    async fn run(self) -> JobResult {
        let count = record_execution(&self.label);
        if count <= self.fail_times {
            return Err(JobError::new(format!("{} failed {count}", self.label)));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Gate;

#[jalari::job(queue = "gate")]
impl Job for Gate {
    const NAME: &'static str = "a_gate";

    async fn run(self) -> JobResult {
        record_execution("gate");
        while !GATE_OPEN.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

    async fn state(&self, id: JobId) -> String {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state FROM {} WHERE id = $1",
            self.table("job")
        )))
        .bind(id.0)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn wait_for_state(&self, id: JobId, state: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.state(id).await != state {
            assert!(
                Instant::now() < deadline,
                "job {id} did not reach {state}, it is {}",
                self.state(id).await
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn ping(label: &str, fail_times: u32) -> Ping {
    Ping {
        label: label.to_owned(),
        fail_times,
    }
}

fn tenant(id: &str) -> serde_json::Value {
    json!({ "tenant_id": id, "source": "test" })
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_test_{}_{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_admin() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let schema_name = unique_schema_name();
    let pool = PgPoolOptions::new()
        .max_connections(10)
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

    check_empty_tables(&fixture).await;
    check_queries_without_a_worker(&fixture).await;

    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .queue("pings", 2)
        .queue("gate", 1)
        .config(config)
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.child_token()));

    check_detail_follows_a_job(&fixture).await;
    check_retry(&fixture).await;
    check_retry_failed_in_batches(&fixture).await;
    check_run_now(&fixture).await;
    check_throughput(&fixture).await;

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

async fn check_empty_tables(fixture: &Fixture) {
    assert_eq!(admin::stats().await.unwrap(), []);
    assert_eq!(admin::count(&JobFilter::new()).await.unwrap(), 0);
    assert_eq!(admin::jobs(&JobFilter::new(), 10, None).await.unwrap(), []);
    assert_eq!(admin::job(JobId(1)).await.unwrap(), None);
    assert_eq!(
        admin::throughput(Duration::from_secs(3600)).await.unwrap(),
        []
    );
    assert_eq!(
        admin::retry(JobId(1)).await.unwrap(),
        RetryOutcome::NotFound
    );
    assert_eq!(
        admin::run_now(JobId(1)).await.unwrap(),
        RunNowOutcome::NotFound
    );
    let summary = admin::retry_failed(&JobFilter::new()).await.unwrap();
    assert_eq!((summary.retried, summary.key_taken), (0, 0));
    let _ = fixture;
}

async fn check_queries_without_a_worker(fixture: &Fixture) {
    let mut acme = Vec::new();
    for n in 0..5 {
        let options = EnqueueOptions::new()
            .context(&tenant("acme"))
            .job_key(&format!("acme:{n}"))
            .delay(Duration::from_secs(3600));
        acme.push(
            jalari::enqueue_with(&ping(&format!("acme:{n}"), 0), options)
                .await
                .unwrap()
                .id(),
        );
    }
    let globex = jalari::enqueue_with(
        &ping("globex", 0),
        EnqueueOptions::new()
            .context(&tenant("globex"))
            .job_key("globex:1")
            .delay(Duration::from_secs(3600)),
    )
    .await
    .unwrap()
    .id();
    let plain = jalari::enqueue_with(
        &ping("plain", 0),
        EnqueueOptions::new()
            .no_context()
            .queue("gate")
            .delay(Duration::from_secs(3600)),
    )
    .await
    .unwrap()
    .id();

    let stats = admin::stats().await.unwrap();
    assert_eq!(stats.len(), 2);
    assert_eq!(
        (&stats[0].queue[..], stats[0].state, stats[0].count),
        ("gate", JobState::Scheduled, 1)
    );
    assert_eq!(
        (&stats[1].queue[..], stats[1].state, stats[1].count),
        ("pings", JobState::Scheduled, 6)
    );

    let by_tenant = JobFilter::new().context(json!({ "tenant_id": "acme" }));
    assert_eq!(admin::count(&by_tenant).await.unwrap(), 5);
    let first_page = admin::jobs(&by_tenant, 2, None).await.unwrap();
    assert_eq!(
        first_page.iter().map(|job| job.id).collect::<Vec<_>>(),
        [acme[4], acme[3]]
    );
    let second_page = admin::jobs(&by_tenant, 2, Some(first_page[1].id))
        .await
        .unwrap();
    assert_eq!(
        second_page.iter().map(|job| job.id).collect::<Vec<_>>(),
        [acme[2], acme[1]]
    );
    let third_page = admin::jobs(&by_tenant, 2, Some(second_page[1].id))
        .await
        .unwrap();
    assert_eq!(
        third_page.iter().map(|job| job.id).collect::<Vec<_>>(),
        [acme[0]]
    );
    assert_eq!(admin::jobs(&by_tenant, 2, Some(acme[0])).await.unwrap(), []);

    let job = &first_page[0];
    assert_eq!(job.task, "a_ping");
    assert_eq!(job.queue, "pings");
    assert_eq!(job.state, JobState::Scheduled);
    assert_eq!(job.codec, "json");
    assert_eq!(job.job_key.as_deref(), Some("acme:4"));
    assert_eq!(job.context, Some(tenant("acme")));
    assert_eq!(job.attempts, 0);
    assert_eq!(job.max_attempts, 2);
    assert_eq!(job.timeout, None);
    assert!(job.run_at > Utc::now() + TimeDelta::minutes(59));
    assert_eq!(job.payload_json::<Ping>().unwrap(), ping("acme:4", 0));
    let err = job.payload_json::<Gate>().unwrap_err();
    assert_eq!(err.kind, ErrorKind::PayloadDecodeFailed);

    let filters = [
        (JobFilter::new(), 7),
        (JobFilter::new().state(JobState::Scheduled), 7),
        (JobFilter::new().state(JobState::Failed), 0),
        (
            JobFilter::new()
                .state(JobState::Failed)
                .state(JobState::Scheduled),
            7,
        ),
        (JobFilter::new().queue("gate"), 1),
        (JobFilter::new().task("a_ping"), 7),
        (JobFilter::new().task("a_gate"), 0),
        (JobFilter::new().job_key("acme:2"), 1),
        (JobFilter::new().job_key("acme"), 0),
        (JobFilter::new().job_key_prefix("acme:"), 5),
        (JobFilter::new().job_key_prefix("acme:%"), 0),
        (
            JobFilter::new().created_after(Utc::now() - TimeDelta::minutes(1)),
            7,
        ),
        (JobFilter::new().created_after(Utc::now()), 0),
        (JobFilter::new().created_before(Utc::now()), 7),
        (
            JobFilter::new().created_before(Utc::now() - TimeDelta::minutes(1)),
            0,
        ),
        (JobFilter::new().context(json!({ "source": "test" })), 6),
        (
            JobFilter::new().context(json!({ "tenant_id": "globex" })),
            1,
        ),
        (
            JobFilter::new().context(json!({ "tenant_id": "nobody" })),
            0,
        ),
        (JobFilter::new().context(json!({})), 6),
        (
            JobFilter::new()
                .context(json!({ "tenant_id": "acme" }))
                .job_key_prefix("acme:1"),
            1,
        ),
    ];
    for (filter, expected) in filters {
        assert_eq!(admin::count(&filter).await.unwrap(), expected, "{filter:?}");
        assert_eq!(
            admin::jobs(&filter, 100, None).await.unwrap().len(),
            usize::try_from(expected).unwrap(),
            "{filter:?}"
        );
    }

    let plain_detail = admin::job(plain).await.unwrap().unwrap();
    assert_eq!(plain_detail.job.context, None);
    assert_eq!(plain_detail.history, []);
    assert_eq!(plain_detail.control, None);
    assert_eq!(
        admin::run_now(globex).await.unwrap(),
        RunNowOutcome::Enqueued
    );
    assert_eq!(fixture.state(globex).await, "enqueued");
    assert_eq!(admin::retry(globex).await.unwrap(), RetryOutcome::NotFailed);

    for id in acme.into_iter().chain([globex, plain]) {
        jalari::cancel(id).await.unwrap();
    }
    assert_eq!(
        admin::count(&JobFilter::new().state(JobState::Deleted))
            .await
            .unwrap(),
        7
    );
}

async fn check_detail_follows_a_job(fixture: &Fixture) {
    let id = jalari::enqueue_with(
        &ping("detail", 1),
        EnqueueOptions::new()
            .context(&tenant("acme"))
            .timeout(Duration::from_secs(30)),
    )
    .await
    .unwrap()
    .id();
    fixture.wait_for_state(id, "succeeded").await;
    let detail = admin::job(id).await.unwrap().unwrap();
    assert_eq!(detail.job.state, JobState::Succeeded);
    assert_eq!(detail.job.attempts, 2);
    assert_eq!(detail.job.timeout, Some(Duration::from_secs(30)));
    assert_eq!(detail.job.last_error, None);
    let states: Vec<_> = detail
        .history
        .iter()
        .map(|entry| (entry.state, entry.attempt))
        .collect();
    assert_eq!(states, [(JobState::Scheduled, 1), (JobState::Succeeded, 2)]);
    assert_eq!(detail.history[0].error.as_deref(), Some("detail failed 1"));
    assert!(detail.history.iter().all(|entry| entry.duration.is_some()));
    assert_eq!(detail.control, None);

    GATE_OPEN.store(false, Ordering::SeqCst);
    let held = jalari::enqueue(&Gate).await.unwrap().id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while executions("gate") == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(jalari::stop(held).await.unwrap(), StopOutcome::Requested);
    let detail = admin::job(held).await.unwrap().unwrap();
    assert_eq!(detail.job.state, JobState::Enqueued);
    assert_eq!(detail.control.unwrap().pending, Some(Command::Stop));
    assert_eq!(admin::run_now(held).await.unwrap(), RunNowOutcome::Running);
    GATE_OPEN.store(true, Ordering::SeqCst);
    fixture.wait_for_state(held, "succeeded").await;
    assert_eq!(
        admin::run_now(held).await.unwrap(),
        RunNowOutcome::NotWaiting
    );
}

async fn check_retry(fixture: &Fixture) {
    let id = jalari::enqueue_with(
        &ping("retry", 10),
        EnqueueOptions::new().job_key("retry:key"),
    )
    .await
    .unwrap()
    .id();
    fixture.wait_for_state(id, "failed").await;
    let failed = admin::job(id).await.unwrap().unwrap();
    assert_eq!(failed.job.attempts, 2);
    assert_eq!(failed.job.last_error.as_deref(), Some("retry failed 2"));

    assert_eq!(admin::retry(id).await.unwrap(), RetryOutcome::Retried);
    assert_eq!(executions("retry"), 2);
    fixture.wait_for_state(id, "failed").await;
    let twice = admin::job(id).await.unwrap().unwrap();
    assert_eq!(twice.job.attempts, 2);
    assert_eq!(twice.job.last_error.as_deref(), Some("retry failed 4"));
    let states: Vec<_> = twice
        .history
        .iter()
        .map(|entry| (entry.state, entry.attempt))
        .collect();
    assert_eq!(
        states,
        [
            (JobState::Scheduled, 1),
            (JobState::Failed, 2),
            (JobState::Enqueued, 0),
            (JobState::Scheduled, 1),
            (JobState::Failed, 2),
        ]
    );
    assert_eq!(executions("retry"), 4);

    let holder = jalari::enqueue_with(
        &ping("retry holder", 0),
        EnqueueOptions::new()
            .job_key("retry:key")
            .delay(Duration::from_secs(3600)),
    )
    .await
    .unwrap()
    .id();
    assert_eq!(
        admin::retry(id).await.unwrap(),
        RetryOutcome::KeyTaken(holder)
    );
    assert_eq!(fixture.state(id).await, "failed");
    jalari::cancel(holder).await.unwrap();
    assert_eq!(admin::retry(id).await.unwrap(), RetryOutcome::Retried);
    fixture.wait_for_state(id, "failed").await;
    assert_eq!(executions("retry"), 6);

    let replaced = jalari::enqueue_with(
        &ping("retry replacement", 0),
        EnqueueOptions::new()
            .job_key("retry:key")
            .on_conflict(OnConflict::Replace),
    )
    .await
    .unwrap();
    assert!(matches!(replaced, jalari::EnqueueOutcome::Inserted(_)));
    fixture.wait_for_state(replaced.id(), "succeeded").await;
    assert_eq!(admin::retry(id).await.unwrap(), RetryOutcome::Retried);
    fixture.wait_for_state(id, "failed").await;
}

async fn check_retry_failed_in_batches(fixture: &Fixture) {
    let mut ids = Vec::new();
    for n in 0..205 {
        let options = EnqueueOptions::new()
            .context(&tenant("batch"))
            .job_key(&format!("batch:{n}"));
        ids.push(
            jalari::enqueue_with(&ping(&format!("batch:{n}"), 1_000), options)
                .await
                .unwrap()
                .id(),
        );
    }
    for id in &ids {
        fixture.wait_for_state(*id, "failed").await;
    }
    let blocker = jalari::enqueue_with(
        &ping("batch blocker", 0),
        EnqueueOptions::new()
            .job_key("batch:7")
            .delay(Duration::from_secs(3600)),
    )
    .await
    .unwrap()
    .id();
    let other_tenant = jalari::enqueue_with(
        &ping("batch other", 1_000),
        EnqueueOptions::new().context(&tenant("elsewhere")),
    )
    .await
    .unwrap()
    .id();
    fixture.wait_for_state(other_tenant, "failed").await;

    let filter = JobFilter::new()
        .context(json!({ "tenant_id": "batch" }))
        .state(JobState::Succeeded);
    let summary = admin::retry_failed(&filter).await.unwrap();
    assert_eq!((summary.retried, summary.key_taken), (204, 1));
    assert_eq!(fixture.state(ids[7]).await, "failed");
    assert_eq!(fixture.state(other_tenant).await, "failed");
    for id in &ids {
        if *id != ids[7] {
            fixture.wait_for_state(*id, "failed").await;
        }
    }
    let again = admin::retry_failed(&filter).await.unwrap();
    assert_eq!((again.retried, again.key_taken), (204, 1));
    for id in &ids {
        if *id != ids[7] {
            fixture.wait_for_state(*id, "failed").await;
            assert_eq!(executions(&format!("batch:{}", id_index(&ids, *id))), 6);
        }
    }
    assert_eq!(executions("batch:7"), 2);
    jalari::cancel(blocker).await.unwrap();
}

fn id_index(ids: &[JobId], id: JobId) -> usize {
    ids.iter().position(|candidate| *candidate == id).unwrap()
}

async fn check_run_now(fixture: &Fixture) {
    let later = jalari::enqueue_with(
        &ping("later", 0),
        EnqueueOptions::new().delay(Duration::from_secs(3600)),
    )
    .await
    .unwrap()
    .id();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(executions("later"), 0);
    assert_eq!(
        admin::run_now(later).await.unwrap(),
        RunNowOutcome::Enqueued
    );
    fixture.wait_for_state(later, "succeeded").await;
    let detail = admin::job(later).await.unwrap().unwrap();
    let states: Vec<_> = detail
        .history
        .iter()
        .map(|entry| (entry.state, entry.attempt))
        .collect();
    assert_eq!(states, [(JobState::Enqueued, 0), (JobState::Succeeded, 1)]);

    let backoff = jalari::enqueue(&ping("backoff", 1)).await.unwrap().id();
    fixture.wait_for_state(backoff, "scheduled").await;
    assert_eq!(executions("backoff"), 1);
    let outcome = admin::run_now(backoff).await.unwrap();
    assert!(
        matches!(outcome, RunNowOutcome::Enqueued | RunNowOutcome::Running),
        "{outcome:?}"
    );
    fixture.wait_for_state(backoff, "succeeded").await;

    let stopped = jalari::enqueue_with(
        &ping("stopped", 0),
        EnqueueOptions::new().delay(Duration::from_secs(3600)),
    )
    .await
    .unwrap()
    .id();
    assert_eq!(jalari::stop(stopped).await.unwrap(), StopOutcome::Stopped);
    assert_eq!(
        admin::run_now(stopped).await.unwrap(),
        RunNowOutcome::NotWaiting
    );
    assert_eq!(
        admin::retry(stopped).await.unwrap(),
        RetryOutcome::NotFailed
    );
    jalari::cancel(stopped).await.unwrap();
}

async fn check_throughput(fixture: &Fixture) {
    let throughput = admin::throughput(Duration::from_secs(3600)).await.unwrap();
    let pings = throughput
        .iter()
        .find(|queue| queue.queue == "pings")
        .unwrap();
    assert!(pings.succeeded >= 3, "{pings:?}");
    assert!(pings.failed >= 205, "{pings:?}");
    assert!(pings.retried >= 205, "{pings:?}");
    assert!(pings.cancelled >= 7, "{pings:?}");
    assert!(pings.average_duration.is_some());
    assert!(pings.max_duration.unwrap() >= pings.average_duration.unwrap());
    let gate = throughput
        .iter()
        .find(|queue| queue.queue == "gate")
        .unwrap();
    assert_eq!((gate.succeeded, gate.stopped, gate.cancelled), (1, 0, 1));
    assert_eq!(admin::throughput(Duration::from_secs(0)).await.unwrap(), []);

    let stats = admin::stats().await.unwrap();
    let total: i64 = stats.iter().map(|count| count.count).sum();
    assert_eq!(total, admin::count(&JobFilter::new()).await.unwrap());
    let _ = fixture;
}
