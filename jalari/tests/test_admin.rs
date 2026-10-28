use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{TimeDelta, Utc};
use jalari::admin::{self, JobFilter, RetryOutcome, RunNowOutcome};
use jalari::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, Command, EnqueueOptions, ErrorKind, Job, JobError, JobId, JobResult,
    JobState, OnConflict, Priority, Schema, StopOutcome, Worker, WorkerConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

static EXECUTIONS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static GATE_OPEN: AtomicBool = AtomicBool::new(false);
static FRAGILE_OPEN: AtomicBool = AtomicBool::new(false);

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

#[derive(Serialize, Deserialize)]
struct Fragile;

#[jalari::job(queue = "gate")]
impl Job for Fragile {
    const NAME: &'static str = "a_fragile";
    const MAX_ATTEMPTS: i32 = 1;

    async fn run(self) -> JobResult {
        record_execution("fragile");
        while !FRAGILE_OPEN.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

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
    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(10)
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
    check_retry_of_an_interrupted_job(&fixture).await;
    check_throughput(&fixture).await;
    check_filter_by_priority().await;
    check_filter_by_parent_and_root(&fixture).await;
    check_stranded_marks_paused_queues_and_skips_expired_jobs(&fixture).await;
    check_throughput_tells_snoozes_and_discards_apart(&fixture).await;

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
    let control = detail.control.unwrap();
    assert_eq!(control.progress, None);
    assert_eq!(control.lease, 4);

    GATE_OPEN.store(false, Ordering::SeqCst);
    let held = jalari::enqueue(&Gate).await.unwrap().id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while executions("gate") == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(jalari::stop(held).await.unwrap(), StopOutcome::Requested);
    let detail = admin::job(held).await.unwrap().unwrap();
    assert_eq!(detail.job.state, JobState::Running);
    assert!(detail.job.worker_id.is_some());
    assert!(detail.job.running_since.is_some());
    assert_eq!(detail.job.pending, Some(Command::Stop));
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

async fn check_retry_of_an_interrupted_job(fixture: &Fixture) {
    FRAGILE_OPEN.store(false, Ordering::SeqCst);
    let id = jalari::enqueue(&Fragile).await.unwrap().id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while executions("fragile") == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let terminated: Vec<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT pg_terminate_backend(activity.pid)
             FROM pg_stat_activity AS activity
             JOIN {job} AS job
                 ON activity.application_name = 'jalari lifeline ' || job.locked_by::text
             WHERE job.state = 'running'",
        job = fixture.table("job"),
    )))
    .fetch_all(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(terminated, [true]);
    tokio::time::sleep(Duration::from_secs(3)).await;
    FRAGILE_OPEN.store(true, Ordering::SeqCst);
    fixture.wait_for_state(id, "failed").await;
    let failed = admin::job(id).await.unwrap().unwrap();
    assert_eq!(
        failed.job.last_error.as_deref(),
        Some("the worker stopped during the last attempt")
    );

    assert_eq!(admin::retry(id).await.unwrap(), RetryOutcome::Retried);
    let waiting = admin::job(id).await.unwrap().unwrap();
    assert!(matches!(
        waiting.job.state,
        JobState::Enqueued | JobState::Running | JobState::Succeeded
    ));
    assert_eq!(
        waiting
            .history
            .iter()
            .find(|entry| entry.attempt == 0 && entry.created_at > failed.job.updated_at)
            .map(|entry| entry.state),
        Some(JobState::Enqueued)
    );
    fixture.wait_for_state(id, "succeeded").await;
    assert_eq!(executions("fragile"), 2);
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
    assert_eq!((gate.succeeded, gate.stopped, gate.cancelled), (2, 0, 1));
    assert_eq!(admin::throughput(Duration::from_secs(0)).await.unwrap(), []);

    let stats = admin::stats().await.unwrap();
    let total: i64 = stats.iter().map(|count| count.count).sum();
    assert_eq!(total, admin::count(&JobFilter::new()).await.unwrap());
    let _ = fixture;
}

async fn enqueue_on(queue: &str, label: &str, options: EnqueueOptions) -> JobId {
    jalari::enqueue_with(&ping(label, 0), options.queue(queue))
        .await
        .unwrap()
        .id()
}

async fn ids(filter: &JobFilter) -> Vec<JobId> {
    let mut ids: Vec<JobId> = admin::jobs(filter, 100, None)
        .await
        .unwrap()
        .into_iter()
        .map(|job| job.id)
        .collect();
    ids.sort_by_key(|id| id.0);
    ids
}

async fn check_filter_by_priority() {
    let later = || EnqueueOptions::new().delay(Duration::from_secs(3600));
    let mut by_priority = HashMap::new();
    for priority in [
        Priority::Low,
        Priority::Normal,
        Priority::High,
        Priority::Urgent,
        Priority::Urgent,
    ] {
        let id = enqueue_on("by-priority", "priority", later().priority(priority)).await;
        by_priority
            .entry(priority)
            .or_insert_with(Vec::new)
            .push(id);
    }
    let queue = JobFilter::new().queue("by-priority");
    assert_eq!(admin::count(&queue).await.unwrap(), 5);
    assert_eq!(
        ids(&queue.clone().priority(Priority::Urgent)).await,
        by_priority[&Priority::Urgent]
    );
    let mut high_or_urgent = by_priority[&Priority::High].clone();
    high_or_urgent.extend(&by_priority[&Priority::Urgent]);
    assert_eq!(
        ids(&queue
            .clone()
            .priority(Priority::High)
            .priority(Priority::Urgent))
        .await,
        high_or_urgent
    );
    assert_eq!(
        admin::count(
            &queue
                .clone()
                .priority(Priority::Low)
                .state(JobState::Failed)
        )
        .await
        .unwrap(),
        0
    );
    for job in admin::jobs(&queue, 100, None).await.unwrap() {
        assert!(by_priority[&job.priority].contains(&job.id), "{job:?}");
    }
}

async fn check_filter_by_parent_and_root(fixture: &Fixture) {
    let later = || EnqueueOptions::new().delay(Duration::from_secs(3600));
    let a = enqueue_on("lineage", "a", later()).await;
    let b = enqueue_on("lineage", "b", later()).await;
    let c = enqueue_on("lineage", "c", EnqueueOptions::new()).await;
    let alone = enqueue_on("lineage", "alone", later()).await;
    for (child, parent) in [(b, a), (c, b)] {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {} SET parent_id = $2, root_id = $3 WHERE id = $1",
            fixture.table("job")
        )))
        .bind(child.0)
        .bind(parent.0)
        .bind(a.0)
        .execute(&fixture.pool)
        .await
        .unwrap();
    }

    assert_eq!(ids(&JobFilter::new().root(a)).await, [a, b, c]);
    assert_eq!(ids(&JobFilter::new().parent(a)).await, [b]);
    assert_eq!(ids(&JobFilter::new().parent(b)).await, [c]);
    assert_eq!(ids(&JobFilter::new().root(alone)).await, [alone]);
    assert_eq!(
        ids(&JobFilter::new().root(a).state(JobState::Scheduled)).await,
        [a, b]
    );
    assert_eq!(
        admin::count(&JobFilter::new().root(a).parent(b))
            .await
            .unwrap(),
        1
    );
    let child = admin::job(c).await.unwrap().unwrap().job;
    assert_eq!((child.parent_id, child.root_id), (Some(b), Some(a)));
    let top = admin::job(a).await.unwrap().unwrap().job;
    assert_eq!((top.parent_id, top.root_id), (None, None));
}

async fn check_stranded_marks_paused_queues_and_skips_expired_jobs(fixture: &Fixture) {
    for label in ["paused-1", "paused-2"] {
        enqueue_on("paused-queue", label, EnqueueOptions::new()).await;
    }
    enqueue_on(
        "expired-queue",
        "expired",
        EnqueueOptions::new().expires_at(Utc::now() - TimeDelta::seconds(1)),
    )
    .await;
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (queue) VALUES ('paused-queue')",
        fixture.table("queue_pause")
    )))
    .execute(&fixture.pool)
    .await
    .unwrap();

    let groups = admin::stranded().await.unwrap();
    let paused = groups
        .iter()
        .find(|group| group.queue == "paused-queue")
        .unwrap_or_else(|| panic!("{groups:?}"));
    assert!(paused.paused);
    assert_eq!(paused.count, 2);
    let lineage = groups
        .iter()
        .find(|group| group.queue == "lineage")
        .unwrap_or_else(|| panic!("{groups:?}"));
    assert!(!lineage.paused);
    assert!(
        groups.iter().all(|group| group.queue != "expired-queue"),
        "{groups:?}"
    );
}

async fn check_throughput_tells_snoozes_and_discards_apart(fixture: &Fixture) {
    let id = enqueue_on(
        "raw-throughput",
        "raw",
        EnqueueOptions::new().delay(Duration::from_secs(3600)),
    )
    .await;
    for (state, attempt, error) in [
        ("scheduled", 1, "the first attempt failed"),
        ("scheduled", 1, "the job asked to run again later"),
        ("scheduled", 1, "the job asked to run again later"),
        ("discarded", 2, "nothing left to do"),
        ("failed", 2, "gave up"),
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {} (job_id, state, attempt, error, duration_ms)
             VALUES ($1, $2, $3, $4, 10)",
            fixture.table("job_history")
        )))
        .bind(id.0)
        .bind(state)
        .bind(attempt)
        .bind(error)
        .execute(&fixture.pool)
        .await
        .unwrap();
    }
    let throughput = admin::throughput(Duration::from_secs(3600)).await.unwrap();
    let raw = throughput
        .iter()
        .find(|queue| queue.queue == "raw-throughput")
        .unwrap();
    assert_eq!(
        (raw.retried, raw.snoozed, raw.discarded, raw.failed),
        (1, 2, 1, 1),
        "{raw:?}"
    );
}
