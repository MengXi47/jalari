mod common;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use jalari::admin::{self, JobFilter, RetryOutcome};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    EnqueueOptions, EnqueueOutcome, ExponentialBackoff, Job, JobError, JobId, JobResult, JobState,
    RestartOutcome, RetryPolicy, StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

use common::{Database, RunningWorker, wait_until, worker_config};

const EXPIRED: &str = "the job expired while waiting to run";
const UNKNOWN_TASK: &str = "no live worker knows this task; start a worker that links it";
const WAIT: Duration = Duration::from_secs(15);
const QUEUES: [&str; 7] = [
    "expiry",
    "expiry_retry",
    "expiry_failed",
    "expiry_batch",
    "expiry_stop",
    "expiry_paused",
    "expiry_held",
];
const STRANDED_OPEN: &str = "expiry_lost";

static RAN: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static WARNINGS: LazyLock<Mutex<Vec<BTreeMap<String, String>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

fn runs(label: &str) -> usize {
    RAN.lock()
        .unwrap()
        .iter()
        .filter(|ran| *ran == label)
        .count()
}

#[derive(Default)]
struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

struct CaptureWarnings;

impl Subscriber for CaptureWarnings {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() == Level::WARN
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        WARNINGS.lock().unwrap().push(fields.0);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

fn warned_stranded(queue: &str) -> bool {
    WARNINGS.lock().unwrap().iter().any(|fields| {
        fields.get("queue").map(String::as_str) == Some(queue)
            && fields.get("message").map(String::as_str) == Some(UNKNOWN_TASK)
    })
}

#[derive(Serialize, Deserialize, Clone, Copy)]
enum Plan {
    Succeed,
    SaveThenRetry,
    Fail,
}

#[derive(Serialize, Deserialize)]
struct Probe {
    label: String,
    plan: Plan,
}

#[jalari::job(queue = "expiry")]
impl Job for Probe {
    const NAME: &'static str = "expiry_probe";
    const MAX_ATTEMPTS: i32 = 3;
    const RETRY_POLICY: Option<&'static dyn RetryPolicy> = Some(&ExponentialBackoff {
        base: Duration::from_secs(3),
        max: Duration::from_secs(3),
        jitter: 0.0,
    });

    async fn run(self) -> JobResult {
        RAN.lock().unwrap().push(self.label.clone());
        match self.plan {
            Plan::Succeed => Ok(()),
            Plan::SaveThenRetry => {
                let (_, checkpoint) = jalari::resume::<u32>()?;
                checkpoint.save(&7).await?;
                Err(JobError::new(format!("{} failed", self.label)))
            }
            Plan::Fail => Err(JobError::permanent(std::io::Error::other(format!(
                "{} failed for good",
                self.label
            )))),
        }
    }
}

fn probe(label: &str, plan: Plan) -> Probe {
    Probe {
        label: label.to_owned(),
        plan,
    }
}

async fn enqueue(label: &str, plan: Plan, options: EnqueueOptions) -> JobId {
    jalari::enqueue_with(&probe(label, plan), options)
        .await
        .unwrap()
        .id()
}

async fn info(id: JobId) -> admin::JobInfo {
    admin::job(id).await.unwrap().unwrap().job
}

async fn wait_state(id: JobId, state: JobState, timeout: Duration) {
    wait_until(&format!("job {id} to be {state}"), timeout, || async {
        info(id).await.state == state
    })
    .await;
}

async fn wait_past_expiry(id: JobId) {
    let expires_at = info(id).await.expires_at.expect("the job has an expiry");
    let left = (expires_at - Utc::now()).to_std().unwrap_or_default();
    tokio::time::sleep(left + Duration::from_millis(50)).await;
}

async fn progress(id: JobId) -> Option<Value> {
    admin::job(id)
        .await
        .unwrap()
        .unwrap()
        .control
        .and_then(|control| control.progress)
}

async fn history(id: JobId) -> Vec<(JobState, i32, Option<String>)> {
    admin::job(id)
        .await
        .unwrap()
        .unwrap()
        .history
        .into_iter()
        .map(|entry| (entry.state, entry.attempt, entry.error))
        .collect()
}

async fn pause(database: &Database, queue: &str) {
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (queue) VALUES ($1)",
        database.table("queue_pause")
    )))
    .bind(queue)
    .execute(&database.pool)
    .await
    .unwrap();
}

async fn strand(database: &Database, queue: &str) {
    let id = enqueue(
        &format!("{queue} stranded"),
        Plan::Succeed,
        EnqueueOptions::new().queue(queue),
    )
    .await;
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET task = 'expiry_unknown' WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .execute(&database.pool)
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_expiry() {
    tracing::subscriber::set_global_default(CaptureWarnings).unwrap();
    let Some(database) = Database::start("expiry", 8).await else {
        return;
    };
    pause(&database, "expiry_held").await;
    pause(&database, "expiry_paused").await;
    strand(&database, "expiry_held").await;
    strand(&database, STRANDED_OPEN).await;

    let mut builder = Worker::builder()
        .config(worker_config(Duration::from_millis(100)))
        .scheduler(false);
    for queue in QUEUES.into_iter().chain([STRANDED_OPEN]) {
        builder = builder.queue(queue, 2);
    }
    let worker = RunningWorker::spawn(builder.build().await.unwrap());

    check_stranded_warning_skips_paused_queues().await;
    check_expired_job_is_discarded().await;
    check_expiry_applies_to_retries().await;
    check_retry_refuses_expired_failed_job().await;
    check_retry_failed_counts_expired().await;
    check_restart_refuses_expired_stopped_job().await;
    check_paused_queue_still_expires().await;

    worker.stop().await;
    database.finish().await;
}

async fn check_stranded_warning_skips_paused_queues() {
    wait_until("the stranded warning", WAIT, || async {
        warned_stranded(STRANDED_OPEN)
    })
    .await;
    assert!(!warned_stranded("expiry_held"));
}

async fn check_expired_job_is_discarded() {
    let key = "expiry:otp";
    let enqueued_at = Instant::now();
    let id = enqueue(
        "otp",
        Plan::Succeed,
        EnqueueOptions::new()
            .job_key(key)
            .delay(Duration::from_secs(2))
            .expire_after(Duration::from_millis(500)),
    )
    .await;
    wait_state(id, JobState::Discarded, Duration::from_secs(2)).await;
    assert!(enqueued_at.elapsed() < Duration::from_secs(2));

    let row = info(id).await;
    assert_eq!(row.last_error.as_deref(), Some(EXPIRED));
    assert_eq!(row.attempts, 0);
    let discarded: Vec<_> = history(id)
        .await
        .into_iter()
        .filter(|(state, ..)| *state == JobState::Discarded)
        .collect();
    assert_eq!(
        discarded,
        [(JobState::Discarded, 0, Some(EXPIRED.to_owned()))]
    );
    assert_eq!(runs("otp"), 0);
    assert_eq!(admin::retry(id).await.unwrap(), RetryOutcome::NotFailed);
    let throughput = admin::throughput(Duration::from_secs(60)).await.unwrap();
    let queue = throughput
        .iter()
        .find(|queue| queue.queue == "expiry")
        .unwrap();
    assert_eq!((queue.discarded, queue.failed), (1, 0));

    let again = jalari::enqueue_with(
        &probe("otp again", Plan::Succeed),
        EnqueueOptions::new().job_key(key),
    )
    .await
    .unwrap();
    assert!(matches!(again, EnqueueOutcome::Inserted(_)), "{again:?}");
    wait_state(again.id(), JobState::Succeeded, WAIT).await;
    assert_eq!(runs("otp"), 0);
}

async fn check_expiry_applies_to_retries() {
    let id = enqueue(
        "retried",
        Plan::SaveThenRetry,
        EnqueueOptions::new()
            .queue("expiry_retry")
            .expire_after(Duration::from_secs(1)),
    )
    .await;
    wait_state(id, JobState::Scheduled, WAIT).await;
    assert_eq!(progress(id).await, Some(Value::from(7)));
    wait_state(id, JobState::Discarded, Duration::from_secs(3)).await;

    let row = info(id).await;
    assert_eq!(row.attempts, 1);
    assert_eq!(row.last_error.as_deref(), Some(EXPIRED));
    assert_eq!(progress(id).await, None);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(runs("retried"), 1);
    assert_eq!(info(id).await.state, JobState::Discarded);
}

async fn check_retry_refuses_expired_failed_job() {
    let id = enqueue(
        "failed",
        Plan::Fail,
        EnqueueOptions::new()
            .queue("expiry_failed")
            .expire_after(Duration::from_secs(1)),
    )
    .await;
    wait_state(id, JobState::Failed, WAIT).await;
    wait_past_expiry(id).await;
    let logged = history(id).await;

    assert_eq!(admin::retry(id).await.unwrap(), RetryOutcome::Expired);
    assert_eq!(info(id).await.state, JobState::Failed);
    assert_eq!(history(id).await, logged);
    assert_eq!(runs("failed"), 1);
}

async fn check_retry_failed_counts_expired() {
    let filter = JobFilter::new().queue("expiry_batch");
    let expiring = enqueue(
        "batch expiring",
        Plan::Fail,
        EnqueueOptions::new()
            .queue("expiry_batch")
            .expire_after(Duration::from_secs(1)),
    )
    .await;
    let lasting = enqueue(
        "batch lasting",
        Plan::Fail,
        EnqueueOptions::new().queue("expiry_batch"),
    )
    .await;
    wait_state(expiring, JobState::Failed, WAIT).await;
    wait_state(lasting, JobState::Failed, WAIT).await;
    wait_past_expiry(expiring).await;

    let summary = admin::retry_failed(&filter).await.unwrap();
    assert_eq!(
        (summary.retried, summary.key_taken, summary.expired),
        (1, 0, 1)
    );
    wait_until("the lasting job to run again", WAIT, || async {
        runs("batch lasting") == 2
    })
    .await;
    assert_eq!(runs("batch expiring"), 1);
    assert_eq!(info(expiring).await.state, JobState::Failed);
}

async fn check_restart_refuses_expired_stopped_job() {
    let id = enqueue(
        "stopped",
        Plan::Succeed,
        EnqueueOptions::new()
            .queue("expiry_stop")
            .delay(Duration::from_secs(3600))
            .expire_after(Duration::from_secs(1)),
    )
    .await;
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Stopped);
    wait_past_expiry(id).await;
    let logged = history(id).await;

    assert_eq!(jalari::restart(id).await.unwrap(), RestartOutcome::Expired);
    assert_eq!(history(id).await, logged);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(info(id).await.state, JobState::Stopped);
    assert_eq!(jalari::restart(id).await.unwrap(), RestartOutcome::Expired);
    assert_eq!(runs("stopped"), 0);
}

async fn check_paused_queue_still_expires() {
    let id = enqueue(
        "paused",
        Plan::Succeed,
        EnqueueOptions::new()
            .queue("expiry_paused")
            .delay(Duration::from_secs(2))
            .expire_after(Duration::from_millis(500)),
    )
    .await;
    wait_state(id, JobState::Discarded, Duration::from_secs(2)).await;
    assert_eq!(info(id).await.last_error.as_deref(), Some(EXPIRED));
    assert_eq!(runs("paused"), 0);
}
