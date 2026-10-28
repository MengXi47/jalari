mod common;

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, wait_until};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{CancelOutcome, Job, JobId, JobResult, StopOutcome, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

const QUEUE: &str = "run_control";
const CONTROL_LATENCY: Duration = Duration::from_millis(2500);
const LOST_LATENCY: Duration = Duration::from_secs(2);
const START: Duration = Duration::from_secs(10);
const PATIENT: Duration = Duration::from_secs(60);
const STOP: &str = "the job was asked to stop";
const CANCEL: &str = "the job was asked to cancel";
const SHUTDOWN: &str = "the worker is shutting down";
const LOST: &str = "the worker no longer owns the job";
const INTERRUPTED: &str = "the worker stopped during the last attempt";
const IMPATIENT: Duration = Duration::from_secs(1);
const ABORT_LATENCY: Duration = Duration::from_secs(5);
const DECLARED: &str = "job declared checkpoints but";
const DROP_WORK: Duration = Duration::from_millis(300);
const ABORT_WAIT: Duration = Duration::from_secs(1);
const IGNORED: &str =
    "job declared checkpoints but kept running after a checkpoint asked it to stop";

static ERRORS: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

struct Message(Option<String>);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

struct CaptureErrors;

impl Subscriber for CaptureErrors {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() == Level::ERROR
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut message = Message(None);
        event.record(&mut message);
        if let Some(message) = message.0 {
            ERRORS.lock().unwrap().push(message);
        }
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

fn silent_gap(message: &str) -> Option<u64> {
    message
        .strip_prefix("job declared checkpoints but went ")?
        .strip_suffix("s without one")?
        .parse()
        .ok()
}

fn assert_one_silence(errors: &[String], at_least: Duration) {
    let [message] = errors else {
        panic!("expected one missed-checkpoint error, got {errors:?}");
    };
    let gap = silent_gap(message).unwrap_or_else(|| panic!("not a silence report: {message}"));
    assert!(gap >= at_least.as_secs(), "{message}");
}

fn take_declared_errors() -> Vec<String> {
    std::mem::take(&mut *ERRORS.lock().unwrap())
        .into_iter()
        .filter(|message| message.starts_with(DECLARED))
        .collect()
}

static HALTS: LazyLock<Mutex<HashMap<(String, &'static str), String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn record_halt(label: &str, by: &'static str, error: &jalari::JobError) {
    HALTS
        .lock()
        .unwrap()
        .insert((label.to_owned(), by), error.msg().to_owned());
}

fn halt_seen(label: &str, by: &'static str) -> Option<String> {
    HALTS.lock().unwrap().get(&(label.to_owned(), by)).cloned()
}

#[derive(Serialize, Deserialize)]
struct Blocked {
    label: String,
}

#[jalari::job(queue = "run_control")]
impl Job for Blocked {
    const NAME: &'static str = "run_control_blocked";

    async fn run(self) -> JobResult {
        let (_, checkpoint) = jalari::resume::<u32>()?;
        checkpoint.save(&1).await?;
        let handed = jalari::current_job().unwrap().interrupted();
        let label = self.label.clone();
        tokio::spawn(async move { record_halt(&label, "spawned", &handed.await) });
        LEDGER.record(&self.label, "start");
        tokio::select! {
            error = jalari::interrupted() => {
                record_halt(&self.label, "job", &error);
                Err(error)
            }
            () = tokio::time::sleep(Duration::from_secs(600)) => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DeclaredSleeper {
    label: String,
}

#[jalari::job(queue = "run_control", checkpoints = true)]
impl Job for DeclaredSleeper {
    const NAME: &'static str = "run_control_declared_sleeper";

    async fn run(self) -> JobResult {
        jalari::checkpoint().await?;
        LEDGER.record(&self.label, "start");
        tokio::time::sleep(Duration::from_secs(600)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Swallower {
    label: String,
}

#[jalari::job(queue = "run_control", checkpoints = true)]
impl Job for Swallower {
    const NAME: &'static str = "run_control_swallower";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        loop {
            let _ = jalari::checkpoint().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[derive(Serialize, Deserialize)]
struct UndeclaredSleeper {
    label: String,
}

#[jalari::job(queue = "run_control")]
impl Job for UndeclaredSleeper {
    const NAME: &'static str = "run_control_undeclared_sleeper";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        tokio::time::sleep(Duration::from_secs(600)).await;
        Ok(())
    }
}

static RELEASE_SPINNER: AtomicBool = AtomicBool::new(false);

struct SlowDrop(String);

impl Drop for SlowDrop {
    fn drop(&mut self) {
        std::thread::sleep(DROP_WORK);
        LEDGER.record(&self.0, "dropped");
    }
}

#[derive(Serialize, Deserialize)]
struct SlowToDrop {
    label: String,
}

#[jalari::job(queue = "run_control")]
impl Job for SlowToDrop {
    const NAME: &'static str = "run_control_slow_to_drop";

    async fn run(self) -> JobResult {
        let _guard = SlowDrop(self.label.clone());
        LEDGER.record(&self.label, "start");
        tokio::time::sleep(Duration::from_secs(600)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Spinner {
    label: String,
}

#[jalari::job(queue = "run_control")]
impl Job for Spinner {
    const NAME: &'static str = "run_control_spinner";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        while !RELEASE_SPINNER.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct Shape {
    state: String,
    attempts: i32,
}

async fn shape(database: &Database, id: JobId) -> Shape {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn history(database: &Database, id: JobId) -> Vec<(String, Option<String>)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, error FROM {} WHERE job_id = $1 ORDER BY id",
        database.table("job_history")
    )))
    .bind(id.0)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn spawn_worker(stop_timeout: Duration, shutdown_timeout: Duration) -> RunningWorker {
    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    config.stop_timeout = stop_timeout;
    config.shutdown_timeout = shutdown_timeout;
    RunningWorker::spawn(
        Worker::builder()
            .queue(QUEUE, 2)
            .config(config)
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn start_blocked(label: &str) -> JobId {
    start(
        &Blocked {
            label: label.to_owned(),
        },
        label,
    )
    .await
}

async fn start<T: Job>(job: &T, label: &str) -> JobId {
    let id = jalari::enqueue(job).await.unwrap().id();
    wait_until(&format!("{label} to start"), START, || async {
        LEDGER.count(label, "start") == 1
    })
    .await;
    id
}

async fn wait_for_state(database: &Database, id: JobId, state: &str, within: Duration) {
    wait_until(&format!("job {id} to be {state}"), within, || async {
        shape(database, id).await.state == state
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_run_control() {
    tracing::subscriber::set_global_default(CaptureErrors).unwrap();
    let Some(database) = Database::start("run_control", 8).await else {
        return;
    };
    check_interrupted_under_stop(&database).await;
    check_interrupted_under_cancel(&database).await;
    check_interrupted_under_shutdown(&database).await;
    check_interrupted_on_lost_run(&database).await;
    check_declared_checkpoints_log_on_abort(&database).await;
    check_swallowed_halt_logs(&database).await;
    check_undeclared_job_logs_nothing(&database).await;
    check_shutdown_abort_logs(&database).await;
    check_close_waits_for_the_aborted_body(&database).await;
    check_close_waits_at_most_a_second_for_a_blocked_body(&database).await;
    database.finish().await;
}

async fn check_interrupted_under_stop(database: &Database) {
    let worker = spawn_worker(PATIENT, PATIENT).await;
    let id = start_blocked("stop").await;
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(database, id, "stopped", CONTROL_LATENCY).await;
    eprintln!("stop: stopped {:?} after the request", asked.elapsed());
    assert_eq!(
        shape(database, id).await,
        Shape {
            state: "stopped".to_owned(),
            attempts: 0,
        }
    );
    assert_eq!(halt_seen("stop", "job").as_deref(), Some(STOP));
    assert_eq!(
        history(database, id).await,
        [("stopped".to_owned(), Some(STOP.to_owned()))]
    );
    worker.stop().await;
}

async fn check_interrupted_under_cancel(database: &Database) {
    let worker = spawn_worker(PATIENT, PATIENT).await;
    let id = start_blocked("cancel").await;
    let asked = Instant::now();
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Requested);
    wait_for_state(database, id, "deleted", CONTROL_LATENCY).await;
    eprintln!("cancel: deleted {:?} after the request", asked.elapsed());
    assert_eq!(shape(database, id).await.attempts, 0);
    assert_eq!(halt_seen("cancel", "job").as_deref(), Some(CANCEL));
    worker.stop().await;
}

async fn check_interrupted_under_shutdown(database: &Database) {
    let worker = spawn_worker(PATIENT, PATIENT).await;
    let id = start_blocked("shutdown").await;
    let asked = Instant::now();
    worker.stop().await;
    let stopped = asked.elapsed();
    eprintln!("shutdown: the worker stopped {stopped:?} after the request");
    assert!(stopped < CONTROL_LATENCY, "{stopped:?}");
    assert_eq!(
        shape(database, id).await,
        Shape {
            state: "enqueued".to_owned(),
            attempts: 0,
        }
    );
    assert_eq!(halt_seen("shutdown", "job").as_deref(), Some(SHUTDOWN));
    let detail = jalari::admin::job(id).await.unwrap().unwrap();
    assert_eq!(
        detail.control.and_then(|control| control.progress),
        Some(serde_json::json!(1))
    );
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Cancelled);
}

async fn check_interrupted_on_lost_run(database: &Database) {
    let worker = spawn_worker(PATIENT, PATIENT).await;
    let id = start_blocked("lost").await;
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET lock_token = lock_token + 1000000 WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .execute(&database.pool)
    .await
    .unwrap();
    let rewritten = Instant::now();
    wait_until("the spawned task to see the loss", LOST_LATENCY, || async {
        halt_seen("lost", "spawned").is_some()
    })
    .await;
    eprintln!(
        "lost: the spawned task saw the loss {:?} after the token changed",
        rewritten.elapsed()
    );
    assert_eq!(halt_seen("lost", "spawned").as_deref(), Some(LOST));
    wait_until("the release of the rewritten claim", START, || async {
        !history(database, id).await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        history(database, id).await,
        [("scheduled".to_owned(), Some(INTERRUPTED.to_owned()))]
    );
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Cancelled);
    worker.stop().await;
}

async fn stop_until_aborted(database: &Database, id: JobId) {
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(database, id, "stopped", ABORT_LATENCY).await;
    assert_eq!(
        history(database, id)
            .await
            .last()
            .map(|(_, error)| error.clone()),
        Some(Some(format!(
            "aborted because it reached no checkpoint within {IMPATIENT:?}"
        )))
    );
}

async fn check_declared_checkpoints_log_on_abort(database: &Database) {
    let worker = spawn_worker(IMPATIENT, PATIENT).await;
    take_declared_errors();
    let label = "declared";
    let id = start(
        &DeclaredSleeper {
            label: label.to_owned(),
        },
        label,
    )
    .await;
    stop_until_aborted(database, id).await;
    assert_one_silence(&take_declared_errors(), IMPATIENT);
    worker.stop().await;
}

async fn check_swallowed_halt_logs(database: &Database) {
    let worker = spawn_worker(IMPATIENT, PATIENT).await;
    take_declared_errors();
    let label = "swallowed";
    let id = start(
        &Swallower {
            label: label.to_owned(),
        },
        label,
    )
    .await;
    stop_until_aborted(database, id).await;
    assert_eq!(take_declared_errors(), [IGNORED]);
    worker.stop().await;
}

async fn check_undeclared_job_logs_nothing(database: &Database) {
    let worker = spawn_worker(IMPATIENT, PATIENT).await;
    take_declared_errors();
    let label = "undeclared";
    let id = start(
        &UndeclaredSleeper {
            label: label.to_owned(),
        },
        label,
    )
    .await;
    stop_until_aborted(database, id).await;
    assert_eq!(take_declared_errors(), Vec::<String>::new());
    worker.stop().await;
}

async fn check_shutdown_abort_logs(database: &Database) {
    let worker = spawn_worker(PATIENT, IMPATIENT).await;
    take_declared_errors();
    let label = "shutdown_abort";
    let id = start(
        &DeclaredSleeper {
            label: label.to_owned(),
        },
        label,
    )
    .await;
    worker.stop().await;
    assert_one_silence(&take_declared_errors(), IMPATIENT);
    assert_eq!(
        history(database, id).await,
        [("scheduled".to_owned(), Some(INTERRUPTED.to_owned()))]
    );
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Cancelled);
}

async fn check_close_waits_for_the_aborted_body(database: &Database) {
    let worker = spawn_worker(IMPATIENT, PATIENT).await;
    let id = start(
        &SlowToDrop {
            label: "slow_drop".to_owned(),
        },
        "slow_drop",
    )
    .await;
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(database, id, "stopped", ABORT_LATENCY).await;
    let stopped_seen = Instant::now();
    let dropped = LEDGER
        .events("slow_drop")
        .into_iter()
        .find(|(event, _)| *event == "dropped")
        .map(|(_, at)| at);
    assert!(
        dropped.is_some_and(|dropped| dropped <= stopped_seen),
        "the outcome was recorded before the aborted body was dropped: {dropped:?}"
    );
    worker.stop().await;
}

async fn check_close_waits_at_most_a_second_for_a_blocked_body(database: &Database) {
    let worker = spawn_worker(IMPATIENT, PATIENT).await;
    let id = start(
        &Spinner {
            label: "spinner".to_owned(),
        },
        "spinner",
    )
    .await;
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(
        database,
        id,
        "stopped",
        CONTROL_LATENCY + IMPATIENT + ABORT_WAIT,
    )
    .await;
    let stopped = asked.elapsed();
    RELEASE_SPINNER.store(true, Ordering::SeqCst);
    eprintln!("spinner: stopped {stopped:?} after the request while its body never yielded");
    assert_eq!(LEDGER.count("spinner", "start"), 1);
    worker.stop().await;
}
