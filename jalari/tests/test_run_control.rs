mod common;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, wait_until};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{CancelOutcome, Job, JobId, JobResult, StopOutcome, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};

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
    let id = jalari::enqueue(&Blocked {
        label: label.to_owned(),
    })
    .await
    .unwrap()
    .id();
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
    let Some(database) = Database::start("run_control", 8).await else {
        return;
    };
    check_interrupted_under_stop(&database).await;
    check_interrupted_under_cancel(&database).await;
    check_interrupted_under_shutdown(&database).await;
    check_interrupted_on_lost_run(&database).await;
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
