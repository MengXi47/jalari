mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::Duration;

use common::{Database, RunningWorker, fast_retry, wait_until};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{Job, JobError, JobId, JobResult, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};

const QUEUE: &str = "progress_durable";
const SETTLED: Duration = Duration::from_secs(15);

struct Tables {
    pool: PgPool,
    job_control: String,
}

static TABLES: OnceLock<Tables> = OnceLock::new();

static OPEN: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

static RESUMED: LazyLock<Mutex<HashMap<String, Vec<Option<String>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static REACHED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

fn open(gate: &str) {
    OPEN.lock().unwrap().insert(gate.to_owned());
}

fn reach(gate: &str) {
    REACHED.lock().unwrap().insert(gate.to_owned());
}

fn reached(gate: &str) -> bool {
    REACHED.lock().unwrap().contains(gate)
}

async fn pass(gate: &str) {
    reach(gate);
    while !OPEN.lock().unwrap().contains(gate) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn resumed(label: &str) -> Vec<Option<String>> {
    RESUMED
        .lock()
        .unwrap()
        .get(label)
        .cloned()
        .unwrap_or_default()
}

static LEASES: LazyLock<Mutex<HashMap<String, Vec<Option<i64>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn leases(label: &str) -> Vec<Option<i64>> {
    LEASES
        .lock()
        .unwrap()
        .get(label)
        .cloned()
        .unwrap_or_default()
}

async fn delete_control_row(job_id: JobId) {
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {} WHERE job_id = $1",
        tables.job_control
    )))
    .bind(job_id.0)
    .execute(&tables.pool)
    .await
    .unwrap();
}

#[derive(Serialize, Deserialize)]
struct Leases {
    label: String,
    drop_control_row: bool,
}

#[jalari::job(queue = "progress_durable", max_attempts = 3)]
impl Job for Leases {
    const NAME: &'static str = "progress_leases";

    async fn run(self) -> JobResult {
        let run = jalari::current_job().unwrap();
        let lease = jalari::admin::job(run.id())
            .await?
            .and_then(|detail| detail.control)
            .map(|control| control.lease);
        LEASES
            .lock()
            .unwrap()
            .entry(self.label.clone())
            .or_default()
            .push(lease);
        if run.attempt() > 1 {
            return Ok(());
        }
        if self.drop_control_row {
            delete_control_row(run.id()).await;
        }
        Err(JobError::new("the first run fails".to_owned()))
    }
}

#[derive(Serialize, Deserialize)]
struct Overtaken {
    label: String,
}

#[jalari::job(queue = "progress_durable", max_attempts = 3)]
impl Job for Overtaken {
    const NAME: &'static str = "progress_overtaken";

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<String>()?;
        RESUMED
            .lock()
            .unwrap()
            .entry(self.label.clone())
            .or_default()
            .push(saved);
        if jalari::current_job().unwrap().attempt() > 1 {
            return Ok(());
        }
        checkpoint.save(&"older".to_owned()).await?;
        pass(&self.label).await;
        Err(JobError::new("the first run fails".to_owned()))
    }
}

#[derive(Serialize, Deserialize)]
struct Parked;

#[jalari::job(queue = "progress_durable")]
impl Job for Parked {
    const NAME: &'static str = "progress_parked";

    async fn run(self) -> JobResult {
        Err(JobError::new("parked for an hour".to_owned()).retry_after(Duration::from_secs(3600)))
    }
}

async fn spawn_worker() -> RunningWorker {
    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    RunningWorker::spawn(
        Worker::builder()
            .queue(QUEUE, 4)
            .config(config)
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn state(id: JobId) -> String {
    jalari::admin::job(id)
        .await
        .unwrap()
        .unwrap()
        .job
        .state
        .as_str()
        .to_owned()
}

async fn run_leases(label: &str, drop_control_row: bool) -> Vec<Option<i64>> {
    let id = jalari::enqueue(&Leases {
        label: label.to_owned(),
        drop_control_row,
    })
    .await
    .unwrap()
    .id();
    wait_until(&format!("{label} to succeed"), SETTLED, || async {
        state(id).await == "succeeded"
    })
    .await;
    leases(label)
}

async fn check_each_run_has_its_own_lease() {
    let seen = run_leases("unique", false).await;
    let [Some(first), Some(second)] = seen[..] else {
        panic!("every run must see its control row, saw {seen:?}");
    };
    assert!(
        second > first,
        "the second run must hold a newer lease than the first: {seen:?}"
    );
}

async fn check_close_without_a_row_still_fences() {
    let seen = run_leases("rowless", true).await;
    let [Some(first), Some(second)] = seen[..] else {
        panic!("every run must see its control row, saw {seen:?}");
    };
    assert_ne!(
        first, second,
        "a run whose control row vanished must not share its lease with the next run"
    );
}

async fn check_close_keeps_a_newer_stored_copy() {
    let label = "overtaken";
    let id = jalari::enqueue(&Overtaken {
        label: label.to_owned(),
    })
    .await
    .unwrap()
    .id();
    wait_until("the first run to save", SETTLED, || async {
        reached(label)
    })
    .await;
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET progress = '\"newer\"', progress_seq = progress_seq + 100
         WHERE job_id = $1",
        tables.job_control
    )))
    .bind(id.0)
    .execute(&tables.pool)
    .await
    .unwrap();
    open(label);
    wait_until("the second run to succeed", SETTLED, || async {
        state(id).await == "succeeded"
    })
    .await;
    assert_eq!(resumed(label), [None, Some("newer".to_owned())]);
}

async fn control_rows(job_ids: &[JobId]) -> i64 {
    let tables = TABLES.get().unwrap();
    let job_ids: Vec<i64> = job_ids.iter().map(|id| id.0).collect();
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE job_id = ANY($1)",
        tables.job_control
    )))
    .bind(&job_ids)
    .fetch_one(&tables.pool)
    .await
    .unwrap()
}

async fn check_housekeeping_removes_control_rows_with_their_jobs() {
    let finished = run_leases("housekept", false).await;
    assert_eq!(finished.len(), 2);
    assert!(finished.iter().all(Option::is_some));
    let finished_id: Vec<JobId> = jalari::admin::jobs(
        &jalari::admin::JobFilter::new().state(jalari::JobState::Succeeded),
        100,
        None,
    )
    .await
    .unwrap()
    .into_iter()
    .map(|job| job.id)
    .collect();
    let parked = jalari::enqueue(&Parked).await.unwrap().id();
    wait_until("the parked job to wait for its retry", SETTLED, || async {
        state(parked).await == "scheduled"
    })
    .await;
    assert_eq!(
        control_rows(&finished_id).await,
        i64::try_from(finished_id.len()).unwrap()
    );
    jalari::cluster::update(|cluster| {
        cluster.housekeeping_enabled = true;
        cluster.housekeeping_interval = Duration::from_secs(1);
        cluster.succeeded_retention = Duration::ZERO;
    })
    .await
    .unwrap();
    wait_until(
        "housekeeping to remove the finished jobs",
        SETTLED,
        || async {
            let mut gone = true;
            for id in &finished_id {
                gone &= jalari::admin::job(*id).await.unwrap().is_none();
            }
            gone
        },
    )
    .await;
    jalari::cluster::update(|cluster| cluster.housekeeping_enabled = false)
        .await
        .unwrap();
    assert_eq!(control_rows(&finished_id).await, 0);
    assert_eq!(control_rows(&[parked]).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_progress_durable() {
    let Some(database) = Database::start("progress_durable", 16).await else {
        return;
    };
    let _ = TABLES.set(Tables {
        pool: database.pool.clone(),
        job_control: database.table("job_control"),
    });
    let worker = spawn_worker().await;
    check_each_run_has_its_own_lease().await;
    check_close_without_a_row_still_fences().await;
    check_close_keeps_a_newer_stored_copy().await;
    check_housekeeping_removes_control_rows_with_their_jobs().await;
    worker.stop().await;
    database.finish().await;
}
