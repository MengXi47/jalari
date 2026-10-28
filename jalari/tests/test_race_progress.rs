mod common;

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, wait_until};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{Job, JobId, JobResult, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};

const QUEUE: &str = "race_progress";
const START: Duration = Duration::from_secs(10);
const FLUSHED: Duration = Duration::from_secs(7);
const TAKEN_OVER: Duration = Duration::from_millis(9500);
const IDLE_LIMIT: Duration = Duration::from_secs(10);
const HELD_TAKEN_OVER: Duration = Duration::from_millis(12_500);
const PATIENT: Duration = Duration::from_secs(30);

static POOL: OnceLock<PgPool> = OnceLock::new();

static RELEASED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

fn release(label: &str) {
    RELEASED.lock().unwrap().insert(label.to_owned());
}

fn is_released(label: &str) -> bool {
    RELEASED.lock().unwrap().contains(label)
}

fn first(label: &str, event: &str) -> Option<Instant> {
    LEDGER
        .events(label)
        .into_iter()
        .find(|(seen, _)| *seen == event)
        .map(|(_, at)| at)
}

async fn idle_until_interrupted() -> JobResult {
    tokio::select! {
        error = jalari::interrupted() => Err(error),
        () = tokio::time::sleep(Duration::from_secs(600)) => Ok(()),
    }
}

#[derive(Serialize, Deserialize)]
struct Holder {
    label: String,
}

#[jalari::job(queue = "race_progress", max_attempts = 5)]
impl Job for Holder {
    const NAME: &'static str = "race_progress_holder";

    async fn run(self) -> JobResult {
        if LEDGER.count(&self.label, "start") > 0 {
            LEDGER.record(&self.label, "rerun");
            return Ok(());
        }
        LEDGER.record(&self.label, "start");
        let (_, checkpoint) = jalari::resume::<u32>()?;
        let mut transaction = POOL.get().unwrap().begin().await?;
        checkpoint.save_in(&mut transaction, &1).await?;
        checkpoint.save(&2).await?;
        LEDGER.record(&self.label, "held");
        let label = self.label.clone();
        tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(60);
            while !is_released(&label) && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let _ = transaction.rollback().await;
        });
        idle_until_interrupted().await
    }
}

#[derive(Serialize, Deserialize)]
struct Busy {
    label: String,
}

#[jalari::job(queue = "race_progress", max_attempts = 5)]
impl Job for Busy {
    const NAME: &'static str = "race_progress_busy";

    async fn run(self) -> JobResult {
        if LEDGER.count(&self.label, "start") > 0 {
            LEDGER.record(&self.label, "rerun");
            return Ok(());
        }
        LEDGER.record(&self.label, "start");
        let (_, checkpoint) = jalari::resume::<u32>()?;
        for count in 0.. {
            checkpoint.save(&count).await?;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Sleeper {
    label: String,
}

#[jalari::job(queue = "race_progress", max_attempts = 5)]
impl Job for Sleeper {
    const NAME: &'static str = "race_progress_sleeper";

    async fn run(self) -> JobResult {
        if LEDGER.count(&self.label, "start") > 0 {
            LEDGER.record(&self.label, "rerun");
            return Ok(());
        }
        LEDGER.record(&self.label, "start");
        idle_until_interrupted().await
    }
}

async fn spawn_worker() -> RunningWorker {
    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    config.stop_timeout = Duration::from_secs(1);
    config.shutdown_timeout = Duration::from_secs(2);
    RunningWorker::spawn(
        Worker::builder()
            .queue(QUEUE, 4)
            .config(config)
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn enqueue<T: Job>(job: &T) -> JobId {
    jalari::enqueue(job).await.unwrap().id()
}

async fn wait_for(label: &str, event: &str, within: Duration) {
    wait_until(&format!("{label} to {event}"), within, || async {
        LEDGER.count(label, event) > 0
    })
    .await;
}

async fn flushed_since(
    database: &Database,
    id: JobId,
    since: chrono::DateTime<chrono::Utc>,
) -> bool {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT EXISTS (SELECT 1 FROM {} WHERE job_id = $1 AND updated_at > $2)",
        database.table("job_control")
    )))
    .bind(id.0)
    .bind(since)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn database_now(database: &Database) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar("SELECT now()")
        .fetch_one(&database.pool)
        .await
        .unwrap()
}

async fn check_flush_skips_a_held_row(database: &Database) {
    let worker = spawn_worker().await;
    let holder = enqueue(&Holder {
        label: "flush_holder".to_owned(),
    })
    .await;
    let busy = enqueue(&Busy {
        label: "flush_busy".to_owned(),
    })
    .await;
    wait_for("flush_busy", "start", START).await;
    wait_for("flush_holder", "held", START).await;
    let held_at = database_now(database).await;
    let held = Instant::now();
    wait_until(
        "the busy job's progress to be flushed while another row is held",
        FLUSHED,
        || async { flushed_since(database, busy, held_at).await },
    )
    .await;
    eprintln!(
        "flush: the busy job's progress was written {:?} after the other job's row was held",
        held.elapsed()
    );
    release("flush_holder");
    for id in [holder, busy] {
        jalari::cancel(id).await.unwrap();
    }
    worker.stop().await;
}

async fn terminate_lifeline_of(database: &Database, id: JobId) {
    let terminated: bool = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT pg_terminate_backend(activity.pid)
         FROM pg_stat_activity AS activity
         JOIN {job} AS job ON activity.application_name = 'jalari lifeline ' || job.locked_by::text
         WHERE job.id = $1 AND activity.datname = current_database()",
        job = database.table("job"),
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(terminated);
}

async fn check_takeover_skips_a_held_row(database: &Database) {
    let victim = spawn_worker().await;
    let holder = enqueue(&Holder {
        label: "takeover_holder".to_owned(),
    })
    .await;
    let others = ["takeover_a", "takeover_b", "takeover_c"];
    for label in others {
        enqueue(&Sleeper {
            label: label.to_owned(),
        })
        .await;
    }
    for label in others {
        wait_for(label, "start", START).await;
    }
    wait_for("takeover_holder", "held", START).await;
    let rescuer = spawn_worker().await;
    terminate_lifeline_of(database, holder).await;
    let terminated = Instant::now();
    let held = first("takeover_holder", "held").unwrap();
    for label in others {
        wait_for(label, "rerun", PATIENT).await;
    }
    wait_for("takeover_holder", "rerun", PATIENT).await;
    let mut slowest = Duration::ZERO;
    for label in others {
        let taken = first(label, "rerun").unwrap() - terminated;
        eprintln!("takeover: {label} ran again {taken:?} after its worker lost its lifeline");
        slowest = slowest.max(taken);
    }
    let holder_taken = first("takeover_holder", "rerun").unwrap() - held;
    eprintln!(
        "takeover: the job holding its row ran again {holder_taken:?} after its transaction went \
         idle, {:?} after its worker lost its lifeline",
        first("takeover_holder", "rerun").unwrap() - terminated
    );
    assert!(
        slowest < TAKEN_OVER,
        "a held control row delayed the takeover of the worker's other jobs: {slowest:?}"
    );
    assert!(
        holder_taken >= IDLE_LIMIT && holder_taken < HELD_TAKEN_OVER,
        "the job holding its row ran again {holder_taken:?} after its transaction went idle"
    );
    release("takeover_holder");
    victim.stop().await;
    rescuer.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_race_progress() {
    let Some(database) = Database::start("race_progress", 24).await else {
        return;
    };
    let _ = POOL.set(database.pool.clone());
    check_flush_skips_a_held_row(&database).await;
    check_takeover_skips_a_held_row(&database).await;
    database.finish().await;
}
