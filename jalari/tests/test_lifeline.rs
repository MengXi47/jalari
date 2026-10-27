mod common;

use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, fast_retry, worker_config};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{Finished, Job, JobError, JobMeta, JobMiddleware, JobResult, Next, Worker};
use serde::{Deserialize, Serialize};

const INTERRUPTED: &str = "the worker stopped during the last attempt";

#[derive(Serialize, Deserialize)]
struct Idle;

#[jalari::job(queue = "lifeline")]
impl Job for Idle {
    const NAME: &'static str = "lifeline_idle";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Ticker {
    label: String,
    millis: u64,
}

#[jalari::job(queue = "lifeline")]
impl Job for Ticker {
    const NAME: &'static str = "lifeline_ticker";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(self.millis) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            LEDGER.record(&self.label, "tick");
            jalari::checkpoint().await?;
        }
        LEDGER.record(&self.label, "done");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Outage {
    label: String,
}

#[jalari::job(queue = "lifeline")]
impl Job for Outage {
    const NAME: &'static str = "lifeline_outage";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        LEDGER.record(&self.label, "done");
        Ok(())
    }
}

struct Finishes;

impl JobMiddleware for Finishes {
    type Provides = ();

    async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
        next.run().await
    }

    async fn finished(&self, job: &JobMeta, _outcome: &Finished) {
        LEDGER.record(&job.id().0.to_string(), "finished");
    }
}

#[derive(Serialize, Deserialize)]
struct Unsaveable;

#[jalari::job(queue = "lifeline")]
impl Job for Unsaveable {
    const NAME: &'static str = "lifeline_unsaveable";
    const MAX_ATTEMPTS: i32 = 2;

    async fn run(self) -> JobResult {
        LEDGER.record("unsaveable", "start");
        let (_, checkpoint) = jalari::resume::<String>()?;
        checkpoint.save(&"\u{0}".to_owned()).await?;
        Err(JobError::new("boom".to_owned()))
    }
}

fn starts(label: &str) -> Vec<Instant> {
    LEDGER
        .events(label)
        .into_iter()
        .filter(|(event, _)| *event == "start")
        .map(|(_, at)| at)
        .collect()
}

fn last_tick_before(label: &str, until: Instant) -> Option<Instant> {
    LEDGER
        .events(label)
        .into_iter()
        .filter(|(event, at)| *event == "tick" && *at < until)
        .map(|(_, at)| at)
        .max()
}

async fn job_shape(database: &Database, id: i64) -> (String, Vec<(String, i32, Option<String>)>) {
    let state: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT state FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let history = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempt, error FROM {} WHERE job_id = $1 ORDER BY id",
        database.table("job_history")
    )))
    .bind(id)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    (state, history)
}

async fn spawn_worker() -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue("lifeline", 2)
            .config(worker_config(Duration::from_millis(100)))
            .retry_policy(fast_retry())
            .middleware(Finishes)
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn holds_advisory_lock(database: &Database, pid: i32) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND granted
         )",
    )
    .bind(pid)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifeline() {
    let Some(database) = Database::start("lifeline", 4).await else {
        return;
    };
    check_lifeline_session(&database).await;
    check_terminated_lifeline_registers_again(&database).await;
    check_terminated_lifeline_aborts_and_hands_over(&database).await;
    check_orphaned_claim_is_released(&database, "orphan", 3).await;
    check_orphaned_claim_is_released(&database, "last_orphan", 1).await;
    check_abandoned_close_counts_the_attempt(&database).await;
    check_close_outlasts_an_outage(&database).await;
    check_close_after_takeover_is_fenced(&database).await;
    database.finish().await;
}

async fn check_lifeline_session(database: &Database) {
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let (pid, worker_id) = database.lifelines().await.remove(0);
    let listed = jalari::admin::workers().await.unwrap();
    assert!(
        listed.iter().any(|worker| worker.id == worker_id),
        "the lifeline's worker {worker_id} is not listed: {listed:?}"
    );
    assert!(holds_advisory_lock(database, pid).await);

    let mut pooled = database.pool.acquire().await.unwrap();
    for setting in [
        "idle_session_timeout",
        "statement_timeout",
        "tcp_user_timeout",
    ] {
        let value: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SHOW {setting}")))
            .fetch_one(&mut *pooled)
            .await
            .unwrap();
        assert_eq!(value, "0", "pooled connections keep their own {setting}");
    }
    drop(pooled);

    worker.stop().await;
    assert!(database.lifelines().await.is_empty());
    assert!(!holds_advisory_lock(database, pid).await);
    assert!(jalari::admin::workers().await.unwrap().is_empty());
}

async fn check_terminated_lifeline_registers_again(database: &Database) {
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let (_, old_id) = database.lifelines().await.remove(0);
    assert_eq!(database.terminate_lifelines(1).await, 1);
    common::wait_until(
        "the worker to register again under a new id",
        Duration::from_secs(10),
        || async {
            let lifelines = database.lifelines().await;
            lifelines.len() == 1 && lifelines[0].1 != old_id
        },
    )
    .await;
    common::wait_until(
        "the old worker row to go away",
        Duration::from_secs(10),
        || async {
            !jalari::admin::workers()
                .await
                .unwrap()
                .iter()
                .any(|worker| worker.id == old_id)
        },
    )
    .await;
    worker.stop().await;
    assert!(database.lifelines().await.is_empty());
}

async fn check_terminated_lifeline_aborts_and_hands_over(database: &Database) {
    let worker = spawn_worker().await;
    let id = jalari::enqueue(&Ticker {
        label: "abort".to_owned(),
        millis: 30_000,
    })
    .await
    .unwrap()
    .id()
    .0;
    common::wait_until("the ticker to start", Duration::from_secs(10), || async {
        !starts("abort").is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(database.terminate_running_lifelines().await, 1);
    let killed = Instant::now();
    common::wait_until("the job to run again", Duration::from_secs(15), || async {
        starts("abort").len() == 2
    })
    .await;
    let rerun = starts("abort")[1];
    let stopped = last_tick_before("abort", rerun).unwrap();
    let aborted_after = stopped.saturating_duration_since(killed);
    let rerun_after = rerun.saturating_duration_since(killed);
    eprintln!(
        "lifeline_kill: the old run stopped {aborted_after:?} and the job ran again \
         {rerun_after:?} after its worker's lifeline was killed"
    );
    assert!(
        aborted_after <= Duration::from_millis(1500),
        "{aborted_after:?}"
    );
    assert!(
        (Duration::from_millis(5800)..=Duration::from_millis(8800)).contains(&rerun_after),
        "{rerun_after:?}"
    );
    assert_eq!(
        job_shape(database, id).await,
        (
            "running".to_owned(),
            vec![("scheduled".to_owned(), 1, Some(INTERRUPTED.to_owned()))]
        )
    );
    assert_eq!(
        jalari::cancel(jalari::JobId(id)).await.unwrap(),
        jalari::CancelOutcome::Requested
    );
    worker.stop().await;
}

async fn check_orphaned_claim_is_released(database: &Database, label: &str, max_attempts: i32) {
    let worker = spawn_worker().await;
    common::wait_until("the lifeline", Duration::from_secs(10), || async {
        database.lifelines().await.len() == 1
    })
    .await;
    let (_, worker_id) = database.lifelines().await.remove(0);
    let payload = serde_json::to_vec(&Ticker {
        label: label.to_owned(),
        millis: 10,
    })
    .unwrap();
    let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {} (queue, task, payload, state, attempts, max_attempts, locked_by,
             locked_at, lock_token)
         VALUES ('lifeline', 'lifeline_ticker', $1, 'running', 1, $3, $2::uuid, now(), -1)
         RETURNING id",
        database.table("job")
    )))
    .bind(&payload)
    .bind(&worker_id)
    .bind(max_attempts)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let inserted = Instant::now();
    common::wait_until(
        "the orphan to be released",
        Duration::from_secs(5),
        || async { job_shape(database, id).await.0 != "running" },
    )
    .await;
    let released_after = inserted.elapsed();
    common::wait_until(
        "the released job to run",
        Duration::from_secs(15),
        || async { job_shape(database, id).await.0 == "succeeded" },
    )
    .await;
    eprintln!("{label}: released {released_after:?} after it appeared");
    assert!(
        released_after <= Duration::from_secs(3),
        "{released_after:?}"
    );
    assert_eq!(starts(label).len(), 1);
    assert_eq!(
        job_shape(database, id).await.1,
        [
            ("scheduled".to_owned(), 0, Some(INTERRUPTED.to_owned())),
            ("succeeded".to_owned(), 1, None)
        ]
    );
    worker.stop().await;
}

async fn check_abandoned_close_counts_the_attempt(database: &Database) {
    let worker = spawn_worker().await;
    let id = jalari::enqueue(&Unsaveable).await.unwrap().id().0;
    common::wait_until(
        "the job to fail after its abandoned close",
        Duration::from_secs(30),
        || async { job_shape(database, id).await.0 == "failed" },
    )
    .await;
    assert_eq!(starts("unsaveable").len(), 2);
    assert_eq!(
        job_shape(database, id).await.1,
        [
            ("scheduled".to_owned(), 1, Some(INTERRUPTED.to_owned())),
            ("failed".to_owned(), 2, Some("boom".to_owned()))
        ]
    );
    worker.stop().await;
}

async fn cut_closes(database: &Database, outage: Duration) {
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE OR REPLACE FUNCTION \"{schema}\".cut_close() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             IF clock_timestamp() < OLD.locked_at + make_interval(secs => {seconds}) THEN
                 PERFORM pg_terminate_backend(pg_backend_pid());
             END IF;
             RETURN NEW;
         END $$;
         DROP TRIGGER IF EXISTS cut_close ON {job};
         CREATE TRIGGER cut_close BEFORE UPDATE ON {job} FOR EACH ROW
         WHEN (OLD.task = 'lifeline_outage' AND OLD.attempts = 1 AND NEW.state = 'succeeded')
         EXECUTE FUNCTION \"{schema}\".cut_close()",
        seconds = outage.as_secs_f64(),
        job = database.table("job"),
    )))
    .execute(&database.pool)
    .await
    .unwrap();
}

async fn attempts(database: &Database, id: i64) -> i32 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT attempts FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn check_close_outlasts_an_outage(database: &Database) {
    cut_closes(database, Duration::from_secs(8)).await;
    let worker = spawn_worker().await;
    let id = jalari::enqueue(&Outage {
        label: "outage".to_owned(),
    })
    .await
    .unwrap()
    .id()
    .0;
    common::wait_until("the job to succeed", Duration::from_secs(25), || async {
        job_shape(database, id).await.0 == "succeeded"
    })
    .await;
    let saved_after = LEDGER.events("outage")[1].1.elapsed();
    common::wait_until(
        "the finished hook to run",
        Duration::from_secs(5),
        || async { LEDGER.count(&id.to_string(), "finished") == 1 },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    eprintln!("outage: the outcome was saved {saved_after:?} after the body returned");
    assert!(
        saved_after >= Duration::from_secs(7),
        "the close was not cut: {saved_after:?}"
    );
    assert_eq!(starts("outage").len(), 1);
    assert_eq!(attempts(database, id).await, 1);
    assert_eq!(
        job_shape(database, id).await,
        (
            "succeeded".to_owned(),
            vec![("succeeded".to_owned(), 1, None)]
        )
    );
    assert_eq!(LEDGER.count(&id.to_string(), "finished"), 1);
    worker.stop().await;
}

async fn check_close_after_takeover_is_fenced(database: &Database) {
    cut_closes(database, Duration::from_secs(20)).await;
    let worker = spawn_worker().await;
    let id = jalari::enqueue(&Outage {
        label: "taken".to_owned(),
    })
    .await
    .unwrap()
    .id()
    .0;
    common::wait_until("the body to return", Duration::from_secs(10), || async {
        LEDGER.count("taken", "done") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(database.terminate_running_lifelines().await, 1);
    common::wait_until("the job to succeed", Duration::from_secs(20), || async {
        job_shape(database, id).await.0 == "succeeded"
    })
    .await;
    common::wait_until(
        "the finished hook to run",
        Duration::from_secs(5),
        || async { LEDGER.count(&id.to_string(), "finished") == 1 },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(starts("taken").len(), 2);
    assert_eq!(
        job_shape(database, id).await,
        (
            "succeeded".to_owned(),
            vec![
                ("scheduled".to_owned(), 1, Some(INTERRUPTED.to_owned())),
                ("succeeded".to_owned(), 2, None)
            ]
        )
    );
    assert_eq!(LEDGER.count(&id.to_string(), "finished"), 1);
    worker.stop().await;
}
