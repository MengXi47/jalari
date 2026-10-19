mod common;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, Rng, RunningWorker, fast_retry, worker_config};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{EnqueueOptions, Job, JobResult, Worker};
use serde::{Deserialize, Serialize};

const ABORT_ROUNDS: u64 = 20;
const ABORT_JOBS: u64 = 400;
const PAUSE_MS: u64 = 500;

static RELEASE: AtomicBool = AtomicBool::new(false);

#[derive(Serialize, Deserialize)]
struct Holder {
    label: String,
}

#[jalari::job(queue = "protocol_hold")]
impl Job for Holder {
    const NAME: &'static str = "protocol_holder";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        let started = Instant::now();
        while !RELEASE.load(Ordering::SeqCst) && started.elapsed() < Duration::from_secs(20) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        LEDGER.record(&self.label, "done");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Gap {
    label: String,
}

#[jalari::job(queue = "protocol_gap")]
impl Job for Gap {
    const NAME: &'static str = "protocol_gap";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Quick {
    label: String,
}

#[jalari::job(queue = "protocol_abort")]
impl Job for Quick {
    const NAME: &'static str = "protocol_quick";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        tokio::time::sleep(Duration::from_millis(2)).await;
        LEDGER.record(&self.label, "done");
        Ok(())
    }
}

async fn settle<F, Fut>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !condition().await {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

async fn spawn_worker(queue: &str, concurrency: usize) -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, concurrency)
            .config(worker_config(Duration::from_millis(50)))
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn advisory_locks(database: &Database) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks AS lock
         JOIN pg_stat_activity AS activity ON activity.pid = lock.pid
         WHERE lock.locktype = 'advisory' AND activity.application_name = $1",
    )
    .bind(&database.schema_name)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn sessions_where(database: &Database, condition: &str) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND application_name = $1 AND {condition}"
    )))
    .bind(&database.schema_name)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn job_state(database: &Database, id: i64) -> (String, i32, bool) {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts, run_at > now() + interval '50 minutes' FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_claim_protocol() {
    let Some(database) = Database::start("protocol", 16).await else {
        return;
    };
    let seed = common::seed("test_claim_protocol");
    let mut failures = Vec::new();
    failures.extend(check_key_held_while_running(&database).await);
    failures.extend(check_relock_refuses_rewritten_row(&database).await);
    failures.extend(check_aborted_claims_leave_nothing(&database, Rng::new(seed)).await);
    database.finish().await;
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

async fn check_key_held_while_running(database: &Database) -> Vec<String> {
    let mut failures = Vec::new();
    let worker = spawn_worker("protocol_hold", 2).await;
    let label = "holder".to_owned();
    let id = jalari::enqueue_with(
        &Holder {
            label: label.clone(),
        },
        EnqueueOptions::new().queue_key("held"),
    )
    .await
    .unwrap()
    .id()
    .0;
    if !settle(Duration::from_secs(5), || async {
        LEDGER.count(&label, "start") == 1
    })
    .await
    {
        worker.stop().await;
        return vec![format!("key_held: holder job {id} never started")];
    }

    let lock_key = format!("jalari:queue_key:{}.:held", database.schema_name);
    let mut probe = database.pool.acquire().await.unwrap();
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
        .bind(&lock_key)
        .fetch_one(&mut *probe)
        .await
        .unwrap();
    if taken {
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
            .bind(&lock_key)
            .execute(&mut *probe)
            .await
            .unwrap();
        failures.push(format!(
            "key_held: another session took queue_key \"held\" while job {id} was running"
        ));
    }
    drop(probe);

    RELEASE.store(true, Ordering::SeqCst);
    let finished = settle(Duration::from_secs(5), || async {
        job_state(database, id).await.0 == "succeeded"
    })
    .await;
    if !finished {
        failures.push(format!(
            "key_held: job {id} was not recorded as succeeded after its run, row {:?}",
            job_state(database, id).await
        ));
    }
    let released = settle(Duration::from_secs(5), || async {
        advisory_locks(database).await == 0
    })
    .await;
    if !released {
        failures.push(format!(
            "key_held: {} advisory locks were still held after job {id} committed",
            advisory_locks(database).await
        ));
    }
    worker.stop().await;
    eprintln!(
        "key_held: probe took the key {taken}, recorded {finished}, locks released {released}"
    );
    failures
}

async fn check_relock_refuses_rewritten_row(database: &Database) -> Vec<String> {
    let mut failures = Vec::new();
    let job = database.table("job");
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE FUNCTION \"{schema}\".protocol_pause() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_sleep({PAUSE_MS} / 1000.0);
             RETURN NULL;
         END
         $$;
         CREATE CONSTRAINT TRIGGER protocol_pause AFTER UPDATE ON {job}
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
             WHEN (NEW.task = 'protocol_gap' AND OLD.attempts = 0 AND NEW.attempts = 1)
             EXECUTE FUNCTION \"{schema}\".protocol_pause();"
    )))
    .execute(&database.pool)
    .await
    .unwrap();

    let worker = spawn_worker("protocol_gap", 1).await;
    let label = "gap".to_owned();
    let id = jalari::enqueue(&Gap {
        label: label.clone(),
    })
    .await
    .unwrap()
    .id()
    .0;
    if !settle(Duration::from_secs(5), || async {
        sessions_where(database, "wait_event = 'PgSleep'").await > 0
    })
    .await
    {
        worker.stop().await;
        return vec![format!("relock: the worker never claimed job {id}")];
    }

    let rewriter = database.connect("rewriter", 1).await;
    let mut rewrite = rewriter.begin().await.unwrap();
    let attempts: i32 = sqlx::query_scalar(AssertSqlSafe(format!(
        "UPDATE {job} SET run_at = now() + interval '1 hour' WHERE id = $1 RETURNING attempts"
    )))
    .bind(id)
    .fetch_one(&mut *rewrite)
    .await
    .unwrap();
    if attempts != 1 {
        failures.push(format!(
            "relock: the rewrite of job {id} saw {attempts} attempts, expected the counted \
             attempt 1"
        ));
    }
    let relocking = settle(Duration::from_secs(5), || async {
        sessions_where(database, "wait_event_type = 'Lock'").await > 0
    })
    .await;
    rewrite.commit().await.unwrap();
    rewriter.close().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    worker.stop().await;

    let row = job_state(database, id).await;
    let starts = LEDGER.count(&label, "start");
    if !relocking {
        failures.push(format!(
            "relock: the worker never waited for the rewritten row of job {id}"
        ));
    }
    if starts != 0 {
        failures.push(format!(
            "relock: job {id} ran {starts} times although its row was rewritten between the \
             claim and the run, row {row:?}"
        ));
    }
    if row != ("enqueued".to_owned(), 1, true) {
        failures.push(format!(
            "relock: job {id} should keep the rewrite with its one counted attempt, row {row:?}"
        ));
    }
    eprintln!("relock: relock waited {relocking}, runs {starts}, row {row:?}");
    failures
}

async fn check_aborted_claims_leave_nothing(database: &Database, mut rng: Rng) -> Vec<String> {
    let mut failures = Vec::new();
    let mut ids = Vec::new();
    for index in 0..ABORT_JOBS {
        let label = format!("quick:{index}");
        let options = if index % 2 == 0 {
            EnqueueOptions::new().queue_key(&format!("abort-{}", index % 16))
        } else {
            EnqueueOptions::new()
        };
        ids.push(
            jalari::enqueue_with(&Quick { label }, options)
                .await
                .unwrap()
                .id()
                .0,
        );
    }

    for round in 0..ABORT_ROUNDS {
        let worker = spawn_worker("protocol_abort", 8).await;
        tokio::time::sleep(rng.millis(5, 60)).await;
        worker.crash().await;
        let clean = settle(Duration::from_secs(5), || async {
            advisory_locks(database).await == 0
                && sessions_where(database, "state LIKE 'idle in transaction%'").await == 0
        })
        .await;
        if !clean {
            let idle: Vec<(String, String)> = sqlx::query_as(
                "SELECT state, query FROM pg_stat_activity
                 WHERE datname = current_database() AND application_name = $1
                     AND state LIKE 'idle in transaction%'",
            )
            .bind(&database.schema_name)
            .fetch_all(&database.pool)
            .await
            .unwrap();
            failures.push(format!(
                "abort round {round}: 5 s after the worker was dropped, {} advisory locks were \
                 held and these sessions were idle in a transaction: {idle:?}",
                advisory_locks(database).await
            ));
        }
    }

    let worker = spawn_worker("protocol_abort", 8).await;
    let count_waiting = || async {
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE id = ANY($1) AND state <> 'succeeded'",
            database.table("job")
        )))
        .bind(&ids)
        .fetch_one(&database.pool)
        .await
        .unwrap()
    };
    let drained = settle(Duration::from_secs(30), || async {
        count_waiting().await == 0
    })
    .await;
    worker.stop().await;
    if !drained {
        failures.push(format!(
            "abort: {} of {ABORT_JOBS} jobs had not succeeded 30 s after the aborted rounds",
            count_waiting().await
        ));
    }
    let done = (0..ABORT_JOBS)
        .filter(|index| LEDGER.count(&format!("quick:{index}"), "done") > 0)
        .count();
    eprintln!(
        "abort: {ABORT_ROUNDS} aborted rounds, {done} of {ABORT_JOBS} jobs completed, drained \
         {drained}"
    );
    failures
}
