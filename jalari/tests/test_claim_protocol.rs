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

static RELEASE: AtomicBool = AtomicBool::new(false);
static RELEASE_FENCED: AtomicBool = AtomicBool::new(false);

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
struct Fenced {
    label: String,
}

#[jalari::job(queue = "protocol_fence")]
impl Job for Fenced {
    const NAME: &'static str = "protocol_fenced";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        let started = Instant::now();
        while !RELEASE_FENCED.load(Ordering::SeqCst) && started.elapsed() < Duration::from_secs(20)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        LEDGER.record(&self.label, "done");
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

async fn running_rows(database: &Database) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE state = 'running'",
        database.table("job")
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn job_state(database: &Database, id: i64) -> (String, i32, Option<i16>) {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts, queue_key_slot FROM {} WHERE id = $1",
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
    failures.extend(check_key_held_by_running_row(&database).await);
    failures.extend(check_stale_close_is_fenced(&database).await);
    failures.extend(check_aborted_claims_leave_nothing(&database, Rng::new(seed)).await);
    database.finish().await;
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

async fn check_key_held_by_running_row(database: &Database) -> Vec<String> {
    let mut failures = Vec::new();
    let job = database.table("job");
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

    let row = job_state(database, id).await;
    if row != ("running".to_owned(), 1, Some(0)) {
        failures.push(format!(
            "key_held: job {id} should be running its attempt 1 in key slot 0, row {row:?}"
        ));
    }
    let sibling: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {job} (queue, task, payload, state, queue_key, max_attempts)
         VALUES ('protocol_nobody', 'protocol_holder', '{{}}', 'enqueued', 'held', 1)
         RETURNING id"
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let taken = sqlx::query(AssertSqlSafe(format!(
        "UPDATE {job}
         SET state = 'running', attempts = 1, locked_by = gen_random_uuid(),
             locked_at = now(), lock_token = 1, queue_key_slot = 0
         WHERE id = $1"
    )))
    .bind(sibling)
    .execute(&database.pool)
    .await;
    let code = taken
        .as_ref()
        .err()
        .and_then(|e| e.as_database_error())
        .and_then(|e| e.code().map(|code| code.into_owned()));
    if code.as_deref() != Some("23505") {
        failures.push(format!(
            "key_held: a second running row with queue_key \"held\" was accepted while job {id} \
             ran: {taken:?}"
        ));
    }
    sqlx::query(AssertSqlSafe(format!("DELETE FROM {job} WHERE id = $1")))
        .bind(sibling)
        .execute(&database.pool)
        .await
        .unwrap();

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
    let row = job_state(database, id).await;
    if row.2.is_some() || running_rows(database).await != 0 {
        failures.push(format!(
            "key_held: job {id} kept its key slot or a row stayed running after the close, \
             row {row:?}"
        ));
    }
    let locks = advisory_locks(database).await;
    let lifelines = database.lifelines().await.len();
    if locks != 0 || lifelines != 1 {
        failures.push(format!(
            "key_held: expected only the live lifeline to hold an advisory lock, found {locks} \
             pooled advisory locks and {lifelines} lifelines"
        ));
    }
    worker.stop().await;
    eprintln!("key_held: second running row rejected with {code:?}, recorded {finished}");
    failures
}

async fn check_stale_close_is_fenced(database: &Database) -> Vec<String> {
    let mut failures = Vec::new();
    let job = database.table("job");
    let history = database.table("job_history");
    let worker = spawn_worker("protocol_fence", 1).await;
    let label = "fenced".to_owned();
    let id = jalari::enqueue(&Fenced {
        label: label.clone(),
    })
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
        return vec![format!("fence: job {id} never started")];
    }

    let stranger: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "UPDATE {job} SET locked_by = gen_random_uuid() WHERE id = $1 AND state = 'running'
         RETURNING locked_by::text"
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    RELEASE_FENCED.store(true, Ordering::SeqCst);
    let done = settle(Duration::from_secs(5), || async {
        LEDGER.count(&label, "done") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let row: (String, Option<String>) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, locked_by::text FROM {job} WHERE id = $1"
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let recorded: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {history} WHERE job_id = $1"
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    worker.stop().await;
    if !done {
        failures.push(format!("fence: the body of job {id} never returned"));
    }
    if row != ("running".to_owned(), Some(stranger.clone())) || recorded != 0 {
        failures.push(format!(
            "fence: after its row was handed to {stranger}, the old run of job {id} must write \
             nothing, but the row is {row:?} with {recorded} history rows"
        ));
    }
    sqlx::query(AssertSqlSafe(format!("DELETE FROM {job} WHERE id = $1")))
        .bind(id)
        .execute(&database.pool)
        .await
        .unwrap();
    eprintln!("fence: body returned {done}, row {row:?}, {recorded} history rows");
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
                && database.lifelines().await.is_empty()
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
                 held, {} lifelines were open and these sessions were idle in a transaction: \
                 {idle:?}",
                advisory_locks(database).await,
                database.lifelines().await.len()
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
    let left_running = running_rows(database).await;
    worker.stop().await;
    if !drained {
        failures.push(format!(
            "abort: {} of {ABORT_JOBS} jobs had not succeeded 30 s after the aborted rounds",
            count_waiting().await
        ));
    }
    if left_running != 0 {
        failures.push(format!(
            "abort: {left_running} rows stayed running after every job succeeded"
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
