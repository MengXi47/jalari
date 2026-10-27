mod common;

use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, Rng, RunningWorker, fast_retry, worker_config};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{EnqueueOptions, Job, JobError, JobResult, Priority, Worker};
use serde::{Deserialize, Serialize};

const ABORT_ROUNDS: u64 = 20;
const ABORT_JOBS: u64 = 400;
const RANK_QUEUE: &str = "protocol_rank";
const PAUSE_QUEUE: &str = "protocol_pause";
const PAUSE_POLL: Duration = Duration::from_millis(250);
const EXPIRY_QUEUE: &str = "protocol_expiry";

static RELEASE: AtomicBool = AtomicBool::new(false);
static RELEASE_FENCED: AtomicBool = AtomicBool::new(false);
static OPENED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
static SEEN: LazyLock<Mutex<Vec<(String, i32, Priority)>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

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

#[derive(Serialize, Deserialize)]
struct Ranked {
    label: String,
    gate: Option<String>,
    fail_first: bool,
}

#[jalari::job(queue = "protocol_rank")]
impl Job for Ranked {
    const NAME: &'static str = "protocol_ranked";

    async fn run(self) -> JobResult {
        let job = jalari::current_job().expect("ranked jobs run inside a job");
        SEEN.lock()
            .unwrap()
            .push((self.label.clone(), job.attempt(), job.priority()));
        LEDGER.record(&self.label, "start");
        if let Some(gate) = &self.gate {
            let started = Instant::now();
            while !OPENED.lock().unwrap().contains(gate)
                && started.elapsed() < Duration::from_secs(20)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        if self.fail_first && job.attempt() == 1 {
            return Err(JobError::new("the first attempt fails".to_owned()));
        }
        LEDGER.record(&self.label, "done");
        Ok(())
    }
}

fn open_gate(gate: &str) {
    OPENED.lock().unwrap().insert(gate.to_owned());
}

async fn enqueue_ranked(label: &str, gate: Option<&str>, options: EnqueueOptions) -> i64 {
    let job = Ranked {
        label: label.to_owned(),
        gate: gate.map(str::to_owned),
        fail_first: false,
    };
    jalari::enqueue_with(&job, options).await.unwrap().id().0
}

fn first_event(label: &str, event: &str) -> Option<Instant> {
    LEDGER
        .events(label)
        .into_iter()
        .find(|(recorded, _)| *recorded == event)
        .map(|(_, at)| at)
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
    spawn_polling(queue, concurrency, Duration::from_millis(50)).await
}

async fn spawn_polling(queue: &str, concurrency: usize, poll_interval: Duration) -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, concurrency)
            .config(worker_config(poll_interval))
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
    failures.extend(check_claim_takes_higher_priority_first().await);
    failures.extend(check_claim_prefers_due_lower_priority_over_future_urgent().await);
    failures.extend(check_claim_falls_through_a_locked_level(&database).await);
    failures.extend(check_claim_falls_through_a_busy_queue_key().await);
    failures.extend(check_retry_keeps_priority(&database).await);
    failures.extend(check_pause_holds_without_notification(&database).await);
    failures.extend(check_busy_worker_never_starts_expired_job(&database).await);
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

async fn check_claim_takes_higher_priority_first() -> Vec<String> {
    let enqueued = [
        Priority::Low,
        Priority::Normal,
        Priority::High,
        Priority::Urgent,
    ];
    for priority in enqueued {
        enqueue_ranked(
            &format!("order:{priority:?}"),
            None,
            EnqueueOptions::new().priority(priority),
        )
        .await;
    }
    let worker = spawn_worker(RANK_QUEUE, 1).await;
    let done = settle(Duration::from_secs(10), || async {
        enqueued
            .iter()
            .all(|priority| LEDGER.count(&format!("order:{priority:?}"), "done") == 1)
    })
    .await;
    worker.stop().await;
    let mut started: Vec<(Option<Instant>, Priority)> = enqueued
        .iter()
        .map(|priority| {
            (
                first_event(&format!("order:{priority:?}"), "start"),
                *priority,
            )
        })
        .collect();
    started.sort();
    let order: Vec<Priority> = started.into_iter().map(|(_, priority)| priority).collect();
    eprintln!("priority_order: ran {order:?}");
    if !done || order != Priority::ALL {
        return vec![format!(
            "priority_order: four due jobs enqueued from Low to Urgent ran in the order \
             {order:?} (all done: {done}), expected {:?}",
            Priority::ALL
        )];
    }
    Vec::new()
}

async fn check_claim_prefers_due_lower_priority_over_future_urgent() -> Vec<String> {
    let delay = Duration::from_millis(1500);
    let enqueued = Instant::now();
    enqueue_ranked(
        "future:urgent",
        None,
        EnqueueOptions::new()
            .priority(Priority::Urgent)
            .delay(delay),
    )
    .await;
    enqueue_ranked(
        "due:low",
        None,
        EnqueueOptions::new().priority(Priority::Low),
    )
    .await;
    let worker = spawn_worker(RANK_QUEUE, 1).await;
    let done = settle(Duration::from_secs(10), || async {
        LEDGER.count("future:urgent", "done") == 1 && LEDGER.count("due:low", "done") == 1
    })
    .await;
    worker.stop().await;
    let low = first_event("due:low", "start").map(|at| at - enqueued);
    let urgent = first_event("future:urgent", "start").map(|at| at - enqueued);
    eprintln!("future_urgent: due Low started after {low:?}, future Urgent after {urgent:?}");
    let in_order = matches!((low, urgent), (Some(low), Some(urgent)) if low < Duration::from_secs(1)
        && urgent >= delay - Duration::from_millis(100));
    if !done || !in_order {
        return vec![format!(
            "future_urgent: a due Low job started after {low:?} and an Urgent job delayed by \
             {delay:?} after {urgent:?} (all done: {done}); the due job must run first and at \
             once"
        )];
    }
    Vec::new()
}

async fn check_claim_falls_through_a_locked_level(database: &Database) -> Vec<String> {
    let mut failures = Vec::new();
    let locked = enqueue_ranked(
        "locked:urgent",
        None,
        EnqueueOptions::new().priority(Priority::Urgent),
    )
    .await;
    enqueue_ranked(
        "locked:high",
        None,
        EnqueueOptions::new().priority(Priority::High),
    )
    .await;
    let locker = database.connect("locker", 1).await;
    let mut transaction = locker.begin().await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM {} WHERE id = $1 FOR UPDATE",
        database.table("job")
    )))
    .bind(locked)
    .execute(&mut *transaction)
    .await
    .unwrap();
    let worker = spawn_worker(RANK_QUEUE, 1).await;
    let high_done = settle(Duration::from_secs(5), || async {
        LEDGER.count("locked:high", "done") == 1
    })
    .await;
    let urgent_while_locked = LEDGER.count("locked:urgent", "start");
    transaction.rollback().await.unwrap();
    locker.close().await;
    let urgent_done = settle(Duration::from_secs(5), || async {
        LEDGER.count("locked:urgent", "done") == 1
    })
    .await;
    worker.stop().await;
    eprintln!(
        "locked_level: High done {high_done} while the Urgent row was locked, Urgent done \
         {urgent_done} after the lock was released"
    );
    if !high_done || urgent_while_locked != 0 {
        failures.push(format!(
            "locked_level: with the only due Urgent job {locked} locked by another session, the \
             due High job must run (done: {high_done}) and the Urgent job must not (started \
             {urgent_while_locked} times)"
        ));
    }
    if !urgent_done {
        failures.push(format!(
            "locked_level: Urgent job {locked} did not run within 5 s of its lock being released"
        ));
    }
    failures
}

async fn check_claim_falls_through_a_busy_queue_key() -> Vec<String> {
    let mut failures = Vec::new();
    let urgent = EnqueueOptions::new()
        .priority(Priority::Urgent)
        .queue_key("busy");
    enqueue_ranked("busy:holder", Some("busy"), urgent.clone()).await;
    let worker = spawn_worker(RANK_QUEUE, 2).await;
    let holding = settle(Duration::from_secs(5), || async {
        LEDGER.count("busy:holder", "start") == 1
    })
    .await;
    enqueue_ranked("busy:waiter", None, urgent).await;
    enqueue_ranked(
        "busy:keyless",
        None,
        EnqueueOptions::new().priority(Priority::High),
    )
    .await;
    let keyless_done = settle(Duration::from_secs(5), || async {
        LEDGER.count("busy:keyless", "done") == 1
    })
    .await;
    let waiter_while_held = LEDGER.count("busy:waiter", "start");
    open_gate("busy");
    let waiter_done = settle(Duration::from_secs(5), || async {
        LEDGER.count("busy:waiter", "done") == 1
    })
    .await;
    worker.stop().await;
    let holder_done = first_event("busy:holder", "done");
    let waiter_started = first_event("busy:waiter", "start");
    eprintln!(
        "busy_key: holder running {holding}, keyless High done {keyless_done}, Urgent waiter \
         done {waiter_done}"
    );
    if !holding || !keyless_done || waiter_while_held != 0 {
        failures.push(format!(
            "busy_key: while an Urgent job held queue_key \"busy\" (running: {holding}), a due \
             High job without a key must run (done: {keyless_done}) and the Urgent job waiting \
             on the key must not (started {waiter_while_held} times)"
        ));
    }
    if !waiter_done || waiter_started < holder_done {
        failures.push(format!(
            "busy_key: the Urgent waiter must start after the holder finished, holder done at \
             {holder_done:?}, waiter started at {waiter_started:?} (done: {waiter_done})"
        ));
    }
    failures
}

async fn check_retry_keeps_priority(database: &Database) -> Vec<String> {
    let job = Ranked {
        label: "retry:high".to_owned(),
        gate: None,
        fail_first: true,
    };
    let id = jalari::enqueue_with(&job, EnqueueOptions::new().priority(Priority::High))
        .await
        .unwrap()
        .id()
        .0;
    let worker = spawn_worker(RANK_QUEUE, 1).await;
    let done = settle(Duration::from_secs(10), || async {
        LEDGER.count("retry:high", "done") == 1
    })
    .await;
    worker.stop().await;
    let seen: Vec<(i32, Priority)> = SEEN
        .lock()
        .unwrap()
        .iter()
        .filter(|(label, _, _)| label == "retry:high")
        .map(|(_, attempt, priority)| (*attempt, *priority))
        .collect();
    let stored: (String, i32, i16) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts, priority FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    eprintln!("retry_priority: attempts saw {seen:?}, row {stored:?}");
    let expected = vec![(1, Priority::High), (2, Priority::High)];
    if !done || seen != expected || stored != ("succeeded".to_owned(), 2, 1) {
        return vec![format!(
            "retry_priority: a High job that fails once must run both attempts as High and keep \
             priority 1, attempts saw {seen:?} and the row is {stored:?} (done: {done})"
        )];
    }
    Vec::new()
}

async fn check_pause_holds_without_notification(database: &Database) -> Vec<String> {
    let mut failures = Vec::new();
    let queue_pause = database.table("queue_pause");
    let options = EnqueueOptions::new().queue(PAUSE_QUEUE);
    let worker = spawn_polling(PAUSE_QUEUE, 1, PAUSE_POLL).await;
    enqueue_ranked("pause:before", None, options.clone()).await;
    let live = settle(Duration::from_secs(5), || async {
        LEDGER.count("pause:before", "done") == 1
    })
    .await;
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {queue_pause} (queue) VALUES ($1)"
    )))
    .bind(PAUSE_QUEUE)
    .execute(&database.pool)
    .await
    .unwrap();
    let held = enqueue_ranked("pause:held", None, options).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let started_while_paused = LEDGER.count("pause:held", "start");
    let paused_row = job_state(database, held).await;
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {queue_pause} WHERE queue = $1"
    )))
    .bind(PAUSE_QUEUE)
    .execute(&database.pool)
    .await
    .unwrap();
    let resumed_at = Instant::now();
    let resume_bound = 4 * PAUSE_POLL;
    let resumed = settle(Duration::from_secs(5), || async {
        LEDGER.count("pause:held", "start") == 1
    })
    .await;
    let resumed_after = first_event("pause:held", "start").map(|at| at - resumed_at);
    worker.stop().await;
    eprintln!(
        "pause: worker live {live}, {started_while_paused} starts while paused, row {paused_row:?}, \
         started {resumed_after:?} after the pause row was deleted"
    );
    if !live {
        failures.push("pause: the first job of the queue never ran".to_owned());
    }
    if started_while_paused != 0 || paused_row.0 != "enqueued" {
        failures.push(format!(
            "pause: job {held} of a queue paused by a raw insert with no notification must not \
             start, it started {started_while_paused} times and its row is {paused_row:?}"
        ));
    }
    if !resumed || resumed_after.is_none_or(|after| after > resume_bound) {
        failures.push(format!(
            "pause: job {held} started {resumed_after:?} after the pause row was deleted with no \
             notification, expected within {resume_bound:?} at a poll interval of {PAUSE_POLL:?}"
        ));
    }
    failures
}

async fn check_busy_worker_never_starts_expired_job(database: &Database) -> Vec<String> {
    let options = EnqueueOptions::new().queue(EXPIRY_QUEUE);
    let worker = spawn_worker(EXPIRY_QUEUE, 1).await;
    enqueue_ranked("expiry:holder", Some("expiry"), options.clone()).await;
    let holding = settle(Duration::from_secs(5), || async {
        LEDGER.count("expiry:holder", "start") == 1
    })
    .await;
    let target = enqueue_ranked(
        "expiry:target",
        None,
        options.expire_after(Duration::from_millis(500)),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    open_gate("expiry");
    let freed = settle(Duration::from_secs(5), || async {
        LEDGER.count("expiry:holder", "done") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let starts = LEDGER.count("expiry:target", "start");
    let row = job_state(database, target).await;
    worker.stop().await;
    eprintln!(
        "expiry: holder running {holding}, freed {freed}, expired job started {starts} times, \
         row {row:?}"
    );
    let waiting_or_discarded = row.0 == "enqueued" || row.0 == "discarded";
    if !holding || !freed || starts != 0 || row.1 != 0 || !waiting_or_discarded {
        return vec![format!(
            "expiry: job {target} expired while the only slot was busy (holder running: \
             {holding}, freed: {freed}); it must never start, but it started {starts} times and \
             its row is {row:?}"
        )];
    }
    Vec::new()
}
