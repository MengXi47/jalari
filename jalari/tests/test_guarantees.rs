mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, OCCUPANCY, RunningWorker, fast_retry, wait_until, worker_config};
use jalari::admin::{self, JobFilter, RetryOutcome, RunNowOutcome};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    EnqueueOptions, EnqueueOutcome, Job, JobId, JobResult, OnConflict, RestartOutcome, StopOutcome,
    Worker, WorkerConfig,
};
use serde::{Deserialize, Serialize};

const INTERRUPTED: &str = "the worker stopped during the last attempt";
const SNOOZED: &str = "the job asked to run again later";

static SNOOZER_RUNS: AtomicU32 = AtomicU32::new(0);
static CLOSE_RELEASED: AtomicBool = AtomicBool::new(false);

static RESUMED: LazyLock<Mutex<HashMap<String, Vec<Option<u64>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn resumed(label: &str) -> Vec<Option<u64>> {
    RESUMED
        .lock()
        .unwrap()
        .get(label)
        .cloned()
        .unwrap_or_default()
}

#[derive(Serialize, Deserialize)]
struct Saver {
    label: String,
}

#[jalari::job(queue = "g_resume")]
impl Job for Saver {
    const NAME: &'static str = "g_saver";

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<u64>()?;
        RESUMED
            .lock()
            .unwrap()
            .entry(self.label.clone())
            .or_default()
            .push(saved);
        let mut next = saved.unwrap_or_default();
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(60) {
            next += 1;
            checkpoint.save(&next).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Sleeper {
    label: String,
    millis: u64,
}

#[jalari::job(queue = "g_sleep")]
impl Job for Sleeper {
    const NAME: &'static str = "g_sleeper";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        LEDGER.record(&self.label, "done");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Keyed {
    label: String,
}

#[jalari::job(queue = "g_key")]
impl Job for Keyed {
    const NAME: &'static str = "g_keyed";

    async fn run(self) -> JobResult {
        let _seat = OCCUPANCY.enter(vec!["g_key:race".to_owned()], self.label.clone());
        LEDGER.record(&self.label, "start");
        tokio::time::sleep(Duration::from_millis(2)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Snoozer;

#[jalari::job(queue = "g_snooze")]
impl Job for Snoozer {
    const NAME: &'static str = "g_snoozer";

    async fn run(self) -> JobResult {
        if SNOOZER_RUNS.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        jalari::snooze(Duration::from_secs(3600))
    }
}

#[derive(Serialize, Deserialize)]
struct Closer;

#[jalari::job(queue = "g_close")]
impl Job for Closer {
    const NAME: &'static str = "g_closer";

    async fn run(self) -> JobResult {
        let attempt = jalari::current_job().unwrap().attempt();
        for index in 0..2 {
            jalari::enqueue_on_success_with(
                &Sleeper {
                    label: format!("closer/{attempt}/{index}"),
                    millis: 0,
                },
                EnqueueOptions::new().queue("g_close_child"),
            )?;
        }
        LEDGER.record("closer", "buffered");
        while !CLOSE_RELEASED.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }
}

async fn spawn(queue: &str, concurrency: usize) -> RunningWorker {
    spawn_with(
        queue,
        concurrency,
        worker_config(Duration::from_millis(100)),
    )
    .await
}

async fn spawn_with(queue: &str, concurrency: usize, config: WorkerConfig) -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, concurrency)
            .config(config)
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn sleeper(label: &str, millis: u64, queue: &str) -> JobId {
    jalari::enqueue_with(
        &Sleeper {
            label: label.to_owned(),
            millis,
        },
        EnqueueOptions::new().queue(queue),
    )
    .await
    .unwrap()
    .id()
}

async fn wait_started(label: &str) {
    wait_until(
        &format!("{label} to start"),
        Duration::from_secs(15),
        || async { LEDGER.count(label, "start") > 0 },
    )
    .await;
}

async fn history(database: &Database, id: JobId) -> Vec<(String, i32, Option<String>)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempt, error FROM {} WHERE job_id = $1 ORDER BY id",
        database.table("job_history")
    )))
    .bind(id.0)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn state(database: &Database, id: JobId) -> String {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT state FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn sqlstate(database: &Database, sql: String) -> Option<String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(&database.pool)
        .await
        .err()
        .and_then(|e| {
            e.as_database_error()
                .and_then(|e| e.code().map(|code| code.into_owned()))
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_guarantees() {
    let Some(database) = Database::start("guarantees", 3).await else {
        return;
    };
    check_running_shape_is_enforced(&database).await;
    check_crashed_job_resumes_from_saved_progress(&database).await;
    check_reaper_survives_cancellation(&database).await;
    check_stop_on_dead_owner(&database).await;
    check_dead_incarnation_cannot_claim(&database).await;
    check_claim_guard(&database).await;
    check_shutdown_abort_window(&database).await;
    check_admin_answers_while_running(&database).await;
    check_key_race().await;
    check_small_pool().await;
    check_snooze_after_interruption_clears_the_marker(&database).await;
    check_crash_during_close_inserts_no_children(&database).await;
    check_horizon_free(&database).await;
    database.finish().await;
}

async fn check_running_shape_is_enforced(database: &Database) {
    let job = database.table("job");
    let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {job} (queue, task, payload, state, queue_key, max_attempts)
         VALUES ('g_nobody', 'g_sleeper', '{{}}', 'enqueued', 'shape', 1) RETURNING id"
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let rejected = [
        format!("UPDATE {job} SET locked_by = gen_random_uuid() WHERE id = {id}"),
        format!("UPDATE {job} SET command = 'stop' WHERE id = {id}"),
        format!(
            "UPDATE {job} SET state = 'running', locked_by = gen_random_uuid(), \
             locked_at = now(), lock_token = 1 WHERE id = {id}"
        ),
        format!("UPDATE {job} SET state = 'running' WHERE id = {id}"),
    ];
    for sql in rejected {
        assert_eq!(
            sqlstate(database, sql.clone()).await.as_deref(),
            Some("23514"),
            "{sql}"
        );
    }
    sqlx::query(AssertSqlSafe(format!("DELETE FROM {job} WHERE id = {id}")))
        .execute(&database.pool)
        .await
        .unwrap();
}

async fn check_crashed_job_resumes_from_saved_progress(database: &Database) {
    let worker = spawn("g_resume", 1).await;
    let id = jalari::enqueue(&Saver {
        label: "resume".to_owned(),
    })
    .await
    .unwrap()
    .id();
    let stored = || async {
        sqlx::query_scalar::<_, Option<i64>>(AssertSqlSafe(format!(
            "SELECT (progress #>> '{{}}')::bigint FROM {} WHERE job_id = $1",
            database.table("job_control")
        )))
        .bind(id.0)
        .fetch_optional(&database.pool)
        .await
        .unwrap()
        .flatten()
    };
    wait_until(
        "the first progress flush",
        Duration::from_secs(15),
        || async { stored().await.is_some() },
    )
    .await;
    worker.crash().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let saved = stored().await.and_then(|saved| u64::try_from(saved).ok());
    tokio::time::sleep(Duration::from_secs(8)).await;
    let rescuer = spawn("g_resume", 1).await;
    wait_until(
        "the crashed job to resume on the next worker",
        Duration::from_secs(15),
        || async { resumed("resume").len() == 2 },
    )
    .await;
    assert_eq!(resumed("resume"), [None, saved]);
    assert_eq!(
        history(database, id).await,
        [("scheduled".to_owned(), 1, Some(INTERRUPTED.to_owned()))]
    );
    assert_eq!(
        jalari::cancel(id).await.unwrap(),
        jalari::CancelOutcome::Requested
    );
    rescuer.stop().await;
}

async fn check_reaper_survives_cancellation(database: &Database) {
    let job = database.table("job");
    let worker_table = database.table("worker");
    let owner: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {worker_table} (hostname, pid, queues, scheduler, heartbeat_at)
         VALUES ('g_dead', 1, '[]', FALSE, now() - interval '1 minute')
         RETURNING id::text"
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let ids: Vec<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {job} (queue, task, payload, state, attempts, max_attempts, locked_by,
             locked_at, lock_token)
         SELECT 'g_orphaned', 'g_sleeper', '{{}}', 'running', 1, 3, $1::uuid, now(), token
         FROM generate_series(1, 1000) AS token
         RETURNING id"
    )))
    .bind(&owner)
    .fetch_all(&database.pool)
    .await
    .unwrap();

    let canceller = database.connect("canceller", 1).await;
    let worker = spawn("g_idle", 1).await;
    let started = Instant::now();
    let mut cancelled = 0;
    while started.elapsed() < Duration::from_secs(4) {
        let hits: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity
             WHERE state = 'active' AND pid <> pg_backend_pid()
                 AND query LIKE '%suspects AS MATERIALIZED%'",
        )
        .fetch_all(&canceller)
        .await
        .unwrap();
        cancelled += hits.into_iter().filter(|hit| *hit).count();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    canceller.close().await;
    wait_until(
        "the reaper to settle every row",
        Duration::from_secs(20),
        || async {
            sqlx::query_scalar::<_, i64>(AssertSqlSafe(format!(
                "SELECT count(*) FROM {job} WHERE id = ANY($1) AND state = 'running'"
            )))
            .bind(&ids)
            .fetch_one(&database.pool)
            .await
            .unwrap()
                == 0
        },
    )
    .await;
    worker.stop().await;

    let settled: Vec<(String, i32, i64)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job.state, job.attempts,
             (SELECT count(*) FROM {history} AS history
              WHERE history.job_id = job.id AND history.error = $2)
         FROM {job} AS job WHERE job.id = ANY($1)",
        history = database.table("job_history"),
    )))
    .bind(&ids)
    .bind(INTERRUPTED)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    eprintln!(
        "reaper_cancel: cancelled {cancelled} reap statements, {} rows settled",
        settled.len()
    );
    assert_eq!(settled.len(), ids.len());
    for row in &settled {
        assert_eq!(row, &("scheduled".to_owned(), 1, 1));
    }
    let owner_left: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {worker_table} WHERE id = $1::uuid"
    )))
    .bind(&owner)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(owner_left, 0, "the dead worker kept its entry");
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {job} WHERE id = ANY($1)"
    )))
    .bind(&ids)
    .execute(&database.pool)
    .await
    .unwrap();
}

async fn check_stop_on_dead_owner(database: &Database) {
    let worker = spawn("g_stop", 1).await;
    let id = sleeper("doomed", 60_000, "g_stop").await;
    wait_started("doomed").await;
    assert_eq!(database.terminate_running_lifelines().await, 1);
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    let mut outcome = StopOutcome::Requested;
    while outcome == StopOutcome::Requested && asked.elapsed() < Duration::from_secs(12) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        outcome = jalari::stop(id).await.unwrap();
    }
    let settled_after = asked.elapsed();
    assert_eq!(outcome, StopOutcome::Stopped);
    assert!(
        settled_after <= Duration::from_secs(10),
        "{settled_after:?}"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(LEDGER.count("doomed", "start"), 1);
    assert_eq!(
        history(database, id).await,
        [("stopped".to_owned(), 0, Some(INTERRUPTED.to_owned()))]
    );
    eprintln!("stop_on_dead_owner: stopped {settled_after:?} after the lifeline was killed");
    jalari::cancel(id).await.unwrap();
    worker.stop().await;
}

async fn check_dead_incarnation_cannot_claim(database: &Database) {
    let worker = spawn("g_flood", 16).await;
    wait_until("the lifeline", Duration::from_secs(10), || async {
        database.lifelines().await.len() == 1
    })
    .await;
    assert_eq!(database.terminate_lifelines(1).await, 1);
    let mut ids = Vec::new();
    for index in 0..100 {
        ids.push(sleeper(&format!("flood:{index}"), 5, "g_flood").await);
    }
    wait_until("the flood to finish", Duration::from_secs(30), || async {
        let mut done = true;
        for &id in &ids {
            done &= state(database, id).await == "succeeded";
        }
        done
    })
    .await;
    worker.stop().await;
    let raw: Vec<i64> = ids.iter().map(|id| id.0).collect();
    let interrupted: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE job_id = ANY($1) AND error = $2",
        database.table("job_history")
    )))
    .bind(&raw)
    .bind(INTERRUPTED)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(
        interrupted, 0,
        "a claim committed under the dead incarnation"
    );
    for index in 0..100 {
        assert_eq!(LEDGER.count(&format!("flood:{index}"), "start"), 1);
    }
}

async fn check_claim_guard(database: &Database) {
    let worker = spawn("g_guard", 1).await;
    wait_until("the lifeline", Duration::from_secs(10), || async {
        database.lifelines().await.len() == 1
    })
    .await;
    let (_, worker_id) = database.lifelines().await.remove(0);
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE FUNCTION \"{schema}\".g_age() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             NEW.heartbeat_at := now() - interval '5 seconds';
             RETURN NEW;
         END
         $$;
         CREATE TRIGGER g_age BEFORE UPDATE ON {worker} FOR EACH ROW
             WHEN (OLD.id = '{worker_id}') EXECUTE FUNCTION \"{schema}\".g_age();",
        worker = database.table("worker"),
    )))
    .execute(&database.pool)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let id = sleeper("guarded", 5, "g_guard").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        LEDGER.count("guarded", "start"),
        0,
        "a worker whose heartbeat is 5 s old claimed a job"
    );
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP TRIGGER g_age ON {worker}; DROP FUNCTION \"{schema}\".g_age();",
        worker = database.table("worker"),
    )))
    .execute(&database.pool)
    .await
    .unwrap();
    wait_until("the guarded job", Duration::from_secs(10), || async {
        state(database, id).await == "succeeded"
    })
    .await;
    worker.stop().await;
}

async fn check_shutdown_abort_window(database: &Database) {
    let mut config = worker_config(Duration::from_millis(100));
    config.shutdown_timeout = Duration::from_secs(1);
    let worker = spawn_with("g_window", 1, config).await;
    let id = sleeper("stubborn", 30_000, "g_window").await;
    wait_started("stubborn").await;
    worker.stop().await;
    let aborted = Instant::now();
    let (row_state, last_error, until): (String, Option<String>, f64) =
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state, last_error, EXTRACT(EPOCH FROM run_at - now())::float8
             FROM {} WHERE id = $1",
            database.table("job")
        )))
        .bind(id.0)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(row_state, "scheduled");
    assert_eq!(last_error.as_deref(), Some(INTERRUPTED));
    assert!(until > 5.0, "the aborted job is due in {until} s");
    assert_eq!(admin::run_now(id).await.unwrap(), RunNowOutcome::Running);
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Stopped);
    assert_eq!(
        jalari::restart(id).await.unwrap(),
        RestartOutcome::Restarted
    );
    let rescuer = spawn("g_window", 1).await;
    wait_until(
        "the aborted job to run again",
        Duration::from_secs(15),
        || async { LEDGER.count("stubborn", "start") == 2 },
    )
    .await;
    let rerun = LEDGER
        .events("stubborn")
        .iter()
        .filter(|(event, _)| *event == "start")
        .map(|(_, at)| *at)
        .nth(1)
        .unwrap();
    let waited = rerun.saturating_duration_since(aborted);
    eprintln!("shutdown_window: the aborted job ran again {waited:?} after the abort");
    assert!(waited >= Duration::from_millis(6500), "{waited:?}");
    jalari::cancel(id).await.unwrap();
    rescuer.stop().await;
}

async fn check_admin_answers_while_running(database: &Database) {
    let worker = spawn("g_admin", 1).await;
    let id = sleeper("admin", 10_000, "g_admin").await;
    wait_started("admin").await;
    let started = Instant::now();
    assert_eq!(admin::retry(id).await.unwrap(), RetryOutcome::NotFailed);
    let retried = started.elapsed();
    let started = Instant::now();
    assert_eq!(admin::run_now(id).await.unwrap(), RunNowOutcome::Running);
    let ran_now = started.elapsed();
    let started = Instant::now();
    admin::clear_fingerprint(&JobFilter::new().queue("g_admin"))
        .await
        .unwrap();
    let cleared = started.elapsed();
    eprintln!("admin: retry {retried:?}, run_now {ran_now:?}, clear_fingerprint {cleared:?}");
    for latency in [retried, ran_now, cleared] {
        assert!(latency < Duration::from_millis(100), "{latency:?}");
    }
    assert_eq!(state(database, id).await, "running");
    jalari::cancel(id).await.unwrap();
    worker.stop().await;
}

async fn check_key_race() {
    let workers = [spawn("g_key", 8).await, spawn("g_key", 8).await];
    let mut ids = Vec::new();
    for index in 0..500 {
        ids.push(
            jalari::enqueue_with(
                &Keyed {
                    label: format!("key:{index}"),
                },
                EnqueueOptions::new().queue_key("race"),
            )
            .await
            .unwrap()
            .id(),
        );
    }
    wait_until(
        "the key race to drain",
        Duration::from_secs(120),
        || async { (0..500).all(|index| LEDGER.count(&format!("key:{index}"), "start") == 1) },
    )
    .await;
    for worker in workers {
        worker.stop().await;
    }
    assert_eq!(OCCUPANCY.violations(), Vec::<String>::new());
}

async fn check_small_pool() {
    let worker = spawn("g_small", 32).await;
    let started = Instant::now();
    for index in 0..1000 {
        sleeper(&format!("small:{index}"), 20, "g_small").await;
    }
    wait_until(
        "the small pool to drain",
        Duration::from_secs(120),
        || async { (0..1000).all(|index| LEDGER.count(&format!("small:{index}"), "done") == 1) },
    )
    .await;
    eprintln!(
        "small_pool: 1000 jobs of 20 ms on 32 slots and 3 pooled connections in {:?}",
        started.elapsed()
    );
    worker.stop().await;
}

async fn check_horizon_free(database: &Database) {
    let worker = spawn("g_horizon", 1).await;
    sleeper("horizon", 10_000, "g_horizon").await;
    wait_started("horizon").await;
    let scratch = database.table("g_scratch");
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {scratch} (id integer) WITH (autovacuum_enabled = false);
         INSERT INTO {scratch} SELECT generate_series(1, 2000);
         DELETE FROM {scratch};"
    )))
    .execute(&database.pool)
    .await
    .unwrap();
    let mut pinned = 0;
    let sampled = Instant::now();
    while sampled.elapsed() < Duration::from_secs(3) {
        let holders: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM pg_stat_activity AS activity
             WHERE activity.pid <> pg_backend_pid()
                 AND (activity.application_name = $1 OR EXISTS (
                     SELECT 1 FROM {worker} AS worker
                     WHERE activity.application_name = 'jalari lifeline ' || worker.id::text))
                 AND activity.state <> 'active'
                 AND (activity.backend_xid IS NOT NULL OR activity.backend_xmin IS NOT NULL)",
            worker = database.table("worker"),
        )))
        .bind(&database.schema_name)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        pinned = pinned.max(holders);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sqlx::raw_sql(AssertSqlSafe(format!("VACUUM {scratch}")))
        .execute(&database.pool)
        .await
        .unwrap();
    let dead: i64 =
        sqlx::query_scalar("SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid = $1::regclass")
            .bind(&scratch)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(
        LEDGER.count("horizon", "done"),
        0,
        "the long job ended early"
    );
    eprintln!(
        "horizon: {pinned} jalari sessions held an xid or an idle snapshot during a 10 s job, \
         {dead} dead tuples left after VACUUM"
    );
    assert_eq!(pinned, 0);
    assert_eq!(dead, 0);
    worker.stop().await;
}

async fn last_error(database: &Database, id: JobId) -> Option<String> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT last_error FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn check_snooze_after_interruption_clears_the_marker(database: &Database) {
    let crashed = spawn("g_snooze", 1).await;
    let options = || EnqueueOptions::new().job_key("g_snooze_key");
    let id = jalari::enqueue_with(&Snoozer, options())
        .await
        .unwrap()
        .id();
    wait_until("the first run", Duration::from_secs(15), || async {
        SNOOZER_RUNS.load(Ordering::SeqCst) == 1
    })
    .await;
    crashed.crash().await;
    let rescuer = spawn("g_snooze", 1).await;
    wait_until("the snoozed rerun", Duration::from_secs(30), || async {
        history(database, id).await.len() == 2
    })
    .await;
    rescuer.stop().await;
    assert_eq!(
        history(database, id).await,
        [
            ("scheduled".to_owned(), 1, Some(INTERRUPTED.to_owned())),
            ("scheduled".to_owned(), 1, Some(SNOOZED.to_owned()))
        ]
    );
    assert_eq!(last_error(database, id).await, None);
    assert_eq!(admin::run_now(id).await.unwrap(), RunNowOutcome::Enqueued);
    assert_eq!(state(database, id).await, "enqueued");
    let replaced = jalari::enqueue_with(&Snoozer, options().on_conflict(OnConflict::Replace))
        .await
        .unwrap();
    assert_eq!(replaced, EnqueueOutcome::Replaced(id));
    assert_eq!(state(database, id).await, "enqueued");
    assert_eq!(last_error(database, id).await, None);
    assert_eq!(
        jalari::cancel(id).await.unwrap(),
        jalari::CancelOutcome::Cancelled
    );
}

async fn check_crash_during_close_inserts_no_children(database: &Database) {
    let job = database.table("job");
    let worker = spawn("g_close", 1).await;
    let id = jalari::enqueue(&Closer).await.unwrap().id();
    wait_until("the closer to buffer", Duration::from_secs(15), || async {
        LEDGER.count("closer", "buffered") > 0
    })
    .await;
    let holder_pool = database.connect("close_holder", 1).await;
    let mut holder = holder_pool.begin().await.unwrap();
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "SELECT 1 FROM {job} WHERE id = $1 FOR UPDATE"
    )))
    .bind(id.0)
    .execute(&mut *holder)
    .await
    .unwrap();
    CLOSE_RELEASED.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(10);
    let closing: i32 = loop {
        let blocked: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
        )
        .bind(holder_pid)
        .fetch_optional(&database.pool)
        .await
        .unwrap();
        if let Some(blocked) = blocked {
            break blocked;
        }
        assert!(
            Instant::now() < deadline,
            "the close never waited on the row"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    worker.crash().await;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(closing)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert!(terminated);
    holder.rollback().await.unwrap();
    holder_pool.close().await;
    let children = || async {
        sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
            "SELECT convert_from(payload, 'UTF8')::jsonb ->> 'label' FROM {job}
             WHERE parent_id = $1 ORDER BY id"
        )))
        .bind(id.0)
        .fetch_all(&database.pool)
        .await
        .unwrap()
    };
    assert_eq!(state(database, id).await, "running");
    assert!(children().await.is_empty());
    let rescuer = spawn("g_close", 1).await;
    wait_until(
        "the crashed close to run again",
        Duration::from_secs(30),
        || async { state(database, id).await == "succeeded" },
    )
    .await;
    rescuer.stop().await;
    assert_eq!(children().await, ["closer/2/0", "closer/2/1"]);
    assert_eq!(
        history(database, id).await,
        [
            ("scheduled".to_owned(), 1, Some(INTERRUPTED.to_owned())),
            ("succeeded".to_owned(), 2, None)
        ]
    );
}
