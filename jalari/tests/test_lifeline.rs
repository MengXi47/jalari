mod common;

use std::fmt;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, fast_retry, worker_config};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{Finished, Job, JobError, JobMeta, JobMiddleware, JobResult, Next, Worker};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

const INTERRUPTED: &str = "the worker stopped during the last attempt";
const WATCHDOG_PREFIX: &str = "the worker's runtime";
const STALLED: &str = "the worker's runtime has not run for ";
const RECOVERED: &str = "the worker's runtime runs again after ";

static WATCHDOG: LazyLock<Mutex<Vec<(Level, String)>>> = LazyLock::new(|| Mutex::new(Vec::new()));

struct Message(Option<String>);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

struct CaptureWatchdog;

impl Subscriber for CaptureWatchdog {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= Level::INFO && metadata.target().starts_with("jalari")
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut message = Message(None);
        event.record(&mut message);
        if let Some(message) = message.0
            && message.starts_with(WATCHDOG_PREFIX)
        {
            WATCHDOG
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message));
        }
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

fn watchdog_lines() -> Vec<(Level, String)> {
    WATCHDOG.lock().unwrap().clone()
}

fn seconds_after(message: &str, prefix: &str) -> u64 {
    message
        .strip_prefix(prefix)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|seconds| seconds.parse().ok())
        .unwrap_or_else(|| panic!("no seconds in {message:?}"))
}

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

#[derive(Serialize, Deserialize)]
struct Blocking {
    label: String,
    millis: u64,
}

#[jalari::job(queue = "lifeline")]
impl Job for Blocking {
    const NAME: &'static str = "lifeline_blocking";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        let millis = self.millis;
        tokio::spawn(async move { std::thread::sleep(Duration::from_millis(millis)) }).await?;
        LEDGER.record(&self.label, "done");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Attempts {
    label: String,
}

#[jalari::job(queue = "lifeline")]
impl Job for Attempts {
    const NAME: &'static str = "lifeline_attempts";

    async fn run(self) -> JobResult {
        let attempt = jalari::current_job().unwrap().attempt();
        let label = format!("{}#{attempt}", self.label);
        LEDGER.record(&label, "start");
        loop {
            tokio::time::sleep(Duration::from_millis(20)).await;
            LEDGER.record(&label, "tick");
            jalari::checkpoint().await?;
        }
    }
}

struct Finishes;

impl JobMiddleware for Finishes {
    type Provides = ();

    async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
        next.run().await
    }

    async fn finished(&self, job: &JobMeta, outcome: &Finished) {
        LEDGER.record(&job.id().0.to_string(), "finished");
        if *outcome == Finished::Succeeded {
            LEDGER.record(&job.id().0.to_string(), "succeeded");
        }
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
    tracing::subscriber::set_global_default(CaptureWatchdog).unwrap();
    let Some(database) = Database::start("lifeline", 4).await else {
        return;
    };
    check_lifeline_session(&database).await;
    check_terminated_lifeline_registers_again(&database).await;
    check_terminated_lifeline_aborts_and_hands_over(&database).await;
    check_orphaned_claim_is_released(&database, "orphan", 3).await;
    check_orphaned_claim_is_released(&database, "last_orphan", 1).await;
    check_abandoned_close_counts_the_attempt(&database).await;
    check_blocked_runtime_keeps_the_lifeline(&database).await;
    check_refused_heartbeats_keep_the_jobs(&database).await;
    check_lost_connection_during_a_stall_aborts_at_once(&database).await;
    check_refusals_end_after_the_limit(&database).await;
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

async fn ran_once(database: &Database, id: i64, label: &str) {
    assert_eq!(starts(label).len(), 1, "{label} started more than once");
    assert_eq!(
        job_shape(database, id).await,
        (
            "succeeded".to_owned(),
            vec![("succeeded".to_owned(), 1, None)]
        ),
        "{label}"
    );
    assert_eq!(LEDGER.count(&id.to_string(), "finished"), 1, "{label}");
    assert_eq!(LEDGER.count(&id.to_string(), "succeeded"), 1, "{label}");
}

async fn check_blocked_runtime_keeps_the_lifeline(database: &Database) {
    const BLOCK: Duration = Duration::from_secs(12);
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let registered = database.lifelines().await;
    assert_eq!(watchdog_lines(), []);
    let labels = ["blocked_0", "blocked_1"];
    let mut ids = Vec::new();
    for label in labels {
        let job = Blocking {
            label: label.to_owned(),
            millis: u64::try_from(BLOCK.as_millis()).unwrap(),
        };
        ids.push(jalari::enqueue(&job).await.unwrap().id().0);
    }
    let enqueued = Instant::now();
    common::wait_until("the blocking jobs to succeed", BLOCK * 3, || async {
        ids.iter()
            .all(|id| LEDGER.count(&id.to_string(), "finished") == 1)
    })
    .await;
    let finished_after = enqueued.elapsed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started: Vec<Instant> = labels.iter().map(|label| starts(label)[0]).collect();
    let apart = started[1]
        .saturating_duration_since(started[0])
        .max(started[0].saturating_duration_since(started[1]));
    eprintln!(
        "blocked: both runtime threads blocked for {BLOCK:?}, the bodies started {apart:?} apart, \
         the jobs finished {finished_after:?} after they were enqueued"
    );
    assert!(apart < Duration::from_secs(1), "{apart:?}");
    for (id, label) in ids.iter().zip(labels) {
        ran_once(database, *id, label).await;
    }
    assert_eq!(database.lifelines().await, registered);
    assert!(
        finished_after < BLOCK + Duration::from_secs(3),
        "{finished_after:?}"
    );
    common::wait_until(
        "the watchdog to see the runtime again",
        Duration::from_secs(3),
        || async { watchdog_lines().len() == 2 },
    )
    .await;
    let lines = watchdog_lines();
    eprintln!("blocked: the watchdog logged {lines:?}");
    let [(Level::ERROR, stalled), (Level::INFO, recovered)] = lines.as_slice() else {
        panic!("expected one error and one info line from the watchdog, got {lines:?}");
    };
    assert!(seconds_after(stalled, STALLED) >= 10, "{stalled}");
    assert!(seconds_after(recovered, RECOVERED) >= 10, "{recovered}");
    assert_eq!(database.lifelines().await, registered);
    worker.stop().await;
}

async fn check_refused_heartbeats_keep_the_jobs(database: &Database) {
    const STALL: Duration = Duration::from_secs(15);
    const PROBE_AFTER: Duration = Duration::from_millis(3500);
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let registered = database.lifelines().await;
    let worker_id = registered[0].1.clone();
    let id = jalari::enqueue(&Ticker {
        label: "stall".to_owned(),
        millis: 18_000,
    })
    .await
    .unwrap()
    .id()
    .0;
    common::wait_until("the ticker to start", Duration::from_secs(10), || async {
        !starts("stall").is_empty()
    })
    .await;
    stall_heartbeats(database).await;
    tokio::time::sleep(PROBE_AFTER).await;
    let probe = jalari::enqueue(&Ticker {
        label: "stall_probe".to_owned(),
        millis: 0,
    })
    .await
    .unwrap()
    .id()
    .0;
    tokio::time::sleep(STALL - PROBE_AFTER).await;
    let heartbeat_age: Option<f64> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT EXTRACT(EPOCH FROM clock_timestamp() - heartbeat_at)::float8
         FROM {} WHERE id = $1::uuid",
        database.table("worker")
    )))
    .bind(&worker_id)
    .fetch_optional(&database.pool)
    .await
    .unwrap();
    end_stall(database).await;
    let released = Instant::now();
    common::wait_until(
        "the ticker and the probe to succeed",
        Duration::from_secs(20),
        || async {
            LEDGER.count(&id.to_string(), "finished") == 1
                && LEDGER.count(&probe.to_string(), "finished") == 1
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let probe_started = starts("stall_probe")[0];
    eprintln!(
        "stall: heartbeats refused for {STALL:?} (last one {heartbeat_age:?} s old at the end), \
         the probe enqueued during it started {:?} after it ended",
        probe_started.saturating_duration_since(released)
    );
    assert!(
        heartbeat_age.is_some_and(|age| age >= 14.0),
        "the worker row is gone or was beaten during the stall: {heartbeat_age:?}"
    );
    ran_once(database, id, "stall").await;
    ran_once(database, probe, "stall_probe").await;
    assert!(
        probe_started >= released,
        "the probe was claimed during the stall"
    );
    assert_eq!(database.lifelines().await, registered);
    worker.stop().await;
}

async fn check_lost_connection_during_a_stall_aborts_at_once(database: &Database) {
    const STALL: Duration = Duration::from_secs(8);
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let worker_id = database.lifelines().await.remove(0).1;
    let id = jalari::enqueue(&Attempts {
        label: "cut".to_owned(),
    })
    .await
    .unwrap()
    .id()
    .0;
    common::wait_until("the job to start", Duration::from_secs(10), || async {
        !starts("cut#1").is_empty()
    })
    .await;
    stall_heartbeats(database).await;
    tokio::time::sleep(STALL).await;
    common::wait_until(
        "a heartbeat that has just started waiting",
        Duration::from_secs(5),
        || async {
            sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE application_name = 'jalari lifeline ' || $1 AND state = 'active'
                         AND clock_timestamp() - query_start < interval '200 milliseconds'
                 )",
            )
            .bind(&worker_id)
            .fetch_one(&database.pool)
            .await
            .unwrap()
        },
    )
    .await;
    assert_eq!(database.terminate_lifelines(1).await, 1);
    let killed = Instant::now();
    end_stall(database).await;
    common::wait_until("the job to run again", Duration::from_secs(15), || async {
        !starts("cut#2").is_empty()
    })
    .await;
    let rerun = starts("cut#2")[0];
    let stopped = LEDGER
        .events("cut#1")
        .into_iter()
        .map(|(_, at)| at)
        .max()
        .unwrap();
    eprintln!(
        "stall_cut: the old run stopped {:?} and the job ran again {:?} after the lifeline was \
         killed during a stall",
        stopped.saturating_duration_since(killed),
        rerun.saturating_duration_since(killed)
    );
    assert!(
        stopped < rerun,
        "the old run kept going after the job ran again"
    );
    assert!(
        stopped.saturating_duration_since(killed) <= Duration::from_millis(300),
        "{:?}",
        stopped.saturating_duration_since(killed)
    );
    assert_ne!(database.lifelines().await[0].1, worker_id);
    assert_eq!(
        jalari::cancel(jalari::JobId(id)).await.unwrap(),
        jalari::CancelOutcome::Requested
    );
    worker.stop().await;
}

async fn check_refusals_end_after_the_limit(database: &Database) {
    const REFUSAL_LIMIT: Duration = Duration::from_secs(30);
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let worker_id = database.lifelines().await.remove(0).1;
    let id = jalari::enqueue(&Attempts {
        label: "refused".to_owned(),
    })
    .await
    .unwrap()
    .id()
    .0;
    common::wait_until("the job to start", Duration::from_secs(10), || async {
        !starts("refused#1").is_empty()
    })
    .await;
    stall_heartbeats(database).await;
    let stalled = Instant::now();
    let last_event = || {
        LEDGER
            .events("refused#1")
            .into_iter()
            .map(|(_, at)| at)
            .max()
            .unwrap()
    };
    common::wait_until(
        "the worker to give up on its refused heartbeats",
        REFUSAL_LIMIT + Duration::from_secs(5),
        || async { last_event().elapsed() > Duration::from_millis(500) },
    )
    .await;
    let gave_up_after = last_event().duration_since(stalled);
    end_stall(database).await;
    common::wait_until(
        "the job to run again under a new lifeline",
        Duration::from_secs(15),
        || async { !starts("refused#2").is_empty() },
    )
    .await;
    eprintln!(
        "refused: the worker gave up and its run stopped {gave_up_after:?} into a stall that \
         refused every heartbeat"
    );
    assert!(
        (REFUSAL_LIMIT - Duration::from_secs(2)..=REFUSAL_LIMIT + Duration::from_secs(3))
            .contains(&gave_up_after),
        "{gave_up_after:?}"
    );
    assert_ne!(database.lifelines().await[0].1, worker_id);
    assert_eq!(
        jalari::cancel(jalari::JobId(id)).await.unwrap(),
        jalari::CancelOutcome::Requested
    );
    worker.stop().await;
}

async fn stall_heartbeats(database: &Database) {
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".beat_stall (since timestamptz NOT NULL);
         CREATE OR REPLACE FUNCTION \"{schema}\".stall_beat() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             WHILE EXISTS (SELECT 1 FROM \"{schema}\".beat_stall) LOOP
                 PERFORM pg_sleep(0.05);
             END LOOP;
             RETURN NEW;
         END $$;
         DROP TRIGGER IF EXISTS stall_beat ON {worker};
         CREATE TRIGGER stall_beat BEFORE UPDATE ON {worker} FOR EACH ROW
         EXECUTE FUNCTION \"{schema}\".stall_beat();
         INSERT INTO \"{schema}\".beat_stall VALUES (now());",
        worker = database.table("worker"),
    )))
    .execute(&database.pool)
    .await
    .unwrap();
}

async fn end_stall(database: &Database) {
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DELETE FROM \"{}\".beat_stall",
        database.schema_name
    )))
    .execute(&database.pool)
    .await
    .unwrap();
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
