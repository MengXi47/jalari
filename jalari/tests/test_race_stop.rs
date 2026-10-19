mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, HistoryRow, JobRow, OCCUPANCY, Rng, RunningWorker};
use jalari::recurring::{self, Cron};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    CancelOutcome, EnqueueOptions, EnqueueOutcome, Job, JobError, JobResult, OnConflict,
    RestartOutcome, StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

const PANIC_MARKER: &str = "race stop planned panic";
const SHUTDOWN_NOTE: &str = "the worker is shutting down";
const INTERRUPTED: &str = "the worker stopped during the last attempt";
const STOP_TIMEOUT: Duration = Duration::from_millis(1500);
const RESOLVE_BOUND: Duration = Duration::from_millis(13_500);
const ABORT_TOLERANCE: Duration = Duration::from_millis(300);
const KEEP_SLACK: Duration = Duration::from_millis(1300);
const CONTROL_LATENCY: Duration = Duration::from_millis(2500);
const REPLACE_LATENCY: Duration = Duration::from_millis(3500);
const STORM_QUEUE: &str = "stop_race";
const COMMAND_QUEUE: &str = "stop_race_cmd";
const SNAPSHOT_QUEUE: &str = "stop_race_snap";
const DELAY_QUEUE: &str = "stop_race_delay";
const SINGLE_QUEUE: &str = "stop_race_single";
const CRON_QUEUE: &str = "stop_race_cron";
const IDLE_QUEUE: &str = "stop_race_idle";

type StuckRow = (i64, String, i32, Option<String>, Option<String>);
type LeftoverRow = (i64, String, Option<String>, bool, Option<String>);

static LOGGED: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static RUNS: LazyLock<Mutex<HashMap<i64, Vec<RunRecord>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static OPS: LazyLock<Mutex<Vec<Op>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static CHAOS: LazyLock<Mutex<Vec<(Instant, Instant)>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static TICKS: LazyLock<Mutex<Vec<Instant>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static RUN_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct LoggedFields {
    message: String,
    error: Option<String>,
}

impl Visit for LoggedFields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "error" => self.error = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

struct LogCapture {
    next_span: AtomicU64,
}

impl Subscriber for LogCapture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("jalari") && *metadata.level() <= Level::WARN
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = LoggedFields::default();
        event.record(&mut fields);
        LOGGED.lock().unwrap().push(match fields.error {
            Some(error) => format!("{}: {error}", fields.message),
            None => fields.message,
        });
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Mark {
    tag: String,
    n: u64,
}

#[derive(Debug, Clone)]
struct RunRecord {
    tag: String,
    attempt: i32,
    start: Instant,
    end: Option<Instant>,
    resumed: Option<Mark>,
    first_n: u64,
    last_n: Option<u64>,
    samples: Vec<(u64, Instant)>,
    halted: Option<u64>,
}

impl RunRecord {
    fn saved(&self, mark: &Mark) -> bool {
        mark.tag == self.tag
            && mark.n >= self.first_n
            && self.last_n.is_some_and(|last| mark.n <= last)
    }

    fn latest_save_of(&self, n: u64) -> Option<Instant> {
        self.samples
            .iter()
            .find(|(sampled, _)| *sampled >= n)
            .map(|(_, at)| *at)
            .or(self.end)
    }
}

#[derive(Debug, Clone)]
struct Op {
    job_id: i64,
    kind: &'static str,
    start: Instant,
    end: Instant,
    outcome: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Body {
    Saver { tight: bool, millis: u64 },
    Sleeper { millis: u64 },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum End {
    Ok,
    Err,
    Panic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Probe {
    body: Body,
    end: End,
    queue_key: Option<String>,
}

#[jalari::job(queue = "stop_race")]
impl Job for Probe {
    const NAME: &'static str = "stop_race_probe";
    const MAX_ATTEMPTS: i32 = 4;

    async fn run(self) -> JobResult {
        perform(self).await
    }
}

#[derive(Serialize, Deserialize)]
struct Single(Probe);

#[jalari::job(queue = "stop_race_single")]
impl Job for Single {
    const NAME: &'static str = "stop_race_single";
    const MAX_ATTEMPTS: i32 = 1;

    async fn run(self) -> JobResult {
        perform(self.0).await
    }
}

#[derive(Serialize, Deserialize)]
struct Tick;

#[jalari::job(queue = "stop_race_cron")]
impl Job for Tick {
    const NAME: &'static str = "stop_race_tick";
    const MAX_ATTEMPTS: i32 = 3;

    async fn run(self) -> JobResult {
        TICKS.lock().unwrap().push(Instant::now());
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(1500) {
            jalari::checkpoint().await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }
}

struct RunGuard {
    job_id: i64,
    index: usize,
    last: AtomicU64,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        let last = self.last.load(Ordering::SeqCst);
        with_run(self.job_id, self.index, |run| {
            run.end = Some(Instant::now());
            run.last_n = (last > 0).then_some(last);
        });
    }
}

fn with_run(job_id: i64, index: usize, update: impl FnOnce(&mut RunRecord)) {
    let mut runs = RUNS.lock().unwrap();
    if let Some(run) = runs.get_mut(&job_id).and_then(|runs| runs.get_mut(index)) {
        update(run);
    }
}

fn runs_of(job_id: i64) -> Vec<RunRecord> {
    RUNS.lock()
        .unwrap()
        .get(&job_id)
        .cloned()
        .unwrap_or_default()
}

fn ops_of(job_id: i64) -> Vec<Op> {
    OPS.lock()
        .unwrap()
        .iter()
        .filter(|op| op.job_id == job_id)
        .cloned()
        .collect()
}

fn chaos(start: Instant, end: Instant) {
    CHAOS.lock().unwrap().push((start, end));
}

async fn perform(probe: Probe) -> JobResult {
    let job = jalari::current_job().expect("a probe runs inside a job");
    let job_id = job.id().0;
    let attempt = job.attempt();
    let (saved, checkpoint) = jalari::resume::<Mark>()?;
    let tag = format!(
        "{job_id}#{attempt}#{}",
        RUN_SEQ.fetch_add(1, Ordering::SeqCst)
    );
    let first_n = saved.as_ref().map_or(0, |mark| mark.n) + 1;
    let index = {
        let mut runs = RUNS.lock().unwrap();
        let runs = runs.entry(job_id).or_default();
        runs.push(RunRecord {
            tag: tag.clone(),
            attempt,
            start: Instant::now(),
            end: None,
            resumed: saved,
            first_n,
            last_n: None,
            samples: Vec::new(),
            halted: None,
        });
        runs.len() - 1
    };
    let mut keys = vec![format!("job:{job_id}")];
    if let Some(queue_key) = &probe.queue_key {
        keys.push(format!("queue_key:{queue_key}"));
    }
    let _seat = OCCUPANCY.enter(keys, tag.clone());
    let guard = RunGuard {
        job_id,
        index,
        last: AtomicU64::new(0),
    };
    match probe.body {
        Body::Saver { tight, millis } => {
            let started = Instant::now();
            let length = Duration::from_millis(millis);
            let mut n = first_n - 1;
            let mut sampled: Option<Instant> = None;
            while started.elapsed() < length {
                n += 1;
                guard.last.store(n, Ordering::SeqCst);
                let now = Instant::now();
                if sampled.is_none_or(|at| now - at >= Duration::from_millis(1)) {
                    with_run(job_id, index, |run| run.samples.push((n, now)));
                    sampled = Some(now);
                }
                if let Err(e) = checkpoint
                    .save(&Mark {
                        tag: tag.clone(),
                        n,
                    })
                    .await
                {
                    with_run(job_id, index, |run| run.halted = Some(n));
                    return Err(e);
                }
                if !tight {
                    tokio::time::sleep(Duration::from_millis(5 + n % 15)).await;
                }
            }
        }
        Body::Sleeper { millis } => tokio::time::sleep(Duration::from_millis(millis)).await,
    }
    match probe.end {
        End::Ok => Ok(()),
        End::Err => Err(JobError::new("planned failure".to_owned())),
        End::Panic => panic!("{PANIC_MARKER}"),
    }
}

async fn record<T, F>(job_id: i64, kind: &'static str, call: F) -> Option<T>
where
    T: fmt::Debug,
    F: Future<Output = jalari::Result<T>>,
{
    let start = Instant::now();
    let result = call.await;
    let end = Instant::now();
    let outcome = match &result {
        Ok(value) => format!("{value:?}"),
        Err(e) => format!("error: {e}"),
    };
    OPS.lock().unwrap().push(Op {
        job_id,
        kind,
        start,
        end,
        outcome,
    });
    result.ok()
}

async fn stop(job_id: i64) -> Option<StopOutcome> {
    record(job_id, "stop", jalari::stop(jalari::JobId(job_id))).await
}

async fn restart(job_id: i64) -> Option<RestartOutcome> {
    record(job_id, "restart", jalari::restart(jalari::JobId(job_id))).await
}

async fn cancel(job_id: i64) -> Option<CancelOutcome> {
    record(job_id, "cancel", jalari::cancel(jalari::JobId(job_id))).await
}

async fn enqueue_probe(
    kind: &'static str,
    probe: &Probe,
    options: EnqueueOptions,
) -> Option<EnqueueOutcome> {
    let start = Instant::now();
    let result = jalari::enqueue_with(probe, options).await;
    let end = Instant::now();
    let (job_id, outcome) = match &result {
        Ok(outcome) => (
            outcome.id().0,
            match outcome {
                EnqueueOutcome::Inserted(_) => "Inserted",
                EnqueueOutcome::Kept(_) => "Kept",
                EnqueueOutcome::Replaced(_) => "Replaced",
                EnqueueOutcome::Running(_) => "Running",
            }
            .to_owned(),
        ),
        Err(e) => (0, format!("error: {e}")),
    };
    OPS.lock().unwrap().push(Op {
        job_id,
        kind,
        start,
        end,
        outcome,
    });
    result.ok()
}

async fn spawn_worker(
    queues: &[(&str, usize)],
    stop_timeout: Duration,
    shutdown_timeout: Duration,
    scheduler: bool,
) -> RunningWorker {
    let mut config = common::worker_config(Duration::from_millis(50));
    config.stop_timeout = stop_timeout;
    config.shutdown_timeout = shutdown_timeout;
    let mut builder = Worker::builder()
        .config(config)
        .retry_policy(common::fast_retry())
        .scheduler(scheduler);
    for (name, concurrency) in queues {
        builder = builder.queue(name, *concurrency);
    }
    RunningWorker::spawn(builder.build().await.unwrap())
}

async fn stop_all(workers: Vec<RunningWorker>) {
    for worker in &workers {
        worker.request_stop();
    }
    for worker in workers {
        worker.stop().await;
    }
}

async fn job_state(database: &Database, job_id: i64) -> Option<String> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT state FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(job_id)
    .fetch_optional(&database.pool)
    .await
    .unwrap()
}

async fn stored_progress(database: &Database, job_id: i64) -> Option<Mark> {
    let progress: Option<Option<serde_json::Value>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT progress FROM {} WHERE job_id = $1",
        database.table("job_control")
    )))
    .bind(job_id)
    .fetch_optional(&database.pool)
    .await
    .unwrap();
    progress
        .flatten()
        .map(|value| serde_json::from_value(value).unwrap())
}

async fn holder(database: &Database, job_key: &str) -> Option<(i64, String)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id, state FROM {} WHERE job_key = $1
             AND state IN ('scheduled', 'enqueued', 'stopped')",
        database.table("job")
    )))
    .bind(job_key)
    .fetch_optional(&database.pool)
    .await
    .unwrap()
}

async fn wait_for(
    description: &str,
    timeout: Duration,
    mut condition: impl FnMut() -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    while !condition() {
        if Instant::now() >= deadline {
            eprintln!("gave up waiting for {description} after {timeout:?}");
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

async fn stop_until_stopped(job_id: i64, timeout: Duration) -> Option<StopOutcome> {
    let deadline = Instant::now() + timeout;
    loop {
        let outcome = stop(job_id).await;
        if !matches!(outcome, Some(StopOutcome::Requested) | None) || Instant::now() >= deadline {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn cancel_until_settled(job_id: i64, timeout: Duration) -> Option<CancelOutcome> {
    let deadline = Instant::now() + timeout;
    loop {
        let outcome = cancel(job_id).await;
        if !matches!(outcome, Some(CancelOutcome::Requested) | None) || Instant::now() >= deadline {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn selected(name: &str) -> bool {
    std::env::var("RACE_ONLY").map_or(true, |only| only.split(',').any(|item| item == name))
}

async fn timed(name: &str, check: impl Future<Output = Vec<String>>) -> Vec<String> {
    let started = Instant::now();
    let failures = check.await;
    eprintln!(
        "{name}: {} violations in {:.1?}",
        failures.len(),
        started.elapsed()
    );
    failures
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn test_race_stop() {
    let Some(database) = Database::start("stop", 60).await else {
        return;
    };
    tracing::subscriber::set_global_default(LogCapture {
        next_span: AtomicU64::new(1),
    })
    .unwrap();
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if !info
            .payload_as_str()
            .is_some_and(|message| message.starts_with(PANIC_MARKER))
        {
            default_hook(info);
        }
    }));
    let seed = common::seed("test_race_stop");
    let deadlocks_before = database.deadlocks().await;

    let mut failures = Vec::new();
    if selected("claim_snapshot") {
        failures.extend(timed("claim_snapshot", check_claim_snapshot(&database, seed)).await);
    }
    if selected("replace_delay") {
        failures.extend(timed("replace_delay", check_replace_delay(&database)).await);
    }
    if selected("replace_pending_cancel") {
        failures.extend(
            timed(
                "replace_pending_cancel",
                check_replace_pending_cancel(&database),
            )
            .await,
        );
    }
    if selected("shutdown_release") {
        failures.extend(timed("shutdown_release", check_shutdown_release(&database)).await);
    }
    if selected("recurring") {
        failures.extend(timed("recurring", check_recurring(&database)).await);
    }
    if selected("command_storm") {
        failures.extend(timed("command_storm", check_command_storm(&database, seed)).await);
    }
    if selected("storm") {
        failures.extend(timed("storm", check_storm(&database, seed)).await);
    }

    let logged = LOGGED.lock().unwrap().clone();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for line in &logged {
        let key: String = line.chars().take(110).collect();
        *counts.entry(key).or_default() += 1;
    }
    eprintln!("library warnings: {counts:#?}");
    for line in logged.iter().filter(|line| line.contains("deadlock")) {
        failures.push(format!("a worker hit a deadlock: {line}"));
    }
    eprintln!(
        "pg_stat_database deadlocks grew by {} during the run (shared database, other tests may add)",
        database.deadlocks().await - deadlocks_before
    );

    database.finish().await;
    assert!(
        failures.is_empty(),
        "{} violations (seed {seed}):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

async fn check_claim_snapshot(database: &Database, seed: u64) -> Vec<String> {
    let label = "claim_snapshot";
    let mut rng = Rng::new(seed).fork(11);
    let mut failures = Vec::new();
    let blocker_payload = serde_json::to_vec(&Probe {
        body: Body::Sleeper { millis: 1 },
        end: End::Ok,
        queue_key: None,
    })
    .unwrap();
    let blockers = 1_000_000 * i64::try_from(common::scale()).unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (queue, task, payload, state, max_attempts, run_at)
         SELECT $1, $2, $3, 'enqueued', 4, timestamptz '2000-01-01' + make_interval(secs => i)
         FROM generate_series(1, $4) AS i",
        database.table("job")
    )))
    .bind(SNAPSHOT_QUEUE)
    .bind(Probe::NAME)
    .bind(&blocker_payload)
    .bind(blockers)
    .execute(&database.pool)
    .await
    .unwrap();
    let blocker_pool = database.connect("blocker", 1).await;
    let mut blocker = blocker_pool.begin().await.unwrap();
    let locked: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM (SELECT id FROM {} WHERE queue = $1 AND run_at < '2001-01-01'
             FOR UPDATE) AS held",
        database.table("job")
    )))
    .bind(SNAPSHOT_QUEUE)
    .fetch_one(&mut *blocker)
    .await
    .unwrap();
    eprintln!("{label}: {locked} rows ahead of the job are locked, so every claim walks past them");

    let long_saver = Probe {
        body: Body::Saver {
            tight: false,
            millis: 600_000,
        },
        end: End::Ok,
        queue_key: None,
    };
    let job_key = "stop_race_snapshot";
    let worker = spawn_worker(
        &[(SNAPSHOT_QUEUE, 4)],
        STOP_TIMEOUT,
        Duration::from_secs(5),
        false,
    )
    .await;
    let id = enqueue_probe(
        "keep",
        &long_saver,
        EnqueueOptions::new().queue(SNAPSHOT_QUEUE).job_key(job_key),
    )
    .await
    .unwrap()
    .id()
    .0;

    let rounds = 10 * common::scale();
    let mut stale_rounds = 0;
    for round in 0..rounds {
        if !wait_for(
            &format!("job {id} to save progress"),
            Duration::from_secs(30),
            || {
                runs_of(id).last().is_some_and(|run| {
                    run.end.is_none()
                        && run
                            .samples
                            .last()
                            .is_some_and(|(n, _)| *n >= run.first_n + 2)
                })
            },
        )
        .await
        {
            failures.push(format!(
                "{label} round {round}: job {id} never ran and saved progress within 30 s"
            ));
            break;
        }
        if stop_until_stopped(id, Duration::from_secs(15)).await != Some(StopOutcome::Stopped) {
            failures.push(format!(
                "{label} round {round}: stop never reported Stopped within 15 s"
            ));
            break;
        }
        let stopped_run = runs_of(id).last().cloned().unwrap();
        let kept = stored_progress(database, id).await;
        if kept.is_none() || kept.as_ref().map(|mark| mark.n) != stopped_run.halted {
            failures.push(format!(
                "{label} round {round}: the stopped job kept {kept:?}, but its run halted at {:?}",
                stopped_run.halted
            ));
        }
        if restart(id).await != Some(RestartOutcome::Restarted) {
            failures.push(format!(
                "{label} round {round}: restart did not report Restarted"
            ));
            break;
        }
        let restarted = Instant::now();
        let replace_start = Instant::now();
        let mut transaction = database.pool.begin().await.unwrap();
        let replaced = jalari::enqueue_in(
            &mut transaction,
            &long_saver,
            EnqueueOptions::new()
                .queue(SNAPSHOT_QUEUE)
                .job_key(job_key)
                .on_conflict(OnConflict::Replace),
        )
        .await
        .ok();
        let pause = rng.millis(300, 1800);
        tokio::time::sleep(pause).await;
        transaction.commit().await.unwrap();
        let replace_end = Instant::now();
        OPS.lock().unwrap().push(Op {
            job_id: id,
            kind: "replace",
            start: replace_start,
            end: replace_end,
            outcome: format!("{replaced:?}"),
        });
        if !wait_for(
            &format!("job {id} to start after the restart"),
            Duration::from_secs(30),
            || runs_of(id).iter().any(|run| run.start > restarted),
        )
        .await
        {
            failures.push(format!(
                "{label} round {round}: job {id} did not run within 30 s of the restart"
            ));
            break;
        }
        let next = runs_of(id)
            .into_iter()
            .find(|run| run.start > restarted)
            .unwrap();
        let claim_wait = next.start - restarted;
        match replaced {
            Some(EnqueueOutcome::Replaced(_)) if next.start < replace_start => {
                failures.push(format!(
                    "{label} round {round}: Replace returned Replaced although run {} had started \
                     {:?} before it",
                    next.tag,
                    replace_start - next.start
                ));
            }
            Some(EnqueueOutcome::Replaced(_)) if next.start < replace_end => {
                eprintln!(
                    "{label} round {round}: the run started while Replace was in flight, skipped"
                );
            }
            Some(EnqueueOutcome::Replaced(_)) if next.resumed.is_some() => {
                stale_rounds += 1;
                failures.push(format!(
                    "{label} round {round} (Replace held {pause:?} in an open transaction, run \
                     started {claim_wait:?} after restart): Replace returned Replaced, yet the \
                     next run {} resumed {:?}, the progress the run {} saved before the Replace",
                    next.tag, next.resumed, stopped_run.tag
                ));
                tokio::time::sleep(Duration::from_millis(6500)).await;
                let flushed = stored_progress(database, id).await;
                let saving = runs_of(id)
                    .into_iter()
                    .find(|run| run.tag == next.tag)
                    .unwrap();
                if flushed.as_ref().is_none_or(|mark| mark.tag != saving.tag) {
                    failures.push(format!(
                        "{label} round {round}: 6.5 s into run {} (saved up to {:?}) the stored \
                         progress is still {flushed:?}: its flushes were fenced off by a stale lease",
                        saving.tag,
                        saving.samples.last().map(|(n, _)| *n)
                    ));
                }
            }
            Some(EnqueueOutcome::Replaced(_)) => {
                eprintln!(
                    "{label} round {round}: Replace committed after {pause:?}, clean (run started \
                     {claim_wait:?} after restart)"
                );
            }
            other => eprintln!(
                "{label} round {round}: Replace held {pause:?} returned {other:?} \
                 (run started {claim_wait:?} after restart)"
            ),
        }
    }
    eprintln!("{label}: {stale_rounds} of {rounds} rounds resumed pre-Replace progress");

    stop_until_stopped(id, Duration::from_secs(15)).await;
    cancel_until_settled(id, Duration::from_secs(15)).await;
    worker.stop().await;
    blocker.rollback().await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {} WHERE queue = $1 AND run_at < '2001-01-01'",
        database.table("job")
    )))
    .bind(SNAPSHOT_QUEUE)
    .execute(&database.pool)
    .await
    .unwrap();
    blocker_pool.close().await;
    failures
}

async fn check_replace_delay(database: &Database) -> Vec<String> {
    let label = "replace_delay";
    let mut failures = Vec::new();
    let worker = spawn_worker(
        &[(DELAY_QUEUE, 1)],
        STOP_TIMEOUT,
        Duration::from_secs(5),
        false,
    )
    .await;
    let job_key = "stop_race_delay";
    let sleeper = Probe {
        body: Body::Sleeper { millis: 60_000 },
        end: End::Ok,
        queue_key: None,
    };
    let id = enqueue_probe(
        "keep",
        &sleeper,
        EnqueueOptions::new().queue(DELAY_QUEUE).job_key(job_key),
    )
    .await
    .unwrap()
    .id()
    .0;
    wait_for(
        &format!("job {id} to start"),
        Duration::from_secs(20),
        || !runs_of(id).is_empty(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while database
        .terminate_job_sessions(1, Duration::from_millis(200))
        .await
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "{label}: no running job session to kill"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let killed = Instant::now();
    chaos(killed, killed);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut replaced = None;
    for _ in 0..20 {
        replaced = enqueue_probe(
            "replace",
            &Probe {
                body: Body::Sleeper { millis: 100 },
                end: End::Ok,
                queue_key: None,
            },
            EnqueueOptions::new()
                .queue(DELAY_QUEUE)
                .job_key(job_key)
                .delay(Duration::from_secs(3600))
                .on_conflict(OnConflict::Replace),
        )
        .await;
        if matches!(replaced, Some(EnqueueOutcome::Replaced(_))) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !matches!(replaced, Some(EnqueueOutcome::Replaced(_))) {
        failures.push(format!(
            "{label}: Replace of the crashed job never reported Replaced (last {replaced:?})"
        ));
        worker.stop().await;
        return failures;
    }
    let row: (String, Option<String>, f64) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, last_error, EXTRACT(EPOCH FROM run_at - now())::float8 FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    eprintln!(
        "{label}: after Replace the job is {row:?} (state, last_error, seconds until run_at)"
    );
    let bound = STOP_TIMEOUT + Duration::from_secs(9);
    let asked = Instant::now();
    let mut outcome = stop(id).await;
    while outcome == Some(StopOutcome::Requested) && asked.elapsed() < bound {
        tokio::time::sleep(Duration::from_millis(250)).await;
        outcome = stop(id).await;
    }
    if outcome == Some(StopOutcome::Requested) {
        failures.push(format!(
            "{label}: stop on a waiting job (scheduled 1 h ahead by Replace after its attempt \
             crashed, last_error = {:?}) still reported Requested {:?} after the first call, \
             {:?} after the crash; it stays Requested until run_at",
            row.1,
            asked.elapsed(),
            killed.elapsed()
        ));
    }
    let cancelled = cancel(id).await;
    if cancelled == Some(CancelOutcome::Requested) {
        failures.push(format!(
            "{label}: cancel on the same waiting job also reported Requested"
        ));
    }
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET run_at = now() WHERE id = $1 AND state IN ('scheduled', 'enqueued')",
        database.table("job")
    )))
    .bind(id)
    .execute(&database.pool)
    .await
    .unwrap();
    cancel_until_settled(id, Duration::from_secs(15)).await;
    worker.stop().await;
    failures
}

async fn crash_running(database: &Database, label: &str, id: i64) -> Instant {
    wait_for(
        &format!("job {id} to start"),
        Duration::from_secs(20),
        || !runs_of(id).is_empty(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while database
        .terminate_job_sessions(1, Duration::from_millis(200))
        .await
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "{label}: no running job session to kill"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let killed = Instant::now();
    chaos(killed, killed);
    killed
}

async fn check_replace_pending_cancel(database: &Database) -> Vec<String> {
    let label = "replace_pending_cancel";
    let mut failures = Vec::new();
    let worker = spawn_worker(
        &[(DELAY_QUEUE, 1)],
        STOP_TIMEOUT,
        Duration::from_secs(5),
        false,
    )
    .await;
    let job_key = "stop_race_pending_cancel";
    let id = enqueue_probe(
        "keep",
        &Probe {
            body: Body::Sleeper { millis: 60_000 },
            end: End::Ok,
            queue_key: None,
        },
        EnqueueOptions::new().queue(DELAY_QUEUE).job_key(job_key),
    )
    .await
    .unwrap()
    .id()
    .0;
    crash_running(database, label, id).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut cancelled = cancel(id).await;
    for _ in 0..20 {
        if cancelled == Some(CancelOutcome::Requested) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancelled = cancel(id).await;
    }
    if cancelled != Some(CancelOutcome::Requested) {
        failures.push(format!(
            "{label}: cancel on job {id} right after its attempt crashed returned {cancelled:?}, \
             expected Requested"
        ));
    }
    let mut replaced = None;
    for _ in 0..20 {
        replaced = enqueue_probe(
            "replace",
            &Probe {
                body: Body::Sleeper { millis: 100 },
                end: End::Ok,
                queue_key: None,
            },
            EnqueueOptions::new()
                .queue(DELAY_QUEUE)
                .job_key(job_key)
                .on_conflict(OnConflict::Replace),
        )
        .await;
        if replaced.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let runs_before = runs_of(id).len();
    tokio::time::sleep(Duration::from_secs(9)).await;
    let state = job_state(database, id).await;
    let more_runs = runs_of(id).len() - runs_before;
    eprintln!(
        "{label}: cancel {cancelled:?}, then Replace {replaced:?}; the job ended {state:?} after \
         {more_runs} more runs"
    );
    if replaced != Some(EnqueueOutcome::Kept(jalari::JobId(id))) {
        failures.push(format!(
            "{label}: Replace on job {id} with a pending cancel returned {replaced:?}; a pending \
             command beats Replace, so it must return Kept"
        ));
    }
    if more_runs != 0 || state.as_deref() != Some("deleted") {
        failures.push(format!(
            "{label}: after Replace kept job {id}, it ran {more_runs} more times and ended \
             {state:?}; the pending cancel must end it deleted without another run"
        ));
    }
    cancel_until_settled(id, Duration::from_secs(15)).await;
    worker.stop().await;
    failures
}

async fn check_shutdown_release(database: &Database) -> Vec<String> {
    let label = "shutdown_release";
    let mut failures = Vec::new();
    let worker = spawn_worker(
        &[(SINGLE_QUEUE, 3)],
        STOP_TIMEOUT,
        Duration::from_secs(10),
        false,
    )
    .await;
    let mut ids = Vec::new();
    for tight in [false, false, true] {
        let outcome = jalari::enqueue_with(
            &Single(Probe {
                body: Body::Saver {
                    tight,
                    millis: 600_000,
                },
                end: End::Ok,
                queue_key: None,
            }),
            EnqueueOptions::new().queue(SINGLE_QUEUE),
        )
        .await
        .unwrap();
        ids.push(outcome.id().0);
    }
    for &id in &ids {
        wait_for(
            &format!("job {id} to save"),
            Duration::from_secs(20),
            || {
                runs_of(id)
                    .last()
                    .is_some_and(|run| run.samples.last().is_some_and(|(n, _)| *n >= 3))
            },
        )
        .await;
    }
    let shutdown = Instant::now();
    worker.stop().await;
    chaos(shutdown, Instant::now());
    for &id in &ids {
        let run = runs_of(id).last().cloned().unwrap();
        let (state, attempts): (String, i32) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state, attempts FROM {} WHERE id = $1",
            database.table("job")
        )))
        .bind(id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        if state != "enqueued" || attempts != 0 {
            failures.push(format!(
                "{label}: job {id} (MAX_ATTEMPTS 1) is {state} with {attempts} attempts after a \
                 graceful shutdown released it; expected enqueued with 0"
            ));
        }
        let kept = stored_progress(database, id).await;
        let expected = run.halted.map(|n| Mark {
            tag: run.tag.clone(),
            n,
        });
        if run.halted.is_none() || kept != expected {
            failures.push(format!(
                "{label}: job {id} kept {kept:?} after shutdown, but its run halted at {:?}",
                run.halted
            ));
        }
    }
    let rescuer = spawn_worker(
        &[(SINGLE_QUEUE, 3)],
        STOP_TIMEOUT,
        Duration::from_secs(10),
        false,
    )
    .await;
    for &id in &ids {
        if !wait_for(
            &format!("job {id} to run again"),
            Duration::from_secs(20),
            || runs_of(id).len() >= 2,
        )
        .await
        {
            failures.push(format!(
                "{label}: job {id} did not run again after the shutdown"
            ));
            continue;
        }
        let runs = runs_of(id);
        let (first, second) = (&runs[0], &runs[1]);
        let expected = first.halted.map(|n| Mark {
            tag: first.tag.clone(),
            n,
        });
        if second.attempt != 1 || second.resumed != expected {
            failures.push(format!(
                "{label}: job {id} ran again as attempt {} resuming {:?}; expected attempt 1 \
                 resuming {expected:?}",
                second.attempt, second.resumed
            ));
        }
    }
    for &id in &ids {
        stop_until_stopped(id, Duration::from_secs(15)).await;
        cancel_until_settled(id, Duration::from_secs(15)).await;
    }
    rescuer.stop().await;
    failures
}

async fn check_recurring(database: &Database) -> Vec<String> {
    let label = "recurring";
    let mut failures = Vec::new();
    let name = "stop_race_tick";
    let job_key = format!("recurring:{name}");
    let worker = spawn_worker(
        &[(CRON_QUEUE, 2)],
        STOP_TIMEOUT,
        Duration::from_secs(5),
        true,
    )
    .await;
    recurring::add_or_update(name, Cron::new("* * * * * *").unwrap(), &Tick)
        .await
        .unwrap();
    if !wait_for("two ticks", Duration::from_secs(20), || {
        TICKS.lock().unwrap().len() >= 2
    })
    .await
    {
        failures.push(format!("{label}: the recurring job never ran twice"));
    }
    let Some((id, _)) = holder(database, &job_key).await else {
        failures.push(format!("{label}: no job holds {job_key}"));
        return failures;
    };
    let outcome = stop_until_stopped(id, Duration::from_secs(15)).await;
    let stopped = Instant::now();
    if outcome != Some(StopOutcome::Stopped) {
        failures.push(format!(
            "{label}: stop on the recurring job ended with {outcome:?}"
        ));
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    let ticks_while_stopped = TICKS
        .lock()
        .unwrap()
        .iter()
        .filter(|at| **at > stopped)
        .count();
    let held: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE job_key = $1 AND state IN ('scheduled', 'enqueued', 'stopped')",
        database.table("job")
    )))
    .bind(&job_key)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    if ticks_while_stopped > 0 || held != 1 {
        failures.push(format!(
            "{label}: while its job was stopped the schedule ran {ticks_while_stopped} times and \
             {held} rows held {job_key}; expected 0 runs and 1 row"
        ));
    }
    let cancelled = cancel(id).await;
    let after_cancel = Instant::now();
    if cancelled != Some(CancelOutcome::Cancelled) {
        failures.push(format!(
            "{label}: cancel on the stopped recurring job returned {cancelled:?}"
        ));
    }
    if !wait_for("the schedule to resume", Duration::from_secs(10), || {
        TICKS.lock().unwrap().iter().any(|at| *at > after_cancel)
    })
    .await
    {
        failures.push(format!("{label}: the schedule did not resume after cancel"));
    }
    recurring::remove(name).await.unwrap();
    worker.stop().await;
    if let Some((id, _)) = holder(database, &job_key).await {
        stop_until_stopped(id, Duration::from_secs(10)).await;
        cancel_until_settled(id, Duration::from_secs(10)).await;
    }
    failures
}

fn random_probe(rng: &mut Rng, queue_key: Option<String>) -> (Probe, Option<Duration>) {
    let roll = rng.below(100);
    let (body, timeout) = if roll < 40 {
        (
            Body::Saver {
                tight: false,
                millis: 200 + rng.below(2800),
            },
            None,
        )
    } else if roll < 55 {
        (
            Body::Saver {
                tight: true,
                millis: 50 + rng.below(550),
            },
            None,
        )
    } else if roll < 85 {
        (
            Body::Sleeper {
                millis: 100 + rng.below(2900),
            },
            None,
        )
    } else {
        (
            Body::Sleeper { millis: 5000 },
            Some(Duration::from_millis(400)),
        )
    };
    let end = match rng.below(100) {
        0..60 => End::Ok,
        60..85 => End::Err,
        _ => End::Panic,
    };
    (
        Probe {
            body,
            end,
            queue_key,
        },
        timeout,
    )
}

fn probe_options(queue: &str, probe: &Probe, timeout: Option<Duration>) -> EnqueueOptions {
    let mut options = EnqueueOptions::new().queue(queue);
    if let Some(queue_key) = &probe.queue_key {
        options = options.queue_key(queue_key);
    }
    if let Some(timeout) = timeout {
        options = options.timeout(timeout);
    }
    options
}

struct Storm {
    ids: Mutex<Vec<i64>>,
    violations: Mutex<Vec<String>>,
    workers: tokio::sync::Mutex<Vec<RunningWorker>>,
}

impl Storm {
    fn pick(&self, rng: &mut Rng) -> Option<i64> {
        let ids = self.ids.lock().unwrap();
        (!ids.is_empty()).then(|| *rng.pick(&ids))
    }

    fn violation(&self, message: String) {
        self.violations.lock().unwrap().push(message);
    }
}

async fn storm_worker() -> RunningWorker {
    spawn_worker(
        &[(STORM_QUEUE, 3)],
        STOP_TIMEOUT,
        Duration::from_secs(2),
        false,
    )
    .await
}

async fn keyed_enqueue(storm: &Storm, database: &Database, rng: &mut Rng, job_key: &str) {
    let (probe, timeout) = random_probe(rng, None);
    let replace = rng.chance(60);
    let options = probe_options(STORM_QUEUE, &probe, timeout)
        .job_key(job_key)
        .on_conflict(if replace {
            OnConflict::Replace
        } else {
            OnConflict::KeepExisting
        });
    let before = holder(database, job_key).await;
    let outcome = enqueue_probe(if replace { "replace" } else { "keep" }, &probe, options).await;
    let after = holder(database, job_key).await;
    let Some(outcome) = outcome else {
        return;
    };
    if let EnqueueOutcome::Inserted(id) = outcome {
        storm.ids.lock().unwrap().push(id.0);
    }
    if let (Some((before_id, before_state)), Some((after_id, after_state))) = (&before, &after)
        && before_id == after_id
        && before_state == "stopped"
        && after_state == "stopped"
        && !matches!(outcome, EnqueueOutcome::Kept(id) if id.0 == *before_id)
    {
        storm.violation(format!(
            "storm: {job_key} was held by stopped job {before_id} before and after an enqueue \
             with {}, which returned {outcome:?} instead of Kept",
            if replace { "Replace" } else { "KeepExisting" }
        ));
    }
}

async fn drive(
    storm: Arc<Storm>,
    database: Arc<Database>,
    mut rng: Rng,
    until: Instant,
    keys: Vec<String>,
) {
    let job_keys = ["stop_race_a", "stop_race_b", "stop_race_c", "stop_race_d"];
    while Instant::now() < until {
        tokio::time::sleep(rng.millis(5, 120)).await;
        let roll = rng.below(100);
        if roll < 12 {
            if storm.ids.lock().unwrap().len() >= 90 {
                continue;
            }
            let queue_key = rng.chance(40).then(|| rng.pick(&keys).clone());
            let (probe, timeout) = random_probe(&mut rng, queue_key);
            if let Some(outcome) = enqueue_probe(
                "enqueue",
                &probe,
                probe_options(STORM_QUEUE, &probe, timeout),
            )
            .await
            {
                storm.ids.lock().unwrap().push(outcome.id().0);
            }
        } else if roll < 22 {
            let job_key = *rng.pick(&job_keys);
            keyed_enqueue(&storm, &database, &mut rng, job_key).await;
        } else if roll < 52 {
            if let Some(id) = storm.pick(&mut rng) {
                stop(id).await;
            }
        } else if roll < 76 {
            if let Some(id) = storm.pick(&mut rng) {
                restart(id).await;
            }
        } else if roll < 82 {
            if let Some(id) = storm.pick(&mut rng) {
                cancel(id).await;
            }
        } else if let Some(id) = storm.pick(&mut rng) {
            stop(id).await;
            tokio::time::sleep(rng.millis(0, 60)).await;
            restart(id).await;
        }
    }
}

async fn unleash(
    storm: Arc<Storm>,
    database: Arc<Database>,
    mut rng: Rng,
    until: Instant,
) -> (usize, usize, usize) {
    let (mut kills, mut graceful, mut crashes) = (0, 0, 0);
    while Instant::now() < until {
        tokio::time::sleep(rng.millis(300, 1500)).await;
        let roll = rng.below(100);
        if roll < 55 {
            let at = Instant::now();
            let killed = database
                .terminate_job_sessions(
                    usize::try_from(1 + rng.below(2)).unwrap(),
                    Duration::from_millis(300),
                )
                .await;
            if killed > 0 {
                kills += killed;
                chaos(at, Instant::now());
            }
        } else if roll < 70 {
            let at = Instant::now();
            let mut workers = storm.workers.lock().await;
            let index = usize::try_from(rng.below(workers.len() as u64)).unwrap();
            let old = workers.swap_remove(index);
            workers.push(storm_worker().await);
            drop(workers);
            old.stop().await;
            graceful += 1;
            chaos(at, Instant::now());
        } else if roll < 80 {
            let at = Instant::now();
            let mut workers = storm.workers.lock().await;
            let index = usize::try_from(rng.below(workers.len() as u64)).unwrap();
            let old = workers.swap_remove(index);
            old.crash().await;
            workers.push(storm_worker().await);
            crashes += 1;
            chaos(at, Instant::now() + Duration::from_secs(1));
        }
    }
    (kills, graceful, crashes)
}

async fn watch_commands(
    database: Arc<Database>,
    queues: Vec<String>,
    done: jalari::CancellationToken,
) -> (Vec<String>, Duration) {
    let mut first_seen: HashMap<i64, Instant> = HashMap::new();
    let mut reported: HashSet<i64> = HashSet::new();
    let mut violations = Vec::new();
    let mut longest = Duration::ZERO;
    while !done.is_cancelled() {
        let pending: Vec<(i64, String, Option<String>, i32, f64, String)> =
            match sqlx::query_as(AssertSqlSafe(format!(
                "SELECT job.id, job.state, job.last_error, job.attempts,
                     EXTRACT(EPOCH FROM job.run_at - now())::float8, control.command
                 FROM {job} AS job JOIN {control} AS control ON control.job_id = job.id
                 WHERE job.state IN ('scheduled', 'enqueued') AND control.command IS NOT NULL
                     AND job.queue = ANY($1)",
                job = database.table("job"),
                control = database.table("job_control"),
            )))
            .bind(&queues)
            .fetch_all(&database.pool)
            .await
            {
                Ok(rows) => rows,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
        let now = Instant::now();
        let current: HashSet<i64> = pending.iter().map(|row| row.0).collect();
        first_seen.retain(|id, _| current.contains(id));
        for row in &pending {
            let since = *first_seen.entry(row.0).or_insert(now);
            let age = now - since;
            longest = longest.max(age);
            if age > RESOLVE_BOUND && reported.insert(row.0) {
                violations.push(format!(
                    "job {} kept a pending {} for over {RESOLVE_BOUND:?} while {} (attempts {}, \
                     last_error {:?}, run_at in {:.1} s)",
                    row.0, row.5, row.1, row.3, row.2, row.4
                ));
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    (violations, longest)
}

async fn check_storm(database: &Database, seed: u64) -> Vec<String> {
    let label = format!("storm seed {seed}");
    let mut rng = Rng::new(seed).fork(31);
    let database = Arc::new(Database {
        options: database.options.clone(),
        pool: database.pool.clone(),
        schema_name: database.schema_name.clone(),
    });
    let keys: Vec<String> = (0..3).map(|key| format!("stop_race_k{key}")).collect();
    let storm = Arc::new(Storm {
        ids: Mutex::new(Vec::new()),
        violations: Mutex::new(Vec::new()),
        workers: tokio::sync::Mutex::new(Vec::new()),
    });
    for _ in 0..3 {
        storm.workers.lock().await.push(storm_worker().await);
    }
    for index in 0..36 {
        let queue_key = (index % 4 == 0).then(|| rng.pick(&keys).clone());
        let (probe, timeout) = random_probe(&mut rng, queue_key);
        let outcome = enqueue_probe(
            "enqueue",
            &probe,
            probe_options(STORM_QUEUE, &probe, timeout),
        )
        .await
        .unwrap();
        storm.ids.lock().unwrap().push(outcome.id().0);
    }
    let done = jalari::CancellationToken::new();
    let watcher = tokio::spawn(watch_commands(
        Arc::clone(&database),
        vec![STORM_QUEUE.to_owned()],
        done.clone(),
    ));
    let length = Duration::from_secs(15 * common::scale());
    let until = Instant::now() + length;
    let mut drivers = Vec::new();
    for driver in 0..3 {
        drivers.push(tokio::spawn(drive(
            Arc::clone(&storm),
            Arc::clone(&database),
            rng.fork(100 + driver),
            until,
            keys.clone(),
        )));
    }
    let chaos_task = tokio::spawn(unleash(
        Arc::clone(&storm),
        Arc::clone(&database),
        rng.fork(200),
        until,
    ));
    for driver in drivers {
        driver.await.unwrap();
    }
    let (kills, graceful, crashes) = chaos_task.await.unwrap();
    let storm_end = Instant::now();
    let ids: Vec<i64> = storm.ids.lock().unwrap().clone();
    eprintln!(
        "{label}: {} jobs, {kills} backends killed, {graceful} graceful worker restarts, \
         {crashes} worker crashes",
        ids.len()
    );

    let mut failures: Vec<String> = Vec::new();
    for &id in &ids {
        let last = ops_of(id)
            .into_iter()
            .rev()
            .find(|op| op.kind == "stop" || op.kind == "cancel");
        if let Some(op) = last
            && op.outcome == "Requested"
        {
            let settled = if op.kind == "stop" {
                stop_until_stopped(id, RESOLVE_BOUND)
                    .await
                    .map(|o| format!("{o:?}"))
            } else {
                cancel_until_settled(id, RESOLVE_BOUND)
                    .await
                    .map(|o| format!("{o:?}"))
            };
            if settled.as_deref() == Some("Requested") {
                failures.push(format!(
                    "{label}: job {id}'s last {} returned Requested and kept returning it {:?} \
                     after the storm ended",
                    op.kind,
                    storm_end.elapsed()
                ));
            }
        }
    }
    for &id in &ids {
        if job_state(&database, id).await.as_deref() == Some("stopped") {
            restart(id).await;
        }
    }
    let settle_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let waiting: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE id = ANY($1)
                 AND state IN ('scheduled', 'enqueued', 'stopped')",
            database.table("job")
        )))
        .bind(&ids)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        if waiting == 0 {
            break;
        }
        if Instant::now() >= settle_deadline {
            let rows: Vec<StuckRow> = sqlx::query_as(AssertSqlSafe(format!(
                "SELECT job.id, job.state, job.attempts, job.last_error, control.command
                     FROM {job} AS job LEFT JOIN {control} AS control ON control.job_id = job.id
                     WHERE job.id = ANY($1) AND job.state IN ('scheduled', 'enqueued', 'stopped')",
                job = database.table("job"),
                control = database.table("job_control"),
            )))
            .bind(&ids)
            .fetch_all(&database.pool)
            .await
            .unwrap();
            failures.push(format!(
                "{label}: {waiting} jobs never finished within 90 s of the storm: {rows:?}"
            ));
            break;
        }
        for &id in &ids {
            if job_state(&database, id).await.as_deref() == Some("stopped") {
                restart(id).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    done.cancel();
    let (watch_violations, longest) = watcher.await.unwrap();
    eprintln!(
        "{label}: the longest pending command on a waiting or running job lasted {longest:?}"
    );
    failures.extend(
        watch_violations
            .into_iter()
            .map(|v| format!("{label}: {v}")),
    );
    let workers = std::mem::take(&mut *storm.workers.lock().await);
    stop_all(workers).await;

    failures.extend(storm.violations.lock().unwrap().drain(..));
    failures.extend(audit(&label, &database, &ids, true).await);
    failures.extend(check_housekeeping(&label, &database, &ids).await);
    failures
}

async fn check_command_storm(database: &Database, seed: u64) -> Vec<String> {
    let label = format!("command_storm seed {seed}");
    let mut rng = Rng::new(seed).fork(21);
    let workers = vec![
        spawn_worker(
            &[(COMMAND_QUEUE, 4)],
            Duration::from_secs(1),
            Duration::from_secs(5),
            false,
        )
        .await,
        spawn_worker(
            &[(COMMAND_QUEUE, 4)],
            Duration::from_secs(1),
            Duration::from_secs(5),
            false,
        )
        .await,
    ];
    let slots = Arc::new(Mutex::new(Vec::new()));
    let all = Arc::new(Mutex::new(Vec::new()));
    for index in 0..8 {
        let id = command_job(&mut rng, index).await;
        slots.lock().unwrap().push(id);
        all.lock().unwrap().push(id);
    }
    let started_ops = OPS.lock().unwrap().len();
    let until = Instant::now() + Duration::from_secs(10 * common::scale());
    let mut hammers = Vec::new();
    for hammer in 0..8 {
        let slots = Arc::clone(&slots);
        let all = Arc::clone(&all);
        let mut rng = rng.fork(300 + hammer);
        hammers.push(tokio::spawn(async move {
            while Instant::now() < until {
                let slot = usize::try_from(rng.below(8)).unwrap();
                let id = slots.lock().unwrap()[slot];
                let finished = match rng.below(100) {
                    0..50 => matches!(stop(id).await, Some(StopOutcome::Finished)),
                    50..97 => {
                        restart(id).await;
                        false
                    }
                    _ => matches!(
                        cancel(id).await,
                        Some(CancelOutcome::Cancelled | CancelOutcome::Finished)
                    ),
                };
                if finished {
                    let fresh = command_job(&mut rng, slot).await;
                    let mut slots = slots.lock().unwrap();
                    if slots[slot] == id {
                        slots[slot] = fresh;
                    }
                    all.lock().unwrap().push(fresh);
                }
                if rng.chance(50) {
                    tokio::time::sleep(rng.millis(0, 8)).await;
                }
            }
        }));
    }
    for hammer in hammers {
        hammer.await.unwrap();
    }
    let ids: Vec<i64> = all.lock().unwrap().clone();
    let mut failures = Vec::new();
    for op in OPS.lock().unwrap()[started_ops..].iter() {
        if op.outcome.starts_with("error") {
            failures.push(format!(
                "{label}: {} on job {} failed without any fault injected: {}",
                op.kind, op.job_id, op.outcome
            ));
        }
    }
    for &id in &ids {
        if matches!(
            job_state(database, id).await.as_deref(),
            Some("succeeded" | "failed" | "deleted")
        ) {
            continue;
        }
        let stopped = stop_until_stopped(id, RESOLVE_BOUND).await;
        if !matches!(stopped, Some(StopOutcome::Stopped | StopOutcome::Finished)) {
            failures.push(format!(
                "{label}: job {id} did not settle under stop within {RESOLVE_BOUND:?}: {stopped:?}"
            ));
        }
        let cancelled = cancel_until_settled(id, RESOLVE_BOUND).await;
        if !matches!(
            cancelled,
            Some(CancelOutcome::Cancelled | CancelOutcome::Finished)
        ) {
            failures.push(format!(
                "{label}: job {id} did not settle under cancel: {cancelled:?}"
            ));
        }
    }
    stop_all(workers).await;
    let count = OPS.lock().unwrap().len() - started_ops;
    eprintln!("{label}: {count} control calls on {} jobs", ids.len());
    failures.extend(audit(&label, database, &ids, true).await);
    failures
}

async fn command_job(rng: &mut Rng, slot: usize) -> i64 {
    let probe = Probe {
        body: Body::Saver {
            tight: slot.is_multiple_of(2),
            millis: 1000 + rng.below(4000),
        },
        end: if slot.is_multiple_of(3) {
            End::Err
        } else {
            End::Ok
        },
        queue_key: (slot % 4 == 1).then(|| "stop_race_cmd_key".to_owned()),
    };
    jalari::enqueue_with(&probe, probe_options(COMMAND_QUEUE, &probe, None))
        .await
        .unwrap()
        .id()
        .0
}

async fn audit(label: &str, database: &Database, ids: &[i64], strict: bool) -> Vec<String> {
    let mut failures = Vec::new();
    for violation in OCCUPANCY.violations() {
        failures.push(format!("{label}: two bodies ran at once: {violation}"));
    }
    let jobs: HashMap<i64, JobRow> = database
        .jobs()
        .await
        .into_iter()
        .map(|job| (job.id, job))
        .collect();
    let mut history: HashMap<i64, Vec<HistoryRow>> = HashMap::new();
    for row in database.history().await {
        history.entry(row.job_id).or_default().push(row);
    }
    let chaos_windows = CHAOS.lock().unwrap().clone();
    let mut latencies: BTreeMap<&'static str, Duration> = BTreeMap::new();
    let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
    let (mut runs_total, mut halted_total) = (0, 0);
    let mut pairs = [0usize; 3];
    for &id in ids {
        let ops = ops_of(id);
        let runs = runs_of(id);
        runs_total += runs.len();
        halted_total += runs.iter().filter(|run| run.halted.is_some()).count();
        let replaced = ops.iter().any(|op| op.outcome == "Replaced");
        for op in &ops {
            *outcomes
                .entry(format!(
                    "{} {}",
                    op.kind,
                    op.outcome.split(':').next().unwrap_or_default()
                ))
                .or_default() += 1;
            let latency = op.end - op.start;
            let entry = latencies.entry(op.kind).or_default();
            *entry = (*entry).max(latency);
            let bound = if op.kind == "replace" || op.kind == "keep" {
                REPLACE_LATENCY
            } else {
                CONTROL_LATENCY
            };
            if latency > bound && !op.outcome.starts_with("error") {
                failures.push(format!(
                    "{label}: {} on job {id} took {latency:?} ({}), over {bound:?}",
                    op.kind, op.outcome
                ));
            }
            if op.outcome.contains("deadlock") {
                failures.push(format!(
                    "{label}: {} on job {id} deadlocked: {}",
                    op.kind, op.outcome
                ));
            }
        }
        failures.extend(check_commands(label, id, &ops, &runs));
        failures.extend(check_progress(
            label,
            id,
            &ops,
            &runs,
            &chaos_windows,
            strict,
            &mut pairs,
        ));
        let Some(job) = jobs.get(&id) else {
            failures.push(format!("{label}: job {id} vanished"));
            continue;
        };
        if job.attempts > job.max_attempts {
            failures.push(format!(
                "{label}: job {id} has {} attempts, over its max of {}",
                job.attempts, job.max_attempts
            ));
        }
        for run in &runs {
            if run.attempt > job.max_attempts {
                failures.push(format!(
                    "{label}: run {} of job {id} was attempt {}, over max {}",
                    run.tag, run.attempt, job.max_attempts
                ));
            }
        }
        if !replaced {
            failures.extend(check_history(
                label,
                job,
                history.get(&id).map_or(&[][..], Vec::as_slice),
            ));
        }
    }
    let leftovers: Vec<LeftoverRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job.id, job.state, control.command, control.progress IS NOT NULL, job.last_error
         FROM {job} AS job JOIN {control} AS control ON control.job_id = job.id
         WHERE job.id = ANY($1)
             AND (control.progress IS NOT NULL OR control.command IS NOT NULL)
             AND job.state IN ('succeeded', 'failed', 'deleted')",
        job = database.table("job"),
        control = database.table("job_control"),
    )))
    .bind(ids)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    let stale_commands = leftovers.iter().filter(|row| row.2.is_some()).count();
    for row in leftovers.iter().filter(|row| row.3) {
        failures.push(format!(
            "{label}: finished job {} ({}, last_error {:?}) still stores progress",
            row.0, row.1, row.4
        ));
    }
    eprintln!(
        "{label}: {runs_total} runs, {halted_total} halted at a checkpoint, {stale_commands} \
         finished jobs keep a command (Ok won the race), slowest calls {latencies:?}"
    );
    eprintln!("{label}: outcomes {outcomes:?}");
    eprintln!(
        "{label}: progress checked on {} consecutive run pairs, {} of them strictly, {} across a \
         Replace",
        pairs[0], pairs[1], pairs[2]
    );
    failures
}

fn check_commands(label: &str, id: i64, ops: &[Op], runs: &[RunRecord]) -> Vec<String> {
    let mut failures = Vec::new();
    for op in ops {
        let settled = matches!(
            (op.kind, op.outcome.as_str()),
            ("stop", "Stopped" | "Finished") | ("cancel", "Cancelled" | "Finished")
        );
        if !settled {
            continue;
        }
        let revived = (op.kind, op.outcome.as_str()) == ("stop", "Stopped");
        let barrier = ops
            .iter()
            .filter(|other| {
                revived
                    && other.kind == "restart"
                    && other.outcome == "Restarted"
                    && other.end > op.start
            })
            .map(|other| other.start)
            .min();
        for run in runs {
            if run.start > op.end && barrier.is_none_or(|barrier| run.start < barrier) {
                failures.push(format!(
                    "{label}: run {} of job {id} started {:?} after {} returned {} with no \
                     restart in between",
                    run.tag,
                    run.start - op.end,
                    op.kind,
                    op.outcome
                ));
            }
            if run.start < op.end && run.end.is_none_or(|end| end > op.end + ABORT_TOLERANCE) {
                failures.push(format!(
                    "{label}: run {} of job {id} was still running {:?} after {} returned {}",
                    run.tag,
                    run.end.map(|end| end - op.end),
                    op.kind,
                    op.outcome
                ));
            }
        }
        if !revived {
            for other in ops {
                if other.kind == "restart" && other.outcome == "Restarted" && other.start > op.end {
                    failures.push(format!(
                        "{label}: restart revived job {id} after {} returned {}",
                        op.kind, op.outcome
                    ));
                }
            }
        }
    }
    failures
}

fn check_progress(
    label: &str,
    id: i64,
    ops: &[Op],
    runs: &[RunRecord],
    chaos_windows: &[(Instant, Instant)],
    strict: bool,
    pairs: &mut [usize; 3],
) -> Vec<String> {
    let mut failures = Vec::new();
    for pair in runs.windows(2) {
        let (prev, next) = (&pair[0], &pair[1]);
        pairs[0] += 1;
        let replaced_between = ops
            .iter()
            .any(|op| op.outcome == "Replaced" && op.start > prev.start && op.end < next.start);
        if replaced_between {
            pairs[2] += 1;
            if next.resumed.is_some() {
                failures.push(format!(
                    "{label}: run {} of job {id} resumed {:?} although Replace overwrote the job \
                     after run {} began",
                    next.tag, next.resumed, prev.tag
                ));
            }
            continue;
        }
        let replace_near = ops
            .iter()
            .any(|op| op.outcome == "Replaced" && op.end > prev.start && op.start < next.start);
        let allowed = next.resumed == prev.resumed
            || next.resumed.as_ref().is_some_and(|mark| prev.saved(mark))
            || (replace_near && next.resumed.is_none());
        if !allowed {
            failures.push(format!(
                "{label}: run {} of job {id} resumed {:?}; the previous run {} resumed {:?} and \
                 saved {}..={:?}",
                next.tag, next.resumed, prev.tag, prev.resumed, prev.first_n, prev.last_n
            ));
            continue;
        }
        let calm = !replace_near
            && !chaos_windows
                .iter()
                .any(|(start, end)| *end >= prev.start && *start <= next.start);
        if !strict || !calm {
            continue;
        }
        pairs[1] += 1;
        match (prev.last_n, prev.halted) {
            (None, _) => {
                if next.resumed != prev.resumed {
                    failures.push(format!(
                        "{label}: run {} of job {id} resumed {:?}, but the previous run {} saved \
                         nothing and resumed {:?}",
                        next.tag, next.resumed, prev.tag, prev.resumed
                    ));
                }
            }
            (Some(_), Some(halted)) => {
                let expected = Mark {
                    tag: prev.tag.clone(),
                    n: halted,
                };
                if next.resumed.as_ref() != Some(&expected) {
                    failures.push(format!(
                        "{label}: run {} of job {id} resumed {:?}, but the previous run halted at \
                         its checkpoint with {expected:?}",
                        next.tag, next.resumed
                    ));
                }
            }
            (Some(_), None) => {
                let fresh = next.resumed.as_ref().is_some_and(|mark| {
                    prev.saved(mark)
                        && prev
                            .latest_save_of(mark.n)
                            .zip(prev.end)
                            .is_some_and(|(saved_at, end)| saved_at + KEEP_SLACK >= end)
                });
                if !fresh {
                    failures.push(format!(
                        "{label}: run {} of job {id} resumed {:?}, older than the last second of \
                         run {} (saved up to {:?})",
                        next.tag, next.resumed, prev.tag, prev.last_n
                    ));
                }
            }
        }
    }
    failures
}

fn check_history(label: &str, job: &JobRow, rows: &[HistoryRow]) -> Vec<String> {
    let mut failures = Vec::new();
    let mut prev = 0;
    for row in rows {
        let at = format!(
            "job {} history {} attempt {} ({:?}) after attempt {prev}",
            job.id, row.state, row.attempt, row.error
        );
        if row.attempt > job.max_attempts {
            failures.push(format!(
                "{label}: {at} exceeds max attempts {}",
                job.max_attempts
            ));
        }
        if row.attempt < prev {
            failures.push(format!("{label}: {at} uncounted a recorded attempt"));
        }
        if row.attempt > prev + 1 {
            failures.push(format!("{label}: {at} counted more than one attempt"));
        }
        let gives_back = matches!(row.state.as_str(), "stopped" | "deleted")
            || (matches!(row.state.as_str(), "scheduled" | "enqueued") && row.error.is_none())
            || row.error.as_deref() == Some(SHUTDOWN_NOTE);
        if gives_back && row.attempt != prev {
            failures.push(format!(
                "{label}: {at} should neither count nor uncount an attempt"
            ));
        }
        if row.state == "succeeded" && row.attempt != prev + 1 {
            failures.push(format!(
                "{label}: {at} succeeded without counting its attempt"
            ));
        }
        if row.error.as_deref() == Some(INTERRUPTED)
            && row.attempt != prev
            && row.attempt != prev + 1
        {
            failures.push(format!("{label}: {at} miscounted an interrupted attempt"));
        }
        prev = row.attempt;
    }
    failures
}

async fn check_housekeeping(label: &str, database: &Database, ids: &[i64]) -> Vec<String> {
    let mut failures = Vec::new();
    jalari::cluster::update(|config| {
        config.housekeeping_enabled = true;
        config.housekeeping_interval = Duration::from_secs(1);
        config.succeeded_retention = Duration::ZERO;
        config.deleted_retention = Duration::ZERO;
        config.failed_retention = Some(Duration::ZERO);
    })
    .await
    .unwrap();
    let sweeper = spawn_worker(
        &[(IDLE_QUEUE, 1)],
        STOP_TIMEOUT,
        Duration::from_secs(5),
        false,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (jobs, orphans): (i64, i64) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT (SELECT count(*) FROM {job} WHERE id = ANY($1)),
                 (SELECT count(*) FROM {control} AS control WHERE NOT EXISTS (
                     SELECT 1 FROM {job} AS job WHERE job.id = control.job_id))",
            job = database.table("job"),
            control = database.table("job_control"),
        )))
        .bind(ids)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        if jobs == 0 && orphans == 0 {
            break;
        }
        if Instant::now() >= deadline {
            failures.push(format!(
                "{label}: 20 s of housekeeping left {jobs} finished jobs and {orphans} job_control \
                 rows without a job"
            ));
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    sweeper.stop().await;
    jalari::cluster::update(|config| config.housekeeping_enabled = false)
        .await
        .unwrap();
    failures
}
