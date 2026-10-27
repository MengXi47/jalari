mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, HistoryRow, JobRow, OCCUPANCY, Rng, RunningWorker};
use jalari::admin::{self, JobFilter, RetryOutcome, RetrySummary, RunNowOutcome};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancelOutcome, EnqueueOptions, EnqueueOutcome, ErrorKind, Job, JobError, JobId, JobResult,
    JobState, OnConflict, RestartOutcome, StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const RETRY_QUEUE: &str = "admin_race_retry";
const KEY_QUEUE: &str = "admin_race_key";
const CONTROL_QUEUE: &str = "admin_race_control";
const RUN_NOW_QUEUE: &str = "admin_race_run_now";
const LIST_QUEUE: &str = "admin_race_list";
const BULK_QUEUE: &str = "admin_race_bulk";
const INTERRUPTED: &str = "the worker stopped during the last attempt";
const ABORT_TOLERANCE: Duration = Duration::from_millis(300);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(40);

static RUNS: LazyLock<Mutex<HashMap<i64, Vec<RunRecord>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static OPS: LazyLock<Mutex<Vec<Op>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static RUN_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum End {
    Ok,
    Retryable,
    Permanent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Probe {
    millis: u64,
    checkpoints: bool,
    end: End,
}

#[derive(Debug, Clone)]
struct RunRecord {
    tag: String,
    attempt: i32,
    start: Instant,
    end: Option<Instant>,
    returned_ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Retry(RetryOutcome),
    RunNow(RunNowOutcome),
    Stop(StopOutcome),
    Cancel(CancelOutcome),
    Restart(RestartOutcome),
    Enqueue(EnqueueOutcome),
    Failed(&'static str, jalari::Error),
}

impl Outcome {
    fn summary(&self) -> String {
        match self {
            Self::Retry(RetryOutcome::KeyTaken(_)) => "retry KeyTaken".to_owned(),
            Self::Retry(outcome) => format!("retry {outcome:?}"),
            Self::RunNow(outcome) => format!("run_now {outcome:?}"),
            Self::Stop(outcome) => format!("stop {outcome:?}"),
            Self::Cancel(outcome) => format!("cancel {outcome:?}"),
            Self::Restart(outcome) => format!("restart {outcome:?}"),
            Self::Enqueue(outcome) => format!(
                "enqueue {}",
                match outcome {
                    EnqueueOutcome::Inserted(_) => "Inserted",
                    EnqueueOutcome::Kept(_) => "Kept",
                    EnqueueOutcome::Replaced(_) => "Replaced",
                    EnqueueOutcome::Running(_) => "Running",
                }
            ),
            Self::Failed(kind, error) => format!("{kind} error {:?}", error.kind),
        }
    }
}

#[derive(Debug, Clone)]
struct Op {
    job_id: i64,
    start: Instant,
    end: Instant,
    outcome: Outcome,
}

#[derive(Serialize, Deserialize)]
struct Flaky(Probe);

#[jalari::job(queue = "admin_race_retry")]
impl Job for Flaky {
    const NAME: &'static str = "admin_race_flaky";
    const MAX_ATTEMPTS: i32 = 3;

    async fn run(self) -> JobResult {
        perform(self.0).await
    }
}

#[derive(Serialize, Deserialize)]
struct Keyed(Probe);

#[jalari::job(queue = "admin_race_key")]
impl Job for Keyed {
    const NAME: &'static str = "admin_race_keyed";
    const MAX_ATTEMPTS: i32 = 2;

    async fn run(self) -> JobResult {
        perform(self.0).await
    }
}

#[derive(Serialize, Deserialize)]
struct Sturdy(Probe);

#[jalari::job(queue = "admin_race_run_now")]
impl Job for Sturdy {
    const NAME: &'static str = "admin_race_sturdy";
    const MAX_ATTEMPTS: i32 = 50;

    async fn run(self) -> JobResult {
        perform(self.0).await
    }
}

#[derive(Serialize, Deserialize)]
struct Single(Probe);

#[jalari::job(queue = "admin_race_bulk")]
impl Job for Single {
    const NAME: &'static str = "admin_race_single";
    const MAX_ATTEMPTS: i32 = 1;

    async fn run(self) -> JobResult {
        perform(self.0).await
    }
}

struct RunGuard {
    job_id: i64,
    index: usize,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        with_run(self.job_id, self.index, |run| {
            run.end = Some(Instant::now())
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

async fn perform(probe: Probe) -> JobResult {
    let job = jalari::current_job().expect("a probe runs inside a job");
    let job_id = job.id().0;
    let attempt = job.attempt();
    let tag = format!(
        "{job_id}#{attempt}#{}",
        RUN_SEQ.fetch_add(1, Ordering::SeqCst)
    );
    let index = {
        let mut runs = RUNS.lock().unwrap();
        let runs = runs.entry(job_id).or_default();
        runs.push(RunRecord {
            tag: tag.clone(),
            attempt,
            start: Instant::now(),
            end: None,
            returned_ok: false,
        });
        runs.len() - 1
    };
    let _seat = OCCUPANCY.enter(vec![format!("job:{job_id}")], tag);
    let _guard = RunGuard { job_id, index };
    let length = Duration::from_millis(probe.millis);
    if probe.checkpoints {
        let started = Instant::now();
        while started.elapsed() < length {
            jalari::checkpoint().await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    } else {
        tokio::time::sleep(length).await;
    }
    match probe.end {
        End::Ok => {
            with_run(job_id, index, |run| run.returned_ok = true);
            Ok(())
        }
        End::Retryable => Err(JobError::new("planned failure".to_owned())),
        End::Permanent => Err(JobError::permanent(std::fmt::Error)),
    }
}

async fn record<T: Clone>(
    job_id: i64,
    kind: &'static str,
    call: impl Future<Output = jalari::Result<T>>,
    wrap: fn(T) -> Outcome,
) -> Option<T> {
    let start = Instant::now();
    let result = call.await;
    let end = Instant::now();
    let outcome = match &result {
        Ok(value) => wrap(value.clone()),
        Err(e) => Outcome::Failed(kind, e.clone()),
    };
    OPS.lock().unwrap().push(Op {
        job_id,
        start,
        end,
        outcome,
    });
    result.ok()
}

async fn retry(id: i64) -> Option<RetryOutcome> {
    record(id, "retry", admin::retry(JobId(id)), Outcome::Retry).await
}

async fn run_now(id: i64) -> Option<RunNowOutcome> {
    record(id, "run_now", admin::run_now(JobId(id)), Outcome::RunNow).await
}

async fn stop(id: i64) -> Option<StopOutcome> {
    record(id, "stop", jalari::stop(JobId(id)), Outcome::Stop).await
}

async fn cancel(id: i64) -> Option<CancelOutcome> {
    record(id, "cancel", jalari::cancel(JobId(id)), Outcome::Cancel).await
}

async fn restart(id: i64) -> Option<RestartOutcome> {
    record(id, "restart", jalari::restart(JobId(id)), Outcome::Restart).await
}

async fn enqueue<J: Job>(job: &J, options: EnqueueOptions) -> Option<EnqueueOutcome> {
    let start = Instant::now();
    let result = jalari::enqueue_with(job, options).await;
    let end = Instant::now();
    let (job_id, outcome) = match &result {
        Ok(outcome) => (outcome.id().0, Outcome::Enqueue(*outcome)),
        Err(e) => (0, Outcome::Failed("enqueue", e.clone())),
    };
    OPS.lock().unwrap().push(Op {
        job_id,
        start,
        end,
        outcome,
    });
    result.ok()
}

async fn spawn_workers(queue: &str, count: usize, concurrency: usize) -> Vec<RunningWorker> {
    let mut workers = Vec::new();
    for _ in 0..count {
        let mut config = common::worker_config(Duration::from_millis(50));
        config.stop_timeout = Duration::from_secs(1);
        config.shutdown_timeout = Duration::from_secs(5);
        let worker = Worker::builder()
            .config(config)
            .retry_policy(common::fast_retry())
            .scheduler(false)
            .queue(queue, concurrency)
            .build()
            .await
            .unwrap();
        workers.push(RunningWorker::spawn(worker));
    }
    workers
}

async fn stop_all(workers: Vec<RunningWorker>) {
    for worker in &workers {
        worker.request_stop();
    }
    for worker in workers {
        worker.stop().await;
    }
}

#[derive(Clone)]
struct Reader {
    pool: PgPool,
    job: String,
}

impl Reader {
    fn new(database: &Database) -> Self {
        Self {
            pool: database.pool.clone(),
            job: database.table("job"),
        }
    }

    async fn state(&self, id: i64) -> Option<String> {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state FROM {} WHERE id = $1",
            self.job
        )))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .unwrap()
    }

    async fn waiting(&self, queue: &str) -> i64 {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE queue = $1
             AND state IN ('scheduled', 'enqueued', 'running')",
            self.job
        )))
        .bind(queue)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn command(&self, database: &Database, id: i64) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(format!(
            "SELECT command FROM {} WHERE job_id = $1",
            database.table("job_control")
        )))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .unwrap()
        .flatten()
    }
}

async fn settle(database: &Database, queue: &str, timeout: Duration) -> bool {
    let reader = Reader::new(database);
    let deadline = Instant::now() + timeout;
    loop {
        if reader.waiting(queue).await == 0 && OCCUPANCY.busy() == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "{queue}: {} jobs still waiting and {} bodies running after {timeout:?}",
                reader.waiting(queue).await,
                OCCUPANCY.busy()
            );
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn jobs_by_id(database: &Database) -> HashMap<i64, JobRow> {
    database
        .jobs()
        .await
        .into_iter()
        .map(|job| (job.id, job))
        .collect()
}

async fn history_by_job(database: &Database) -> HashMap<i64, Vec<HistoryRow>> {
    let mut history: HashMap<i64, Vec<HistoryRow>> = HashMap::new();
    for row in database.history().await {
        history.entry(row.job_id).or_default().push(row);
    }
    history
}

fn is_reset(previous: Option<&HistoryRow>, row: &HistoryRow) -> bool {
    matches!(row.state.as_str(), "enqueued" | "scheduled")
        && row.attempt == 0
        && row.error.is_none()
        && previous.is_some_and(|previous| previous.state == "failed")
}

fn resets(rows: &[HistoryRow]) -> usize {
    rows.iter()
        .enumerate()
        .filter(|(index, row)| is_reset(index.checked_sub(1).map(|i| &rows[i]), row))
        .count()
}

fn check_history(label: &str, job: &JobRow, rows: &[HistoryRow]) -> Vec<String> {
    let mut failures = Vec::new();
    let mut prev = 0;
    for (index, row) in rows.iter().enumerate() {
        let previous = index.checked_sub(1).map(|i| &rows[i]);
        let at = format!(
            "job {} history #{index} {} attempt {} ({:?}) after attempt {prev}",
            job.id, row.state, row.attempt, row.error
        );
        if let Some(previous) = previous
            && matches!(previous.state.as_str(), "succeeded" | "deleted")
        {
            failures.push(format!(
                "{label}: {at} follows a finished {} entry",
                previous.state
            ));
        }
        if is_reset(previous, row) {
            prev = 0;
            continue;
        }
        if previous.is_some_and(|previous| previous.state == "failed") {
            failures.push(format!(
                "{label}: {at} changed a failed job without a retry"
            ));
        }
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
        let interrupted = row.error.as_deref() == Some(INTERRUPTED);
        let gives_back = matches!(row.state.as_str(), "stopped" | "deleted")
            || (row.state == "enqueued" && !interrupted)
            || (row.state == "scheduled" && row.error.is_none());
        if gives_back && row.attempt != prev {
            failures.push(format!(
                "{label}: {at} should neither count nor uncount an attempt"
            ));
        }
        let closes = row.state == "succeeded"
            || (matches!(row.state.as_str(), "failed" | "scheduled")
                && row.error.is_some()
                && !interrupted);
        if closes && row.attempt != prev + 1 {
            failures.push(format!("{label}: {at} closed an attempt it did not count"));
        }
        prev = row.attempt;
    }
    if let Some(last) = rows.last()
        && (last.state != job.state || last.attempt != job.attempts)
    {
        failures.push(format!(
            "{label}: job {} is {} with {} attempts, but its last history entry is {} attempt {}",
            job.id, job.state, job.attempts, last.state, last.attempt
        ));
    }
    failures
}

fn op_errors(label: &str, ops: &[Op], tolerated: &[ErrorKind]) -> (Vec<String>, usize) {
    let mut failures = Vec::new();
    let mut skipped = 0;
    for op in ops {
        if let Outcome::Failed(kind, error) = &op.outcome {
            if *kind == "enqueue" && tolerated.contains(&error.kind) {
                skipped += 1;
            } else {
                failures.push(format!(
                    "{label}: {kind} on job {} returned an undocumented error: {error:?}",
                    op.job_id
                ));
            }
        }
    }
    (failures, skipped)
}

fn summarize(ops: &[Op]) -> BTreeMap<String, usize> {
    let mut outcomes = BTreeMap::new();
    for op in ops {
        *outcomes.entry(op.outcome.summary()).or_default() += 1;
    }
    outcomes
}

fn ops_since(mark: usize) -> Vec<Op> {
    OPS.lock().unwrap()[mark..].to_vec()
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

fn scaled(base: u64) -> u64 {
    base * common::scale()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_race_admin() {
    let Some(database) = Database::start("admin", 80).await else {
        return;
    };
    let seed = common::seed("test_race_admin");
    let deadlocks_before = database.deadlocks().await;

    let mut failures = Vec::new();
    if selected("retry_races_workers") {
        failures.extend(
            timed(
                "retry_races_workers",
                check_retry_races_workers(&database, seed),
            )
            .await,
        );
    }
    if selected("retry_vs_replace_and_enqueue") {
        failures.extend(
            timed(
                "retry_vs_replace_and_enqueue",
                check_retry_vs_replace_and_enqueue(&database, seed),
            )
            .await,
        );
    }
    if selected("retry_vs_stop_cancel") {
        failures.extend(
            timed(
                "retry_vs_stop_cancel",
                check_retry_vs_stop_cancel(&database, seed),
            )
            .await,
        );
    }
    if selected("run_now_vs_workers") {
        failures.extend(
            timed(
                "run_now_vs_workers",
                check_run_now_vs_workers(&database, seed),
            )
            .await,
        );
    }
    if selected("listing_is_consistent_under_load") {
        failures.extend(
            timed(
                "listing_is_consistent_under_load",
                check_listing_is_consistent_under_load(&database, seed),
            )
            .await,
        );
    }
    if selected("retry_failed_vs_housekeeping") {
        failures.extend(
            timed(
                "retry_failed_vs_housekeeping",
                check_retry_failed_vs_housekeeping(&database, seed),
            )
            .await,
        );
    }

    for violation in OCCUPANCY.violations() {
        failures.push(format!("two bodies ran at once: {violation}"));
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

async fn check_retry_races_workers(database: &Database, seed: u64) -> Vec<String> {
    let label = "retry_races_workers";
    let mut rng = Rng::new(seed).fork(1);
    let mut failures = Vec::new();
    let mark = OPS.lock().unwrap().len();
    let workers = spawn_workers(RETRY_QUEUE, 3, 4).await;
    let mut ids = Vec::new();
    for index in 0..scaled(60) {
        let probe = Probe {
            millis: rng.below(40),
            checkpoints: false,
            end: if index.is_multiple_of(2) {
                End::Permanent
            } else {
                End::Retryable
            },
        };
        let outcome = jalari::enqueue_with(&Flaky(probe), EnqueueOptions::new())
            .await
            .unwrap();
        ids.push(outcome.id().0);
    }
    let ids = Arc::new(ids);
    let until = Instant::now() + Duration::from_secs(scaled(8));
    let mut hammers = Vec::new();
    for hammer in 0..6 {
        let ids = Arc::clone(&ids);
        let mut rng = rng.fork(100 + hammer);
        hammers.push(tokio::spawn(async move {
            while Instant::now() < until {
                retry(*rng.pick(&ids)).await;
                if rng.chance(30) {
                    tokio::time::sleep(rng.millis(0, 10)).await;
                }
            }
        }));
    }
    for hammer in hammers {
        hammer.await.unwrap();
    }
    if !settle(database, RETRY_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the queue did not settle"));
    }
    stop_all(workers).await;

    let ops = ops_since(mark);
    failures.extend(op_errors(label, &ops, &[]).0);
    let jobs = jobs_by_id(database).await;
    let history = history_by_job(database).await;
    let (mut retried_total, mut runs_total) = (0, 0);
    for &id in ids.iter() {
        let runs = runs_of(id);
        runs_total += runs.len();
        let mut retried: Vec<Op> = ops_of(id)
            .into_iter()
            .filter(|op| op.outcome == Outcome::Retry(RetryOutcome::Retried))
            .collect();
        retried.sort_by_key(|op| op.start);
        retried_total += retried.len();
        let Some(job) = jobs.get(&id) else {
            failures.push(format!("{label}: job {id} vanished"));
            continue;
        };
        let rows = history.get(&id).map_or(&[][..], Vec::as_slice);
        failures.extend(check_history(label, job, rows));
        if job.state != "failed" {
            failures.push(format!(
                "{label}: job {id} settled as {}, not failed",
                job.state
            ));
        }
        if !(0..=job.max_attempts).contains(&job.attempts) {
            failures.push(format!(
                "{label}: job {id} ended with {} attempts, outside 0..={}",
                job.attempts, job.max_attempts
            ));
        }
        if resets(rows) != retried.len() {
            failures.push(format!(
                "{label}: job {id} got {} Retried outcomes but {} retry entries in its history",
                retried.len(),
                resets(rows)
            ));
        }
        let fresh: Vec<&RunRecord> = runs.iter().filter(|run| run.attempt == 1).collect();
        if fresh.len() != retried.len() + 1 {
            failures.push(format!(
                "{label}: job {id} ran attempt 1 {} times after {} Retried outcomes",
                fresh.len(),
                retried.len()
            ));
        }
        for (index, op) in retried.iter().enumerate() {
            if fresh.get(index + 1).is_none_or(|run| run.start <= op.start) {
                failures.push(format!(
                    "{label}: Retried #{index} on job {id} was not followed by a run from attempt 1"
                ));
            }
        }
        if runs.first().is_some_and(|run| run.attempt != 1) {
            failures.push(format!(
                "{label}: the first run of job {id} was attempt {}",
                runs[0].attempt
            ));
        }
        for pair in runs.windows(2) {
            if pair[1].attempt != 1 && pair[1].attempt != pair[0].attempt + 1 {
                failures.push(format!(
                    "{label}: run {} of job {id} followed run {} without a retry in between",
                    pair[1].tag, pair[0].tag
                ));
            }
        }
        for run in &runs {
            if run.attempt > job.max_attempts {
                failures.push(format!(
                    "{label}: run {} of job {id} was attempt {}, over max {}",
                    run.tag, run.attempt, job.max_attempts
                ));
            }
        }
    }
    eprintln!(
        "{label}: {} jobs, {runs_total} runs, {retried_total} retried, outcomes {:?}",
        ids.len(),
        summarize(&ops)
    );
    failures
}

async fn watch_job_keys(pool: PgPool, job: String, done: Arc<AtomicBool>) -> (usize, Vec<String>) {
    let mut failures = Vec::new();
    let mut snapshots = 0;
    while !done.load(Ordering::SeqCst) {
        let duplicates: Vec<(String, i64)> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT job_key, count(*) FROM {job}
             WHERE job_key IS NOT NULL AND state IN ('scheduled', 'enqueued', 'running', 'stopped')
             GROUP BY job_key HAVING count(*) > 1"
        )))
        .fetch_all(&pool)
        .await
        .unwrap();
        snapshots += 1;
        for (job_key, count) in duplicates {
            failures.push(format!("{count} jobs held job_key {job_key} at once"));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (snapshots, failures)
}

async fn check_retry_vs_replace_and_enqueue(database: &Database, seed: u64) -> Vec<String> {
    let label = "retry_vs_replace_and_enqueue";
    let mut rng = Rng::new(seed).fork(2);
    let mut failures = Vec::new();
    let mark = OPS.lock().unwrap().len();
    let workers = spawn_workers(KEY_QUEUE, 2, 4).await;
    let keys: Vec<String> = (0..scaled(12))
        .map(|index| format!("admin_race_key_{index}"))
        .collect();
    let known: Arc<Mutex<HashMap<String, Vec<i64>>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut initial = HashSet::new();
    for key in &keys {
        let probe = Probe {
            millis: 1,
            checkpoints: false,
            end: End::Permanent,
        };
        let id = jalari::enqueue_with(&Keyed(probe), EnqueueOptions::new().job_key(key))
            .await
            .unwrap()
            .id()
            .0;
        known.lock().unwrap().insert(key.clone(), vec![id]);
        initial.insert(id);
    }
    if !settle(database, KEY_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the first jobs never failed"));
    }

    let done = Arc::new(AtomicBool::new(false));
    let watcher = tokio::spawn(watch_job_keys(
        database.pool.clone(),
        database.table("job"),
        Arc::clone(&done),
    ));
    let keys = Arc::new(keys);
    let until = Instant::now() + Duration::from_secs(scaled(8));
    let mut hammers = Vec::new();
    for hammer in 0..6 {
        let keys = Arc::clone(&keys);
        let known = Arc::clone(&known);
        let mut rng = rng.fork(200 + hammer);
        hammers.push(tokio::spawn(async move {
            while Instant::now() < until {
                let key = rng.pick(&keys).clone();
                if rng.chance(50) {
                    let candidates = known.lock().unwrap()[&key].clone();
                    retry(*rng.pick(&candidates)).await;
                } else {
                    let probe = Probe {
                        millis: rng.below(150),
                        checkpoints: false,
                        end: if rng.chance(50) {
                            End::Permanent
                        } else {
                            End::Ok
                        },
                    };
                    let mut options = EnqueueOptions::new().job_key(&key);
                    if rng.chance(30) {
                        options = options.delay(rng.millis(50, 400));
                    }
                    if rng.chance(50) {
                        options = options.on_conflict(OnConflict::Replace);
                    }
                    if let Some(EnqueueOutcome::Inserted(id)) =
                        enqueue(&Keyed(probe), options).await
                    {
                        known.lock().unwrap().get_mut(&key).unwrap().push(id.0);
                    }
                }
                if rng.chance(30) {
                    tokio::time::sleep(rng.millis(0, 10)).await;
                }
            }
        }));
    }
    for hammer in hammers {
        hammer.await.unwrap();
    }
    done.store(true, Ordering::SeqCst);
    let (snapshots, duplicates) = watcher.await.unwrap();
    failures.extend(duplicates.into_iter().map(|d| format!("{label}: {d}")));
    if !settle(database, KEY_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the queue did not settle"));
    }
    stop_all(workers).await;

    let ops = ops_since(mark);
    let (errors, contended) = op_errors(label, &ops, &[ErrorKind::JobKeyContention]);
    failures.extend(errors);
    let jobs = jobs_by_id(database).await;
    let history = history_by_job(database).await;
    let ids: Vec<i64> = known.lock().unwrap().values().flatten().copied().collect();
    let inserted_at: HashMap<i64, Instant> = ops
        .iter()
        .filter_map(|op| match op.outcome {
            Outcome::Enqueue(EnqueueOutcome::Inserted(id)) => Some((id.0, op.start)),
            _ => None,
        })
        .collect();
    let mut key_taken = 0;
    for op in &ops {
        let Outcome::Retry(RetryOutcome::KeyTaken(holder)) = op.outcome else {
            continue;
        };
        key_taken += 1;
        let key = jobs.get(&op.job_id).and_then(|job| job.job_key.clone());
        let holder_key = jobs.get(&holder.0).and_then(|job| job.job_key.clone());
        if holder.0 == op.job_id || key.is_none() || holder_key != key {
            failures.push(format!(
                "{label}: retry of job {} (key {key:?}) reported KeyTaken by job {holder} \
                 (key {holder_key:?})",
                op.job_id
            ));
        }
        let existed = initial.contains(&holder.0)
            || inserted_at.get(&holder.0).is_some_and(|at| *at < op.end);
        if !existed {
            failures.push(format!(
                "{label}: retry of job {} reported KeyTaken by job {holder}, which was not \
                 inserted before the retry returned",
                op.job_id
            ));
        }
    }
    for &id in &ids {
        let Some(job) = jobs.get(&id) else {
            failures.push(format!("{label}: job {id} vanished"));
            continue;
        };
        let job_ops = ops_of(id);
        let rows = history.get(&id).map_or(&[][..], Vec::as_slice);
        let retried = job_ops
            .iter()
            .filter(|op| op.outcome == Outcome::Retry(RetryOutcome::Retried))
            .count();
        if resets(rows) != retried {
            failures.push(format!(
                "{label}: job {id} got {retried} Retried outcomes but {} retry entries in its \
                 history",
                resets(rows)
            ));
        }
        let replaced = job_ops
            .iter()
            .any(|op| matches!(op.outcome, Outcome::Enqueue(EnqueueOutcome::Replaced(_))));
        if !replaced {
            failures.extend(check_history(label, job, rows));
        }
    }
    let leftovers: Vec<(i64, String, i32, Option<String>)> =
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT id, state, attempts, job_key FROM {} WHERE queue = $1
                 AND ((state = 'failed' AND attempts = 0)
                     OR state IN ('scheduled', 'enqueued', 'running', 'stopped'))",
            database.table("job")
        )))
        .bind(KEY_QUEUE)
        .fetch_all(&database.pool)
        .await
        .unwrap();
    for (id, state, attempts, job_key) in leftovers {
        failures.push(format!(
            "{label}: job {id} ({job_key:?}) ended {state} with {attempts} attempts"
        ));
    }
    eprintln!(
        "{label}: {} jobs on {} keys, {snapshots} key snapshots, {key_taken} KeyTaken, \
         {contended} enqueues gave up on contention, outcomes {:?}",
        ids.len(),
        keys.len(),
        summarize(&ops)
    );
    failures
}

async fn control_job(rng: &mut Rng) -> i64 {
    let probe = Probe {
        millis: 100 + rng.below(700),
        checkpoints: true,
        end: match rng.below(100) {
            0..30 => End::Ok,
            30..60 => End::Retryable,
            _ => End::Permanent,
        },
    };
    jalari::enqueue_with(&Flaky(probe), EnqueueOptions::new().queue(CONTROL_QUEUE))
        .await
        .unwrap()
        .id()
        .0
}

async fn check_retry_vs_stop_cancel(database: &Database, seed: u64) -> Vec<String> {
    let label = "retry_vs_stop_cancel";
    let mut rng = Rng::new(seed).fork(3);
    let mut failures = Vec::new();
    let mark = OPS.lock().unwrap().len();
    let workers = spawn_workers(CONTROL_QUEUE, 2, 4).await;
    let slots = Arc::new(Mutex::new(Vec::new()));
    let all = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..8 {
        let id = control_job(&mut rng).await;
        slots.lock().unwrap().push(id);
        all.lock().unwrap().push(id);
    }
    let reader = Reader::new(database);
    let until = Instant::now() + Duration::from_secs(scaled(8));
    let mut hammers = Vec::new();
    for hammer in 0..6 {
        let slots = Arc::clone(&slots);
        let all = Arc::clone(&all);
        let reader = reader.clone();
        let mut rng = rng.fork(300 + hammer);
        hammers.push(tokio::spawn(async move {
            while Instant::now() < until {
                let slot = usize::try_from(rng.below(8)).unwrap();
                let id = slots.lock().unwrap()[slot];
                let finished = match rng.below(100) {
                    0..35 => {
                        retry(id).await;
                        false
                    }
                    35..60 => matches!(stop(id).await, Some(StopOutcome::Finished)),
                    60..85 => {
                        restart(id).await;
                        false
                    }
                    _ => matches!(
                        cancel(id).await,
                        Some(CancelOutcome::Cancelled | CancelOutcome::Finished)
                    ),
                };
                if finished
                    && matches!(
                        reader.state(id).await.as_deref(),
                        Some("succeeded" | "deleted")
                    )
                {
                    let fresh = control_job(&mut rng).await;
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
    if !settle(database, CONTROL_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the queue did not settle"));
    }
    stop_all(workers).await;

    let ops = ops_since(mark);
    failures.extend(op_errors(label, &ops, &[]).0);
    let jobs = jobs_by_id(database).await;
    let history = history_by_job(database).await;
    let ids: Vec<i64> = all.lock().unwrap().clone();
    let mut finals: BTreeMap<String, usize> = BTreeMap::new();
    for &id in &ids {
        let Some(job) = jobs.get(&id) else {
            failures.push(format!("{label}: job {id} vanished"));
            continue;
        };
        *finals.entry(job.state.clone()).or_default() += 1;
        let job_ops = ops_of(id);
        let runs = runs_of(id);
        let command = reader.command(database, id).await;
        failures.extend(check_control(
            label,
            job,
            command.as_deref(),
            &job_ops,
            &runs,
        ));
        failures.extend(check_history(
            label,
            job,
            history.get(&id).map_or(&[][..], Vec::as_slice),
        ));
    }
    eprintln!(
        "{label}: {} jobs settled as {finals:?}, outcomes {:?}",
        ids.len(),
        summarize(&ops)
    );
    failures
}

fn latest(ops: &[Op], outcome: &[Outcome], at: fn(&Op) -> Instant) -> Option<Instant> {
    ops.iter()
        .filter(|op| outcome.contains(&op.outcome))
        .map(at)
        .max()
}

fn check_control(
    label: &str,
    job: &JobRow,
    command: Option<&str>,
    ops: &[Op],
    runs: &[RunRecord],
) -> Vec<String> {
    let mut failures = Vec::new();
    let id = job.id;
    let retried = Outcome::Retry(RetryOutcome::Retried);
    let restarted = Outcome::Restart(RestartOutcome::Restarted);
    for op in ops {
        let cancelled = op.outcome == Outcome::Cancel(CancelOutcome::Cancelled);
        let requested = op.outcome == Outcome::Cancel(CancelOutcome::Requested);
        if cancelled || requested {
            let grace = if requested {
                ABORT_TOLERANCE
            } else {
                Duration::ZERO
            };
            for run in runs.iter().filter(|run| run.start > op.end + grace) {
                failures.push(format!(
                    "{label}: run {} of job {id} started {:?} after cancel returned {:?}",
                    run.tag,
                    run.start - op.end,
                    op.outcome
                ));
            }
            for later in ops.iter().filter(|later| later.start > op.end) {
                if later.outcome == retried || (cancelled && later.outcome == restarted) {
                    failures.push(format!(
                        "{label}: {:?} revived job {id} after cancel returned {:?}",
                        later.outcome, op.outcome
                    ));
                }
            }
        }
        if cancelled && job.state != "deleted" {
            failures.push(format!(
                "{label}: job {id} ended {} although cancel returned Cancelled",
                job.state
            ));
        }
        if requested
            && !matches!(job.state.as_str(), "deleted" | "succeeded")
            && !(job.state == "stopped" && command == Some("cancel"))
        {
            failures.push(format!(
                "{label}: job {id} ended {} (pending {command:?}) after cancel returned Requested",
                job.state
            ));
        }
        if op.outcome == Outcome::Stop(StopOutcome::Stopped) {
            let barrier = ops
                .iter()
                .filter(|other| other.outcome == restarted && other.end > op.start)
                .map(|other| other.start)
                .min();
            for run in runs {
                if run.start > op.end && barrier.is_none_or(|barrier| run.start < barrier) {
                    failures.push(format!(
                        "{label}: run {} of job {id} started after stop returned Stopped with no \
                         restart in between",
                        run.tag
                    ));
                }
                if run.start < op.end && run.end.is_none_or(|end| end > op.end + ABORT_TOLERANCE) {
                    failures.push(format!(
                        "{label}: run {} of job {id} was still running {:?} after stop returned \
                         Stopped",
                        run.tag,
                        run.end.map(|end| end - op.end)
                    ));
                }
            }
        }
    }
    let last_run = runs.iter().max_by_key(|run| run.start);
    match job.state.as_str() {
        "deleted" => {
            let cancelled = ops.iter().any(|op| {
                matches!(
                    op.outcome,
                    Outcome::Cancel(CancelOutcome::Cancelled | CancelOutcome::Requested)
                )
            });
            if !cancelled {
                failures.push(format!(
                    "{label}: job {id} ended deleted, but no cancel was applied or requested"
                ));
            }
        }
        "stopped" => {
            let stopped = latest(
                ops,
                &[
                    Outcome::Stop(StopOutcome::Stopped),
                    Outcome::Stop(StopOutcome::Requested),
                ],
                |op| op.end,
            );
            let revived = latest(ops, std::slice::from_ref(&restarted), |op| op.start);
            if stopped.is_none_or(|stopped| revived.is_some_and(|revived| revived > stopped)) {
                failures.push(format!(
                    "{label}: job {id} ended stopped, but its last stop ({stopped:?}) came before \
                     its last restart ({revived:?})"
                ));
            }
        }
        "failed" => {
            let revived = latest(ops, std::slice::from_ref(&retried), |op| op.start);
            match last_run {
                Some(run) if run.returned_ok => failures.push(format!(
                    "{label}: job {id} ended failed, but its last run {} returned Ok",
                    run.tag
                )),
                Some(run) if revived.is_some_and(|revived| revived > run.start) => {
                    failures.push(format!(
                        "{label}: job {id} ended failed, but a retry returned Retried after its \
                         last run {} began",
                        run.tag
                    ));
                }
                Some(_) => {}
                None => failures.push(format!("{label}: job {id} failed without running")),
            }
        }
        "succeeded" => {
            if !last_run.is_some_and(|run| run.returned_ok) {
                failures.push(format!(
                    "{label}: job {id} ended succeeded, but its last run did not return Ok"
                ));
            }
        }
        state => failures.push(format!("{label}: job {id} never settled, it is {state}")),
    }
    failures
}

async fn enqueue_sturdy(rng: &mut Rng) -> i64 {
    let probe = Probe {
        millis: 200 + rng.below(1000),
        checkpoints: false,
        end: End::Ok,
    };
    let options = EnqueueOptions::new().delay(rng.millis(300, 5000));
    jalari::enqueue_with(&Sturdy(probe), options)
        .await
        .unwrap()
        .id()
        .0
}

async fn check_run_now_vs_workers(database: &Database, seed: u64) -> Vec<String> {
    let label = "run_now_vs_workers";
    let mut rng = Rng::new(seed).fork(4);
    let mut failures = Vec::new();
    let mark = OPS.lock().unwrap().len();
    let workers = spawn_workers(RUN_NOW_QUEUE, 3, 3).await;
    let ids = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..scaled(40) {
        let id = enqueue_sturdy(&mut rng).await;
        ids.lock().unwrap().push(id);
    }
    let until = Instant::now() + Duration::from_secs(scaled(10));
    let mut hammers = Vec::new();
    for hammer in 0..4 {
        let ids = Arc::clone(&ids);
        let mut rng = rng.fork(400 + hammer);
        hammers.push(tokio::spawn(async move {
            while Instant::now() < until {
                let recent: Vec<i64> = {
                    let ids = ids.lock().unwrap();
                    ids[ids.len().saturating_sub(40)..].to_vec()
                };
                run_now(*rng.pick(&recent)).await;
                tokio::time::sleep(rng.millis(0, 20)).await;
            }
        }));
    }
    let enqueuer = {
        let ids = Arc::clone(&ids);
        let mut rng = rng.fork(450);
        tokio::spawn(async move {
            while Instant::now() < until {
                tokio::time::sleep(rng.millis(100, 250)).await;
                let id = enqueue_sturdy(&mut rng).await;
                ids.lock().unwrap().push(id);
            }
        })
    };
    let mut kills = 0;
    while Instant::now() < until {
        tokio::time::sleep(rng.millis(300, 900)).await;
        kills += database.terminate_lifelines(1).await;
    }
    for hammer in hammers {
        hammer.await.unwrap();
    }
    enqueuer.await.unwrap();
    let ids: Vec<i64> = ids.lock().unwrap().clone();
    if !settle(database, RUN_NOW_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the queue did not settle"));
    }
    stop_all(workers).await;

    let ops = ops_since(mark);
    failures.extend(op_errors(label, &ops, &[]).0);
    let jobs = jobs_by_id(database).await;
    let history = history_by_job(database).await;
    let (mut runs_total, mut interrupted_total, mut covered_running) = (0, 0, 0);
    for &id in ids.iter() {
        let runs = runs_of(id);
        runs_total += runs.len();
        for op in ops_of(id) {
            let covering = runs
                .iter()
                .find(|run| run.start < op.start && run.end.is_none_or(|end| end > op.end));
            match (&op.outcome, covering) {
                (Outcome::RunNow(RunNowOutcome::Enqueued), Some(run)) => {
                    failures.push(format!(
                        "{label}: run_now on job {id} returned Enqueued while run {} was running",
                        run.tag
                    ));
                }
                (Outcome::RunNow(RunNowOutcome::Running), Some(_)) => covered_running += 1,
                (
                    Outcome::RunNow(RunNowOutcome::NotWaiting | RunNowOutcome::NotFound),
                    Some(run),
                ) => {
                    failures.push(format!(
                        "{label}: run_now on job {id} returned {:?} while run {} was running",
                        op.outcome, run.tag
                    ));
                }
                _ => {}
            }
        }
        let Some(job) = jobs.get(&id) else {
            failures.push(format!("{label}: job {id} vanished"));
            continue;
        };
        let rows = history.get(&id).map_or(&[][..], Vec::as_slice);
        interrupted_total += rows
            .iter()
            .filter(|row| row.error.as_deref() == Some(INTERRUPTED))
            .count();
        failures.extend(check_history(label, job, rows));
        let succeeded = rows.iter().filter(|row| row.state == "succeeded").count();
        if job.state != "succeeded" || succeeded != 1 {
            failures.push(format!(
                "{label}: job {id} ended {} with {succeeded} succeeded entries, expected one",
                job.state
            ));
        }
        if !runs.iter().any(|run| run.returned_ok) {
            failures.push(format!("{label}: no run of job {id} returned Ok"));
        }
        if runs.len() > usize::try_from(job.attempts).unwrap_or(0) {
            failures.push(format!(
                "{label}: job {id} ran {} times but counts {} attempts",
                runs.len(),
                job.attempts
            ));
        }
        if job.attempts > job.max_attempts {
            failures.push(format!(
                "{label}: job {id} has {} attempts, over its max of {}",
                job.attempts, job.max_attempts
            ));
        }
    }
    eprintln!(
        "{label}: {} jobs, {runs_total} runs, {kills} sessions killed, {interrupted_total} \
         interrupted entries, {covered_running} Running answers during a run, outcomes {:?}",
        ids.len(),
        summarize(&ops)
    );
    failures
}

fn listing_context(index: u64) -> Value {
    if index.is_multiple_of(3) {
        json!({ "race": "other", "n": index })
    } else {
        json!({ "race": "listing", "n": index })
    }
}

fn listing_probe(rng: &mut Rng) -> Probe {
    Probe {
        millis: rng.below(300),
        checkpoints: false,
        end: match rng.below(100) {
            0..40 => End::Ok,
            40..70 => End::Retryable,
            _ => End::Permanent,
        },
    }
}

async fn enqueue_listed(rng: &mut Rng, index: u64) -> i64 {
    let options = EnqueueOptions::new()
        .queue(LIST_QUEUE)
        .context(&listing_context(index));
    jalari::enqueue_with(&Flaky(listing_probe(rng)), options)
        .await
        .unwrap()
        .id()
        .0
}

fn check_page(
    label: &str,
    page: &[admin::JobInfo],
    limit: u32,
    before: Option<JobId>,
    seen: &mut HashSet<i64>,
) -> Vec<String> {
    let mut failures = Vec::new();
    if page.len() > usize::try_from(limit).unwrap() {
        failures.push(format!(
            "{label}: a page of limit {limit} held {} jobs",
            page.len()
        ));
    }
    for pair in page.windows(2) {
        if pair[1].id >= pair[0].id {
            failures.push(format!(
                "{label}: page not strictly decreasing: {} then {}",
                pair[0].id, pair[1].id
            ));
        }
    }
    for job in page {
        if before.is_some_and(|before| job.id >= before) {
            failures.push(format!(
                "{label}: job {} came back on a page before {before:?}",
                job.id
            ));
        }
        if !seen.insert(job.id.0) {
            failures.push(format!("{label}: job {} appeared on two pages", job.id));
        }
        let race = job
            .context
            .as_ref()
            .and_then(|context| context.get("race"))
            .and_then(Value::as_str);
        if race != Some("listing") {
            failures.push(format!(
                "{label}: job {} with context {:?} matched the listing filter",
                job.id, job.context
            ));
        }
        if !(0..=job.max_attempts).contains(&job.attempts) {
            failures.push(format!(
                "{label}: job {} listed with {} attempts of {}",
                job.id, job.attempts, job.max_attempts
            ));
        }
    }
    failures
}

async fn page_all(
    label: &str,
    filter: &JobFilter,
    rng: &mut Rng,
) -> (HashSet<i64>, usize, Vec<String>) {
    let mut failures = Vec::new();
    let mut seen = HashSet::new();
    let mut pages = 0;
    let mut before = None;
    loop {
        let limit = u32::try_from(1 + rng.below(40)).unwrap();
        let page = match admin::jobs(filter, limit, before).await {
            Ok(page) => page,
            Err(e) => {
                failures.push(format!("{label}: admin::jobs failed: {e:?}"));
                break;
            }
        };
        pages += 1;
        failures.extend(check_page(label, &page, limit, before, &mut seen));
        let Some(last) = page.last() else {
            break;
        };
        before = Some(last.id);
    }
    (seen, pages, failures)
}

fn active_run(ids: &HashSet<i64>, rng: &mut Rng) -> Option<(i64, RunRecord)> {
    let runs = RUNS.lock().unwrap();
    let active: Vec<(i64, RunRecord)> = runs
        .iter()
        .filter(|(id, _)| ids.contains(id))
        .flat_map(|(id, runs)| {
            runs.iter()
                .filter(|run| run.end.is_none())
                .map(|run| (*id, run.clone()))
        })
        .collect();
    (!active.is_empty()).then(|| rng.pick(&active).clone())
}

fn still_running(id: i64, tag: &str, after: Instant) -> bool {
    runs_of(id)
        .iter()
        .any(|run| run.tag == tag && run.end.is_none_or(|end| end > after))
}

async fn inspect_running(
    label: &str,
    ids: Arc<Mutex<HashSet<i64>>>,
    mut rng: Rng,
    until: Instant,
) -> (usize, Vec<String>) {
    let mut failures = Vec::new();
    let mut inspected = 0;
    while Instant::now() < until {
        let snapshot = ids.lock().unwrap().clone();
        let Some((id, run)) = active_run(&snapshot, &mut rng) else {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };
        let detail = admin::job(JobId(id)).await;
        let returned = Instant::now();
        if !still_running(id, &run.tag, returned) {
            continue;
        }
        inspected += 1;
        let detail = match detail {
            Ok(Some(detail)) => detail,
            other => {
                failures.push(format!(
                    "{label}: admin::job({id}) returned {other:?} while run {} was running",
                    run.tag
                ));
                continue;
            }
        };
        if detail.job.state != JobState::Running {
            failures.push(format!(
                "{label}: admin::job({id}) reported {} while run {} was running",
                detail.job.state, run.tag
            ));
        }
        if detail.job.attempts != run.attempt {
            failures.push(format!(
                "{label}: admin::job({id}) reported {} attempts while run {} was attempt {}",
                detail.job.attempts, run.tag, run.attempt
            ));
        }
        if let Some(last) = detail.history.last()
            && last.attempt != detail.job.attempts - 1
            && last.attempt != detail.job.attempts
        {
            failures.push(format!(
                "{label}: admin::job({id}) at {} attempts ended its history with {} attempt {}",
                detail.job.attempts, last.state, last.attempt
            ));
        }
    }
    (inspected, failures)
}

async fn check_listing_is_consistent_under_load(database: &Database, seed: u64) -> Vec<String> {
    let label = "listing_is_consistent_under_load";
    let mut rng = Rng::new(seed).fork(6);
    let mut failures = Vec::new();
    let mark = OPS.lock().unwrap().len();
    let filter = JobFilter::new().context(json!({ "race": "listing" }));
    let workers = spawn_workers(LIST_QUEUE, 3, 3).await;
    let next = Arc::new(AtomicU64::new(0));
    let ids = Arc::new(Mutex::new(HashSet::new()));
    for _ in 0..scaled(150) {
        let index = next.fetch_add(1, Ordering::SeqCst);
        let id = enqueue_listed(&mut rng, index).await;
        ids.lock().unwrap().insert(id);
    }
    let until = Instant::now() + Duration::from_secs(scaled(8));
    let mut hammers = Vec::new();
    for hammer in 0..3 {
        let ids = Arc::clone(&ids);
        let mut rng = rng.fork(600 + hammer);
        hammers.push(tokio::spawn(async move {
            while Instant::now() < until {
                let candidates: Vec<i64> = ids.lock().unwrap().iter().copied().collect();
                let id = *rng.pick(&candidates);
                if rng.chance(50) {
                    retry(id).await;
                } else {
                    run_now(id).await;
                }
                tokio::time::sleep(rng.millis(0, 10)).await;
            }
        }));
    }
    let enqueuer = {
        let ids = Arc::clone(&ids);
        let next = Arc::clone(&next);
        let mut rng = rng.fork(650);
        tokio::spawn(async move {
            while Instant::now() < until {
                let index = next.fetch_add(1, Ordering::SeqCst);
                let id = enqueue_listed(&mut rng, index).await;
                ids.lock().unwrap().insert(id);
                tokio::time::sleep(rng.millis(20, 80)).await;
            }
        })
    };
    let lister = {
        let filter = filter.clone();
        let mut rng = rng.fork(660);
        tokio::spawn(async move {
            let mut failures = Vec::new();
            let (mut passes, mut pages_total) = (0, 0);
            while Instant::now() < until {
                let (_, pages, found) = page_all(label, &filter, &mut rng).await;
                passes += 1;
                pages_total += pages;
                failures.extend(found);
            }
            (passes, pages_total, failures)
        })
    };
    let inspector = tokio::spawn(inspect_running(
        label,
        Arc::clone(&ids),
        rng.fork(670),
        until,
    ));
    for hammer in hammers {
        hammer.await.unwrap();
    }
    enqueuer.await.unwrap();
    let (passes, pages_total, listed) = lister.await.unwrap();
    failures.extend(listed);
    let (inspected, inspection) = inspector.await.unwrap();
    failures.extend(inspection);
    if !settle(database, LIST_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the queue did not settle"));
    }
    stop_all(workers).await;

    let ops = ops_since(mark);
    failures.extend(op_errors(label, &ops, &[]).0);
    let (seen, _, quiet) = page_all(label, &filter, &mut rng).await;
    failures.extend(quiet);
    let counted = admin::count(&filter).await.unwrap();
    let stored: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE context @> $1",
        database.table("job")
    )))
    .bind(json!({ "race": "listing" }))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    if counted != stored || usize::try_from(counted).unwrap() != seen.len() {
        failures.push(format!(
            "{label}: count says {counted}, paging saw {}, the table holds {stored}",
            seen.len()
        ));
    }
    let total: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {}",
        database.table("job")
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let stats: i64 = admin::stats()
        .await
        .unwrap()
        .iter()
        .map(|count| count.count)
        .sum();
    if stats != total {
        failures.push(format!(
            "{label}: stats add up to {stats} jobs, the table holds {total}"
        ));
    }
    let jobs = jobs_by_id(database).await;
    let history = history_by_job(database).await;
    let ids: Vec<i64> = ids.lock().unwrap().iter().copied().collect();
    for &id in &ids {
        let Some(job) = jobs.get(&id) else {
            failures.push(format!("{label}: job {id} vanished"));
            continue;
        };
        failures.extend(check_history(
            label,
            job,
            history.get(&id).map_or(&[][..], Vec::as_slice),
        ));
    }
    eprintln!(
        "{label}: {} jobs, {passes} paging passes over {pages_total} pages, {inspected} running \
         jobs inspected, outcomes {:?}",
        ids.len(),
        summarize(&ops)
    );
    failures
}

async fn enqueue_failing(rng: &mut Rng) -> i64 {
    let probe = Probe {
        millis: rng.below(5),
        checkpoints: false,
        end: End::Permanent,
    };
    jalari::enqueue_with(&Single(probe), EnqueueOptions::new())
        .await
        .unwrap()
        .id()
        .0
}

async fn check_retry_failed_vs_housekeeping(database: &Database, seed: u64) -> Vec<String> {
    let label = "retry_failed_vs_housekeeping";
    let mut rng = Rng::new(seed).fork(5);
    let mut failures = Vec::new();
    let workers = spawn_workers(BULK_QUEUE, 3, 8).await;
    let ids = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..scaled(400) {
        let id = enqueue_failing(&mut rng).await;
        ids.lock().unwrap().push(id);
    }
    if !settle(database, BULK_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the first runs did not settle"));
    }

    let until = Instant::now() + Duration::from_secs(scaled(6));
    let mut retriers = Vec::new();
    for retrier in 0..2 {
        let mut rng = rng.fork(500 + retrier);
        retriers.push(tokio::spawn(async move {
            let mut summaries: Vec<jalari::Result<RetrySummary>> = Vec::new();
            let filter = JobFilter::new().queue(BULK_QUEUE);
            while Instant::now() < until {
                summaries.push(admin::retry_failed(&filter).await);
                tokio::time::sleep(rng.millis(20, 200)).await;
            }
            summaries
        }));
    }
    let enqueuer = {
        let ids = Arc::clone(&ids);
        let mut rng = rng.fork(550);
        tokio::spawn(async move {
            while Instant::now() < until {
                let id = enqueue_failing(&mut rng).await;
                ids.lock().unwrap().push(id);
                tokio::time::sleep(rng.millis(2, 15)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    jalari::cluster::update(|config| {
        config.housekeeping_enabled = true;
        config.housekeeping_interval = Duration::from_secs(1);
        config.failed_retention = Some(Duration::ZERO);
    })
    .await
    .unwrap();
    let sweeper = spawn_workers(BULK_QUEUE, 1, 2).await;
    let mut summaries = Vec::new();
    for retrier in retriers {
        summaries.extend(retrier.await.unwrap());
    }
    enqueuer.await.unwrap();
    let ids: Vec<i64> = ids.lock().unwrap().clone();
    let count = u64::try_from(ids.len()).unwrap();
    if !settle(database, BULK_QUEUE, SETTLE_TIMEOUT).await {
        failures.push(format!("{label}: the retried jobs did not settle"));
    }
    stop_all(sweeper).await;
    stop_all(workers).await;
    jalari::cluster::update(|config| {
        config.housekeeping_enabled = false;
        config.failed_retention = None;
    })
    .await
    .unwrap();

    let (mut retried, mut key_taken) = (0, 0);
    for summary in &summaries {
        match summary {
            Ok(summary) => {
                retried += summary.retried;
                key_taken += summary.key_taken;
            }
            Err(e) => failures.push(format!("{label}: retry_failed returned an error: {e:?}")),
        }
    }
    if key_taken != 0 {
        failures.push(format!(
            "{label}: {key_taken} jobs without a job_key were reported as KeyTaken"
        ));
    }
    let runs: usize = ids.iter().map(|&id| runs_of(id).len()).sum();
    if u64::try_from(runs).unwrap() != count + retried {
        failures.push(format!(
            "{label}: {runs} runs, but {count} jobs plus {retried} retried should give {}",
            count + retried
        ));
    }
    let jobs = jobs_by_id(database).await;
    let history = history_by_job(database).await;
    let mut present = 0;
    for &id in &ids {
        let Some(job) = jobs.get(&id) else {
            continue;
        };
        present += 1;
        let rows = history.get(&id).map_or(&[][..], Vec::as_slice);
        let ran = runs_of(id).len();
        if resets(rows) + 1 != ran {
            failures.push(format!(
                "{label}: job {id} ran {ran} times with {} retry entries in its history",
                resets(rows)
            ));
        }
        failures.extend(check_history(label, job, rows));
    }
    if present == ids.len() || retried == 0 {
        failures.push(format!(
            "{label}: housekeeping removed {} jobs and retry_failed retried {retried}, so the \
             race never happened",
            ids.len() - present
        ));
    }
    let impossible: Vec<(i64, String, i32, i32)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id, state, attempts, max_attempts FROM {}
         WHERE (state = 'failed' AND attempts = 0)
             OR (state IN ('scheduled', 'enqueued', 'running') AND attempts > max_attempts)",
        database.table("job")
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap();
    for (id, state, attempts, max_attempts) in impossible {
        failures.push(format!(
            "{label}: job {id} is {state} with {attempts} of {max_attempts} attempts"
        ));
    }
    eprintln!(
        "{label}: {count} jobs, {} retry_failed calls retried {retried}, {runs} runs, {} removed \
         by housekeeping",
        summaries.len(),
        ids.len() - present
    );
    failures
}
