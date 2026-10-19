mod common;

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use common::{Database, OCCUPANCY, Rng, RunningWorker};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, EnqueueOptions, EnqueueOutcome, ErrorKind, ExponentialBackoff, Job,
    JobError, JobId, JobResult, OnConflict, Worker,
};
use serde::{Deserialize, Serialize};

const POOL: u32 = 12;
const RUNNING_GRACE: Duration = Duration::from_millis(300);
const WAKEUP_BOUND: Duration = Duration::from_secs(5);
const STALL_BOUND: Duration = Duration::from_secs(5);
const TAKEOVER_DELAY: Duration = Duration::from_secs(6);
const CLOCK_SLACK: Duration = Duration::from_millis(50);
const INTERRUPTED: &str = "the worker stopped during the last attempt";

static NEXT_TOKEN: AtomicI64 = AtomicI64::new(1);
static RUNS: LazyLock<Mutex<Vec<Run>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static SAW_USER_ROW: LazyLock<Mutex<HashMap<i64, bool>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static CHILD_ENQUEUES: LazyLock<Mutex<Vec<ChildEnqueue>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));
static PROBE: OnceLock<(PgPool, String)> = OnceLock::new();

fn token() -> i64 {
    NEXT_TOKEN.fetch_add(1, Ordering::SeqCst)
}

#[derive(Clone, Debug)]
struct Run {
    kind: &'static str,
    job: i64,
    token: i64,
    attempt: i32,
    started: Instant,
    finished: Option<Instant>,
    ok: bool,
}

struct RunGuard {
    index: usize,
    ok: bool,
}

impl RunGuard {
    fn start(kind: &'static str, token: i64) -> Self {
        let job = jalari::current_job().expect("job bodies run inside a job");
        let mut runs = RUNS.lock().unwrap();
        runs.push(Run {
            kind,
            job: job.id().0,
            token,
            attempt: job.attempt(),
            started: Instant::now(),
            finished: None,
            ok: false,
        });
        Self {
            index: runs.len() - 1,
            ok: false,
        }
    }

    fn succeed(mut self) -> JobResult {
        self.ok = true;
        Ok(())
    }
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        let mut runs = RUNS.lock().unwrap();
        runs[self.index].finished = Some(Instant::now());
        runs[self.index].ok = self.ok;
    }
}

fn runs_of(kind: &str) -> Vec<Run> {
    RUNS.lock()
        .unwrap()
        .iter()
        .filter(|run| run.kind == kind)
        .cloned()
        .collect()
}

#[derive(Serialize, Deserialize)]
struct Storm {
    key: String,
    token: i64,
    work_ms: u64,
    fail_first: bool,
}

#[jalari::job(queue = "storm")]
impl Job for Storm {
    const NAME: &'static str = "race_storm";

    async fn run(self) -> JobResult {
        let job = jalari::current_job().unwrap();
        let _seat = OCCUPANCY.enter(
            vec![
                format!("storm-key:{}", self.key),
                format!("storm-job:{}", job.id()),
            ],
            format!(
                "job {} token {} attempt {}",
                job.id(),
                self.token,
                job.attempt()
            ),
        );
        let run = RunGuard::start("storm", self.token);
        tokio::time::sleep(Duration::from_millis(self.work_ms)).await;
        if self.fail_first && job.attempt() == 1 {
            return Err(JobError::new(format!(
                "token {} fails its first attempt",
                self.token
            )));
        }
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct Holder {
    token: i64,
    hold_ms: u64,
}

#[jalari::job(queue = "gate")]
impl Job for Holder {
    const NAME: &'static str = "race_holder";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("holder", self.token);
        tokio::time::sleep(Duration::from_millis(self.hold_ms)).await;
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct Gated {
    token: i64,
}

#[jalari::job(queue = "gate")]
impl Job for Gated {
    const NAME: &'static str = "race_gated";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("gated", self.token);
        tokio::time::sleep(Duration::from_millis(20)).await;
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct Window {
    token: i64,
}

#[jalari::job(queue = "window")]
impl Job for Window {
    const NAME: &'static str = "race_window";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("window", self.token);
        tokio::time::sleep(Duration::from_millis(50)).await;
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct InTransaction {
    token: i64,
}

#[jalari::job(queue = "txn")]
impl Job for InTransaction {
    const NAME: &'static str = "race_txn";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("txn", self.token);
        let (probe, users) = PROBE.get().expect("probe pool is set");
        let seen: bool = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM {users} WHERE token = $1)"
        )))
        .bind(self.token)
        .fetch_one(probe)
        .await?;
        SAW_USER_ROW.lock().unwrap().insert(self.token, seen);
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct Wake {
    token: i64,
    work_ms: u64,
}

#[jalari::job(queue = "wake")]
impl Job for Wake {
    const NAME: &'static str = "race_wake";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("wake", self.token);
        tokio::time::sleep(Duration::from_millis(self.work_ms)).await;
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct Parent {
    token: i64,
    children: Vec<i64>,
}

#[jalari::job(queue = "family")]
impl Job for Parent {
    const NAME: &'static str = "race_parent";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("parent", self.token);
        for child in &self.children {
            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                jalari::enqueue(&Child { token: *child }),
            )
            .await;
            let error = match result {
                Ok(Ok(_)) => None,
                Ok(Err(e)) => Some(format!("{:?}: {}", e.kind, e.msg)),
                Err(_) => Some("did not finish within 10 s".to_owned()),
            };
            CHILD_ENQUEUES.lock().unwrap().push(ChildEnqueue {
                took: started.elapsed(),
                error: error.clone(),
            });
            if let Some(error) = error {
                return Err(JobError::new(format!(
                    "parent token {} could not enqueue child {child}: {error}",
                    self.token
                )));
            }
        }
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct Child {
    token: i64,
}

#[jalari::job(queue = "family")]
impl Job for Child {
    const NAME: &'static str = "race_child";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("child", self.token);
        tokio::time::sleep(Duration::from_millis(30)).await;
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct Gap {
    token: i64,
    hold_ms: u64,
    fail: bool,
}

#[jalari::job(queue = "gap")]
impl Job for Gap {
    const NAME: &'static str = "race_gap";

    async fn run(self) -> JobResult {
        let run = RunGuard::start("gap", self.token);
        tokio::time::sleep(Duration::from_millis(self.hold_ms)).await;
        if self.fail {
            return Err(JobError::new(format!("token {} fails", self.token)));
        }
        run.succeed()
    }
}

#[derive(Serialize, Deserialize)]
struct GapOnce {
    token: i64,
    hold_ms: u64,
}

#[jalari::job(queue = "gap")]
impl Job for GapOnce {
    const NAME: &'static str = "race_gap_once";
    const MAX_ATTEMPTS: i32 = 1;

    async fn run(self) -> JobResult {
        let run = RunGuard::start("gap", self.token);
        tokio::time::sleep(Duration::from_millis(self.hold_ms)).await;
        run.succeed()
    }
}

#[derive(Clone, Debug)]
struct ChildEnqueue {
    took: Duration,
    error: Option<String>,
}

#[derive(Clone, Debug)]
struct Call {
    key: String,
    token: i64,
    started: Instant,
    ended: Instant,
    result: Result<EnqueueOutcome, ErrorKind>,
}

#[derive(Debug, sqlx::FromRow)]
struct PayloadRow {
    id: i64,
    state: String,
    attempts: i32,
    max_attempts: i32,
    token: i64,
}

async fn payload_rows(database: &Database, task: &str) -> Vec<PayloadRow> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id, state, attempts, max_attempts,
             (convert_from(payload, 'UTF8')::jsonb ->> 'token')::bigint AS token
         FROM {} WHERE task = $1 ORDER BY id",
        database.table("job")
    )))
    .bind(task)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn pending_of(database: &Database, task: &str) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE task = $1 AND state IN ('scheduled', 'enqueued')",
        database.table("job")
    )))
    .bind(task)
    .fetch_one(&database.pool)
    .await
    .unwrap()
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

fn build_worker(queue: &str, concurrency: usize, poll: Duration) -> jalari::WorkerBuilder {
    Worker::builder()
        .queue(queue, concurrency)
        .config(common::worker_config(poll))
        .retry_policy(common::fast_retry())
        .scheduler(false)
}

async fn start_worker(queue: &str, concurrency: usize, poll: Duration) -> RunningWorker {
    RunningWorker::spawn(
        build_worker(queue, concurrency, poll)
            .build()
            .await
            .unwrap(),
    )
}

fn outcome_counts(calls: &[Call]) -> String {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for call in calls {
        let label = match &call.result {
            Ok(EnqueueOutcome::Inserted(_)) => "Inserted".to_owned(),
            Ok(EnqueueOutcome::Kept(_)) => "Kept".to_owned(),
            Ok(EnqueueOutcome::Replaced(_)) => "Replaced".to_owned(),
            Ok(EnqueueOutcome::Running(_)) => "Running".to_owned(),
            Err(kind) => format!("{kind:?}"),
        };
        *counts.entry(label).or_default() += 1;
    }
    let mut counts: Vec<_> = counts.into_iter().collect();
    counts.sort();
    format!("{counts:?}")
}

fn written_id(call: &Call) -> Option<i64> {
    match call.result {
        Ok(EnqueueOutcome::Inserted(id) | EnqueueOutcome::Replaced(id)) => Some(id.0),
        _ => None,
    }
}

fn running_at(runs: &[Run], job: i64, from: Instant, to: Instant) -> bool {
    runs.iter()
        .any(|run| run.job == job && run.started <= to && held_until(run, from))
}

fn held_until(run: &Run, moment: Instant) -> bool {
    run.finished
        .is_none_or(|finished| finished + RUNNING_GRACE >= moment)
}

fn next_start(runs: &[Run], job: i64, after: Instant) -> Option<Duration> {
    runs.iter()
        .filter(|run| run.job == job && run.started > after)
        .map(|run| run.started - after)
        .min()
}

async fn sample_duplicates(
    observer: PgPool,
    job_table: String,
    active: Arc<AtomicBool>,
) -> Vec<String> {
    let mut found = Vec::new();
    while active.load(Ordering::SeqCst) {
        let rows: Vec<(String, i64)> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT job_key, count(*) FROM {job_table}
             WHERE job_key IS NOT NULL AND state IN ('scheduled', 'enqueued')
             GROUP BY job_key HAVING count(*) > 1"
        )))
        .fetch_all(&observer)
        .await
        .unwrap();
        for (key, count) in rows {
            found.push(format!(
                "{count} jobs were pending at once for job_key {key}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    found
}

fn audit_runs_against_calls(seed: u64, label: &str, calls: &[Call], runs: &[Run]) -> Vec<String> {
    let mut violations = Vec::new();
    let by_token: HashMap<i64, &Call> = calls.iter().map(|call| (call.token, call)).collect();
    for run in runs {
        let Some(call) = by_token.get(&run.token) else {
            violations.push(format!(
                "{label} seed {seed}: token {} ran as job {} attempt {} but was never enqueued \
                 by a committed call",
                run.token, run.job, run.attempt
            ));
            continue;
        };
        if written_id(call) != Some(run.job) {
            violations.push(format!(
                "{label} seed {seed}: token {} ran as job {} attempt {}, expected only tokens \
                 whose enqueue returned Inserted/Replaced for that job, but {} returned {:?}",
                run.token, run.job, run.attempt, call.key, call.result
            ));
        }
        for later in calls {
            if later.token != run.token
                && later.result == Ok(EnqueueOutcome::Replaced(JobId(run.job)))
                && call.ended < later.started
                && later.ended < run.started
            {
                violations.push(format!(
                    "{label} seed {seed}: job {} ran stale token {} (attempt {}) {:?} after \
                     Replace with token {} returned Replaced, expected only the newer payload",
                    run.job,
                    run.token,
                    run.attempt,
                    run.started - later.ended,
                    later.token
                ));
            }
        }
    }
    violations
}

fn timeline(job: i64, calls: &[Call], runs: &[Run]) -> String {
    let mut events: Vec<(Instant, String)> = Vec::new();
    for call in calls {
        if call
            .result
            .as_ref()
            .is_ok_and(|outcome| outcome.id().0 == job)
        {
            events.push((call.started, format!("call token {} started", call.token)));
            events.push((
                call.ended,
                format!("call token {} returned {:?}", call.token, call.result),
            ));
        }
    }
    for run in runs.iter().filter(|run| run.job == job) {
        events.push((
            run.started,
            format!("run token {} attempt {} started", run.token, run.attempt),
        ));
        if let Some(finished) = run.finished {
            events.push((
                finished,
                format!(
                    "run token {} attempt {} ok {}",
                    run.token, run.attempt, run.ok
                ),
            ));
        }
    }
    events.sort_by_key(|(at, _)| *at);
    let Some((origin, _)) = events.first().cloned() else {
        return "no events".to_owned();
    };
    events
        .iter()
        .map(|(at, event)| format!("+{:?} {event}", *at - origin))
        .collect::<Vec<_>>()
        .join("; ")
}

fn audit_final_payloads(
    seed: u64,
    label: &str,
    rows: &[PayloadRow],
    calls: &[Call],
    runs: &[Run],
) -> Vec<String> {
    let mut violations = Vec::new();
    for row in rows {
        let successes: Vec<&Run> = runs
            .iter()
            .filter(|run| run.job == row.id && run.ok)
            .collect();
        if row.state != "succeeded" {
            violations.push(format!(
                "{label} seed {seed}: job {} ended {}, expected succeeded: {row:?}",
                row.id, row.state
            ));
        }
        if row.attempts > row.max_attempts {
            violations.push(format!(
                "{label} seed {seed}: job {} has {} attempts over max {}: {row:?}",
                row.id, row.attempts, row.max_attempts
            ));
        }
        match successes.as_slice() {
            [run] => {
                if run.token != row.token {
                    violations.push(format!(
                        "{label} seed {seed}: job {} succeeded running token {} but its stored \
                         payload is token {}, expected the last written payload to run: {row:?}; \
                         timeline: {}",
                        row.id,
                        run.token,
                        row.token,
                        timeline(row.id, calls, runs)
                    ));
                }
                if run.attempt != row.attempts {
                    violations.push(format!(
                        "{label} seed {seed}: job {} succeeded on attempt {} but the row \
                         records {} attempts: {row:?}",
                        row.id, run.attempt, row.attempts
                    ));
                }
            }
            successes => violations.push(format!(
                "{label} seed {seed}: job {} succeeded {} times, expected exactly once: {row:?}",
                row.id,
                successes.len()
            )),
        }
    }
    violations
}

fn audit_kept_and_running(
    seed: u64,
    label: &str,
    calls: &[Call],
    runs: &[Run],
) -> (Vec<String>, usize) {
    let mut violations = Vec::new();
    let mut claim_in_progress = 0;
    for call in calls {
        match call.result {
            Ok(EnqueueOutcome::Kept(id)) => {
                if !runs
                    .iter()
                    .any(|run| run.job == id.0 && held_until(run, call.started))
                {
                    violations.push(format!(
                        "{label} seed {seed}: enqueue of token {} on {} returned Kept({id}) but \
                         job {id} had finished before the call started or never ran",
                        call.token, call.key
                    ));
                }
            }
            Ok(EnqueueOutcome::Running(id)) => {
                if running_at(runs, id.0, call.started, call.ended) {
                    continue;
                }
                match next_start(runs, id.0, call.ended) {
                    Some(gap) if gap <= RUNNING_GRACE => claim_in_progress += 1,
                    gap => violations.push(format!(
                        "{label} seed {seed}: replace of token {} on {} returned Running({id}) \
                         but job {id} was not running during the call; its next run started \
                         {gap:?} after the call returned",
                        call.token, call.key
                    )),
                }
            }
            _ => {}
        }
    }
    (violations, claim_in_progress)
}

async fn check_storm(database: &Database, observer: &PgPool, seed: u64) -> Vec<String> {
    let scale = common::scale();
    let mut rng = Rng::new(seed);
    let keys: Vec<String> = (0..10).map(|k| format!("storm-{k}")).collect();
    let deadline = Instant::now() + Duration::from_secs(6 * scale);
    let workers = vec![
        start_worker("storm", 3, Duration::from_millis(100)).await,
        start_worker("storm", 3, Duration::from_millis(100)).await,
    ];
    let sampling = Arc::new(AtomicBool::new(true));
    let sampler = tokio::spawn(sample_duplicates(
        observer.clone(),
        database.table("job"),
        Arc::clone(&sampling),
    ));
    let mut tasks = Vec::new();
    for task in 0..8 {
        let mut rng = rng.fork(task);
        let keys = keys.clone();
        tasks.push(tokio::spawn(async move {
            let mut calls = Vec::new();
            while Instant::now() < deadline {
                let key = rng.pick(&keys).clone();
                let mode = if rng.chance(50) {
                    OnConflict::Replace
                } else {
                    OnConflict::KeepExisting
                };
                let job = Storm {
                    key: key.clone(),
                    token: token(),
                    work_ms: 5 + rng.below(40),
                    fail_first: rng.chance(15),
                };
                let mut options = EnqueueOptions::new().job_key(&key).on_conflict(mode);
                if rng.chance(25) {
                    options = options.delay(rng.millis(10, 60));
                }
                let started = Instant::now();
                let result = jalari::enqueue_with(&job, options)
                    .await
                    .map_err(|e| e.kind);
                calls.push(Call {
                    key,
                    token: job.token,
                    started,
                    ended: Instant::now(),
                    result,
                });
                tokio::time::sleep(rng.millis(0, 15)).await;
            }
            calls
        }));
    }
    let mut calls = Vec::new();
    for task in tasks {
        calls.extend(task.await.unwrap());
    }
    let settled = settle(Duration::from_secs(30), || async {
        pending_of(database, "race_storm").await == 0
    })
    .await;
    sampling.store(false, Ordering::SeqCst);
    let mut violations = sampler.await.unwrap();
    for worker in workers {
        worker.stop().await;
    }

    if !settled {
        violations.push(format!(
            "storm seed {seed}: jobs still pending 30 s after the last enqueue"
        ));
    }
    violations.extend(
        OCCUPANCY
            .violations()
            .into_iter()
            .filter(|v| v.starts_with("storm-"))
            .map(|v| format!("storm seed {seed}: overlap {v}")),
    );
    let contention = calls
        .iter()
        .filter(|call| call.result == Err(ErrorKind::JobKeyContention))
        .count();
    for call in &calls {
        if let Err(kind) = &call.result
            && *kind != ErrorKind::JobKeyContention
        {
            violations.push(format!(
                "storm seed {seed}: enqueue of token {} on {} failed with {kind:?}",
                call.token, call.key
            ));
        }
    }
    let runs = runs_of("storm");
    let rows = payload_rows(database, "race_storm").await;
    violations.extend(audit_runs_against_calls(seed, "storm", &calls, &runs));
    violations.extend(audit_final_payloads(seed, "storm", &rows, &calls, &runs));
    let (kept_running, claim_in_progress) = audit_kept_and_running(seed, "storm", &calls, &runs);
    violations.extend(kept_running);
    eprintln!(
        "storm: {} calls {}, {} jobs, {} runs, JobKeyContention {contention}, Running while \
         the claim was still in progress {claim_in_progress}",
        calls.len(),
        outcome_counts(&calls),
        rows.len(),
        runs.len()
    );
    violations
}

async fn check_running_outcome(database: &Database, seed: u64) -> Vec<String> {
    let scale = common::scale();
    let mut rng = Rng::new(seed);
    let worker = start_worker("gate", 4, Duration::from_millis(5)).await;
    let mut violations = Vec::new();
    let mut all_calls = Vec::new();
    for round in 0..scale {
        let queue_key = format!("gate-q-{round}");
        let job_key = format!("gate-k-{round}");
        let holder = token();
        jalari::enqueue_with(
            &Holder {
                token: holder,
                hold_ms: 1500,
            },
            EnqueueOptions::new().queue_key(&queue_key),
        )
        .await
        .unwrap();
        if !settle(Duration::from_secs(5), || async {
            runs_of("holder").iter().any(|run| run.token == holder)
        })
        .await
        {
            violations.push(format!(
                "running_outcome seed {seed}: holder {holder} did not start"
            ));
            continue;
        }
        let mut calls = Vec::new();
        loop {
            let holder_done = runs_of("holder")
                .iter()
                .any(|run| run.token == holder && run.finished.is_some());
            if holder_done {
                break;
            }
            let gated = Gated { token: token() };
            let options = EnqueueOptions::new()
                .job_key(&job_key)
                .queue_key(&queue_key)
                .on_conflict(OnConflict::Replace);
            let started = Instant::now();
            let result = jalari::enqueue_with(&gated, options)
                .await
                .map_err(|e| e.kind);
            calls.push(Call {
                key: job_key.clone(),
                token: gated.token,
                started,
                ended: Instant::now(),
                result,
            });
            tokio::time::sleep(rng.millis(1, 5)).await;
        }
        let settled = settle(Duration::from_secs(10), || async {
            pending_of(database, "race_gated").await == 0
        })
        .await;
        if !settled {
            violations.push(format!(
                "running_outcome seed {seed}: gated job on {job_key} still pending 10 s after \
                 its queue_key was released"
            ));
        }
        let runs = runs_of("gated");
        let holder_run = runs_of("holder")
            .into_iter()
            .find(|run| run.token == holder)
            .unwrap();
        let mut dropped = 0;
        for call in &calls {
            if let Ok(EnqueueOutcome::Running(id)) = call.result
                && !running_at(&runs, id.0, call.started, call.ended)
            {
                dropped += 1;
                if dropped <= 3 {
                    violations.push(format!(
                        "running_outcome seed {seed}: Replace of token {} on {job_key} returned \
                         Running({id}) {:?} into holder {holder}'s run, but job {id} was not \
                         running (its queue_key {queue_key} was held); its next run started {:?} \
                         after the call returned",
                        call.token,
                        call.started - holder_run.started,
                        next_start(&runs, id.0, call.ended)
                    ));
                }
            }
        }
        if dropped > 3 {
            violations.push(format!(
                "running_outcome seed {seed}: {} more Replace calls on {job_key} returned Running \
                 for a job that was not running",
                dropped - 3
            ));
        }
        eprintln!(
            "running_outcome round {round}: {} calls {}, Running for a waiting job {dropped}",
            calls.len(),
            outcome_counts(&calls)
        );
        all_calls.extend(calls);
    }
    worker.stop().await;
    let runs = runs_of("gated");
    let rows = payload_rows(database, "race_gated").await;
    violations.extend(audit_runs_against_calls(
        seed,
        "running_outcome",
        &all_calls,
        &runs,
    ));
    violations.extend(audit_final_payloads(
        seed,
        "running_outcome",
        &rows,
        &all_calls,
        &runs,
    ));
    violations
}

struct IsolatedWorker {
    shutdown: CancellationToken,
    freeze: tokio::sync::mpsc::UnboundedSender<(
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    thread: std::thread::JoinHandle<()>,
}

impl IsolatedWorker {
    async fn spawn(queue: &'static str) -> Self {
        let shutdown = CancellationToken::new();
        let (freeze, mut requests) = tokio::sync::mpsc::unbounded_channel::<(
            tokio::sync::oneshot::Sender<()>,
            std::sync::mpsc::Receiver<()>,
        )>();
        let (ready, started) = tokio::sync::oneshot::channel();
        let token = shutdown.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                tokio::spawn(async move {
                    while let Some((frozen, thaw)) = requests.recv().await {
                        frozen.send(()).ok();
                        thaw.recv().ok();
                    }
                });
                let worker = build_worker(queue, 1, Duration::from_millis(50))
                    .build()
                    .await
                    .unwrap();
                ready.send(()).unwrap();
                worker.run(token).await;
            });
        });
        started.await.unwrap();
        Self {
            shutdown,
            freeze,
            thread,
        }
    }

    async fn freeze(&self) -> std::sync::mpsc::Sender<()> {
        let (frozen, confirmed) = tokio::sync::oneshot::channel();
        let (thaw, thawed) = std::sync::mpsc::channel();
        self.freeze.send((frozen, thawed)).unwrap();
        confirmed.await.unwrap();
        thaw
    }

    async fn stop(self) {
        self.shutdown.cancel();
        tokio::task::spawn_blocking(move || self.thread.join().unwrap())
            .await
            .unwrap();
    }
}

async fn sleeping_backends(database: &Database, observer: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND application_name = $1
             AND wait_event = 'PgSleep'",
    )
    .bind(&database.schema_name)
    .fetch_one(observer)
    .await
    .unwrap()
}

async fn warm_pool(database: &Database) {
    let acquiring: Vec<_> = (0..POOL)
        .map(|_| tokio::spawn(database.pool.acquire()))
        .collect();
    let mut connections = Vec::new();
    for handle in acquiring {
        connections.push(handle.await.unwrap().unwrap());
    }
}

async fn check_window(database: &Database, observer: &PgPool, seed: u64) -> Vec<String> {
    let scale = common::scale();
    let gate = database.table("race_gate");
    let mut violations = Vec::new();
    warm_pool(database).await;
    let frozen_worker = IsolatedWorker::spawn("window").await;
    for round in 0..2 * scale {
        let key = format!("window-{seed}-{round}");
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {gate} (job_key, sleep_ms) VALUES ($1, 300)"
        )))
        .bind(&key)
        .execute(observer)
        .await
        .unwrap();
        let first = token();
        let options = || {
            EnqueueOptions::new()
                .job_key(&key)
                .on_conflict(OnConflict::Replace)
        };
        let inserted = jalari::enqueue_with(&Window { token: first }, options())
            .await
            .unwrap();
        let id = inserted.id();
        if !settle(Duration::from_secs(5), || async {
            sleeping_backends(database, observer).await > 0
        })
        .await
        {
            violations.push(format!(
                "window seed {seed} round {round}: the isolated worker never claimed job {id}"
            ));
            continue;
        }
        let thaw = frozen_worker.freeze().await;
        let frozen_at = Instant::now();
        let committed = settle(Duration::from_secs(5), || async {
            payload_rows(database, "race_window")
                .await
                .iter()
                .any(|row| row.id == id.0 && row.attempts == 1)
        })
        .await;
        let second = token();
        let replaced_at = Instant::now();
        let replaced = jalari::enqueue_with(&Window { token: second }, options()).await;
        let replaced_at = replaced_at.elapsed();
        let rival = start_worker("window", 1, Duration::from_millis(50)).await;
        let rival_claimed = settle(Duration::from_secs(5), || async {
            sleeping_backends(database, observer).await > 0
        })
        .await;
        thaw.send(()).unwrap();
        let frozen_for = frozen_at.elapsed();
        let settled = settle(Duration::from_secs(10), || async {
            pending_of(database, "race_window").await == 0
        })
        .await;
        rival.stop().await;
        let rows = payload_rows(database, "race_window").await;
        let row = rows.iter().find(|row| row.id == id.0);
        let runs: Vec<Run> = runs_of("window")
            .into_iter()
            .filter(|run| run.job == id.0)
            .collect();
        let history: Vec<(String, i32)> = database
            .history()
            .await
            .into_iter()
            .filter(|entry| entry.job_id == id.0)
            .map(|entry| (entry.state, entry.attempt))
            .collect();
        let ran: Vec<(i64, i32, bool)> = runs
            .iter()
            .map(|run| (run.token, run.attempt, run.ok))
            .collect();
        eprintln!(
            "window round {round}: claim committed {committed}, replace {replaced:?} in \
             {replaced_at:?}, rival claimed {rival_claimed}, first worker frozen for \
             {frozen_for:?}, settled {settled}, runs \
             (token, attempt, ok) {ran:?}, history {history:?}, row {row:?}"
        );
        if replaced != Ok(EnqueueOutcome::Replaced(id)) {
            violations.push(format!(
                "window seed {seed} round {round}: replace after the claim committed returned \
                 {replaced:?}, expected Replaced({id}); row {row:?}"
            ));
            continue;
        }
        if runs.iter().any(|run| run.token == first) {
            violations.push(format!(
                "window seed {seed} round {round}: job {id} ran the replaced token {first} after \
                 Replace returned Replaced({id}) with token {second}; runs (token, attempt, ok) \
                 {ran:?}, row {row:?}"
            ));
        }
        if runs
            .iter()
            .filter(|run| run.token == second && run.ok)
            .count()
            != 1
        {
            violations.push(format!(
                "window seed {seed} round {round}: job {id} never ran token {second}, the payload \
                 Replace returned Replaced({id}) for; runs (token, attempt, ok) {ran:?}, row {row:?}"
            ));
        }
    }
    frozen_worker.stop().await;
    violations
}

fn gap_runs(token: i64) -> Vec<Run> {
    runs_of("gap")
        .into_iter()
        .filter(|run| run.token == token)
        .collect()
}

async fn job_history(database: &Database, id: JobId) -> Vec<(String, i32, Option<String>)> {
    database
        .history()
        .await
        .into_iter()
        .filter(|entry| entry.job_id == id.0)
        .map(|entry| (entry.state, entry.attempt, entry.error))
        .collect()
}

fn interruptions(history: &[(String, i32, Option<String>)]) -> usize {
    history
        .iter()
        .filter(|(_, _, error)| error.as_deref() == Some(INTERRUPTED))
        .count()
}

async fn owner_sessions(database: &Database, observer: &PgPool, id: JobId) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {job} AS job
         JOIN pg_locks AS locks ON locks.locktype = 'transactionid'
             AND locks.transactionid = job.xmax AND locks.mode = 'ExclusiveLock'
         WHERE job.id = $1",
        job = database.table("job")
    )))
    .bind(id.0)
    .fetch_one(observer)
    .await
    .unwrap()
}

async fn freeze_and_kill(
    database: &Database,
    observer: &PgPool,
    worker: &IsolatedWorker,
    id: JobId,
    token: i64,
) -> Result<std::sync::mpsc::Sender<()>, String> {
    if !settle(Duration::from_secs(5), || async {
        !gap_runs(token).is_empty()
    })
    .await
    {
        return Err(format!("token {token} of job {id} never started"));
    }
    let thaw = worker.freeze().await;
    let terminated: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FILTER (WHERE pg_terminate_backend(locks.pid)) FROM {job} AS job
         JOIN pg_locks AS locks ON locks.locktype = 'transactionid'
             AND locks.transactionid = job.xmax AND locks.mode = 'ExclusiveLock'
         WHERE job.id = $1",
        job = database.table("job")
    )))
    .bind(id.0)
    .fetch_one(observer)
    .await
    .unwrap();
    let gone = settle(Duration::from_secs(5), || async {
        owner_sessions(database, observer, id).await == 0
    })
    .await;
    if terminated != 1 || !gone {
        thaw.send(()).ok();
        return Err(format!(
            "could not end the session running job {id}: terminated {terminated}, gone {gone}"
        ));
    }
    Ok(thaw)
}

async fn check_replace_interrupted(
    database: &Database,
    observer: &PgPool,
    seed: u64,
) -> Vec<String> {
    let mut violations = Vec::new();
    warm_pool(database).await;
    let frozen_worker = IsolatedWorker::spawn("gap").await;
    for round in 0..2 {
        let exhausted = round == 1;
        let job_key = format!("gap-a-{seed}-{round}");
        let queue_key = format!("gap-a-q-{seed}-{round}");
        let options = || {
            EnqueueOptions::new()
                .job_key(&job_key)
                .queue_key(&queue_key)
                .on_conflict(OnConflict::Replace)
        };
        let old = token();
        let inserted = if exhausted {
            jalari::enqueue_with(
                &GapOnce {
                    token: old,
                    hold_ms: 30_000,
                },
                options(),
            )
            .await
        } else {
            jalari::enqueue_with(
                &Gap {
                    token: old,
                    hold_ms: 30_000,
                    fail: false,
                },
                options(),
            )
            .await
        };
        let id = inserted.unwrap().id();
        if !settle(Duration::from_secs(5), || async {
            !gap_runs(old).is_empty()
        })
        .await
        {
            violations.push(format!(
                "replace_interrupted seed {seed} round {round}: token {old} never started"
            ));
            continue;
        }
        let sibling = token();
        jalari::enqueue_with(
            &Gap {
                token: sibling,
                hold_ms: 20,
                fail: false,
            },
            EnqueueOptions::new().queue_key(&queue_key),
        )
        .await
        .unwrap();
        let thaw = match freeze_and_kill(database, observer, &frozen_worker, id, old).await {
            Ok(thaw) => thaw,
            Err(e) => {
                violations.push(format!(
                    "replace_interrupted seed {seed} round {round}: {e}"
                ));
                continue;
            }
        };
        let new = token();
        let wait = if exhausted {
            TAKEOVER_DELAY
        } else {
            Duration::from_secs(8)
        };
        let replace_started = Instant::now();
        let replaced = if exhausted {
            jalari::enqueue_with(
                &GapOnce {
                    token: new,
                    hold_ms: 20,
                },
                options(),
            )
            .await
        } else {
            jalari::enqueue_with(
                &Gap {
                    token: new,
                    hold_ms: 20,
                    fail: false,
                },
                options().delay(wait),
            )
            .await
        }
        .map_err(|e| e.kind);
        let rival = start_worker("gap", 2, Duration::from_millis(50)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        thaw.send(()).unwrap();
        let settled = settle(Duration::from_secs(20), || async {
            pending_of(database, "race_gap").await == 0
                && pending_of(database, "race_gap_once").await == 0
        })
        .await;
        rival.stop().await;

        let old_runs = gap_runs(old);
        let new_runs = gap_runs(new);
        let sibling_runs = gap_runs(sibling);
        let history = job_history(database, id).await;
        let interrupted_until = old_runs.first().and_then(|run| run.finished);
        let after = |run: &Run| run.started.saturating_duration_since(replace_started);
        eprintln!(
            "replace_interrupted round {round}: exhausted {exhausted}, replace {replaced:?}, \
             settled {settled}, old runs {}, new run started {:?} after the replace, sibling \
             started {:?} after the replace, history {history:?}",
            old_runs.len(),
            new_runs.iter().map(after).collect::<Vec<_>>(),
            sibling_runs.iter().map(after).collect::<Vec<_>>(),
        );
        let label = format!("replace_interrupted seed {seed} round {round} (job {id})");
        if replaced != Ok(EnqueueOutcome::Replaced(id)) {
            violations.push(format!(
                "{label}: Replace of a job whose worker session had ended returned {replaced:?}, \
                 expected Replaced({id})"
            ));
        }
        if interruptions(&history) != 1 {
            violations.push(format!(
                "{label}: the interrupted attempt of token {old} was recorded {} times, expected \
                 exactly one {INTERRUPTED:?} history row; history {history:?}",
                interruptions(&history)
            ));
        }
        if old_runs.len() != 1 {
            violations.push(format!(
                "{label}: replaced token {old} ran {} times, expected only its interrupted run",
                old_runs.len()
            ));
        }
        match new_runs.as_slice() {
            [run] if run.ok => {
                if after(run) + CLOCK_SLACK < wait {
                    violations.push(format!(
                        "{label}: the new payload (token {new}) started {:?} after the Replace, \
                         expected at least {wait:?} because the attempt it replaced was \
                         interrupted",
                        after(run)
                    ));
                }
                if interrupted_until.is_none_or(|until| run.started < until) {
                    violations.push(format!(
                        "{label}: the new payload (token {new}) started while the interrupted \
                         run of token {old} was still running"
                    ));
                }
            }
            runs => violations.push(format!(
                "{label}: the new payload (token {new}) ran {} times (ok {:?}), expected once",
                runs.len(),
                runs.iter().map(|run| run.ok).collect::<Vec<_>>()
            )),
        }
        match sibling_runs.as_slice() {
            [run] if run.ok => {
                if after(run) + CLOCK_SLACK < TAKEOVER_DELAY {
                    violations.push(format!(
                        "{label}: same-queue_key sibling (token {sibling}) started {:?} after the \
                         Replace, expected the queue_key fence to hold it for {TAKEOVER_DELAY:?}",
                        after(run)
                    ));
                }
                if interrupted_until.is_none_or(|until| run.started < until) {
                    violations.push(format!(
                        "{label}: same-queue_key sibling (token {sibling}) started while the \
                         interrupted run of token {old} still held the queue_key"
                    ));
                }
                if after(run) >= wait.max(TAKEOVER_DELAY + Duration::from_secs(1)) {
                    violations.push(format!(
                        "{label}: same-queue_key sibling (token {sibling}) started {:?} after the \
                         Replace, expected the fence to end {TAKEOVER_DELAY:?} after the \
                         interruption rather than at the replaced job's run time",
                        after(run)
                    ));
                }
            }
            runs => violations.push(format!(
                "{label}: sibling token {sibling} ran {} times, expected once",
                runs.len()
            )),
        }
    }
    frozen_worker.stop().await;
    violations
}

async fn blocked_markers(database: &Database, observer: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND application_name = $1
             AND wait_event_type = 'Lock' AND wait_event = 'advisory'",
    )
    .bind(&database.schema_name)
    .fetch_one(observer)
    .await
    .unwrap()
}

async fn check_replace_marking(database: &Database, observer: &PgPool, seed: u64) -> Vec<String> {
    let mark_gate = database.table("race_mark_gate");
    let job_key = format!("gap-c-{seed}");
    let lock_id = 0x6761_7000_0000 + i64::from(std::process::id());
    let options = || {
        EnqueueOptions::new()
            .job_key(&job_key)
            .on_conflict(OnConflict::Replace)
    };
    warm_pool(database).await;
    let frozen_worker = IsolatedWorker::spawn("gap").await;
    let mut holder = observer.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_id)
        .execute(&mut *holder)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {mark_gate} (job_key, lock_id) VALUES ($1, $2)"
    )))
    .bind(&job_key)
    .bind(lock_id)
    .execute(&mut *holder)
    .await
    .unwrap();
    let old = token();
    let id = jalari::enqueue_with(
        &Gap {
            token: old,
            hold_ms: 30_000,
            fail: false,
        },
        options(),
    )
    .await
    .unwrap()
    .id();
    let mut violations = Vec::new();
    let mut outcome = None;
    match freeze_and_kill(database, observer, &frozen_worker, id, old).await {
        Err(e) => violations.push(format!("replace_marking seed {seed}: {e}")),
        Ok(thaw) => {
            let rival = start_worker("gap", 1, Duration::from_millis(50)).await;
            let marking = settle(Duration::from_secs(10), || async {
                blocked_markers(database, observer).await > 0
            })
            .await;
            let new = token();
            let replace_started = Instant::now();
            let mut replace = tokio::spawn({
                let options = options();
                async move {
                    jalari::enqueue_with(
                        &Gap {
                            token: new,
                            hold_ms: 20,
                            fail: false,
                        },
                        options,
                    )
                    .await
                    .map_err(|e| e.kind)
                }
            });
            let early = tokio::time::timeout(Duration::from_millis(25), &mut replace)
                .await
                .ok();
            sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(lock_id)
                .execute(&mut *holder)
                .await
                .unwrap();
            let replaced = match early {
                Some(joined) => joined.unwrap(),
                None => replace.await.unwrap(),
            };
            let replace_took = replace_started.elapsed();
            thaw.send(()).unwrap();
            let settled = settle(Duration::from_secs(15), || async {
                pending_of(database, "race_gap").await == 0
            })
            .await;
            rival.stop().await;
            outcome = Some((
                marking,
                new,
                replace_started,
                replaced,
                replace_took,
                settled,
            ));
        }
    }
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(&mut *holder)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {mark_gate} WHERE job_key = $1"
    )))
    .bind(&job_key)
    .execute(&mut *holder)
    .await
    .unwrap();
    drop(holder);
    frozen_worker.stop().await;
    let Some((marking, new, replace_started, replaced, replace_took, settled)) = outcome else {
        return violations;
    };

    let old_runs = gap_runs(old);
    let new_runs = gap_runs(new);
    let history = job_history(database, id).await;
    eprintln!(
        "replace_marking: marker held the row {marking}, replace {replaced:?} in \
         {replace_took:?}, settled {settled}, old runs {}, new run started {:?} after the \
         replace, history {history:?}",
        old_runs.len(),
        new_runs
            .iter()
            .map(|run| run.started.saturating_duration_since(replace_started))
            .collect::<Vec<_>>(),
    );
    let label = format!("replace_marking seed {seed} (job {id})");
    if !marking {
        violations.push(format!(
            "{label}: no claimer was seen holding the dead row while marking it"
        ));
    }
    if replaced != Ok(EnqueueOutcome::Replaced(id)) {
        violations.push(format!(
            "{label}: Replace while a claimer held the dead row for a few milliseconds to mark it \
             interrupted returned {replaced:?} in {replace_took:?}, expected Replaced({id})"
        ));
    }
    if old_runs.len() != 1 {
        violations.push(format!(
            "{label}: replaced token {old} ran {} times, expected only its interrupted run; the \
             old payload ran again after the Replace",
            old_runs.len()
        ));
    }
    match new_runs.as_slice() {
        [run] if run.ok => {
            let after = run.started.saturating_duration_since(replace_started);
            if after + Duration::from_millis(500) < TAKEOVER_DELAY {
                violations.push(format!(
                    "{label}: the new payload (token {new}) started {after:?} after the Replace, \
                     expected the takeover delay of the interruption to hold"
                ));
            }
        }
        runs => violations.push(format!(
            "{label}: the new payload (token {new}) ran {} times, expected once",
            runs.len()
        )),
    }
    if interruptions(&history) != 1 {
        violations.push(format!(
            "{label}: the interrupted attempt was recorded {} times, expected once; history \
             {history:?}",
            interruptions(&history)
        ));
    }
    violations
}

async fn check_replace_timestamp(database: &Database, app: &PgPool, seed: u64) -> Vec<String> {
    let worker = RunningWorker::spawn(
        Worker::builder()
            .queue("gap", 1)
            .config(common::worker_config(Duration::from_millis(50)))
            .retry_policy(ExponentialBackoff {
                base: Duration::from_secs(60),
                max: Duration::from_secs(60),
                jitter: 0.0,
            })
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    );
    let job_key = format!("gap-b-{seed}");
    let options = || {
        EnqueueOptions::new()
            .job_key(&job_key)
            .on_conflict(OnConflict::Replace)
    };
    let mut transaction = app.begin().await.unwrap();
    sqlx::query("SELECT 1")
        .execute(&mut *transaction)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let old = token();
    let id = jalari::enqueue_with(
        &Gap {
            token: old,
            hold_ms: 20,
            fail: true,
        },
        EnqueueOptions::new().job_key(&job_key),
    )
    .await
    .unwrap()
    .id();
    let recorded = settle(Duration::from_secs(5), || async {
        !job_history(database, id).await.is_empty()
    })
    .await;
    let current = token();
    let replaced = jalari::enqueue_in(
        &mut transaction,
        &Gap {
            token: current,
            hold_ms: 2500,
            fail: false,
        },
        options(),
    )
    .await
    .map_err(|e| e.kind);
    transaction.commit().await.unwrap();
    let running = settle(Duration::from_secs(5), || async {
        !gap_runs(current).is_empty()
    })
    .await;
    let late = token();
    let late_started = Instant::now();
    let late_outcome = jalari::enqueue_with(
        &Gap {
            token: late,
            hold_ms: 20,
            fail: false,
        },
        options(),
    )
    .await
    .map_err(|e| e.kind);
    let late_took = late_started.elapsed();
    let settled = settle(Duration::from_secs(10), || async {
        pending_of(database, "race_gap").await == 0
    })
    .await;
    worker.stop().await;

    let current_runs = gap_runs(current);
    let history = job_history(database, id).await;
    eprintln!(
        "replace_timestamp: attempt of token {old} recorded {recorded}, replace inside the older \
         transaction {replaced:?}, token {current} running {running}, replace while it ran \
         {late_outcome:?} in {late_took:?}, settled {settled}, history {history:?}"
    );
    let label = format!("replace_timestamp seed {seed} (job {id})");
    let mut violations = Vec::new();
    if !recorded || !running {
        violations.push(format!(
            "{label}: setup failed, attempt of token {old} recorded {recorded}, token {current} \
             running {running}"
        ));
        return violations;
    }
    if replaced != Ok(EnqueueOutcome::Replaced(id)) {
        violations.push(format!(
            "{label}: Replace inside a transaction returned {replaced:?}, expected Replaced({id})"
        ));
    }
    if late_outcome != Ok(EnqueueOutcome::Running(id)) {
        violations.push(format!(
            "{label}: Replace while token {current} ran returned {late_outcome:?} after \
             {late_took:?}, expected Running({id}); the Replace committed by a transaction that \
             began before attempt 1 of token {old} was recorded made the new attempt 1 look \
             finished"
        ));
    }
    if !gap_runs(late).is_empty() {
        violations.push(format!(
            "{label}: token {late} ran although its Replace did not return Replaced"
        ));
    }
    if current_runs.len() != 1 || !current_runs[0].ok {
        violations.push(format!(
            "{label}: token {current} ran {} times, expected once",
            current_runs.len()
        ));
    }
    violations
}

async fn check_transactions(database: &Database, app: &PgPool, seed: u64) -> Vec<String> {
    let scale = common::scale();
    let mut rng = Rng::new(seed);
    let users = database.table("race_user");
    let worker = start_worker("txn", 4, Duration::from_millis(100)).await;
    let mut tasks = Vec::new();
    for task in 0..4 {
        let mut rng = rng.fork(task);
        let app = app.clone();
        let users = users.clone();
        tasks.push(tokio::spawn(async move {
            let mut calls = Vec::new();
            for _ in 0..150 * scale {
                let job = InTransaction { token: token() };
                let roll = rng.below(100);
                let key = format!("txn-{}", rng.below(3));
                let options = match roll {
                    0..60 => EnqueueOptions::new(),
                    60..80 => EnqueueOptions::new().job_key(&key),
                    _ => EnqueueOptions::new()
                        .job_key(&key)
                        .on_conflict(OnConflict::Replace),
                };
                let commit = rng.chance(50);
                let pause = rng.millis(0, 5);
                let started = Instant::now();
                let mut transaction = app.begin().await.unwrap();
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {users} (token) VALUES ($1)"
                )))
                .bind(job.token)
                .execute(&mut *transaction)
                .await
                .unwrap();
                let result = jalari::enqueue_in(&mut transaction, &job, options)
                    .await
                    .map_err(|e| e.kind);
                tokio::time::sleep(pause).await;
                let committed = commit && result.is_ok();
                if committed {
                    transaction.commit().await.unwrap();
                } else {
                    transaction.rollback().await.unwrap();
                }
                let key = if roll < 60 { String::new() } else { key };
                calls.push((
                    committed,
                    Call {
                        key,
                        token: job.token,
                        started,
                        ended: Instant::now(),
                        result,
                    },
                ));
            }
            calls
        }));
    }
    let mut calls = Vec::new();
    for task in tasks {
        calls.extend(task.await.unwrap());
    }
    let settled = settle(Duration::from_secs(30), || async {
        pending_of(database, "race_txn").await == 0
    })
    .await;
    worker.stop().await;

    let mut violations = Vec::new();
    if !settled {
        violations.push(format!(
            "transactions seed {seed}: jobs still pending 30 s after the last commit"
        ));
    }
    let runs = runs_of("txn");
    let rows = payload_rows(database, "race_txn").await;
    let seen = SAW_USER_ROW.lock().unwrap().clone();
    let committed: Vec<Call> = calls
        .iter()
        .filter(|(committed, _)| *committed)
        .map(|(_, call)| call.clone())
        .collect();
    for (was_committed, call) in &calls {
        if let Err(kind) = &call.result {
            violations.push(format!(
                "transactions seed {seed}: enqueue_in of token {} failed with {kind:?}",
                call.token
            ));
        }
        if !was_committed && runs.iter().any(|run| run.token == call.token) {
            violations.push(format!(
                "transactions seed {seed}: token {} ran although its transaction rolled back \
                 (enqueue returned {:?})",
                call.token, call.result
            ));
        }
    }
    for call in &committed {
        if let Ok(EnqueueOutcome::Kept(id) | EnqueueOutcome::Running(id)) = call.result
            && !rows.iter().any(|row| row.id == id.0)
        {
            violations.push(format!(
                "transactions seed {seed}: committed enqueue of token {} on {} returned {:?} \
                 naming a job that was never committed",
                call.token, call.key, call.result
            ));
        }
        if let Ok(EnqueueOutcome::Inserted(id)) = call.result
            && call.key.is_empty()
        {
            let ok_runs = runs
                .iter()
                .filter(|run| run.token == call.token && run.ok)
                .count();
            if ok_runs != 1 {
                violations.push(format!(
                    "transactions seed {seed}: committed job {id} (token {}) succeeded {ok_runs} \
                     times, expected exactly once",
                    call.token
                ));
            }
        }
    }
    for run in &runs {
        if seen.get(&run.token) != Some(&true) {
            violations.push(format!(
                "transactions seed {seed}: job {} (token {}) did not see the user row written \
                 in its enqueueing transaction",
                run.job, run.token
            ));
        }
    }
    violations.extend(audit_runs_against_calls(
        seed,
        "transactions",
        &committed,
        &runs,
    ));
    violations.extend(audit_final_payloads(
        seed,
        "transactions",
        &rows,
        &committed,
        &runs,
    ));
    violations.extend(audit_kept_and_running(seed, "transactions", &committed, &runs).0);
    eprintln!(
        "transactions: {} transactions, {} committed {}, {} jobs, {} runs",
        calls.len(),
        committed.len(),
        outcome_counts(&committed),
        rows.len(),
        runs.len()
    );
    violations
}

async fn enqueue_wake(work_ms: u64) -> (i64, Instant) {
    let job = Wake {
        token: token(),
        work_ms,
    };
    jalari::enqueue(&job).await.unwrap();
    (job.token, Instant::now())
}

fn audit_wakeups(seed: u64, label: &str, enqueued: &[(i64, Instant)]) -> Vec<String> {
    let runs = runs_of("wake");
    let mut violations = Vec::new();
    let mut worst = Duration::ZERO;
    for (token, at) in enqueued {
        match runs.iter().find(|run| run.token == *token) {
            Some(run) => {
                let waited = run.started.saturating_duration_since(*at);
                worst = worst.max(waited);
                if waited > WAKEUP_BOUND {
                    violations.push(format!(
                        "{label} seed {seed}: token {token} waited {waited:?} after its enqueue \
                         returned, expected under {WAKEUP_BOUND:?} with a 60 s poll interval"
                    ));
                }
            }
            None => violations.push(format!(
                "{label} seed {seed}: token {token} never started, expected a NOTIFY wake-up \
                 well before the 60 s poll"
            )),
        }
    }
    eprintln!(
        "{label}: {} jobs, worst start delay {worst:?}",
        enqueued.len()
    );
    violations
}

fn peak_concurrency(runs: &[Run]) -> usize {
    let mut edges: Vec<(Instant, i32)> = Vec::new();
    for run in runs {
        edges.push((run.started, 1));
        edges.push((run.finished.unwrap_or_else(Instant::now), -1));
    }
    edges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut current = 0;
    let mut peak = 0;
    for (_, delta) in edges {
        current += delta;
        peak = peak.max(current);
    }
    usize::try_from(peak).unwrap()
}

async fn check_wakeups(database: &Database, seed: u64) -> Vec<String> {
    let scale = common::scale();
    let mut rng = Rng::new(seed);
    let worker = start_worker("wake", 4, Duration::from_secs(60)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut enqueued = Vec::new();
    for _ in 0..3 * scale {
        tokio::time::sleep(rng.millis(50, 300)).await;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            tasks.push(tokio::spawn(async move {
                let mut tokens = Vec::new();
                for _ in 0..6 {
                    tokens.push(enqueue_wake(20).await);
                }
                tokens
            }));
        }
        for task in tasks {
            enqueued.extend(task.await.unwrap());
        }
        settle(Duration::from_secs(10), || async {
            pending_of(database, "race_wake").await == 0
        })
        .await;
    }
    let mut tasks = Vec::new();
    for task in 0..6 {
        let mut rng = rng.fork(task);
        tasks.push(tokio::spawn(async move {
            let mut tokens = Vec::new();
            for _ in 0..15 * scale {
                tokio::time::sleep(rng.millis(0, 40)).await;
                tokens.push(enqueue_wake(rng.below(30)).await);
            }
            tokens
        }));
    }
    for task in tasks {
        enqueued.extend(task.await.unwrap());
    }
    settle(Duration::from_secs(15), || async {
        pending_of(database, "race_wake").await == 0
    })
    .await;
    worker.stop().await;
    audit_wakeups(seed, "wakeups", &enqueued)
}

async fn check_batch_wakeup(database: &Database, app: &PgPool, seed: u64) -> Vec<String> {
    let worker = start_worker("wake", 4, Duration::from_secs(60)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = runs_of("wake").len();
    let mut transaction = app.begin().await.unwrap();
    let mut tokens = Vec::new();
    for _ in 0..12 {
        let job = Wake {
            token: token(),
            work_ms: 400,
        };
        jalari::enqueue_in(&mut transaction, &job, EnqueueOptions::new())
            .await
            .unwrap();
        tokens.push(job.token);
    }
    transaction.commit().await.unwrap();
    let committed = Instant::now();
    let settled = settle(Duration::from_secs(15), || async {
        pending_of(database, "race_wake").await == 0
    })
    .await;
    worker.stop().await;
    let runs: Vec<Run> = runs_of("wake").into_iter().skip(before).collect();
    let peak = peak_concurrency(&runs);
    let last_start = runs
        .iter()
        .map(|run| run.started.saturating_duration_since(committed))
        .max()
        .unwrap_or_default();
    eprintln!(
        "batch_wakeup: 12 jobs of 400 ms committed in one transaction, peak concurrency {peak} \
         of 4, last start {last_start:?} after commit, settled {settled}"
    );
    let mut violations = Vec::new();
    if peak < 4 || last_start > Duration::from_millis(2500) {
        violations.push(format!(
            "batch_wakeup seed {seed}: 12 ready jobs committed in one transaction ran at peak \
             concurrency {peak} of 4 and the last started {last_start:?} after commit, expected \
             4 at once and every start within 2.5 s (3 rounds of 400 ms); idle loops waited for \
             the 60 s poll"
        ));
    }
    violations
}

async fn check_children(database: &Database, seed: u64) -> Vec<String> {
    let scale = common::scale();
    let concurrency = usize::try_from(POOL).unwrap() - 2;
    let parents = 40 * scale;
    let mut expected_children = Vec::new();
    for _ in 0..parents {
        let children: Vec<i64> = (0..3).map(|_| token()).collect();
        expected_children.extend(children.iter().copied());
        jalari::enqueue(&Parent {
            token: token(),
            children,
        })
        .await
        .unwrap();
    }
    let started = Instant::now();
    let worker = start_worker("family", concurrency, Duration::from_millis(500)).await;
    let settled = settle(Duration::from_secs(40), || async {
        pending_of(database, "race_parent").await == 0
            && pending_of(database, "race_child").await == 0
    })
    .await;
    let elapsed = started.elapsed();
    worker.stop().await;

    let mut violations = Vec::new();
    if !settled {
        violations.push(format!(
            "children seed {seed}: parents or children still pending after 40 s with the pool at \
             the minimum of {POOL} for concurrency {concurrency}"
        ));
    }
    let enqueues = CHILD_ENQUEUES.lock().unwrap().clone();
    let slowest = enqueues
        .iter()
        .map(|enqueue| enqueue.took)
        .max()
        .unwrap_or_default();
    for ChildEnqueue { took, error } in &enqueues {
        if let Some(error) = error {
            violations.push(format!(
                "children seed {seed}: a child enqueue from inside a job failed after {took:?}: \
                 {error}"
            ));
        } else if *took > STALL_BOUND {
            violations.push(format!(
                "children seed {seed}: a child enqueue from inside a job stalled {took:?}, \
                 expected under {STALL_BOUND:?}"
            ));
        }
    }
    let child_runs = runs_of("child");
    for child in &expected_children {
        let ok = child_runs
            .iter()
            .filter(|run| run.token == *child && run.ok)
            .count();
        if ok != 1 {
            violations.push(format!(
                "children seed {seed}: child token {child} succeeded {ok} times, expected once"
            ));
        }
    }
    eprintln!(
        "children: {parents} parents, {} child enqueues, slowest {slowest:?}, all done in \
         {elapsed:?}",
        enqueues.len()
    );
    violations
}

fn selected(only: Option<&str>, name: &str) -> bool {
    only.is_none_or(|only| only == name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_race_enqueue() {
    let Some(database) = Database::start("enq", POOL).await else {
        return;
    };
    let observer = database.connect("observer", 2).await;
    let app = database.connect("app", 2).await;
    let users = database.table("race_user");
    let gate = database.table("race_gate");
    let mark_gate = database.table("race_mark_gate");
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {users} (token bigint PRIMARY KEY);
         CREATE TABLE {gate} (job_key text PRIMARY KEY, sleep_ms int NOT NULL);
         CREATE TABLE {mark_gate} (job_key text PRIMARY KEY, lock_id bigint NOT NULL);
         CREATE FUNCTION \"{schema}\".race_mark_gate() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE
             gate_lock bigint;
         BEGIN
             SELECT lock_id INTO gate_lock FROM {mark_gate} WHERE job_key = NEW.job_key;
             IF gate_lock IS NOT NULL THEN
                 PERFORM pg_advisory_xact_lock(gate_lock);
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER race_mark_gate BEFORE UPDATE ON {job} FOR EACH ROW
             WHEN (NEW.last_error IS DISTINCT FROM OLD.last_error)
             EXECUTE FUNCTION \"{schema}\".race_mark_gate();
         CREATE FUNCTION \"{schema}\".race_gate_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE
             pause int;
         BEGIN
             SELECT sleep_ms INTO pause FROM {gate} WHERE job_key = NEW.job_key;
             IF pause IS NOT NULL THEN
                 PERFORM pg_sleep(pause / 1000.0);
             END IF;
             RETURN NULL;
         END $$;
         CREATE CONSTRAINT TRIGGER race_gate_commit AFTER UPDATE ON {job}
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
             WHEN (NEW.attempts = OLD.attempts + 1)
             EXECUTE FUNCTION \"{schema}\".race_gate_commit();",
        job = database.table("job"),
    )))
    .execute(&observer)
    .await
    .unwrap();
    PROBE
        .set((database.connect("probe", 2).await, users.clone()))
        .ok()
        .unwrap();

    let only = std::env::var("RACE_ONLY").ok();
    let only = only.as_deref();
    let mut failures = Vec::new();
    let mut report = |name: &str, started: Instant, violations: Vec<String>| {
        eprintln!(
            "{name}: {} violations in {:?}",
            violations.len(),
            started.elapsed()
        );
        failures.extend(violations);
    };

    if selected(only, "storm") {
        let seed = common::seed("storm");
        let started = Instant::now();
        report(
            "storm",
            started,
            check_storm(&database, &observer, seed).await,
        );
    }
    if selected(only, "running_outcome") {
        let seed = common::seed("running_outcome");
        let started = Instant::now();
        report(
            "running_outcome",
            started,
            check_running_outcome(&database, seed).await,
        );
    }
    if selected(only, "window") {
        let seed = common::seed("window");
        let started = Instant::now();
        report(
            "window",
            started,
            check_window(&database, &observer, seed).await,
        );
    }
    if selected(only, "replace_interrupted") {
        let seed = common::seed("replace_interrupted");
        let started = Instant::now();
        report(
            "replace_interrupted",
            started,
            check_replace_interrupted(&database, &observer, seed).await,
        );
    }
    if selected(only, "replace_timestamp") {
        let seed = common::seed("replace_timestamp");
        let started = Instant::now();
        report(
            "replace_timestamp",
            started,
            check_replace_timestamp(&database, &app, seed).await,
        );
    }
    if selected(only, "replace_marking") {
        let seed = common::seed("replace_marking");
        let started = Instant::now();
        report(
            "replace_marking",
            started,
            check_replace_marking(&database, &observer, seed).await,
        );
    }
    if selected(only, "transactions") {
        let seed = common::seed("transactions");
        let started = Instant::now();
        report(
            "transactions",
            started,
            check_transactions(&database, &app, seed).await,
        );
    }
    if selected(only, "wakeups") {
        let seed = common::seed("wakeups");
        let started = Instant::now();
        report("wakeups", started, check_wakeups(&database, seed).await);
    }
    if selected(only, "batch_wakeup") {
        let seed = common::seed("batch_wakeup");
        let started = Instant::now();
        report(
            "batch_wakeup",
            started,
            check_batch_wakeup(&database, &app, seed).await,
        );
    }
    if selected(only, "children") {
        let seed = common::seed("children");
        let started = Instant::now();
        report("children", started, check_children(&database, seed).await);
    }

    database.finish().await;
    assert!(
        failures.is_empty(),
        "{} violations:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
