mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, HistoryRow, JobRow, LEDGER, OCCUPANCY, Rng, RunningWorker};
use jalari::sqlx::types::chrono::Utc;
use jalari::sqlx::{self, AssertSqlSafe, PgConnection, PgPool};
use jalari::{CancellationToken, EnqueueOptions, Job, JobError, JobResult, Worker};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

const MIXED_TIMEOUT: Duration = Duration::from_millis(200);
const BOUNDED_TIMEOUT: Duration = Duration::from_millis(300);
const STALL: Duration = Duration::from_secs(3);
const MAX_DELAY_MS: u64 = 2000;
const POSTPONED_SECS: f64 = 3.0;
const PANIC_MARKER: &str = "race claim panic";
const UNKNOWN_TASK: &str = "race_claim_unknown";
const INTERRUPTED: &str = "the worker stopped during the last attempt";
const WAITING: &str = "state IN ('scheduled', 'enqueued')";
const UNSETTLED: &str = "state IN ('scheduled', 'enqueued', 'running')";
const SHOWN_FAILURES: usize = 25;
const TAKEOVER_LOG: &str = "job was interrupted, running it again";
const POSTPONE_LOG: &str = "waiting for an interrupted job with the same queue_key";

static LOGGED: LazyLock<Mutex<Vec<Logged>>> = LazyLock::new(|| Mutex::new(Vec::new()));

#[derive(Debug, Clone)]
struct Logged {
    at: Instant,
    message: String,
    job_id: Option<i64>,
    error: Option<String>,
}

#[derive(Default)]
struct LoggedFields {
    message: String,
    job_id: Option<i64>,
    error: Option<String>,
}

impl Visit for LoggedFields {
    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "job_id" {
            self.job_id = Some(value);
        }
    }

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
        metadata.target().starts_with("jalari") && *metadata.level() <= Level::DEBUG
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = LoggedFields::default();
        event.record(&mut fields);
        let expected = fields.message.starts_with("job failed") && fields.error.is_some();
        let kept = (*event.metadata().level() <= Level::WARN && !expected)
            || fields.message == POSTPONE_LOG;
        if kept {
            LOGGED.lock().unwrap().push(Logged {
                at: Instant::now(),
                message: fields.message,
                job_id: fields.job_id,
                error: fields.error,
            });
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

fn logged_since(mark: usize) -> Vec<Logged> {
    LOGGED.lock().unwrap()[mark..].to_vec()
}

fn logged_mark() -> usize {
    LOGGED.lock().unwrap().len()
}

fn report_logged(tag: &str, logged: &[Logged]) {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for entry in logged {
        let key = match &entry.error {
            Some(error) if entry.job_id.is_none() => format!("{} ({error})", entry.message),
            _ => entry.message.clone(),
        };
        *counts.entry(key).or_default() += 1;
    }
    eprintln!("{tag}: library log {counts:?}");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Kind {
    Instant,
    Sleep(u64),
    Fail(u32),
    Panic(u32),
    Stall(u32),
    Permanent,
}

impl Kind {
    fn failures(self) -> u32 {
        match self {
            Self::Instant | Self::Sleep(_) | Self::Permanent => 0,
            Self::Fail(failures) | Self::Panic(failures) | Self::Stall(failures) => failures,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Spec {
    kind: Kind,
    queue_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Mixed {
    spec: Spec,
}

#[jalari::job(queue = "alpha")]
impl Job for Mixed {
    const NAME: &'static str = "race_claim_mixed";
    const MAX_ATTEMPTS: i32 = 4;

    async fn run(self) -> JobResult {
        perform(self.spec).await
    }
}

#[derive(Serialize, Deserialize)]
struct Bounded {
    spec: Spec,
}

#[jalari::job(queue = "beta")]
impl Job for Bounded {
    const NAME: &'static str = "race_claim_bounded";
    const MAX_ATTEMPTS: i32 = 3;
    const TIMEOUT: Option<Duration> = Some(BOUNDED_TIMEOUT);

    async fn run(self) -> JobResult {
        perform(self.spec).await
    }
}

fn attempt_label(id: i64, attempt: i32) -> String {
    format!("claim:{id}:{attempt}")
}

async fn perform(spec: Spec) -> JobResult {
    let job = jalari::current_job().expect("perform runs inside a job");
    let id = job.id().0;
    let attempt = job.attempt();
    let mut keys = vec![format!("job:{id}")];
    if let Some(queue_key) = &spec.queue_key {
        keys.push(format!("qk:{queue_key}"));
    }
    let _seat = OCCUPANCY.enter(keys, format!("job {id} attempt {attempt}"));
    let label = attempt_label(id, attempt);
    LEDGER.record(&label, "start");
    let failing = i64::from(attempt) <= i64::from(spec.kind.failures());
    let result = match spec.kind {
        Kind::Sleep(millis) => {
            tokio::time::sleep(Duration::from_millis(millis)).await;
            Ok(())
        }
        Kind::Fail(_) if failing => Err(JobError::new(format!("fail {attempt}"))),
        Kind::Panic(_) if failing => panic!("{PANIC_MARKER} {attempt}"),
        Kind::Stall(_) if failing => {
            tokio::time::sleep(STALL).await;
            LEDGER.record(&label, "zombie");
            Ok(())
        }
        Kind::Permanent => Err(JobError::permanent(io::Error::other("permanent"))),
        Kind::Instant | Kind::Fail(_) | Kind::Panic(_) | Kind::Stall(_) => Ok(()),
    };
    LEDGER.record(&label, if result.is_ok() { "done" } else { "fail" });
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Task {
    Mixed,
    Bounded,
}

impl Task {
    fn max_attempts(self) -> i32 {
        match self {
            Self::Mixed => Mixed::MAX_ATTEMPTS,
            Self::Bounded => Bounded::MAX_ATTEMPTS,
        }
    }

    fn timeout(self) -> Duration {
        match self {
            Self::Mixed => MIXED_TIMEOUT,
            Self::Bounded => BOUNDED_TIMEOUT,
        }
    }

    fn options(self, spec: &Spec) -> EnqueueOptions {
        let mut options = EnqueueOptions::new();
        if let Some(queue_key) = &spec.queue_key {
            options = options.queue_key(queue_key);
        }
        if self == Self::Mixed && matches!(spec.kind, Kind::Stall(_)) {
            options = options.timeout(MIXED_TIMEOUT);
        }
        options
    }

    async fn enqueue(self, spec: Spec, options: EnqueueOptions) -> i64 {
        let outcome = match self {
            Self::Mixed => jalari::enqueue_with(&Mixed { spec }, options).await,
            Self::Bounded => jalari::enqueue_with(&Bounded { spec }, options).await,
        };
        outcome.unwrap().id().0
    }

    async fn enqueue_in(
        self,
        connection: &mut PgConnection,
        spec: Spec,
        options: EnqueueOptions,
    ) -> i64 {
        let outcome = match self {
            Self::Mixed => jalari::enqueue_in(connection, &Mixed { spec }, options).await,
            Self::Bounded => jalari::enqueue_in(connection, &Bounded { spec }, options).await,
        };
        outcome.unwrap().id().0
    }
}

#[derive(Debug, Clone)]
struct Planned {
    id: i64,
    task: Task,
    spec: Spec,
    enqueued: Instant,
    not_before: Option<Instant>,
}

impl Planned {
    fn succeeds(&self) -> bool {
        self.spec.kind != Kind::Permanent
            && i64::from(self.spec.kind.failures()) < i64::from(self.task.max_attempts())
    }

    fn expected_state(&self) -> &'static str {
        if self.succeeds() {
            "succeeded"
        } else {
            "failed"
        }
    }

    fn expected_attempts(&self) -> i32 {
        let failures = i32::try_from(self.spec.kind.failures()).unwrap();
        (failures + 1).min(self.task.max_attempts())
    }

    fn error_at(&self, attempt: i32) -> Option<String> {
        if i64::from(attempt) > i64::from(self.spec.kind.failures()) {
            return (self.spec.kind == Kind::Permanent).then(|| "permanent".to_owned());
        }
        match self.spec.kind {
            Kind::Fail(_) => Some(format!("fail {attempt}")),
            Kind::Panic(_) => Some(format!("panicked: {PANIC_MARKER} {attempt}")),
            Kind::Stall(_) => Some(format!("timed out after {:?}", self.task.timeout())),
            Kind::Instant | Kind::Sleep(_) | Kind::Permanent => None,
        }
    }

    fn expected_history(&self) -> Vec<(i32, String, Option<String>)> {
        let attempts = self.expected_attempts();
        (1..=attempts)
            .map(|attempt| {
                let state = if attempt < attempts {
                    "scheduled"
                } else {
                    self.expected_state()
                };
                (attempt, state.to_owned(), self.error_at(attempt))
            })
            .collect()
    }
}

#[derive(Clone, Copy)]
struct Profile {
    queue: Option<&'static str>,
    draw: fn(&mut Rng, &[String]) -> (Task, Spec),
    delayed_percent: u64,
    priority_percent: u64,
    rollback_percent: u64,
    pause_percent: u64,
}

#[derive(Default)]
struct Production {
    planned: Vec<Planned>,
    rolled_back: Vec<i64>,
}

fn draw_mix(rng: &mut Rng, keys: &[String]) -> (Task, Spec) {
    let task = if rng.chance(70) {
        Task::Mixed
    } else {
        Task::Bounded
    };
    let queue_key = rng.chance(30).then(|| rng.pick(keys).clone());
    let max_attempts = u64::try_from(task.max_attempts()).unwrap();
    let failures = u32::try_from(1 + rng.below(max_attempts)).unwrap();
    let kind = match rng.below(100) {
        0..30 | 89.. => Kind::Instant,
        58..60 if task == Task::Mixed && queue_key.is_none() => Kind::Sleep(2200 + rng.below(800)),
        58..60 if task == Task::Mixed => Kind::Sleep(50 + rng.below(100)),
        30..60 => Kind::Sleep(1 + rng.below(30)),
        60..72 => Kind::Fail(failures),
        72..80 => Kind::Panic(failures),
        80..83 => Kind::Stall(failures),
        _ => Kind::Permanent,
    };
    (task, Spec { kind, queue_key })
}

fn draw_same_key(rng: &mut Rng, keys: &[String]) -> (Task, Spec) {
    let queue_key = (!rng.chance(10)).then(|| rng.pick(keys).clone());
    let kind = match rng.below(100) {
        0..80 => Kind::Instant,
        80..95 => Kind::Sleep(1 + rng.below(5)),
        _ => Kind::Fail(1),
    };
    (Task::Mixed, Spec { kind, queue_key })
}

async fn submit_in(
    pool: &PgPool,
    job_table: &str,
    task: Task,
    spec: Spec,
    options: EnqueueOptions,
    priority: Option<i16>,
    commit: bool,
) -> i64 {
    let mut transaction = pool.begin().await.unwrap();
    let id = task.enqueue_in(&mut transaction, spec, options).await;
    if let Some(priority) = priority {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {job_table} SET priority = $2 WHERE id = $1"
        )))
        .bind(id)
        .bind(priority)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    if commit {
        transaction.commit().await.unwrap();
    } else {
        transaction.rollback().await.unwrap();
    }
    id
}

async fn produce(
    pool: PgPool,
    job_table: String,
    profile: Profile,
    keys: Arc<[String]>,
    mut rng: Rng,
    count: u64,
) -> Production {
    let mut production = Production::default();
    for _ in 0..count {
        let (task, spec) = (profile.draw)(&mut rng, &keys);
        let mut options = task.options(&spec);
        if let Some(queue) = profile.queue {
            options = options.queue(queue);
        }
        let mut not_before = None;
        let roll = rng.below(100);
        if roll < profile.delayed_percent {
            let delay = rng.millis(100, MAX_DELAY_MS);
            let now = Instant::now();
            not_before = Some(now + delay);
            options = if roll.is_multiple_of(2) {
                options.delay(delay)
            } else {
                options.run_at(Utc::now() + delay)
            };
        }
        let roll = rng.below(100);
        let id = if roll < profile.rollback_percent {
            let id = submit_in(&pool, &job_table, task, spec, options, None, false).await;
            production.rolled_back.push(id);
            continue;
        } else if roll < profile.rollback_percent + profile.priority_percent {
            let priority = i16::try_from(rng.below(4)).unwrap() - 1;
            submit_in(
                &pool,
                &job_table,
                task,
                spec.clone(),
                options,
                Some(priority),
                true,
            )
            .await
        } else {
            task.enqueue(spec.clone(), options).await
        };
        production.planned.push(Planned {
            id,
            task,
            spec,
            enqueued: Instant::now(),
            not_before,
        });
        if rng.chance(profile.pause_percent) {
            tokio::time::sleep(rng.millis(0, 3)).await;
        }
    }
    production
}

async fn produce_concurrently(
    database: &Database,
    profile: Profile,
    keys: &[String],
    rng: &mut Rng,
    producers: u64,
    total: u64,
) -> Production {
    let keys: Arc<[String]> = keys.into();
    let handles: Vec<_> = (0..producers)
        .map(|producer| {
            let count = total / producers + u64::from(producer < total % producers);
            tokio::spawn(produce(
                database.pool.clone(),
                database.table("job"),
                profile,
                Arc::clone(&keys),
                rng.fork(producer),
                count,
            ))
        })
        .collect();
    let mut production = Production::default();
    for handle in handles {
        let part = handle.await.unwrap();
        production.planned.extend(part.planned);
        production.rolled_back.extend(part.rolled_back);
    }
    production
}

async fn enqueue_on(
    database: &Database,
    queue: &str,
    kind: Kind,
    queue_key: Option<&str>,
    priority: i16,
) -> Planned {
    let spec = Spec {
        kind,
        queue_key: queue_key.map(str::to_owned),
    };
    let options = Task::Mixed.options(&spec).queue(queue);
    let id = submit_in(
        &database.pool,
        &database.table("job"),
        Task::Mixed,
        spec.clone(),
        options,
        Some(priority),
        true,
    )
    .await;
    Planned {
        id,
        task: Task::Mixed,
        spec,
        enqueued: Instant::now(),
        not_before: None,
    }
}

async fn build_worker(
    queues: &[(&str, usize)],
    scheduler: bool,
    poll_interval: Duration,
    max_claim_tries: u32,
) -> Worker {
    let mut config = common::worker_config(poll_interval);
    config.max_claim_tries = max_claim_tries;
    let mut builder = Worker::builder()
        .config(config)
        .retry_policy(common::fast_retry())
        .scheduler(scheduler);
    for (name, concurrency) in queues {
        builder = builder.queue(name, *concurrency);
    }
    builder.build().await.unwrap()
}

async fn stop_all(workers: Vec<RunningWorker>) {
    for worker in &workers {
        worker.request_stop();
    }
    for worker in workers {
        worker.stop().await;
    }
}

async fn insert_unknown(
    database: &Database,
    queue: &str,
    queue_key: Option<&str>,
    priority: i16,
) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {} (queue, task, payload, state, max_attempts, queue_key, priority)
         VALUES ($1, $2, $3, 'enqueued', 3, $4, $5)
         RETURNING id",
        database.table("job")
    )))
    .bind(queue)
    .bind(UNKNOWN_TASK)
    .bind(b"{}".to_vec())
    .bind(queue_key)
    .bind(priority)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn waiting(database: &Database, ids: &[i64]) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE id = ANY($1) AND {UNSETTLED}",
        database.table("job")
    )))
    .bind(ids)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn settle(database: &Database, ids: &[i64], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if waiting(database, ids).await == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Debug, sqlx::FromRow)]
#[expect(
    dead_code,
    reason = "the fields are read through Debug in failure messages"
)]
struct WaitingRow {
    id: i64,
    state: String,
    attempts: i32,
    queue_key: Option<String>,
    last_error: Option<String>,
    due_in: String,
    priority: i16,
}

async fn describe_waiting(database: &Database, ids: &[i64]) -> Vec<WaitingRow> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id, state, attempts, queue_key, last_error, (run_at - now())::text AS due_in,
             priority
         FROM {} WHERE id = ANY($1) AND {UNSETTLED} ORDER BY id LIMIT 10",
        database.table("job")
    )))
    .bind(ids)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn stray_waiting(database: &Database) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE {WAITING} AND task <> $1",
        database.table("job")
    )))
    .bind(UNKNOWN_TASK)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn watch_postponed(database: &Database, stop: CancellationToken) -> HashMap<i64, WaitingRow> {
    let sql = format!(
        "SELECT id, state, attempts, queue_key, last_error, (run_at - now())::text AS due_in,
             priority
         FROM {} WHERE {WAITING} AND run_at > now() + make_interval(secs => $1)",
        database.table("job")
    );
    let mut seen = HashMap::new();
    while !stop.is_cancelled() {
        let rows: Vec<WaitingRow> = sqlx::query_as(AssertSqlSafe(sql.clone()))
            .bind(POSTPONED_SECS)
            .fetch_all(&database.pool)
            .await
            .unwrap();
        for row in rows {
            seen.entry(row.id).or_insert(row);
        }
        tokio::select! {
            () = stop.cancelled() => {}
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    seen
}

fn first(label: &str, event: &str) -> Option<Instant> {
    LEDGER
        .events(label)
        .into_iter()
        .find(|(recorded, _)| *recorded == event)
        .map(|(_, at)| at)
}

fn first_start(id: i64) -> Option<Instant> {
    first(&attempt_label(id, 1), "start")
}

fn done_at(id: i64) -> Option<Instant> {
    first(&attempt_label(id, 1), "done")
}

fn since(origin: Instant, at: Option<Instant>) -> Option<Duration> {
    at.map(|at| at.saturating_duration_since(origin))
}

fn takeover_evidence(
    id: i64,
    recorded: &[(i32, String, Option<String>)],
    logged: &[Logged],
) -> String {
    let Some((attempt, _, _)) = recorded
        .iter()
        .find(|(_, _, error)| error.as_deref() == Some(INTERRUPTED))
    else {
        return String::new();
    };
    let Some(started) = first(&attempt_label(id, *attempt), "start") else {
        return format!("; attempt {attempt} never started");
    };
    let takeovers: Vec<String> = logged
        .iter()
        .filter(|entry| entry.job_id == Some(id) && entry.message == TAKEOVER_LOG)
        .map(|entry| {
            if entry.at <= started {
                format!("{:?} before", started.duration_since(entry.at))
            } else {
                format!("{:?} after", entry.at.duration_since(started))
            }
        })
        .collect();
    format!(
        "; another worker logged {TAKEOVER_LOG:?} for this job {takeovers:?} attempt {attempt} \
         started, yet its claimer went on to run attempt {attempt}"
    )
}

async fn audit(
    database: &Database,
    tag: &str,
    planned: &[Planned],
    logged: &[Logged],
) -> Vec<String> {
    let rows: HashMap<i64, JobRow> = database
        .jobs()
        .await
        .into_iter()
        .map(|row| (row.id, row))
        .collect();
    let mut history: HashMap<i64, Vec<HistoryRow>> = HashMap::new();
    for row in database.history().await {
        history.entry(row.job_id).or_default().push(row);
    }
    let mut failures = Vec::new();
    let mut interrupted = 0;
    let mut taken_while_claimed = 0;
    for job in planned {
        let context = format!("{tag} job {} {:?} {:?}", job.id, job.task, job.spec);
        let Some(row) = rows.get(&job.id) else {
            failures.push(format!("{context}: the committed job row is missing"));
            continue;
        };
        let attempts = job.expected_attempts();
        let state = job.expected_state();
        let last_error = job.error_at(attempts);
        if row.state != state || row.attempts != attempts || row.last_error != last_error {
            failures.push(format!(
                "{context}: expected {state} after {attempts} attempts with last_error \
                 {last_error:?}, got {row:?}"
            ));
        }
        let recorded: Vec<(i32, String, Option<String>)> = history
            .get(&job.id)
            .map(|rows| {
                rows.iter()
                    .map(|row| (row.attempt, row.state.clone(), row.error.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for (attempt, _, error) in &recorded {
            if error.as_deref() != Some(INTERRUPTED) {
                continue;
            }
            interrupted += 1;
            let started = first(&attempt_label(job.id, *attempt), "start");
            let logged_first = logged.iter().any(|entry| {
                entry.job_id == Some(job.id)
                    && entry.message == TAKEOVER_LOG
                    && started.is_some_and(|started| entry.at <= started)
            });
            taken_while_claimed += usize::from(logged_first);
        }
        let expected = job.expected_history();
        if recorded != expected {
            failures.push(format!(
                "{context}: job_history disagrees, expected {expected:?}, got {recorded:?}, \
                 row {row:?}{}",
                takeover_evidence(job.id, &recorded, logged)
            ));
        }
        for attempt in 1..=attempts + 1 {
            let starts = LEDGER.count(&attempt_label(job.id, attempt), "start");
            let expected = usize::from(attempt <= attempts);
            if starts != expected {
                failures.push(format!(
                    "{context}: attempt {attempt} started {starts} times, expected {expected}, \
                     row {row:?}"
                ));
            }
        }
        let done: usize = (1..=attempts + 1)
            .map(|attempt| LEDGER.count(&attempt_label(job.id, attempt), "done"))
            .sum();
        let expected_done = usize::from(job.succeeds());
        if done != expected_done {
            failures.push(format!(
                "{context}: the body completed {done} times, expected {expected_done}, row {row:?}"
            ));
        }
        let zombies: usize = (1..=attempts)
            .map(|attempt| LEDGER.count(&attempt_label(job.id, attempt), "zombie"))
            .sum();
        if zombies > 0 {
            failures.push(format!(
                "{context}: {zombies} timed-out attempts kept running after their timeout"
            ));
        }
        if let (Some(not_before), Some(started)) = (job.not_before, first_start(job.id))
            && started < not_before
        {
            failures.push(format!(
                "{context}: started {:?} before its run_at",
                not_before.duration_since(started)
            ));
        }
    }
    if interrupted > 0 {
        failures.push(format!(
            "{tag}: {interrupted} job_history rows say {INTERRUPTED:?} although no worker \
             stopped; for {taken_while_claimed} of them another worker logged the takeover \
             before the very attempt it declared interrupted had started"
        ));
    }
    failures
}

fn cap(mut failures: Vec<String>) -> Vec<String> {
    if failures.len() > SHOWN_FAILURES {
        let hidden = failures.len() - SHOWN_FAILURES;
        failures.truncate(SHOWN_FAILURES);
        failures.push(format!("... and {hidden} more"));
    }
    failures
}

fn percentiles(label: &str, mut samples: Vec<Duration>) {
    if samples.is_empty() {
        return;
    }
    samples.sort();
    let at = |percent: usize| samples[(samples.len() - 1) * percent / 100];
    eprintln!(
        "{label}: n={} p50={:?} p90={:?} p99={:?} max={:?}",
        samples.len(),
        at(50),
        at(90),
        at(99),
        samples[samples.len() - 1]
    );
}

fn report_rates(tag: &str, planned: &[Planned]) {
    let mut starts = Vec::new();
    let mut immediate = Vec::new();
    let mut delayed = Vec::new();
    let mut keyed = Vec::new();
    for job in planned {
        for attempt in 1..=job.expected_attempts() {
            if let Some(at) = first(&attempt_label(job.id, attempt), "start") {
                starts.push(at);
            }
        }
        let Some(started) = first_start(job.id) else {
            continue;
        };
        match job.not_before {
            Some(not_before) => delayed.push(started.saturating_duration_since(not_before)),
            None if job.spec.queue_key.is_some() => {
                keyed.push(started.saturating_duration_since(job.enqueued));
            }
            None => immediate.push(started.saturating_duration_since(job.enqueued)),
        }
    }
    if let (Some(earliest), Some(latest)) = (starts.iter().min(), starts.iter().max()) {
        let window = latest.duration_since(*earliest);
        eprintln!(
            "{tag}: {} jobs, {} attempts started over {window:?}, {:.0} claims/s",
            planned.len(),
            starts.len(),
            starts.len() as f64 / window.as_secs_f64().max(0.001)
        );
    }
    percentiles(&format!("{tag} enqueue-to-start, no queue_key"), immediate);
    percentiles(&format!("{tag} enqueue-to-start, with queue_key"), keyed);
    percentiles(&format!("{tag} run_at-to-start, delayed"), delayed);
}

fn postponed_failures(
    tag: &str,
    postponed: &HashMap<i64, WaitingRow>,
    logged: &[Logged],
) -> Vec<String> {
    let mut rows: Vec<&WaitingRow> = postponed.values().collect();
    rows.sort_by_key(|row| row.id);
    rows.into_iter()
        .map(|row| {
            let causes: Vec<&str> = logged
                .iter()
                .filter(|entry| entry.job_id == Some(row.id))
                .map(|entry| entry.message.as_str())
                .collect();
            format!(
                "{tag} job {}: run_at was pushed {} into the future although no worker stopped \
                 and no delay above {MAX_DELAY_MS} ms was requested, library log for it \
                 {causes:?}: {row:?}",
                row.id, row.due_in
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn test_race_claim() {
    let Some(database) = Database::start("claim", 16).await else {
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

    let only = std::env::var("RACE_ONLY").unwrap_or_default();
    let mut failures = Vec::new();
    if only.is_empty() || only == "mix" {
        failures.extend(check_mix(&database).await);
    }
    if only.is_empty() || only == "same_key" {
        failures.extend(check_same_key(&database).await);
    }
    if only.is_empty() || only == "front_block" {
        failures.extend(check_front_block(&database).await);
    }
    if only.is_empty() || only == "locked_rows" {
        failures.extend(check_locked_rows(&database).await);
    }
    if only.is_empty() || only == "locked_sibling" {
        failures.extend(check_locked_sibling_does_not_hold_key(&database).await);
    }
    database.finish().await;
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

async fn check_mix(database: &Database) -> Vec<String> {
    let seed = common::seed("mix");
    let tag = format!("[mix seed {seed}]");
    let scale = common::scale();
    let started = Instant::now();
    let mut rng = Rng::new(seed);
    let deadlocks = database.deadlocks().await;
    let overlaps = OCCUPANCY.violations().len();
    let logs = logged_mark();
    let keys: Vec<String> = (0..8).map(|i| format!("mix-group-{i}")).collect();

    let mut unknown = Vec::new();
    for i in 0..20 {
        let queue = if i % 2 == 0 { "alpha" } else { "beta" };
        let queue_key = (i % 3 == 0).then(|| keys[i % keys.len()].as_str());
        unknown.push(insert_unknown(database, queue, queue_key, 2).await);
    }
    let workers = vec![
        RunningWorker::spawn(
            build_worker(
                &[("alpha", 3), ("beta", 1)],
                true,
                Duration::from_millis(50),
                10,
            )
            .await,
        ),
        RunningWorker::spawn(
            build_worker(
                &[("alpha", 2), ("beta", 2)],
                false,
                Duration::from_millis(50),
                10,
            )
            .await,
        ),
        RunningWorker::spawn(
            build_worker(&[("alpha", 2)], false, Duration::from_millis(50), 10).await,
        ),
    ];
    let profile = Profile {
        queue: None,
        draw: draw_mix,
        delayed_percent: 15,
        priority_percent: 35,
        rollback_percent: 3,
        pause_percent: 20,
    };
    let watch = CancellationToken::new();
    let (postponed, (production, settled)) =
        tokio::join!(watch_postponed(database, watch.clone()), async {
            let production =
                produce_concurrently(database, profile, &keys, &mut rng, 4, 2000 * scale).await;
            let ids: Vec<i64> = production.planned.iter().map(|job| job.id).collect();
            let settled = settle(database, &ids, Duration::from_secs(60 * scale)).await;
            watch.cancel();
            (production, settled)
        });
    stop_all(workers).await;

    let mut failures = Vec::new();
    let ids: Vec<i64> = production.planned.iter().map(|job| job.id).collect();
    if !settled {
        failures.push(format!(
            "{tag}: {} committed jobs were still waiting after {}s: {:?}",
            waiting(database, &ids).await,
            60 * scale,
            describe_waiting(database, &ids).await
        ));
    }
    let logged = logged_since(logs);
    report_logged(&tag, &logged);
    failures.extend(audit(database, &tag, &production.planned, &logged).await);
    failures.extend(postponed_failures(&tag, &postponed, &logged));
    let rows: HashMap<i64, JobRow> = database
        .jobs()
        .await
        .into_iter()
        .map(|row| (row.id, row))
        .collect();
    let history_ids: HashSet<i64> = database
        .history()
        .await
        .into_iter()
        .map(|row| row.job_id)
        .collect();
    for id in &production.rolled_back {
        let starts = LEDGER.count(&attempt_label(*id, 1), "start");
        if rows.contains_key(id) || starts > 0 {
            failures.push(format!(
                "{tag} rolled-back job {id}: row {:?}, started {starts} times",
                rows.get(id)
            ));
        }
    }
    for id in &unknown {
        let row = &rows[id];
        if row.state != "enqueued" || row.attempts != 0 || history_ids.contains(id) {
            failures.push(format!(
                "{tag} job {id} with an unregistered task was touched: {row:?}"
            ));
        }
    }
    let stray = stray_waiting(database).await;
    if stray > 0 {
        failures.push(format!(
            "{tag}: {stray} registered jobs still waiting at the end"
        ));
    }
    for overlap in &OCCUPANCY.violations()[overlaps..] {
        failures.push(format!("{tag} overlap: {overlap}"));
    }
    let grown = database.deadlocks().await - deadlocks;
    if grown > 0 {
        failures.push(format!(
            "{tag}: pg_stat_database.deadlocks grew by {grown} (database-wide counter)"
        ));
    }
    report_rates(&tag, &production.planned);
    eprintln!(
        "{tag}: {} committed, {} rolled back, {} unknown, {} postponed, {} failures, {:?}",
        production.planned.len(),
        production.rolled_back.len(),
        unknown.len(),
        postponed.len(),
        failures.len(),
        started.elapsed()
    );
    cap(failures)
}

async fn check_same_key(database: &Database) -> Vec<String> {
    let seed = common::seed("same_key");
    let tag = format!("[same_key seed {seed}]");
    let scale = common::scale();
    let started = Instant::now();
    let mut rng = Rng::new(seed);
    let deadlocks = database.deadlocks().await;
    let overlaps = OCCUPANCY.violations().len();
    let logs = logged_mark();
    let keys: Vec<String> = (0..3).map(|i| format!("hot-{i}")).collect();
    let poll = Duration::from_millis(50);
    let workers = vec![
        RunningWorker::spawn(build_worker(&[("kappa", 4)], false, poll, 10).await),
        RunningWorker::spawn(build_worker(&[("kappa", 3)], false, poll, 10).await),
        RunningWorker::spawn(build_worker(&[("kappa", 3)], false, poll, 10).await),
    ];
    let profile = Profile {
        queue: Some("kappa"),
        draw: draw_same_key,
        delayed_percent: 0,
        priority_percent: 20,
        rollback_percent: 0,
        pause_percent: 0,
    };
    let watch = CancellationToken::new();
    let (postponed, (production, settled)) =
        tokio::join!(watch_postponed(database, watch.clone()), async {
            let production =
                produce_concurrently(database, profile, &keys, &mut rng, 3, 600 * scale).await;
            let ids: Vec<i64> = production.planned.iter().map(|job| job.id).collect();
            let settled = settle(database, &ids, Duration::from_secs(60 * scale)).await;
            watch.cancel();
            (production, settled)
        });
    stop_all(workers).await;

    let mut failures = Vec::new();
    let ids: Vec<i64> = production.planned.iter().map(|job| job.id).collect();
    if !settled {
        failures.push(format!(
            "{tag}: {} committed jobs were still waiting after {}s: {:?}",
            waiting(database, &ids).await,
            60 * scale,
            describe_waiting(database, &ids).await
        ));
    }
    let logged = logged_since(logs);
    report_logged(&tag, &logged);
    failures.extend(audit(database, &tag, &production.planned, &logged).await);
    failures.extend(postponed_failures(&tag, &postponed, &logged));
    for overlap in &OCCUPANCY.violations()[overlaps..] {
        failures.push(format!("{tag} overlap: {overlap}"));
    }
    let grown = database.deadlocks().await - deadlocks;
    if grown > 0 {
        failures.push(format!(
            "{tag}: pg_stat_database.deadlocks grew by {grown} (database-wide counter)"
        ));
    }
    report_rates(&tag, &production.planned);
    eprintln!(
        "{tag}: {} committed, {} postponed, {} failures, {:?}",
        production.planned.len(),
        postponed.len(),
        failures.len(),
        started.elapsed()
    );
    cap(failures)
}

async fn wait_started(ids: &[i64], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !ids.iter().all(|id| first_start(*id).is_some()) {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    true
}

async fn hold_keys(
    database: &Database,
    holds: &[(&str, u64)],
    planned: &mut Vec<Planned>,
) -> Vec<i64> {
    let mut holders = Vec::new();
    for (queue_key, millis) in holds {
        let holder = enqueue_on(database, "gamma", Kind::Sleep(*millis), Some(queue_key), 0).await;
        holders.push(holder.id);
        planned.push(holder);
    }
    holders
}

struct FrontPart {
    name: &'static str,
    holds: &'static [(&'static str, u64)],
    front_per_key: usize,
}

async fn check_front_block(database: &Database) -> Vec<String> {
    let seed = common::seed("front_block");
    let tag = format!("[front_block seed {seed}]");
    let started = Instant::now();
    let deadlocks = database.deadlocks().await;
    let overlaps = OCCUPANCY.violations().len();
    let logs = logged_mark();
    let max_claim_tries = 3;
    let worker = RunningWorker::spawn(
        build_worker(
            &[("gamma", 4)],
            false,
            Duration::from_millis(200),
            max_claim_tries,
        )
        .await,
    );
    let mut planned = Vec::new();
    let mut failures = Vec::new();

    let parts = [
        FrontPart {
            name: "one busy key",
            holds: &[("fb-a", 3000)],
            front_per_key: 50,
        },
        FrontPart {
            name: "busy keys below max_claim_tries",
            holds: &[("fb-b1", 3000), ("fb-b2", 3000)],
            front_per_key: 20,
        },
        FrontPart {
            name: "busy keys at max_claim_tries",
            holds: &[("fb-c1", 1500), ("fb-c2", 4500), ("fb-c3", 4500)],
            front_per_key: 5,
        },
    ];
    for FrontPart {
        name: part,
        holds,
        front_per_key,
    } in parts
    {
        let origin = Instant::now();
        let first_planned = planned.len();
        let holders = hold_keys(database, holds, &mut planned).await;
        if !wait_started(&holders, Duration::from_secs(10)).await {
            failures.push(format!("{tag} {part}: the key holders never started"));
        }
        for (queue_key, _) in holds {
            for _ in 0..front_per_key {
                planned
                    .push(enqueue_on(database, "gamma", Kind::Instant, Some(queue_key), 2).await);
            }
        }
        let unrelated = enqueue_on(database, "gamma", Kind::Instant, None, 0).await;
        let unrelated_id = unrelated.id;
        planned.push(unrelated);
        let ids: Vec<i64> = planned[first_planned..].iter().map(|job| job.id).collect();
        if !settle(database, &ids, Duration::from_secs(30)).await {
            failures.push(format!(
                "{tag} {part}: jobs still waiting after 30s: {:?}",
                describe_waiting(database, &ids).await
            ));
        }
        let unrelated_start = since(origin, first_start(unrelated_id));
        let holder_done: Vec<Option<Duration>> = holders
            .iter()
            .map(|id| since(origin, done_at(*id)))
            .collect();
        let earliest_done = holder_done.iter().flatten().min().copied();
        eprintln!(
            "{tag} {part}: unrelated job started at {unrelated_start:?}, holders finished at \
             {holder_done:?}"
        );
        if !matches!((unrelated_start, earliest_done), (Some(start), Some(done)) if start < done) {
            failures.push(format!(
                "{tag} {part}: the unrelated ready job {unrelated_id} started at \
                 {unrelated_start:?}, not before the first busy key freed at {earliest_done:?}; \
                 a claim skips every busy key in one statement, whatever max_claim_tries is"
            ));
        }
    }
    worker.stop().await;

    let logged = logged_since(logs);
    report_logged(&tag, &logged);
    failures.extend(audit(database, &tag, &planned, &logged).await);
    for overlap in &OCCUPANCY.violations()[overlaps..] {
        failures.push(format!("{tag} overlap: {overlap}"));
    }
    let grown = database.deadlocks().await - deadlocks;
    if grown > 0 {
        failures.push(format!(
            "{tag}: pg_stat_database.deadlocks grew by {grown} (database-wide counter)"
        ));
    }
    eprintln!(
        "{tag}: {} jobs, {} failures, {:?}",
        planned.len(),
        failures.len(),
        started.elapsed()
    );
    cap(failures)
}

async fn check_locked_rows(database: &Database) -> Vec<String> {
    let seed = common::seed("locked_rows");
    let tag = format!("[locked_rows seed {seed}]");
    let scale = common::scale();
    let started = Instant::now();
    let mut rng = Rng::new(seed);
    let deadlocks = database.deadlocks().await;
    let overlaps = OCCUPANCY.violations().len();
    let logs = logged_mark();
    let mut failures = Vec::new();

    let mut front = Vec::new();
    let mut transaction = database.pool.begin().await.unwrap();
    for _ in 0..1500 * scale {
        let spec = Spec {
            kind: Kind::Instant,
            queue_key: None,
        };
        let options = Task::Mixed.options(&spec).queue("delta");
        let id = Task::Mixed
            .enqueue_in(&mut transaction, spec.clone(), options)
            .await;
        front.push(Planned {
            id,
            task: Task::Mixed,
            spec,
            enqueued: Instant::now(),
            not_before: None,
        });
    }
    let front_ids: Vec<i64> = front.iter().map(|job| job.id).collect();
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET priority = 2 WHERE id = ANY($1)",
        database.table("job")
    )))
    .bind(&front_ids)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let locker = database.connect("locker", 1).await;
    let mut lock = locker.begin().await.unwrap();
    let locked: Vec<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT id FROM {} WHERE id = ANY($1) FOR UPDATE",
        database.table("job")
    )))
    .bind(&front_ids)
    .fetch_all(&mut *lock)
    .await
    .unwrap();
    assert_eq!(locked.len(), front_ids.len());

    let worker = RunningWorker::spawn(
        build_worker(&[("delta", 4)], false, Duration::from_millis(100), 10).await,
    );
    let mut behind = Vec::new();
    for _ in 0..300 * scale {
        let kind = if rng.chance(20) {
            Kind::Sleep(1 + rng.below(10))
        } else {
            Kind::Instant
        };
        behind.push(enqueue_on(database, "delta", kind, None, 0).await);
    }
    let behind_ids: Vec<i64> = behind.iter().map(|job| job.id).collect();
    let behind_timeout = Duration::from_secs(30 * scale);
    if !settle(database, &behind_ids, behind_timeout).await {
        failures.push(format!(
            "{tag}: {} ready jobs behind {} locked rows were still waiting after {behind_timeout:?}: {:?}",
            waiting(database, &behind_ids).await,
            front_ids.len(),
            describe_waiting(database, &behind_ids).await
        ));
    }
    let leaked = front_ids
        .iter()
        .filter(|id| first_start(**id).is_some())
        .count();
    if leaked > 0 {
        failures.push(format!(
            "{tag}: {leaked} rows locked by another transaction were run anyway"
        ));
    }
    let released = Instant::now();
    lock.commit().await.unwrap();
    locker.close().await;
    let front_timeout = Duration::from_secs(60 * scale);
    if !settle(database, &front_ids, front_timeout).await {
        failures.push(format!(
            "{tag}: {} released rows were still waiting after {front_timeout:?}: {:?}",
            waiting(database, &front_ids).await,
            describe_waiting(database, &front_ids).await
        ));
    }
    let front_elapsed = released.elapsed();
    worker.stop().await;

    let mut planned = front;
    planned.extend(behind);
    let logged = logged_since(logs);
    report_logged(&tag, &logged);
    failures.extend(audit(database, &tag, &planned, &logged).await);
    for overlap in &OCCUPANCY.violations()[overlaps..] {
        failures.push(format!("{tag} overlap: {overlap}"));
    }
    let grown = database.deadlocks().await - deadlocks;
    if grown > 0 {
        failures.push(format!(
            "{tag}: pg_stat_database.deadlocks grew by {grown} (database-wide counter)"
        ));
    }
    let behind_starts: Vec<Instant> = behind_ids
        .iter()
        .filter_map(|id| first_start(*id))
        .collect();
    if let (Some(earliest), Some(latest)) = (behind_starts.iter().min(), behind_starts.iter().max())
    {
        let window = latest.duration_since(*earliest);
        eprintln!(
            "{tag}: {} jobs claimed past {} locked rows in {window:?}, {:.0} claims/s",
            behind_starts.len(),
            front_ids.len(),
            behind_starts.len() as f64 / window.as_secs_f64().max(0.001)
        );
    }
    eprintln!(
        "{tag}: {} released rows drained in {front_elapsed:?}, {:.0} jobs/s",
        front_ids.len(),
        front_ids.len() as f64 / front_elapsed.as_secs_f64().max(0.001)
    );
    report_rates(&tag, &planned[front_ids.len()..]);
    eprintln!(
        "{tag}: {} jobs, {} failures, {:?}",
        planned.len(),
        failures.len(),
        started.elapsed()
    );
    cap(failures)
}

async fn check_locked_sibling_does_not_hold_key(database: &Database) -> Vec<String> {
    let seed = common::seed("locked_sibling");
    let tag = format!("[locked_sibling seed {seed}]");
    let scale = common::scale();
    let started = Instant::now();
    let deadlocks = database.deadlocks().await;
    let overlaps = OCCUPANCY.violations().len();
    let logs = logged_mark();
    let job_table = database.table("job");
    let worker = RunningWorker::spawn(
        build_worker(&[("omega", 2)], false, Duration::from_millis(100), 10).await,
    );
    let claimer = database.connect("claimer", 1).await;
    let mut planned = Vec::new();
    let mut failures = Vec::new();

    for round in 0..2 * scale {
        let queue_key = format!("fw-{round}");
        let claimed = enqueue_on(database, "omega-held", Kind::Instant, Some(&queue_key), 0).await;
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {job_table} SET attempts = attempts + 1, run_at = now() + interval '2 seconds'
             WHERE id = $1"
        )))
        .bind(claimed.id)
        .execute(&database.pool)
        .await
        .unwrap();
        let mut relock = claimer.begin().await.unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "SELECT attempts FROM {job_table} WHERE id = $1 FOR UPDATE"
        )))
        .bind(claimed.id)
        .execute(&mut *relock)
        .await
        .unwrap();

        let keyed = enqueue_on(database, "omega", Kind::Instant, Some(&queue_key), 0).await;
        let control = enqueue_on(database, "omega", Kind::Instant, None, 0).await;
        let patience = Duration::from_secs(3);
        let deadline = keyed.enqueued + patience;
        while first_start(keyed.id).is_none() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let keyed_row = describe_waiting(database, &[keyed.id]).await;
        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {job_table} WHERE id = $1"
        )))
        .bind(claimed.id)
        .execute(&mut *relock)
        .await
        .unwrap();
        relock.commit().await.unwrap();

        let keyed_latency = since(keyed.enqueued, first_start(keyed.id));
        let control_latency = since(control.enqueued, first_start(control.id));
        eprintln!(
            "{tag} round {round}: keyed job started after {keyed_latency:?}, control after \
             {control_latency:?}"
        );
        if keyed_latency.is_none_or(|latency| latency >= patience) {
            failures.push(format!(
                "{tag} round {round}: job {} with queue_key {queue_key:?} had not started \
                 {patience:?} after enqueue, while control job {} without a key started after \
                 {control_latency:?}; its only sibling, job {}, was claimed by a session that is \
                 alive and holds the row lock but never set it running, so it must not hold the \
                 key: {keyed_row:?}",
                keyed.id, control.id, claimed.id
            ));
        }
        planned.push(keyed);
        planned.push(control);
    }
    claimer.close().await;
    let ids: Vec<i64> = planned.iter().map(|job| job.id).collect();
    if !settle(database, &ids, Duration::from_secs(15)).await {
        failures.push(format!(
            "{tag}: jobs still waiting after 15s: {:?}",
            describe_waiting(database, &ids).await
        ));
    }
    worker.stop().await;

    let logged = logged_since(logs);
    report_logged(&tag, &logged);
    failures.extend(audit(database, &tag, &planned, &logged).await);
    for overlap in &OCCUPANCY.violations()[overlaps..] {
        failures.push(format!("{tag} overlap: {overlap}"));
    }
    let grown = database.deadlocks().await - deadlocks;
    if grown > 0 {
        failures.push(format!(
            "{tag}: pg_stat_database.deadlocks grew by {grown} (database-wide counter)"
        ));
    }
    eprintln!(
        "{tag}: {} jobs, {} failures, {:?}",
        planned.len(),
        failures.len(),
        started.elapsed()
    );
    cap(failures)
}
