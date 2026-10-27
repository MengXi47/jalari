mod common;

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::ops::Range;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, OCCUPANCY, Rng, RunningWorker};
use jalari::sqlx::postgres::{PgConnectOptions, PgListener, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{CancellationToken, EnqueueOptions, Job, JobResult, Schema, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use tokio::task::{JoinHandle, JoinSet};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

const QUEUE: &str = "stress";
const POOL_SIZE: u32 = 84;
const LOAD_BATCH: usize = 500;
const LOADERS: usize = 8;
const PRODUCERS: usize = 16;
const IDLE_BEFORE_FEED: Duration = Duration::from_secs(1);
const EARLY_WINDOW: Duration = Duration::from_secs(1);
const SLOW_START: Duration = Duration::from_secs(5);
const TIMED_QUERY_ROUNDS: u32 = 200;
const TAKEOVER_DELAY: Duration = Duration::from_secs(6);
const INTERRUPTED: &str = "the worker stopped during the last attempt";
const POSTPONE_LOG: &str = "waiting for an interrupted job with the same queue_key";
const SCENARIOS: [&str; 8] = [
    "probe",
    "drain",
    "steady",
    "bodies",
    "queue_key",
    "big_table",
    "batch",
    "processes",
];
const OPT_IN_SCENARIOS: [&str; 1] = ["processes"];

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
static STARTS: LazyLock<Mutex<Vec<Start>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static LOG: LazyLock<Mutex<LogCounts>> = LazyLock::new(|| Mutex::new(LogCounts::default()));
static CURRENT_RUN: AtomicU64 = AtomicU64::new(0);
static STALE: AtomicU64 = AtomicU64::new(0);
static FINISHED: AtomicU64 = AtomicU64::new(0);
static FINISHED_AT: AtomicU64 = AtomicU64::new(0);
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static OVERSHOOT_US: AtomicU64 = AtomicU64::new(0);
static SLEEPS: AtomicU64 = AtomicU64::new(0);

struct Start {
    id: u64,
    at: Instant,
    in_flight: usize,
}

#[derive(Default)]
struct LogCounts {
    warnings: BTreeMap<String, usize>,
    postpones: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Stress {
    run: u64,
    id: u64,
    sleep_us: u64,
    key: Option<u64>,
}

struct Flight;

impl Drop for Flight {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

#[jalari::job(queue = "stress")]
impl Job for Stress {
    const NAME: &'static str = "stress";

    async fn run(self) -> JobResult {
        let in_flight = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        let _flight = Flight;
        let _seat = self.key.map(|key| {
            OCCUPANCY.enter(
                vec![format!("qk:{key}")],
                format!("run {} job {}", self.run, self.id),
            )
        });
        if self.run == CURRENT_RUN.load(Ordering::SeqCst) {
            STARTS.lock().unwrap().push(Start {
                id: self.id,
                at: Instant::now(),
                in_flight,
            });
        } else {
            STALE.fetch_add(1, Ordering::SeqCst);
        }
        if self.sleep_us > 0 {
            let requested = Duration::from_micros(self.sleep_us);
            let slept = Instant::now();
            tokio::time::sleep(requested).await;
            let overshoot = slept.elapsed().saturating_sub(requested);
            OVERSHOOT_US.fetch_add(
                u64::try_from(overshoot.as_micros()).unwrap_or(u64::MAX),
                Ordering::SeqCst,
            );
            SLEEPS.fetch_add(1, Ordering::SeqCst);
        }
        FINISHED.fetch_add(1, Ordering::SeqCst);
        FINISHED_AT.fetch_max(nanos_since_epoch(Instant::now()), Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct Fields {
    message: String,
    error: Option<String>,
}

impl Visit for Fields {
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
        metadata.target().starts_with("jalari")
            && (*metadata.level() <= Level::WARN
                || (*metadata.level() == Level::DEBUG
                    && metadata.is_event()
                    && metadata.target() == "jalari::worker::execute"))
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let mut log = LOG.lock().unwrap();
        if *event.metadata().level() <= Level::WARN {
            let key = match fields.error {
                Some(error) => format!("{}: {error}", fields.message),
                None => fields.message,
            };
            *log.warnings.entry(key).or_default() += 1;
        } else if fields.message == POSTPONE_LOG {
            log.postpones += 1;
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[derive(Debug, Clone, Copy)]
struct Load {
    jobs: u64,
    keys: u64,
    sleep_ms: (u64, u64),
}

#[derive(Debug, Clone, Copy)]
enum Feed {
    Backlog,
    Rate(f64),
    Batch,
    Separate,
}

#[derive(Debug, Clone, Copy)]
struct Filler {
    finished: u64,
    future: u64,
    keys: u64,
}

struct Point {
    label: String,
    load: Load,
    feed: Feed,
    concurrency: usize,
    filler: Option<Filler>,
    time_queries: bool,
    processes: usize,
}

struct Committed {
    range: Range<usize>,
    at: Instant,
    ids: Vec<i64>,
    errors: Vec<String>,
}

struct Fed {
    enqueued_at: Vec<Option<Instant>>,
    ids: Vec<i64>,
    errors: Vec<String>,
    started: Instant,
    finished: Instant,
    lag_ms: Vec<f64>,
}

#[derive(Debug, Clone, Default)]
struct Sample {
    jobs: usize,
    elapsed: f64,
    throughput: f64,
    enqueue_rate: f64,
    latency: [f64; 4],
    producer_lag_p99: f64,
    notify_lag: Option<[f64; 3]>,
    early_peak: usize,
    warnings: usize,
    enqueue_errors: usize,
    postpones: usize,
    interrupted: i64,
    slow_starts: usize,
    queries_us: Option<[f64; 3]>,
    db_throughput: f64,
    overshoot_ms: f64,
}

struct Settings {
    runs: usize,
    timeout: Duration,
    max_workers: usize,
    concurrency: Vec<u64>,
    drain_jobs: u64,
    drain_per_slot: u64,
    steady_rates: Vec<u64>,
    steady_seconds: u64,
    steady_concurrency: usize,
    body_ms: (u64, u64),
    body_seconds: u64,
    keys: Vec<u64>,
    key_jobs: u64,
    key_concurrency: usize,
    big_finished: u64,
    big_future: u64,
    big_jobs: u64,
    big_key_jobs: u64,
    big_concurrency: usize,
    big_keys: u64,
    batch_jobs: u64,
    batch_sleep_ms: u64,
    batch_concurrency: usize,
    processes: Vec<u64>,
    process_concurrency: usize,
}

impl Settings {
    fn from_env() -> Self {
        Self {
            runs: env_u64("STRESS_RUNS", 5) as usize,
            timeout: Duration::from_secs(env_u64("STRESS_TIMEOUT_SECS", 120)),
            max_workers: env_u64("STRESS_WORKERS", 4) as usize,
            concurrency: env_list("STRESS_CONCURRENCY", &[1, 4, 16, 32, 64]),
            drain_jobs: env_u64("STRESS_DRAIN_JOBS", 50_000),
            drain_per_slot: env_u64("STRESS_DRAIN_PER_SLOT", 5_000),
            steady_rates: env_list("STRESS_STEADY_RATES", &[1000, 2000, 3000, 4000, 6000]),
            steady_seconds: env_u64("STRESS_STEADY_SECONDS", 8),
            steady_concurrency: env_u64("STRESS_STEADY_CONCURRENCY", 32) as usize,
            body_ms: (
                env_u64("STRESS_BODY_MIN_MS", 5),
                env_u64("STRESS_BODY_MAX_MS", 50),
            ),
            body_seconds: env_u64("STRESS_BODY_SECONDS", 6),
            keys: env_list("STRESS_KEYS", &[1, 8, 64]),
            key_jobs: env_u64("STRESS_KEY_JOBS", 5_000),
            key_concurrency: env_u64("STRESS_KEY_CONCURRENCY", 16) as usize,
            big_finished: env_u64("STRESS_BIG_FINISHED", 500_000),
            big_future: env_u64("STRESS_BIG_FUTURE", 100_000),
            big_jobs: env_u64("STRESS_BIG_JOBS", 20_000),
            big_key_jobs: env_u64("STRESS_BIG_KEY_JOBS", 3_000),
            big_concurrency: env_u64("STRESS_BIG_CONCURRENCY", 16) as usize,
            big_keys: env_u64("STRESS_BIG_KEYS", 64),
            batch_jobs: env_u64("STRESS_BATCH_JOBS", 400),
            batch_sleep_ms: env_u64("STRESS_BATCH_SLEEP_MS", 10),
            batch_concurrency: env_u64("STRESS_BATCH_CONCURRENCY", 16) as usize,
            processes: env_list("STRESS_PROCESSES", &[1, 2, 4]),
            process_concurrency: env_u64("STRESS_PROCESS_CONCURRENCY", 32) as usize,
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn nanos_since_epoch(at: Instant) -> u64 {
    u64::try_from(at.saturating_duration_since(*EPOCH).as_nanos()).unwrap_or(u64::MAX)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn spread(values: &[f64]) -> (f64, f64, f64) {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = if sorted.is_empty() {
        f64::NAN
    } else if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    };
    (
        median,
        sorted.first().copied().unwrap_or(f64::NAN),
        sorted.last().copied().unwrap_or(f64::NAN),
    )
}

fn show(name: &str, values: &[f64]) -> String {
    let (median, low, high) = spread(values);
    format!("{name} {median:.1} [{low:.1}..{high:.1}]")
}

fn uptime() -> String {
    Command::new("uptime")
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|e| format!("uptime failed: {e}"))
}

fn topology(total: usize, max_workers: usize) -> (usize, usize) {
    let workers = (1..=max_workers.min(total).max(1))
        .rev()
        .find(|workers| total.is_multiple_of(*workers))
        .unwrap_or(1);
    (workers, total / workers)
}

fn options(job: &Stress) -> EnqueueOptions {
    let options = EnqueueOptions::new();
    match job.key {
        Some(key) => options.queue_key(&format!("k{key}")),
        None => options,
    }
}

fn plan(run: u64, load: Load, rng: &mut Rng) -> Vec<Stress> {
    (0..load.jobs)
        .map(|id| Stress {
            run,
            id,
            sleep_us: if load.sleep_ms.1 == 0 {
                0
            } else {
                u64::try_from(rng.millis(load.sleep_ms.0, load.sleep_ms.1).as_micros()).unwrap()
            },
            key: (load.keys > 0).then(|| id % load.keys),
        })
        .collect()
}

async fn execute(pool: &PgPool, sql: String) {
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap();
}

async fn reset(database: &Database) {
    execute(
        &database.pool,
        format!(
            "TRUNCATE {}, {} RESTART IDENTITY",
            database.table("job"),
            database.table("job_history")
        ),
    )
    .await;
    STARTS.lock().unwrap().clear();
    FINISHED.store(0, Ordering::SeqCst);
    FINISHED_AT.store(0, Ordering::SeqCst);
    STALE.store(0, Ordering::SeqCst);
    OVERSHOOT_US.store(0, Ordering::SeqCst);
    SLEEPS.store(0, Ordering::SeqCst);
    *LOG.lock().unwrap() = LogCounts::default();
}

async fn fill(database: &Database, filler: Filler) {
    let job = database.table("job");
    let history = database.table("job_history");
    let key = if filler.keys > 0 {
        format!("'k' || (g % {})", filler.keys)
    } else {
        "NULL".to_owned()
    };
    let payload = "convert_to('{\"run\":0,\"id\":0,\"sleep_us\":0,\"key\":null}', 'UTF8')";
    execute(
        &database.pool,
        format!(
            "INSERT INTO {job} (queue, task, payload, state, attempts, max_attempts, run_at,
                 created_at, updated_at, queue_key)
             SELECT '{QUEUE}', 'stress', {payload}, 'succeeded', 1, 10,
                 now() - interval '3 hours', now() - interval '3 hours',
                 now() - make_interval(secs => g % 7200), {key}
             FROM generate_series(1, {}) AS g",
            filler.finished
        ),
    )
    .await;
    execute(
        &database.pool,
        format!(
            "INSERT INTO {history} (job_id, state, attempt, duration_ms, created_at)
             SELECT id, 'succeeded', 1, 1, updated_at FROM {job} WHERE state = 'succeeded'"
        ),
    )
    .await;
    execute(
        &database.pool,
        format!(
            "INSERT INTO {job} (queue, task, payload, state, attempts, max_attempts, run_at,
                 queue_key)
             SELECT '{QUEUE}', 'stress', {payload}, 'scheduled', 0, 10,
                 now() + interval '1 day' + make_interval(secs => g), {key}
             FROM generate_series(1, {}) AS g",
            filler.future
        ),
    )
    .await;
    execute(&database.pool, format!("VACUUM ANALYZE {job}, {history}")).await;
}

async fn check_filler(database: &Database, filler: Filler) -> Vec<String> {
    let (finished, future): (i64, i64) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT count(*) FILTER (WHERE state = 'succeeded' AND created_at < now() - interval '1 hour'),
             count(*) FILTER (WHERE state = 'scheduled' AND attempts = 0
                 AND run_at > now() + interval '1 hour')
         FROM {}",
        database.table("job")
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let mut violations = Vec::new();
    if finished != i64::try_from(filler.finished).unwrap()
        || future != i64::try_from(filler.future).unwrap()
    {
        violations.push(format!(
            "filler rows changed: expected {} finished and {} future, found {finished} and {future}",
            filler.finished, filler.future
        ));
    }
    violations
}

async fn feed_transactions(
    pool: &PgPool,
    jobs: &Arc<Vec<Stress>>,
    batch: usize,
    loaders: usize,
) -> Fed {
    let started = Instant::now();
    let chunks = jobs.len().div_ceil(batch);
    let next = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for _ in 0..loaders {
        let pool = pool.clone();
        let jobs = Arc::clone(jobs);
        let next = Arc::clone(&next);
        tasks.spawn(async move {
            let mut done: Vec<Committed> = Vec::new();
            loop {
                let chunk = next.fetch_add(1, Ordering::SeqCst);
                if chunk >= chunks {
                    break done;
                }
                let range = chunk * batch..((chunk + 1) * batch).min(jobs.len());
                let mut ids = Vec::new();
                let mut errors = Vec::new();
                let mut transaction = pool.begin().await.unwrap();
                for job in &jobs[range.clone()] {
                    match jalari::enqueue_in(&mut transaction, job, options(job)).await {
                        Ok(outcome) => ids.push(outcome.id().0),
                        Err(e) => errors.push(e.to_string()),
                    }
                }
                if let Err(e) = transaction.commit().await {
                    errors.push(format!("commit of {} jobs failed: {e}", ids.len()));
                    ids.clear();
                }
                done.push(Committed {
                    range,
                    at: Instant::now(),
                    ids,
                    errors,
                });
            }
        });
    }
    let mut fed = Fed {
        enqueued_at: vec![None; jobs.len()],
        ids: Vec::new(),
        errors: Vec::new(),
        started,
        finished: started,
        lag_ms: Vec::new(),
    };
    while let Some(done) = tasks.join_next().await {
        for committed in done.unwrap() {
            if !committed.ids.is_empty() {
                for slot in &mut fed.enqueued_at[committed.range] {
                    *slot = Some(committed.at);
                }
            }
            fed.finished = fed.finished.max(committed.at);
            fed.ids.extend(committed.ids);
            fed.errors.extend(committed.errors);
        }
    }
    fed
}

async fn feed_rate(jobs: Arc<Vec<Stress>>, rate: Option<f64>, producers: usize) -> Fed {
    let started = Instant::now();
    let next = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for _ in 0..producers {
        let jobs = Arc::clone(&jobs);
        let next = Arc::clone(&next);
        tasks.spawn(async move {
            let mut done: Vec<(usize, Instant, Result<i64, String>, f64)> = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::SeqCst);
                if index >= jobs.len() {
                    break done;
                }
                let due = rate.map_or(started, |rate| {
                    started + Duration::from_secs_f64(index as f64 / rate)
                });
                tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                let lag = millis(Instant::now().saturating_duration_since(due));
                let job = &jobs[index];
                let result = jalari::enqueue_with(job, options(job))
                    .await
                    .map(|outcome| outcome.id().0)
                    .map_err(|e| e.to_string());
                done.push((index, Instant::now(), result, lag));
            }
        });
    }
    let mut fed = Fed {
        enqueued_at: vec![None; jobs.len()],
        ids: Vec::new(),
        errors: Vec::new(),
        started,
        finished: started,
        lag_ms: Vec::new(),
    };
    while let Some(done) = tasks.join_next().await {
        for (index, at, result, lag) in done.unwrap() {
            fed.finished = fed.finished.max(at);
            fed.lag_ms.push(lag);
            match result {
                Ok(id) => {
                    fed.enqueued_at[index] = Some(at);
                    fed.ids.push(id);
                }
                Err(e) => fed.errors.push(e),
            }
        }
    }
    fed
}

async fn wait_finished(target: u64, timeout: Duration) -> Option<Instant> {
    let deadline = Instant::now() + timeout;
    loop {
        if FINISHED.load(Ordering::SeqCst) >= target {
            return Some(*EPOCH + Duration::from_nanos(FINISHED_AT.load(Ordering::SeqCst)));
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn start_workers(total: usize, max_workers: usize) -> (Vec<RunningWorker>, Instant) {
    let (workers, concurrency) = topology(total, max_workers);
    let mut built = Vec::new();
    for _ in 0..workers {
        built.push(
            Worker::builder()
                .queue(QUEUE, concurrency)
                .scheduler(false)
                .config(WorkerConfig::default())
                .build()
                .await
                .unwrap(),
        );
    }
    let spawned = Instant::now();
    (
        built.into_iter().map(RunningWorker::spawn).collect(),
        spawned,
    )
}

fn claim_sql(database: &Database) -> String {
    format!(
        "SELECT id, task, payload, attempts, max_attempts, timeout_ms, queue_key, updated_at,
             context
         FROM {}
         WHERE queue = $1
             AND state IN ('scheduled', 'enqueued')
             AND run_at <= now()
             AND task = ANY($2)
             AND (queue_key IS NULL OR queue_key <> ALL($3))
         ORDER BY priority DESC, run_at, id
         LIMIT 1
         FOR UPDATE SKIP LOCKED",
        database.table("job")
    )
}

fn fence_sql(database: &Database) -> String {
    let job = database.table("job");
    let history = database.table("job_history");
    format!(
        "SELECT max(until) FROM (
             SELECT CASE WHEN state = 'failed' THEN updated_at + $3 ELSE run_at END AS until
             FROM {job}
             WHERE queue_key = $1 AND id <> $2 AND last_error = $4
                 AND ((state IN ('scheduled', 'enqueued') AND run_at > now())
                     OR (state = 'failed' AND updated_at > now() - $3))
             UNION ALL
             SELECT now() + $3 AS until
             FROM {job} AS job
             WHERE queue_key = $1 AND id <> $2 AND state IN ('scheduled', 'enqueued')
                 AND attempts > 0
                 AND NOT EXISTS (
                     SELECT 1 FROM {history} AS history
                     WHERE history.job_id = job.id AND history.attempt = job.attempts
                         AND history.created_at >= job.updated_at
                 )
         ) AS fences"
    )
}

fn siblings_sql(database: &Database) -> String {
    format!(
        "SELECT id, task, payload, attempts, max_attempts, timeout_ms, queue_key, updated_at,
             context
         FROM {}
         WHERE queue_key = $1 AND id <> $2 AND state IN ('scheduled', 'enqueued')
             AND attempts > 0
         FOR UPDATE SKIP LOCKED",
        database.table("job")
    )
}

async fn time_queries(database: &Database, probe: &PgPool, explain: bool) -> [f64; 3] {
    let mut connection = probe.acquire().await.unwrap();
    sqlx::raw_sql("BEGIN")
        .execute(&mut *connection)
        .await
        .unwrap();
    let tasks = vec!["stress".to_owned()];
    let excluded: Vec<String> = Vec::new();
    let mut means = [0.0; 3];
    for (slot, sql) in [
        claim_sql(database),
        fence_sql(database),
        siblings_sql(database),
    ]
    .into_iter()
    .enumerate()
    {
        let started = Instant::now();
        for _ in 0..TIMED_QUERY_ROUNDS {
            let query = sqlx::query(AssertSqlSafe(sql.clone()));
            let query = if slot == 0 {
                query.bind(QUEUE).bind(&tasks).bind(&excluded)
            } else if slot == 1 {
                query
                    .bind("k0")
                    .bind(0_i64)
                    .bind(TAKEOVER_DELAY)
                    .bind(INTERRUPTED)
            } else {
                query.bind("k0").bind(0_i64)
            };
            query.fetch_all(&mut *connection).await.unwrap();
        }
        means[slot] = started.elapsed().as_secs_f64() * 1e6 / f64::from(TIMED_QUERY_ROUNDS);
    }
    if explain {
        let statements = [
            format!(
                "PREPARE stress_claim(text, text[], text[]) AS {}",
                claim_sql(database)
            ),
            format!(
                "PREPARE stress_fence(text, bigint, interval, text) AS {}",
                fence_sql(database)
            ),
            format!(
                "PREPARE stress_siblings(text, bigint) AS {}",
                siblings_sql(database)
            ),
            "SET LOCAL plan_cache_mode = force_generic_plan".to_owned(),
        ];
        for statement in statements {
            execute_on(&mut connection, statement).await;
        }
        for statement in [
            format!("EXECUTE stress_claim('{QUEUE}', ARRAY['stress'], ARRAY[]::text[])"),
            format!("EXECUTE stress_fence('k0', 0, interval '6 seconds', '{INTERRUPTED}')"),
            "EXECUTE stress_siblings('k0', 0)".to_owned(),
        ] {
            let plan: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) {statement}"
            )))
            .fetch_all(&mut *connection)
            .await
            .unwrap();
            eprintln!("PLAN {statement}\n  {}", plan.join("\n  "));
        }
        for name in ["stress_claim", "stress_fence", "stress_siblings"] {
            execute_on(&mut connection, format!("DEALLOCATE {name}")).await;
        }
    }
    sqlx::raw_sql("ROLLBACK")
        .execute(&mut *connection)
        .await
        .unwrap();
    means
}

async fn execute_on(connection: &mut sqlx::PgConnection, sql: String) {
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(connection)
        .await
        .unwrap();
}

async fn verify(
    database: &Database,
    ids: &[i64],
    planned: usize,
    in_process: bool,
) -> (Vec<String>, i64, f64) {
    let mut violations = if in_process {
        check_starts(planned, ids.len())
    } else {
        Vec::new()
    };
    let states: Vec<(String, i32, i64)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts, count(*) FROM {} WHERE id = ANY($1)
         GROUP BY state, attempts ORDER BY state, attempts",
        database.table("job")
    )))
    .bind(ids)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    let succeeded_once = states
        .iter()
        .find(|(state, attempts, _)| state == "succeeded" && *attempts == 1)
        .map_or(0, |(_, _, count)| *count);
    if succeeded_once != i64::try_from(ids.len()).unwrap() {
        violations.push(format!(
            "expected all {} rows succeeded with attempts 1, found (state, attempts, rows) {states:?}",
            ids.len()
        ));
    }
    let (succeeded, distinct, interrupted, other, span): (i64, i64, i64, i64, Option<f64>) =
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT count(*) FILTER (WHERE state = 'succeeded'),
                 count(DISTINCT job_id) FILTER (WHERE state = 'succeeded'),
                 count(*) FILTER (WHERE error = $2),
                 count(*) FILTER (WHERE state <> 'succeeded' AND error IS DISTINCT FROM $2),
                 extract(epoch FROM max(created_at) FILTER (WHERE state = 'succeeded')
                     - min(created_at) FILTER (WHERE state = 'succeeded'))::float8
             FROM {} WHERE job_id = ANY($1)",
            database.table("job_history")
        )))
        .bind(ids)
        .bind(INTERRUPTED)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    let expected = i64::try_from(ids.len()).unwrap();
    if succeeded != expected || distinct != expected || other != 0 {
        violations.push(format!(
            "history: expected {expected} succeeded rows for {expected} jobs, found {succeeded} rows \
             for {distinct} jobs and {other} other non-interrupted rows"
        ));
    }
    let db_throughput = span.map_or(f64::NAN, |span| (succeeded - 1) as f64 / span);
    (violations, interrupted, db_throughput)
}

fn check_starts(planned: usize, enqueued: usize) -> Vec<String> {
    let mut violations = Vec::new();
    let mut counts = vec![0_u32; planned];
    for start in STARTS.lock().unwrap().iter() {
        match counts.get_mut(usize::try_from(start.id).unwrap()) {
            Some(count) => *count += 1,
            None => violations.push(format!("job {} ran but was never planned", start.id)),
        }
    }
    let repeated: Vec<usize> = (0..planned).filter(|id| counts[*id] > 1).collect();
    let never: Vec<usize> = (0..planned).filter(|id| counts[*id] == 0).collect();
    if !repeated.is_empty() {
        violations.push(format!(
            "{} jobs ran more than once, first {:?}",
            repeated.len(),
            &repeated[..repeated.len().min(10)]
        ));
    }
    if never.len() != planned - enqueued {
        violations.push(format!(
            "{} of {enqueued} enqueued jobs never ran, first local ids {:?}",
            never.len().saturating_sub(planned - enqueued),
            &never[..never.len().min(10)]
        ));
    }
    let stale = STALE.load(Ordering::SeqCst);
    if stale > 0 {
        violations.push(format!("{stale} jobs from an earlier run executed"));
    }
    violations
}

enum Host {
    InProcess(Vec<RunningWorker>),
    Children(Vec<Child>),
}

struct NotificationLog {
    arrivals: Arc<Mutex<Vec<Instant>>>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

async fn listen_notifications(database: &Database, probe: &PgPool) -> NotificationLog {
    let mut listener = PgListener::connect_with(probe).await.unwrap();
    listener
        .listen(&format!("{}.job", database.schema_name))
        .await
        .unwrap();
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let arrivals = Arc::clone(&arrivals);
        let stop = stop.clone();
        async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    message = listener.recv() => {
                        message.unwrap();
                        arrivals.lock().unwrap().push(Instant::now());
                    }
                }
            }
        }
    });
    NotificationLog {
        arrivals,
        stop,
        task,
    }
}

async fn notification_lag(fed: &Fed, log: NotificationLog) -> [f64; 3] {
    let deadline = Instant::now() + Duration::from_secs(5);
    while log.arrivals.lock().unwrap().len() < fed.ids.len() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    log.stop.cancel();
    log.task.await.unwrap();
    let arrivals = log.arrivals.lock().unwrap().clone();
    let mut commits: Vec<Instant> = fed.enqueued_at.iter().flatten().copied().collect();
    commits.sort();
    if arrivals.len() != commits.len() {
        eprintln!(
            "  {} notifications arrived for {} enqueues",
            arrivals.len(),
            commits.len()
        );
    }
    let mut lags: Vec<f64> = arrivals
        .iter()
        .zip(&commits)
        .map(|(arrival, commit)| {
            if arrival >= commit {
                millis(arrival.duration_since(*commit))
            } else {
                -millis(commit.duration_since(*arrival))
            }
        })
        .collect();
    lags.sort_by(f64::total_cmp);
    [
        percentile(&lags, 0.5),
        percentile(&lags, 0.99),
        lags.last().copied().unwrap_or(f64::NAN),
    ]
}

fn spawn_children(
    database: &Database,
    processes: usize,
    total: usize,
    max_workers: usize,
) -> Vec<Child> {
    let (workers, concurrency) = topology(total / processes, (max_workers / processes).max(1));
    (0..processes)
        .map(|_| {
            Command::new(std::env::current_exe().unwrap())
                .args(["test_stress", "--exact", "--ignored", "--nocapture"])
                .env("STRESS_CHILD_SCHEMA", &database.schema_name)
                .env("STRESS_CHILD_WORKERS", workers.to_string())
                .env("STRESS_CHILD_CONCURRENCY", concurrency.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap()
        })
        .collect()
}

async fn stop_children(mut children: Vec<Child>) {
    for child in &mut children {
        drop(child.stdin.take());
    }
    tokio::task::spawn_blocking(move || {
        for mut child in children {
            let status = child.wait().unwrap();
            assert!(status.success(), "worker process exited with {status}");
        }
    })
    .await
    .unwrap();
}

async fn wait_drained(database: &Database, probe: &PgPool, timeout: Duration) -> Option<Instant> {
    let deadline = Instant::now() + timeout;
    let sql = format!(
        "SELECT count(*) FROM {} WHERE state IN ('scheduled', 'enqueued', 'running')",
        database.table("job")
    );
    loop {
        let pending: i64 = sqlx::query_scalar(AssertSqlSafe(sql.clone()))
            .fetch_one(probe)
            .await
            .unwrap();
        if pending == 0 {
            return Some(Instant::now());
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run_child(schema: String) {
    let workers = env_u64("STRESS_CHILD_WORKERS", 1) as usize;
    let concurrency = env_u64("STRESS_CHILD_CONCURRENCY", 8) as usize;
    let options: PgConnectOptions = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(u32::try_from(workers * (concurrency + 2) + 2).unwrap())
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options.application_name(&format!("{schema}_child")))
        .await
        .unwrap();
    jalari::init(pool)
        .schema(Schema::named(&schema).unwrap())
        .migrate()
        .await
        .unwrap();
    let mut running = Vec::new();
    for _ in 0..workers {
        running.push(RunningWorker::spawn(
            Worker::builder()
                .queue(QUEUE, concurrency)
                .scheduler(false)
                .config(WorkerConfig::default())
                .build()
                .await
                .unwrap(),
        ));
    }
    tokio::task::spawn_blocking(|| {
        let mut sink = String::new();
        std::io::stdin().read_to_string(&mut sink)
    })
    .await
    .unwrap()
    .unwrap();
    for worker in running {
        worker.stop().await;
    }
}

async fn run_once(
    database: &Database,
    probe: &PgPool,
    settings: &Settings,
    point: &Point,
    rng: &mut Rng,
    explain: bool,
) -> (Sample, Vec<String>) {
    reset(database).await;
    let run = CURRENT_RUN.fetch_add(1, Ordering::SeqCst) + 1;
    let jobs = Arc::new(plan(run, point.load, rng));
    if let Some(filler) = point.filler {
        fill(database, filler).await;
    }
    let overlaps_before = OCCUPANCY.violations().len();
    let mut queries_us = None;
    let mut notify_lag = None;
    let (workers, fed, spawned) = match point.feed {
        Feed::Backlog if point.processes > 0 => {
            let fed = feed_transactions(&database.pool, &jobs, LOAD_BATCH, LOADERS).await;
            let spawned = Instant::now();
            let children = spawn_children(
                database,
                point.processes,
                point.concurrency,
                settings.max_workers,
            );
            (Host::Children(children), fed, spawned)
        }
        Feed::Backlog => {
            let fed = feed_transactions(&database.pool, &jobs, LOAD_BATCH, LOADERS).await;
            if point.time_queries {
                queries_us = Some(time_queries(database, probe, explain).await);
            }
            let (workers, spawned) = start_workers(point.concurrency, settings.max_workers).await;
            (Host::InProcess(workers), fed, spawned)
        }
        Feed::Rate(rate) => {
            let (workers, spawned) = start_workers(point.concurrency, settings.max_workers).await;
            let listener = listen_notifications(database, probe).await;
            tokio::time::sleep(IDLE_BEFORE_FEED).await;
            let fed = feed_rate(Arc::clone(&jobs), Some(rate), PRODUCERS).await;
            notify_lag = Some(notification_lag(&fed, listener).await);
            (Host::InProcess(workers), fed, spawned)
        }
        Feed::Separate => {
            let (workers, spawned) = start_workers(point.concurrency, settings.max_workers).await;
            tokio::time::sleep(IDLE_BEFORE_FEED).await;
            (
                Host::InProcess(workers),
                feed_rate(Arc::clone(&jobs), None, 1).await,
                spawned,
            )
        }
        Feed::Batch => {
            let (workers, spawned) = start_workers(point.concurrency, settings.max_workers).await;
            tokio::time::sleep(IDLE_BEFORE_FEED).await;
            let fed = feed_transactions(&database.pool, &jobs, jobs.len(), 1).await;
            (Host::InProcess(workers), fed, spawned)
        }
    };
    let target = u64::try_from(fed.ids.len()).unwrap();
    let in_process = matches!(workers, Host::InProcess(_));
    let done = match workers {
        Host::InProcess(workers) => {
            let done = wait_finished(target, settings.timeout).await;
            for worker in workers {
                worker.stop().await;
            }
            done
        }
        Host::Children(children) => {
            let done = wait_drained(database, probe, settings.timeout).await;
            stop_children(children).await;
            done
        }
    };

    let mut violations = Vec::new();
    let finished_at = done.unwrap_or_else(|| {
        violations.push(format!(
            "timed out after {:?} with {} of {target} jobs finished in this process",
            settings.timeout,
            FINISHED.load(Ordering::SeqCst)
        ));
        Instant::now()
    });
    let origin = match point.feed {
        Feed::Backlog => spawned,
        Feed::Rate(_) | Feed::Separate | Feed::Batch => fed.started,
    };
    let elapsed = finished_at.saturating_duration_since(origin).as_secs_f64();
    let first_enqueued = fed.enqueued_at.iter().flatten().min().copied();
    let mut latencies = Vec::new();
    let mut early_peak = 0;
    for start in STARTS.lock().unwrap().iter() {
        if let Some(Some(enqueued)) = fed.enqueued_at.get(usize::try_from(start.id).unwrap()) {
            latencies.push(millis(start.at.saturating_duration_since(*enqueued)));
        }
        if first_enqueued.is_some_and(|first| start.at <= first + EARLY_WINDOW) {
            early_peak = early_peak.max(start.in_flight);
        }
    }
    latencies.sort_by(f64::total_cmp);
    let mut lags = fed.lag_ms.clone();
    lags.sort_by(f64::total_cmp);
    let (mut db_violations, interrupted, db_throughput) =
        verify(database, &fed.ids, jobs.len(), in_process).await;
    violations.append(&mut db_violations);
    if let Some(filler) = point.filler {
        violations.extend(check_filler(database, filler).await);
    }
    for overlap in &OCCUPANCY.violations()[overlaps_before..] {
        violations.push(format!("queue_key overlap: {overlap}"));
    }
    if !fed.errors.is_empty() {
        violations.push(format!(
            "{} enqueues failed, first {:?}",
            fed.errors.len(),
            &fed.errors[..fed.errors.len().min(3)]
        ));
    }
    let log = LOG.lock().unwrap();
    if !log.warnings.is_empty() {
        eprintln!("  library warnings {:?}", log.warnings);
    }
    let sample = Sample {
        jobs: fed.ids.len(),
        elapsed,
        throughput: fed.ids.len() as f64 / elapsed,
        enqueue_rate: fed.ids.len() as f64
            / fed
                .finished
                .saturating_duration_since(fed.started)
                .as_secs_f64(),
        latency: [
            percentile(&latencies, 0.5),
            percentile(&latencies, 0.9),
            percentile(&latencies, 0.99),
            latencies.last().copied().unwrap_or(f64::NAN),
        ],
        producer_lag_p99: percentile(&lags, 0.99),
        notify_lag,
        early_peak,
        warnings: log.warnings.values().sum(),
        enqueue_errors: fed.errors.len(),
        postpones: log.postpones,
        interrupted,
        slow_starts: latencies
            .iter()
            .filter(|ms| **ms > millis(SLOW_START))
            .count(),
        queries_us,
        db_throughput,
        overshoot_ms: OVERSHOOT_US.load(Ordering::SeqCst) as f64
            / 1000.0
            / SLEEPS.load(Ordering::SeqCst) as f64,
    };
    (sample, violations)
}

fn describe(sample: &Sample) -> String {
    let mut line = format!(
        "jobs={} elapsed={:.2}s throughput={:.0}/s db_throughput={:.0}/s enqueue={:.0}/s \
         latency_ms p50={:.1} p90={:.1} p99={:.1} max={:.1} early_peak={} warnings={} \
         enqueue_errors={} postpones={} interrupted={} slow_starts={}",
        sample.jobs,
        sample.elapsed,
        sample.throughput,
        sample.db_throughput,
        sample.enqueue_rate,
        sample.latency[0],
        sample.latency[1],
        sample.latency[2],
        sample.latency[3],
        sample.early_peak,
        sample.warnings,
        sample.enqueue_errors,
        sample.postpones,
        sample.interrupted,
        sample.slow_starts
    );
    if !sample.producer_lag_p99.is_nan() {
        line.push_str(&format!(
            " producer_lag_p99={:.1}ms",
            sample.producer_lag_p99
        ));
    }
    if !sample.overshoot_ms.is_nan() {
        line.push_str(&format!(" sleep_overshoot={:.2}ms", sample.overshoot_ms));
    }
    if let Some([p50, p99, max]) = sample.notify_lag {
        line.push_str(&format!(
            " notify_lag_ms p50={p50:.1} p99={p99:.1} max={max:.1}"
        ));
    }
    if let Some([claim, fence, siblings]) = sample.queries_us {
        line.push_str(&format!(
            " claim_us={claim:.0} fence_us={fence:.0} siblings_us={siblings:.0}"
        ));
    }
    line
}

fn summarize(scenario: &str, point: &Point, samples: &[Sample]) {
    let collect = |metric: fn(&Sample) -> f64| samples.iter().map(metric).collect::<Vec<f64>>();
    let mut line = format!(
        "SUMMARY {scenario} {} runs={} {} {} {} {} {} {} {} {} warnings={} enqueue_errors={} \
         postpones={} interrupted={} slow_starts={}",
        point.label,
        samples.len(),
        show("throughput/s", &collect(|s| s.throughput)),
        show("db_throughput/s", &collect(|s| s.db_throughput)),
        show("enqueue/s", &collect(|s| s.enqueue_rate)),
        show("p50ms", &collect(|s| s.latency[0])),
        show("p90ms", &collect(|s| s.latency[1])),
        show("p99ms", &collect(|s| s.latency[2])),
        show("maxms", &collect(|s| s.latency[3])),
        show("early_peak", &collect(|s| s.early_peak as f64)),
        samples.iter().map(|s| s.warnings).sum::<usize>(),
        samples.iter().map(|s| s.enqueue_errors).sum::<usize>(),
        samples.iter().map(|s| s.postpones).sum::<usize>(),
        samples.iter().map(|s| s.interrupted).sum::<i64>(),
        samples.iter().map(|s| s.slow_starts).sum::<usize>(),
    );
    if samples.iter().any(|s| s.queries_us.is_some()) {
        let query = |slot: usize| {
            samples
                .iter()
                .filter_map(|s| s.queries_us.map(|q| q[slot]))
                .collect::<Vec<f64>>()
        };
        line.push_str(&format!(
            " {} {} {}",
            show("claim_us", &query(0)),
            show("fence_us", &query(1)),
            show("siblings_us", &query(2))
        ));
    }
    if samples.iter().any(|s| s.notify_lag.is_some()) {
        let lag = |slot: usize| {
            samples
                .iter()
                .filter_map(|s| s.notify_lag.map(|l| l[slot]))
                .collect::<Vec<f64>>()
        };
        line.push_str(&format!(
            " {} {}",
            show("notify_lag_p50_ms", &lag(0)),
            show("notify_lag_p99_ms", &lag(1))
        ));
    }
    if samples.iter().any(|s| !s.overshoot_ms.is_nan()) {
        line.push_str(&format!(
            " {}",
            show("sleep_overshoot_ms", &collect(|s| s.overshoot_ms))
        ));
    }
    eprintln!("{line}");
}

async fn measure(
    database: &Database,
    probe: &PgPool,
    settings: &Settings,
    scenario: &str,
    points: &[Point],
    rng: &mut Rng,
    failures: &mut Vec<String>,
) -> Vec<Vec<Sample>> {
    eprintln!("SCENARIO {scenario} uptime: {}", uptime());
    let mut samples: Vec<Vec<Sample>> = points.iter().map(|_| Vec::new()).collect();
    for run in 0..settings.runs {
        for (index, point) in points.iter().enumerate() {
            let (sample, violations) =
                run_once(database, probe, settings, point, rng, run == 0).await;
            eprintln!(
                "RUN {scenario} {} #{}: {}",
                point.label,
                run + 1,
                describe(&sample)
            );
            for violation in violations {
                eprintln!("  VIOLATION {violation}");
                failures.push(format!(
                    "{scenario} {} run {}: {violation}",
                    point.label,
                    run + 1
                ));
            }
            samples[index].push(sample);
        }
    }
    for (point, samples) in points.iter().zip(&samples) {
        summarize(scenario, point, samples);
    }
    eprintln!("SCENARIO {scenario} done, uptime: {}", uptime());
    samples
}

async fn probe_round_trips(database: &Database, probe: &PgPool) {
    eprintln!("SCENARIO probe uptime: {}", uptime());
    let mut connection = probe.acquire().await.unwrap();
    let rounds = 2000_u32;
    let started = Instant::now();
    for _ in 0..rounds {
        sqlx::query("SELECT 1")
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    let select_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(rounds);
    let table = database.table("stress_probe");
    execute_on(
        &mut connection,
        format!(
            "CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY, n BIGINT NOT NULL);
             INSERT INTO {table} VALUES (1, 0) ON CONFLICT DO NOTHING"
        ),
    )
    .await;
    let update = format!("UPDATE {table} SET n = n + 1 WHERE id = 1");
    let rounds = 1000_u32;
    let started = Instant::now();
    for _ in 0..rounds {
        sqlx::raw_sql("BEGIN")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(update.clone()))
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::raw_sql("COMMIT")
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    let commit_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(rounds);
    drop(connection);
    reset(database).await;
    let job = Stress {
        run: 0,
        id: 0,
        sleep_us: 0,
        key: None,
    };
    let started = Instant::now();
    for _ in 0..rounds {
        jalari::enqueue_with(&job, EnqueueOptions::new())
            .await
            .unwrap();
    }
    let enqueue_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(rounds);
    reset(database).await;
    eprintln!(
        "PROBE select_1_rtt_us={select_us:.1} begin_update_commit_us={commit_us:.1} \
         sequential_enqueue_us={enqueue_us:.1}"
    );
}

fn drain_points(settings: &Settings) -> Vec<Point> {
    settings
        .concurrency
        .iter()
        .map(|&total| Point {
            label: format!("T{total}"),
            load: Load {
                jobs: settings.drain_jobs.min(settings.drain_per_slot * total),
                keys: 0,
                sleep_ms: (0, 0),
            },
            feed: Feed::Backlog,
            concurrency: usize::try_from(total).unwrap(),
            filler: None,
            time_queries: false,
            processes: 0,
        })
        .collect()
}

fn steady_points(settings: &Settings) -> Vec<Point> {
    settings
        .steady_rates
        .iter()
        .map(|&rate| Point {
            label: format!("R{rate}"),
            load: Load {
                jobs: rate * settings.steady_seconds,
                keys: 0,
                sleep_ms: (0, 0),
            },
            feed: Feed::Rate(rate as f64),
            concurrency: settings.steady_concurrency,
            filler: None,
            time_queries: false,
            processes: 0,
        })
        .collect()
}

fn body_points(settings: &Settings) -> Vec<Point> {
    let mean_ms = (settings.body_ms.0 + settings.body_ms.1) as f64 / 2.0;
    settings
        .concurrency
        .iter()
        .map(|&total| Point {
            label: format!("T{total}"),
            load: Load {
                jobs: ((settings.body_seconds as f64 * 1000.0 / mean_ms) * total as f64) as u64,
                keys: 0,
                sleep_ms: settings.body_ms,
            },
            feed: Feed::Backlog,
            concurrency: usize::try_from(total).unwrap(),
            filler: None,
            time_queries: false,
            processes: 0,
        })
        .collect()
}

fn key_points(settings: &Settings) -> Vec<Point> {
    let mut points = vec![Point {
        label: "K0".to_owned(),
        load: Load {
            jobs: settings.key_jobs,
            keys: 0,
            sleep_ms: (0, 0),
        },
        feed: Feed::Backlog,
        concurrency: settings.key_concurrency,
        filler: None,
        time_queries: false,
        processes: 0,
    }];
    points.extend(settings.keys.iter().map(|&keys| Point {
        label: format!("K{keys}"),
        load: Load {
            jobs: settings.key_jobs,
            keys,
            sleep_ms: (0, 0),
        },
        feed: Feed::Backlog,
        concurrency: settings.key_concurrency,
        filler: None,
        time_queries: true,
        processes: 0,
    }));
    points
}

fn big_points(settings: &Settings) -> Vec<Point> {
    let mut points = Vec::new();
    for keys in [0, settings.big_keys] {
        for big in [false, true] {
            points.push(Point {
                label: format!(
                    "{}_{}",
                    if big { "big" } else { "empty" },
                    if keys > 0 {
                        format!("K{keys}")
                    } else {
                        "nokey".to_owned()
                    }
                ),
                load: Load {
                    jobs: if keys > 0 {
                        settings.big_key_jobs
                    } else {
                        settings.big_jobs
                    },
                    keys,
                    sleep_ms: (0, 0),
                },
                feed: Feed::Backlog,
                concurrency: settings.big_concurrency,
                filler: big.then_some(Filler {
                    finished: settings.big_finished,
                    future: settings.big_future,
                    keys,
                }),
                time_queries: true,
                processes: 0,
            });
        }
    }
    points
}

fn process_points(settings: &Settings) -> Vec<Point> {
    settings
        .processes
        .iter()
        .map(|&processes| Point {
            label: format!("P{processes}"),
            load: Load {
                jobs: settings.drain_jobs,
                keys: 0,
                sleep_ms: (0, 0),
            },
            feed: Feed::Backlog,
            concurrency: settings.process_concurrency,
            filler: None,
            time_queries: false,
            processes: usize::try_from(processes).unwrap(),
        })
        .collect()
}

fn batch_points(settings: &Settings) -> Vec<Point> {
    [("batched", Feed::Batch), ("separate", Feed::Separate)]
        .into_iter()
        .map(|(label, feed)| Point {
            label: label.to_owned(),
            load: Load {
                jobs: settings.batch_jobs,
                keys: 0,
                sleep_ms: (settings.batch_sleep_ms, settings.batch_sleep_ms),
            },
            feed,
            concurrency: settings.batch_concurrency,
            filler: None,
            time_queries: false,
            processes: 0,
        })
        .collect()
}

fn report_steady(settings: &Settings, samples: &[Vec<Sample>]) {
    let p99 =
        |samples: &[Sample]| spread(&samples.iter().map(|s| s.latency[2]).collect::<Vec<_>>()).0;
    let Some(baseline) = samples.first().map(|first| p99(first)) else {
        return;
    };
    let mut flat = None;
    for (rate, samples) in settings.steady_rates.iter().zip(samples) {
        let throughput = spread(&samples.iter().map(|s| s.throughput).collect::<Vec<_>>()).0;
        let enqueue = spread(&samples.iter().map(|s| s.enqueue_rate).collect::<Vec<_>>()).0;
        let kept_up = throughput >= 0.95 * *rate as f64 && enqueue >= 0.95 * *rate as f64;
        let level = p99(samples) <= (2.0 * baseline).max(baseline + 5.0);
        eprintln!(
            "STEADY R{rate}: median p99 {:.1} ms (baseline {baseline:.1}), throughput {throughput:.0}/s, \
             enqueue {enqueue:.0}/s, kept_up={kept_up} flat={level}",
            p99(samples)
        );
        if kept_up && level {
            flat = Some(*rate);
        }
    }
    eprintln!("STEADY highest flat rate: {flat:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "load test; run with --ignored --nocapture --release"]
async fn test_stress() {
    if let Ok(schema) = std::env::var("STRESS_CHILD_SCHEMA") {
        run_child(schema).await;
        return;
    }
    let Some(database) = Database::start("stress", POOL_SIZE).await else {
        return;
    };
    tracing::subscriber::set_global_default(LogCapture {
        next_span: AtomicU64::new(1),
    })
    .unwrap();
    LazyLock::force(&EPOCH);
    let probe = database.connect("probe", 2).await;
    let settings = Settings::from_env();
    let mut rng = Rng::new(common::seed("stress"));
    let only = std::env::var("STRESS_ONLY").unwrap_or_default();
    let selected = |name: &str| {
        if only.is_empty() {
            !OPT_IN_SCENARIOS.contains(&name)
        } else {
            only.split(',').any(|item| item == name)
        }
    };
    let mut failures = Vec::new();
    for scenario in SCENARIOS {
        if !selected(scenario) {
            continue;
        }
        let points = match scenario {
            "probe" => {
                probe_round_trips(&database, &probe).await;
                continue;
            }
            "drain" => drain_points(&settings),
            "steady" => steady_points(&settings),
            "bodies" => body_points(&settings),
            "queue_key" => key_points(&settings),
            "big_table" => big_points(&settings),
            "batch" => batch_points(&settings),
            _ => process_points(&settings),
        };
        let samples = measure(
            &database,
            &probe,
            &settings,
            scenario,
            &points,
            &mut rng,
            &mut failures,
        )
        .await;
        if scenario == "steady" {
            report_steady(&settings, &samples);
        }
    }
    probe.close().await;
    database.finish().await;
    assert!(
        failures.is_empty(),
        "{} correctness violations under load:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
