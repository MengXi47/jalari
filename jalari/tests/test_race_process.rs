mod common;

use std::future::Future;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{Database, HistoryRow, JobRow, Rng};
use jalari::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{CancellationToken, EnqueueOptions, Job, JobError, JobResult, Schema, Worker};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

const CHILD_ENV: &str = "JALARI_RACE_CHILD";
const SCHEMA_ENV: &str = "JALARI_RACE_CHILD_SCHEMA";
const QUEUES_ENV: &str = "JALARI_RACE_CHILD_QUEUES";
const LINK_ENV: &str = "JALARI_RACE_CHILD_LINK";
const STEP: Duration = Duration::from_millis(100);
const POLL: Duration = Duration::from_millis(200);
const INTERRUPTED: &str = "the worker stopped during the last attempt";

struct Probe {
    pool: PgPool,
    table: String,
    child: String,
    runtime: tokio::runtime::Handle,
    runs: AtomicU64,
}

static PROBE: OnceLock<Probe> = OnceLock::new();

struct Run {
    name: String,
    job_id: i64,
    attempt: i32,
    queue_key: Option<String>,
}

impl Run {
    async fn begin(queue_key: Option<String>) -> Self {
        let probe = PROBE
            .get()
            .expect("the probe is set up before the worker runs");
        let job = jalari::current_job().expect("a run begins inside a job");
        let run = Self {
            name: format!(
                "{}#{}",
                probe.child,
                probe.runs.fetch_add(1, Ordering::SeqCst)
            ),
            job_id: job.id().0,
            attempt: job.attempt(),
            queue_key,
        };
        run.record("start").await;
        run
    }

    async fn record(&self, kind: &str) {
        insert_event(
            &self.name,
            self.job_id,
            self.attempt,
            self.queue_key.as_deref(),
            kind,
        )
        .await;
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        let probe = PROBE
            .get()
            .expect("the probe is set up before the worker runs");
        let name = self.name.clone();
        let job_id = self.job_id;
        let attempt = self.attempt;
        let queue_key = self.queue_key.clone();
        probe.runtime.spawn(async move {
            insert_event(&name, job_id, attempt, queue_key.as_deref(), "stop").await;
        });
    }
}

async fn insert_event(run: &str, job_id: i64, attempt: i32, queue_key: Option<&str>, kind: &str) {
    let probe = PROBE
        .get()
        .expect("the probe is set up before the worker runs");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (run, job_id, attempt, child, queue_key, kind)
         VALUES ($1, $2, $3, $4, $5, $6)",
        probe.table
    )))
    .bind(run)
    .bind(job_id)
    .bind(attempt)
    .bind(&probe.child)
    .bind(queue_key)
    .bind(kind)
    .execute(&probe.pool)
    .await
    .expect("failed to record a probe event");
}

#[derive(Serialize, Deserialize)]
struct Work {
    key: Option<String>,
    first_ms: u64,
    later_ms: u64,
    fail_attempts: i32,
}

#[jalari::job(queue = "chaos")]
impl Job for Work {
    const NAME: &'static str = "proc_work";
    const MAX_ATTEMPTS: i32 = 20;

    async fn run(self) -> JobResult {
        perform(self).await
    }
}

#[derive(Serialize, Deserialize)]
struct Fragile(Work);

#[jalari::job(queue = "chaos")]
impl Job for Fragile {
    const NAME: &'static str = "proc_fragile";
    const MAX_ATTEMPTS: i32 = 2;

    async fn run(self) -> JobResult {
        perform(self.0).await
    }
}

async fn perform(work: Work) -> JobResult {
    let run = Run::begin(work.key).await;
    let length = Duration::from_millis(if run.attempt == 1 {
        work.first_ms
    } else {
        work.later_ms
    });
    let started = Instant::now();
    while started.elapsed() < length {
        tokio::time::sleep(STEP).await;
        run.record("tick").await;
    }
    if run.attempt <= work.fail_attempts {
        run.record("fail").await;
        return Err(JobError::new(format!(
            "planned failure of attempt {}",
            run.attempt
        )));
    }
    run.record("finish").await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "runs only inside the worker processes that test_race_process spawns"]
async fn test_race_process_child() {
    let Ok(child) = std::env::var(CHILD_ENV) else {
        return;
    };
    std::thread::spawn(|| {
        let _ = std::io::copy(&mut std::io::stdin(), &mut std::io::sink());
        std::process::exit(0);
    });
    let schema_name = std::env::var(SCHEMA_ENV).unwrap();
    let queues: Vec<(String, usize)> = std::env::var(QUEUES_ENV)
        .unwrap()
        .split(',')
        .map(|entry| {
            let (name, concurrency) = entry.split_once(':').unwrap();
            (name.to_owned(), concurrency.parse().unwrap())
        })
        .collect();
    let concurrency: usize = queues.iter().map(|(_, concurrency)| concurrency).sum();
    let direct: PgConnectOptions = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    let application = format!("{schema_name}_{child}");
    let link = Link::parse(&std::env::var(LINK_ENV).unwrap());
    let job_options = match link {
        Link::Proxied(port) => direct.clone().host("127.0.0.1").port(port),
        Link::Shared | Link::OwnProbePool => direct.clone(),
    };
    let pool = PgPoolOptions::new()
        .max_connections(u32::try_from(concurrency + 2).unwrap())
        .acquire_timeout(Duration::from_secs(60))
        .connect_with(job_options.application_name(&application))
        .await
        .unwrap();
    let probe_pool = match link {
        Link::Shared => pool.clone(),
        Link::OwnProbePool | Link::Proxied(_) => PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(60))
            .connect_with(direct.application_name(&format!("{application}_probe")))
            .await
            .unwrap(),
    };
    let probe = Probe {
        pool: probe_pool,
        table: format!("\"{schema_name}\".probe"),
        child,
        runtime: tokio::runtime::Handle::current(),
        runs: AtomicU64::new(0),
    };
    assert!(PROBE.set(probe).is_ok(), "the probe is set up once");
    jalari::init(pool)
        .schema(Schema::named(&schema_name).unwrap())
        .await
        .unwrap();
    let mut builder = Worker::builder()
        .config(common::worker_config(POLL))
        .retry_policy(common::fast_retry())
        .scheduler(false);
    for (queue, concurrency) in &queues {
        builder = builder.queue(queue, *concurrency);
    }
    builder
        .build()
        .await
        .unwrap()
        .run(CancellationToken::new())
        .await;
}

#[derive(Clone, Copy)]
enum Link {
    Shared,
    OwnProbePool,
    Proxied(u16),
}

impl Link {
    fn encode(self) -> String {
        match self {
            Self::Shared => "shared".to_owned(),
            Self::OwnProbePool => "own_probe_pool".to_owned(),
            Self::Proxied(port) => port.to_string(),
        }
    }

    fn parse(encoded: &str) -> Self {
        match encoded {
            "shared" => Self::Shared,
            "own_probe_pool" => Self::OwnProbePool,
            port => Self::Proxied(port.parse().unwrap()),
        }
    }
}

struct ChildWorker {
    name: String,
    application: String,
    process: Child,
}

impl ChildWorker {
    fn spawn(database: &Database, name: &str, queues: &[(&str, usize)], link: Link) -> Self {
        let queues = queues
            .iter()
            .map(|(queue, concurrency)| format!("{queue}:{concurrency}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "test_race_process_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ENV, name)
            .env(SCHEMA_ENV, &database.schema_name)
            .env(QUEUES_ENV, queues)
            .env(LINK_ENV, link.encode())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        Self {
            name: name.to_owned(),
            application: format!("{}_{name}", database.schema_name),
            process: command.spawn().unwrap(),
        }
    }

    fn pid(&self) -> i32 {
        i32::try_from(self.process.id()).unwrap()
    }

    async fn wait_ready(&self) {
        let pid = self.pid();
        common::wait_until(
            &format!("child {} to register its worker", self.name),
            Duration::from_secs(30),
            move || async move {
                jalari::admin::workers()
                    .await
                    .unwrap()
                    .iter()
                    .any(|worker| worker.pid == pid)
            },
        )
        .await;
    }

    fn signal(&self, signal: &str) {
        let status = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.pid().to_string())
            .status()
            .unwrap();
        assert!(status.success(), "kill -{signal} {} failed", self.pid());
    }

    async fn kill(mut self, database: &Database) -> f64 {
        self.process.kill().unwrap();
        self.process.wait().unwrap();
        let died = db_now(&database.pool).await;
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {probe} (run, job_id, attempt, child, queue_key, kind)
             SELECT DISTINCT run, job_id, attempt, child, queue_key, 'killed'
             FROM {probe} AS event
             WHERE child = $1 AND NOT EXISTS (
                 SELECT 1 FROM {probe} AS last
                 WHERE last.run = event.run AND last.kind IN ('stop', 'killed')
             )",
            probe = database.table("probe")
        )))
        .bind(&self.name)
        .execute(&database.pool)
        .await
        .unwrap();
        died
    }
}

impl Drop for ChildWorker {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

struct Proxy {
    port: u16,
    blocked: watch::Sender<bool>,
    accept: JoinHandle<()>,
}

impl Proxy {
    async fn start(target: (String, u16), latency: Duration) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (blocked, gate) = watch::channel(false);
        let accept = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((client, _)) = listener.accept().await {
                let mut gate = gate.clone();
                let target = target.clone();
                connections.spawn(async move {
                    if !wait_open(&mut gate).await {
                        return;
                    }
                    let Ok(server) = TcpStream::connect((target.0.as_str(), target.1)).await else {
                        return;
                    };
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    tokio::join!(
                        pump(client_read, server_write, gate.clone(), latency),
                        pump(server_read, client_write, gate, latency),
                    );
                });
            }
        });
        Self {
            port,
            blocked,
            accept,
        }
    }

    fn set_blocked(&self, blocked: bool) {
        self.blocked.send_replace(blocked);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn wait_open(gate: &mut watch::Receiver<bool>) -> bool {
    gate.wait_for(|blocked| !*blocked).await.is_ok()
}

async fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    mut gate: watch::Receiver<bool>,
    latency: Duration,
) {
    let mut buffer = vec![0; 16 * 1024];
    loop {
        if !wait_open(&mut gate).await {
            return;
        }
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        if !wait_open(&mut gate).await || to.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
    if wait_open(&mut gate).await {
        let _ = to.shutdown().await;
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct RunRow {
    run: String,
    job_id: i64,
    attempt: i32,
    child: String,
    queue_key: Option<String>,
    started: f64,
    last_effect: f64,
    ended: Option<f64>,
    finished: bool,
    failed: bool,
}

impl RunRow {
    fn shares_with(&self, other: &Self) -> bool {
        self.job_id == other.job_id
            || (self.queue_key.is_some() && self.queue_key == other.queue_key)
    }

    fn alive_until(&self) -> f64 {
        self.ended.unwrap_or(self.last_effect)
    }

    fn cut_short(&self) -> bool {
        !self.finished && !self.failed
    }

    fn describe(&self, base: f64) -> String {
        format!(
            "{} (job {}, attempt {}, key {:?}) worked {:+.3}..{:+.3} s, stopped {}",
            self.run,
            self.job_id,
            self.attempt,
            self.queue_key,
            self.started - base,
            self.last_effect - base,
            self.ended.map_or_else(
                || "never".to_owned(),
                |ended| format!("{:+.3} s", ended - base)
            ),
        )
    }
}

#[derive(Default)]
struct Audit {
    violations: Vec<String>,
    overlaps: Vec<String>,
    alive_overlaps: Vec<String>,
    exhausted: usize,
}

fn audit(
    label: &str,
    ids: &[i64],
    jobs: &[JobRow],
    history: &[HistoryRow],
    runs: &[RunRow],
    base: f64,
) -> Audit {
    let mut audit = Audit::default();
    for &id in ids {
        let job_runs: Vec<&RunRow> = runs.iter().filter(|run| run.job_id == id).collect();
        let entries: Vec<&HistoryRow> = history.iter().filter(|entry| entry.job_id == id).collect();
        let Some(row) = jobs.iter().find(|job| job.id == id) else {
            audit
                .violations
                .push(format!("{label}: job {id} is missing from the job table"));
            continue;
        };
        let described: Vec<String> = job_runs.iter().map(|run| run.describe(base)).collect();
        let context = format!("row {row:?}, history {entries:?}, runs {described:?}");
        let mut problems = Vec::new();
        match row.state.as_str() {
            "succeeded" => {}
            "failed" if row.attempts == row.max_attempts => audit.exhausted += 1,
            state => problems.push(format!(
                "expected succeeded, or failed after all {} attempts, found {state} after {}",
                row.max_attempts, row.attempts
            )),
        }
        if row.attempts > row.max_attempts {
            problems.push(format!(
                "recorded {} attempts, more than MAX_ATTEMPTS {}",
                row.attempts, row.max_attempts
            ));
        }
        if job_runs.len() > usize::try_from(row.max_attempts).unwrap() {
            problems.push(format!(
                "ran {} times, more than MAX_ATTEMPTS {}",
                job_runs.len(),
                row.max_attempts
            ));
        }
        let mut recorded: Vec<i32> = entries.iter().map(|entry| entry.attempt).collect();
        recorded.sort_unstable();
        let expected: Vec<i32> = (1..=row.attempts).collect();
        if recorded != expected {
            problems.push(format!(
                "history records attempts {recorded:?}, expected each of 1..={} exactly once",
                row.attempts
            ));
        }
        let successes = entries
            .iter()
            .filter(|entry| entry.state == "succeeded")
            .count();
        if successes != usize::from(row.state == "succeeded") {
            problems.push(format!("history records {successes} successes"));
        }
        let mut attempts: Vec<i32> = job_runs.iter().map(|run| run.attempt).collect();
        attempts.sort_unstable();
        if attempts.windows(2).any(|pair| pair[0] == pair[1]) {
            problems.push(format!("one attempt number ran twice: {attempts:?}"));
        }
        if let Some(run) = job_runs.iter().find(|run| run.attempt > row.attempts) {
            problems.push(format!(
                "run {} has attempt {} beyond the {} recorded",
                run.run, run.attempt, row.attempts
            ));
        }
        if row.state == "succeeded"
            && !job_runs
                .iter()
                .any(|run| run.attempt == row.attempts && run.finished)
        {
            problems.push(format!(
                "succeeded without a finished run of attempt {}",
                row.attempts
            ));
        }
        for run in job_runs.iter().filter(|run| run.cut_short()) {
            if let Some(entry) = entries.iter().find(|entry| entry.attempt == run.attempt)
                && entry.error.as_deref() != Some(INTERRUPTED)
            {
                problems.push(format!(
                    "run {} was cut short but history records {entry:?} for its attempt",
                    run.run
                ));
            }
        }
        for problem in problems {
            audit
                .violations
                .push(format!("{label}: job {id} {problem}; {context}"));
        }
    }
    for (index, first) in runs.iter().enumerate() {
        for second in &runs[index + 1..] {
            if !first.shares_with(second) {
                continue;
            }
            let pair = format!("{} and {}", first.describe(base), second.describe(base));
            if first.started < second.last_effect && second.started < first.last_effect {
                audit
                    .overlaps
                    .push(format!("{label}: runs did work at the same time: {pair}"));
            } else if first.started < second.alive_until() && second.started < first.alive_until() {
                audit.alive_overlaps.push(format!(
                    "{label}: a run started before the other stopped: {pair}"
                ));
            }
        }
    }
    audit
}

async fn runs(database: &Database, ids: &[i64]) -> Vec<RunRow> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT run, job_id, attempt, child, queue_key,
             extract(epoch FROM min(at) FILTER (WHERE kind = 'start'))::float8 AS started,
             extract(epoch FROM max(at) FILTER (
                 WHERE kind IN ('start', 'tick', 'finish', 'fail')))::float8 AS last_effect,
             extract(epoch FROM min(at) FILTER (WHERE kind IN ('stop', 'killed')))::float8 AS ended,
             bool_or(kind = 'finish') AS finished,
             bool_or(kind = 'fail') AS failed
         FROM {}
         WHERE job_id = ANY($1)
         GROUP BY run, job_id, attempt, child, queue_key
         HAVING bool_or(kind = 'start')
         ORDER BY started",
        database.table("probe")
    )))
    .bind(ids)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn effects_after(database: &Database, run: &str, after: f64) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {}
         WHERE run = $1 AND kind IN ('tick', 'finish', 'fail') AND at > to_timestamp($2)",
        database.table("probe")
    )))
    .bind(run)
    .bind(after)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn busy_children(database: &Database) -> Vec<String> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT DISTINCT child FROM {probe} AS event
         WHERE kind = 'start' AND NOT EXISTS (
             SELECT 1 FROM {probe} AS last
             WHERE last.run = event.run AND last.kind IN ('stop', 'killed')
         )",
        probe = database.table("probe")
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn db_now(pool: &PgPool) -> f64 {
    sqlx::query_scalar("SELECT extract(epoch FROM clock_timestamp())::float8")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn job_transaction(pool: &PgPool, application: &str) -> Option<(i32, String)> {
    sqlx::query_as(
        "SELECT pid, xact_start::text FROM pg_stat_activity
         WHERE application_name = $1 AND xact_start IS NOT NULL
         ORDER BY xact_start
         LIMIT 1",
    )
    .bind(application)
    .fetch_optional(pool)
    .await
    .unwrap()
}

fn watch_transaction(
    pool: PgPool,
    transaction: Option<(i32, String)>,
    limit: Duration,
) -> JoinHandle<Option<f64>> {
    tokio::spawn(async move {
        let (pid, xact_start) = transaction?;
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            let ended: Option<f64> = sqlx::query_scalar(
                "SELECT extract(epoch FROM clock_timestamp())::float8
                 WHERE NOT EXISTS (
                     SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND xact_start::text = $2
                 )",
            )
            .bind(pid)
            .bind(&xact_start)
            .fetch_optional(&pool)
            .await
            .unwrap();
            if ended.is_some() {
                return ended;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    })
}

async fn wait_started(database: &Database, job_id: i64, attempt: i32, child: &str) {
    let probe = database.table("probe");
    let pool = &database.pool;
    let probe = &probe;
    common::wait_until(
        &format!("attempt {attempt} of job {job_id} to start on {child}"),
        Duration::from_secs(30),
        move || async move {
            sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT EXISTS (
                     SELECT 1 FROM {probe}
                     WHERE job_id = $1 AND attempt = $2 AND child = $3 AND kind = 'tick'
                 )"
            )))
            .bind(job_id)
            .bind(attempt)
            .bind(child)
            .fetch_one(pool)
            .await
            .unwrap()
        },
    )
    .await;
}

async fn wait_terminal(database: &Database, ids: &[i64], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let open: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE id = ANY($1) AND state NOT IN ('succeeded', 'failed')",
            database.table("job")
        )))
        .bind(ids)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        if open == 0 {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn enqueue_work(queue: &str, key: Option<&str>, first_ms: u64, later_ms: u64) -> i64 {
    let work = Work {
        key: key.map(str::to_owned),
        first_ms,
        later_ms,
        fail_attempts: 0,
    };
    let mut options = EnqueueOptions::new().queue(queue);
    if let Some(key) = key {
        options = options.queue_key(key);
    }
    jalari::enqueue_with(&work, options).await.unwrap().id().0
}

async fn enqueue_random(rng: &mut Rng) -> i64 {
    let key = rng.chance(50).then(|| format!("key{}", rng.below(3)));
    let fragile = rng.chance(20);
    let work = Work {
        key: key.clone(),
        first_ms: if fragile {
            1500 + rng.below(2000)
        } else {
            200 + rng.below(1800)
        },
        later_ms: if fragile {
            1000 + rng.below(1500)
        } else {
            100 + rng.below(700)
        },
        fail_attempts: i32::from(rng.chance(15)),
    };
    let mut options = EnqueueOptions::new();
    if let Some(key) = &key {
        options = options.queue_key(key);
    }
    let outcome = if fragile {
        jalari::enqueue_with(&Fragile(work), options).await
    } else {
        jalari::enqueue_with(&work, options).await
    };
    outcome.unwrap().id().0
}

fn selected(name: &str) -> bool {
    std::env::var("RACE_ONLY").map_or(true, |only| only == name)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_race_process() {
    let Some(database) = Database::start("proc", 3).await else {
        return;
    };
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {probe} (
             id bigserial PRIMARY KEY,
             run text NOT NULL,
             job_id bigint NOT NULL,
             attempt integer NOT NULL,
             child text NOT NULL,
             queue_key text,
             kind text NOT NULL,
             at timestamptz NOT NULL DEFAULT clock_timestamp()
         );
         CREATE INDEX ON {probe} (job_id);
         CREATE INDEX ON {probe} (run)",
        probe = database.table("probe")
    )))
    .execute(&database.pool)
    .await
    .unwrap();
    let seed = common::seed("test_race_process");

    let mut failures = Vec::new();
    if selected("kill_storm") {
        failures.extend(timed("kill_storm", check_kill_storm(&database, seed)).await);
    }
    if selected("sole_worker_kill") {
        failures.extend(timed("sole_worker_kill", check_sole_worker_kill(&database, seed)).await);
    }
    if selected("false_interruptions") {
        failures.extend(
            timed(
                "false_interruptions",
                check_false_interruptions(&database, seed),
            )
            .await,
        );
    }
    if selected("freezes") {
        failures.extend(timed("freezes", check_freezes(&database, seed)).await);
    }
    if selected("partitions") {
        failures.extend(timed("partitions", check_partitions(&database, seed)).await);
    }

    database.finish().await;
    assert!(
        failures.is_empty(),
        "{} violations (seed {seed}):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

async fn check_kill_storm(database: &Database, seed: u64) -> Vec<String> {
    let label = format!("kill_storm seed {seed}");
    let mut rng = Rng::new(seed).fork(1);
    let scale = common::scale();
    let total_jobs = usize::try_from(25 * scale).unwrap();
    let storm = Duration::from_secs(8 * scale);
    let mut spawned = 0;
    let mut children = Vec::new();
    for _ in 0..3 {
        children.push(ChildWorker::spawn(
            database,
            &format!("k{spawned}"),
            &[("chaos", 2)],
            Link::Shared,
        ));
        spawned += 1;
    }
    for child in &children {
        child.wait_ready().await;
    }

    let started = Instant::now();
    let mut ids = Vec::new();
    let (mut kills, mut busy_kills, mut burst_kills) = (0, 0, 0);
    while started.elapsed() < storm {
        tokio::time::sleep(rng.millis(100, 600)).await;
        let roll = rng.below(100);
        let victim = if roll < 55 && ids.len() < total_jobs {
            for _ in 0..=rng.below(2) {
                ids.push(enqueue_random(&mut rng).await);
            }
            if !rng.chance(25) {
                continue;
            }
            tokio::time::sleep(rng.millis(0, 40)).await;
            burst_kills += 1;
            usize::try_from(rng.below(children.len() as u64)).unwrap()
        } else if roll < 85 {
            let busy = busy_children(database).await;
            let candidates: Vec<usize> = (0..children.len())
                .filter(|&index| busy.contains(&children[index].name))
                .collect();
            if !candidates.is_empty() && rng.chance(70) {
                busy_kills += 1;
                *rng.pick(&candidates)
            } else {
                usize::try_from(rng.below(children.len() as u64)).unwrap()
            }
        } else {
            continue;
        };
        kills += 1;
        children.swap_remove(victim).kill(database).await;
        children.push(ChildWorker::spawn(
            database,
            &format!("k{spawned}"),
            &[("chaos", 2)],
            Link::Shared,
        ));
        spawned += 1;
    }
    while ids.len() < total_jobs {
        ids.push(enqueue_random(&mut rng).await);
    }
    for child in &children {
        child.wait_ready().await;
    }

    let mut failures = Vec::new();
    if !wait_terminal(database, &ids, Duration::from_secs(90)).await {
        failures.push(format!(
            "{label}: jobs were still waiting 90 s after the storm ended"
        ));
    }
    for child in children {
        child.kill(database).await;
    }

    let runs = runs(database, &ids).await;
    let jobs = database.jobs().await;
    let history = database.history().await;
    let base = runs.first().map_or(0.0, |run| run.started);
    let audit = audit(&label, &ids, &jobs, &history, &runs, base);
    let interrupted = history
        .iter()
        .filter(|entry| ids.contains(&entry.job_id) && entry.error.as_deref() == Some(INTERRUPTED))
        .count();
    let cut_short = runs.iter().filter(|run| run.cut_short()).count();
    eprintln!(
        "{label}: {} jobs, {} runs, {kills} kills ({busy_kills} aimed at busy workers, \
         {burst_kills} right after an enqueue), {cut_short} runs cut short, \
         {interrupted} interrupted attempts recorded, {} jobs used up their attempts, \
         {} near misses where a run started before a killed run's kill was observed",
        ids.len(),
        runs.len(),
        audit.exhausted,
        audit.alive_overlaps.len()
    );
    for near_miss in &audit.alive_overlaps {
        eprintln!("  near miss: {near_miss}");
    }
    failures.extend(audit.violations);
    failures.extend(audit.overlaps);
    failures
}

async fn check_sole_worker_kill(database: &Database, seed: u64) -> Vec<String> {
    let label = format!("sole_worker_kill seed {seed}");
    let mut rng = Rng::new(seed).fork(2);
    let victim = ChildWorker::spawn(database, "s_victim", &[("sole", 2)], Link::Shared);
    victim.wait_ready().await;
    let long = enqueue_work("sole", Some("sole"), 60_000, 300).await;
    let last_try = Fragile(Work {
        key: Some("exhaust".to_owned()),
        first_ms: 100,
        later_ms: 60_000,
        fail_attempts: 1,
    });
    let exhausted = jalari::enqueue_with(
        &last_try,
        EnqueueOptions::new().queue("sole").queue_key("exhaust"),
    )
    .await
    .unwrap()
    .id()
    .0;
    wait_started(database, long, 1, "s_victim").await;
    wait_started(database, exhausted, 2, "s_victim").await;
    let sibling = enqueue_work("sole", Some("sole"), 300, 300).await;
    let exhausted_sibling = enqueue_work("sole", Some("exhaust"), 300, 300).await;
    let rescuer = ChildWorker::spawn(database, "s_rescuer", &[("sole", 2)], Link::Shared);
    rescuer.wait_ready().await;
    tokio::time::sleep(rng.millis(200, 600)).await;
    let died = victim.kill(database).await;

    let ids = [long, sibling, exhausted, exhausted_sibling];
    let mut failures = Vec::new();
    if !wait_terminal(database, &ids, Duration::from_secs(30)).await {
        failures.push(format!(
            "{label}: jobs were still waiting 30 s after the kill"
        ));
    }
    rescuer.kill(database).await;

    let runs = runs(database, &ids).await;
    let jobs = database.jobs().await;
    let history = database.history().await;
    let audit = audit(
        &label,
        &[long, sibling, exhausted_sibling],
        &jobs,
        &history,
        &runs,
        died,
    );
    failures.extend(audit.violations);
    failures.extend(audit.overlaps);
    failures.extend(audit.alive_overlaps);

    let takeover = runs
        .iter()
        .find(|run| run.job_id == long && run.child == "s_rescuer");
    let sibling_run = runs.iter().find(|run| run.job_id == sibling);
    match takeover {
        Some(run) => {
            let delay = run.started - died;
            eprintln!("{label}: the long job was taken over {delay:.2} s after the kill");
            if !(5.5..=10.0).contains(&delay) {
                failures.push(format!(
                    "{label}: the long job restarted {delay:.2} s after its worker was killed, \
                     expected about 6 s (docs/guide.md:319); {}",
                    run.describe(died)
                ));
            }
        }
        None => failures.push(format!(
            "{label}: the rescuer never ran the long job; runs {runs:?}"
        )),
    }
    match sibling_run {
        Some(run) => {
            let delay = run.started - died;
            eprintln!("{label}: the queue_key sibling started {delay:.2} s after the kill");
            if delay < 5.5 {
                failures.push(format!(
                    "{label}: a job sharing the killed job's queue_key started {delay:.2} s \
                     after the kill, before the 6 s takeover wait (docs/guide.md:325); {}",
                    run.describe(died)
                ));
            }
        }
        None => failures.push(format!("{label}: the sibling job never ran")),
    }
    let entries: Vec<&HistoryRow> = history
        .iter()
        .filter(|entry| entry.job_id == long)
        .collect();
    let shape: Vec<(&str, i32, Option<&str>)> = entries
        .iter()
        .map(|entry| (entry.state.as_str(), entry.attempt, entry.error.as_deref()))
        .collect();
    if shape != [("scheduled", 1, Some(INTERRUPTED)), ("succeeded", 2, None)] {
        failures.push(format!(
            "{label}: the long job's history should be the interrupted attempt 1 then a \
             successful attempt 2, found {entries:?}"
        ));
    }
    let row = jobs.iter().find(|job| job.id == exhausted);
    let entries: Vec<&HistoryRow> = history
        .iter()
        .filter(|entry| entry.job_id == exhausted)
        .collect();
    let shape: Vec<(&str, i32, Option<&str>)> = entries
        .iter()
        .map(|entry| (entry.state.as_str(), entry.attempt, entry.error.as_deref()))
        .collect();
    let expected = [
        ("scheduled", 1, Some("planned failure of attempt 1")),
        ("failed", 2, Some(INTERRUPTED)),
    ];
    if shape != expected {
        failures.push(format!(
            "{label}: a job whose last attempt was killed while a job with the same queue_key \
             waited should be marked failed once for that attempt (docs/guide.md:288-290, \
             docs/guide.md:318-319), expected history {expected:?}, found {entries:?}; row {row:?}"
        ));
    }
    if let Some(run) = runs.iter().find(|run| run.job_id == exhausted_sibling) {
        eprintln!(
            "{label}: the exhausted job's queue_key sibling started {:.2} s after the kill",
            run.started - died
        );
    }
    failures
}

async fn check_false_interruptions(database: &Database, seed: u64) -> Vec<String> {
    let label = format!("false_interruptions seed {seed}");
    let mut rng = Rng::new(seed).fork(6);
    let target = (
        database.options.get_host().to_owned(),
        database.options.get_port(),
    );
    let slow = Proxy::start(target, Duration::from_millis(5)).await;
    let children = [
        ChildWorker::spawn(
            database,
            "q_slow0",
            &[("quiet", 2)],
            Link::Proxied(slow.port),
        ),
        ChildWorker::spawn(
            database,
            "q_slow1",
            &[("quiet", 2)],
            Link::Proxied(slow.port),
        ),
        ChildWorker::spawn(database, "q_fast", &[("quiet", 2)], Link::Shared),
    ];
    for child in &children {
        child.wait_ready().await;
    }
    let mut ids = Vec::new();
    for index in 0..200 * common::scale() {
        let key = (index % 4 == 0).then(|| *rng.pick(&["qa", "qb"]));
        let length = if key.is_some() { 0 } else { 20 };
        ids.push(enqueue_work("quiet", key, length, length).await);
    }

    let mut failures = Vec::new();
    if !wait_terminal(database, &ids, Duration::from_secs(60)).await {
        failures.push(format!("{label}: jobs were still waiting after 60 s"));
    }
    for child in children {
        child.kill(database).await;
    }
    drop(slow);

    let runs = runs(database, &ids).await;
    let jobs = database.jobs().await;
    let history = database.history().await;
    let base = runs.first().map_or(0.0, |run| run.started);
    let audit = audit(&label, &ids, &jobs, &history, &runs, base);
    let false_interruptions: Vec<&HistoryRow> = history
        .iter()
        .filter(|entry| ids.contains(&entry.job_id) && entry.error.as_deref() == Some(INTERRUPTED))
        .collect();
    eprintln!(
        "{label}: {} jobs, {} runs, {} interrupted attempts recorded although no worker died \
         or lost its connection",
        ids.len(),
        runs.len(),
        false_interruptions.len()
    );
    let marked: Vec<(i64, f64)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job_id, extract(epoch FROM created_at)::float8 FROM {}
         WHERE job_id = ANY($1) AND error = $2",
        database.table("job_history")
    )))
    .bind(&ids)
    .bind(INTERRUPTED)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    for (job_id, marked_at) in marked {
        for run in runs.iter().filter(|run| run.job_id == job_id) {
            eprintln!(
                "  job {job_id} was marked interrupted {:.1} ms before run {} of attempt {} \
                 started",
                (run.started - marked_at) * 1000.0,
                run.run,
                run.attempt
            );
        }
    }
    if !false_interruptions.is_empty() {
        failures.push(format!(
            "{label}: no worker died or lost its connection, yet {} attempts were recorded as \
             interrupted (docs/guide.md:318-319 records an interruption only when a worker \
             dies): {false_interruptions:?}",
            false_interruptions.len()
        ));
    }
    failures.extend(audit.violations);
    failures.extend(audit.overlaps);
    failures.extend(audit.alive_overlaps);
    failures
}

fn freeze_lengths() -> Vec<u64> {
    if let Ok(list) = std::env::var("RACE_FREEZES") {
        return list
            .split(',')
            .map(|seconds| seconds.trim().parse().unwrap())
            .collect();
    }
    if common::scale() >= 2 {
        vec![1, 4, 8, 12, 20]
    } else {
        vec![1, 4, 8]
    }
}

async fn check_freezes(database: &Database, seed: u64) -> Vec<String> {
    let mut rng = Rng::new(seed).fork(3);
    let mut failures = Vec::new();
    for (wave, lengths) in freeze_lengths().chunks(3).enumerate() {
        failures.extend(freeze_wave(database, seed, wave, lengths, &mut rng).await);
    }
    failures
}

async fn freeze_wave(
    database: &Database,
    seed: u64,
    wave: usize,
    lengths: &[u64],
    rng: &mut Rng,
) -> Vec<String> {
    let label = format!("freezes seed {seed}");
    let queues: Vec<String> = (0..lengths.len())
        .map(|trial| format!("freeze{wave}_{trial}"))
        .collect();
    let keys: Vec<String> = (0..lengths.len())
        .map(|trial| format!("fk{wave}_{trial}"))
        .collect();
    let children: Vec<ChildWorker> = queues
        .iter()
        .zip(lengths)
        .enumerate()
        .map(|(trial, (queue, &seconds))| {
            let link = if seconds < 10 {
                Link::Shared
            } else {
                Link::OwnProbePool
            };
            ChildWorker::spawn(database, &format!("f{wave}_{trial}"), &[(queue, 1)], link)
        })
        .collect();
    for child in &children {
        child.wait_ready().await;
    }
    let mut firsts = Vec::new();
    for ((queue, key), seconds) in queues.iter().zip(&keys).zip(lengths) {
        firsts.push(enqueue_work(queue, Some(key), seconds * 1000 + 2500, 500).await);
    }
    for (child, first) in children.iter().zip(&firsts) {
        wait_started(database, *first, 1, &child.name).await;
    }
    let mut siblings = Vec::new();
    for (queue, key) in queues.iter().zip(&keys) {
        siblings.push(enqueue_work(queue, Some(key), 300, 300).await);
    }
    let rescuer_queues: Vec<(&str, usize)> =
        queues.iter().map(|queue| (queue.as_str(), 1)).collect();
    let rescuer_name = format!("fr{wave}");
    let rescuer = ChildWorker::spawn(database, &rescuer_name, &rescuer_queues, Link::Shared);
    rescuer.wait_ready().await;
    tokio::time::sleep(rng.millis(200, 900)).await;

    let mut transactions = Vec::new();
    for child in &children {
        transactions.push(job_transaction(&database.pool, &child.application).await);
    }
    let mut stopped = Vec::new();
    for child in &children {
        child.signal("STOP");
        stopped.push((Instant::now(), db_now(&database.pool).await));
    }
    let watchers: Vec<JoinHandle<Option<f64>>> = transactions
        .into_iter()
        .zip(lengths)
        .map(|(transaction, seconds)| {
            watch_transaction(
                database.pool.clone(),
                transaction,
                Duration::from_secs(seconds + 25),
            )
        })
        .collect();
    let mut order: Vec<usize> = (0..lengths.len()).collect();
    order.sort_by_key(|&trial| lengths[trial]);
    let mut resumed = vec![0.0; lengths.len()];
    for trial in order {
        let due = stopped[trial].0 + Duration::from_secs(lengths[trial]);
        tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
        children[trial].signal("CONT");
        resumed[trial] = db_now(&database.pool).await;
    }

    let ids: Vec<i64> = firsts.iter().chain(&siblings).copied().collect();
    let mut failures = Vec::new();
    if !wait_terminal(database, &ids, Duration::from_secs(40)).await {
        failures.push(format!(
            "{label}: wave {wave} jobs were still waiting 40 s after the last resume"
        ));
    }
    let mut transaction_ends = Vec::new();
    for watcher in watchers {
        transaction_ends.push(watcher.await.unwrap());
    }
    for child in children {
        child.kill(database).await;
    }
    rescuer.kill(database).await;

    let jobs = database.jobs().await;
    let history = database.history().await;
    for (trial, &seconds) in lengths.iter().enumerate() {
        let trial_label = format!("{label} freeze {seconds}s");
        let (first, sibling) = (firsts[trial], siblings[trial]);
        let base = stopped[trial].1;
        let runs = runs(database, &[first, sibling]).await;
        let audit = audit(
            &trial_label,
            &[first, sibling],
            &jobs,
            &history,
            &runs,
            base,
        );
        let old = runs
            .iter()
            .find(|run| run.job_id == first && run.attempt == 1);
        let takeover = runs
            .iter()
            .find(|run| run.job_id == first && run.attempt > 1);
        let sibling_run = runs.iter().find(|run| run.job_id == sibling);
        let finishes = runs
            .iter()
            .filter(|run| run.job_id == first && run.finished)
            .count();
        let late_effects = match (old, takeover) {
            (Some(old), Some(takeover)) => {
                effects_after(database, &old.run, takeover.started).await
            }
            _ => 0,
        };
        let relative = |time: Option<f64>| {
            time.map_or_else(
                || "never".to_owned(),
                |time| format!("{:+.2} s", time - base),
            )
        };
        eprintln!(
            "{trial_label}: job transaction ended {}, takeover started {}{}, resumed {}, \
             old run last worked {} and stopped {}, old run work after takeover {late_effects}, \
             old run alive after takeover {}, sibling started {}, job body finished {finishes} \
             times, {} overlapping runs, {} runs started before another stopped",
            relative(transaction_ends[trial]),
            relative(takeover.map(|run| run.started)),
            takeover.map_or_else(String::new, |run| format!(" on {}", run.child)),
            relative(Some(resumed[trial])),
            relative(old.map(|run| run.last_effect)),
            relative(old.and_then(|run| run.ended)),
            match (old, takeover) {
                (Some(old), Some(takeover)) if old.alive_until() > takeover.started =>
                    format!("{:.2} s", old.alive_until() - takeover.started),
                _ => "0 s".to_owned(),
            },
            relative(sibling_run.map(|run| run.started)),
            audit.overlaps.len(),
            audit.alive_overlaps.len(),
        );
        failures.extend(audit.violations);
        if seconds < 10 {
            failures.extend(audit.overlaps);
            failures.extend(audit.alive_overlaps);
            if let Some(takeover) = takeover {
                failures.push(format!(
                    "{trial_label}: a {seconds} s freeze is shorter than the 10 s idle session \
                     timeout (docs/guide.md:332), yet the job was taken over: {}",
                    takeover.describe(base)
                ));
            }
        } else {
            for overlap in audit.overlaps.iter().chain(&audit.alive_overlaps) {
                eprintln!("  documented limitation (docs/guide.md:327): {overlap}");
            }
        }
    }
    failures
}

async fn check_partitions(database: &Database, seed: u64) -> Vec<String> {
    let label = format!("partitions seed {seed}");
    let mut rng = Rng::new(seed).fork(4);
    let target = (
        database.options.get_host().to_owned(),
        database.options.get_port(),
    );
    let cut = Proxy::start(target.clone(), Duration::ZERO).await;
    let heal = Proxy::start(target, Duration::ZERO).await;
    let cut_child = ChildWorker::spawn(database, "p_cut", &[("cut", 1)], Link::Proxied(cut.port));
    let heal_child = ChildWorker::spawn(
        database,
        "p_heal",
        &[("heal", 1), ("healed", 1)],
        Link::Proxied(heal.port),
    );
    cut_child.wait_ready().await;
    heal_child.wait_ready().await;
    let cut_job = enqueue_work("cut", Some("pcut"), 60_000, 300).await;
    let heal_job = enqueue_work("heal", Some("pheal"), 60_000, 300).await;
    wait_started(database, cut_job, 1, "p_cut").await;
    wait_started(database, heal_job, 1, "p_heal").await;
    let cut_sibling = enqueue_work("cut", Some("pcut"), 300, 300).await;
    let heal_sibling = enqueue_work("heal", Some("pheal"), 300, 300).await;
    let rescuer = ChildWorker::spawn(
        database,
        "p_rescuer",
        &[("cut", 1), ("heal", 1)],
        Link::Shared,
    );
    rescuer.wait_ready().await;
    tokio::time::sleep(rng.millis(200, 900)).await;

    let cut_transaction = job_transaction(&database.pool, &cut_child.application).await;
    let heal_transaction = job_transaction(&database.pool, &heal_child.application).await;
    cut.set_blocked(true);
    heal.set_blocked(true);
    let partitioned = db_now(&database.pool).await;
    let cut_watch = watch_transaction(
        database.pool.clone(),
        cut_transaction,
        Duration::from_secs(40),
    );
    let heal_watch = watch_transaction(
        database.pool.clone(),
        heal_transaction,
        Duration::from_secs(40),
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    heal.set_blocked(false);
    let healed = db_now(&database.pool).await;
    let after_heal = enqueue_work("healed", None, 300, 300).await;

    let ids = [cut_job, cut_sibling, heal_job, heal_sibling, after_heal];
    let mut failures = Vec::new();
    if !wait_terminal(database, &ids, Duration::from_secs(45)).await {
        failures.push(format!(
            "{label}: jobs were still waiting 45 s after the partition"
        ));
    }
    let cut_ended = cut_watch.await.unwrap();
    let heal_ended = heal_watch.await.unwrap();
    cut_child.kill(database).await;
    heal_child.kill(database).await;
    rescuer.kill(database).await;
    drop(cut);
    drop(heal);

    let runs = runs(database, &ids).await;
    let jobs = database.jobs().await;
    let history = database.history().await;
    let audit = audit(&label, &ids, &jobs, &history, &runs, partitioned);
    failures.extend(audit.violations);
    failures.extend(audit.overlaps);
    failures.extend(audit.alive_overlaps);

    for (name, job, sibling, child, ended, permanent) in [
        ("permanent", cut_job, cut_sibling, "p_cut", cut_ended, true),
        (
            "healed after 5 s",
            heal_job,
            heal_sibling,
            "p_heal",
            heal_ended,
            false,
        ),
    ] {
        let case = format!("{label} {name} partition");
        let old = runs
            .iter()
            .find(|run| run.job_id == job && run.child == child && run.attempt == 1);
        let takeover = runs.iter().find(|run| run.job_id == job && run.attempt > 1);
        let sibling_run = runs.iter().find(|run| run.job_id == sibling);
        let relative = |time: Option<f64>| {
            time.map_or_else(
                || "never".to_owned(),
                |time| format!("{:+.2} s", time - partitioned),
            )
        };
        eprintln!(
            "{case}: old run last worked {} and stopped {}, job transaction ended {}, \
             takeover started {}{}, sibling started {}",
            relative(old.map(|run| run.last_effect)),
            relative(old.and_then(|run| run.ended)),
            relative(ended),
            relative(takeover.map(|run| run.started)),
            takeover.map_or_else(String::new, |run| format!(" on {}", run.child)),
            relative(sibling_run.map(|run| run.started)),
        );
        let Some(old) = old else {
            failures.push(format!("{case}: the partitioned run is missing; {runs:?}"));
            continue;
        };
        match old.ended {
            Some(stopped) if stopped - partitioned <= 4.5 => {}
            _ => failures.push(format!(
                "{case}: the partitioned worker should abort its job within 3 s \
                 (docs/guide.md:323), but {}",
                old.describe(partitioned)
            )),
        }
        let Some(takeover) = takeover else {
            failures.push(format!("{case}: the job was never taken over; {runs:?}"));
            continue;
        };
        if takeover.started < old.alive_until() {
            failures.push(format!(
                "{case}: the new run started before the old run stopped (docs/guide.md:325): \
                 {} and {}",
                old.describe(partitioned),
                takeover.describe(partitioned)
            ));
        }
        if permanent {
            match ended {
                Some(ended) if (8.0..=12.5).contains(&(ended - partitioned)) => {
                    let earliest = ended + 5.5;
                    if takeover.started < earliest || takeover.started - partitioned > 20.0 {
                        failures.push(format!(
                            "{case}: takeover should come 6 s after PostgreSQL ends the session, \
                             about 16 s after the failure (docs/guide.md:333), but the session \
                             ended {:+.2} s and {}",
                            ended - partitioned,
                            takeover.describe(partitioned)
                        ));
                    }
                    if let Some(run) = sibling_run
                        && run.started < earliest
                    {
                        failures.push(format!(
                            "{case}: a job sharing the queue_key started before the 6 s takeover \
                             wait (docs/guide.md:325): {}",
                            run.describe(partitioned)
                        ));
                    }
                }
                _ => failures.push(format!(
                    "{case}: PostgreSQL should end the frozen session about 10 s after the last \
                     check (docs/guide.md:332), but it ended {}",
                    relative(ended)
                )),
            }
        }
    }
    match runs.iter().find(|run| run.job_id == after_heal) {
        Some(run) => eprintln!(
            "{label}: the healed worker ran a new job {:.2} s after the heal",
            run.started - healed
        ),
        None => failures.push(format!(
            "{label}: the healed worker never ran the job enqueued after the heal"
        )),
    }
    failures
}
