#![allow(
    dead_code,
    reason = "each test binary uses a different part of the harness"
)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{CancellationToken, ExponentialBackoff, Schema, Worker, WorkerConfig};
use tokio::task::JoinHandle;

pub fn scale() -> u64 {
    std::env::var("JALARI_RACE_SCALE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
        .max(1)
}

pub fn seed(name: &str) -> u64 {
    let seed = std::env::var("JALARI_RACE_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| {
            u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
                    % u128::from(u64::MAX),
            )
            .unwrap()
        })
        | 1;
    eprintln!("{name} seed {seed} (rerun with JALARI_RACE_SEED={seed})");
    seed
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn fork(&mut self, stream: u64) -> Self {
        Self::new(self.next() ^ stream.wrapping_mul(0x9e37_79b9_7f4a_7c15))
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    pub fn millis(&mut self, low: u64, high: u64) -> Duration {
        Duration::from_millis(low + self.below(high - low + 1))
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[usize::try_from(self.below(items.len() as u64)).unwrap()]
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct JobRow {
    pub id: i64,
    pub task: String,
    pub state: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub job_key: Option<String>,
    pub queue_key: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HistoryRow {
    pub job_id: i64,
    pub state: String,
    pub attempt: i32,
    pub error: Option<String>,
}

pub struct Database {
    pub options: PgConnectOptions,
    pub pool: PgPool,
    pub schema_name: String,
}

impl Database {
    pub async fn start(tag: &str, max_connections: u32) -> Option<Self> {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("DATABASE_URL is not set, skipping");
            return None;
        };
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let schema_name = format!("jalari_race_{tag}_{}_{nanos}", std::process::id());
        let options: PgConnectOptions = url.parse().unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .connect_with(options.clone().application_name(&schema_name))
            .await
            .unwrap();
        jalari::init(pool.clone())
            .schema(Schema::named(&schema_name).unwrap())
            .migrate()
            .await
            .unwrap();
        Some(Self {
            options,
            pool,
            schema_name,
        })
    }

    pub async fn connect(&self, role: &str, max_connections: u32) -> PgPool {
        PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .connect_with(
                self.options
                    .clone()
                    .application_name(&format!("{}_{role}", self.schema_name)),
            )
            .await
            .unwrap()
    }

    pub fn table(&self, name: &str) -> String {
        format!("\"{}\".{name}", self.schema_name)
    }

    pub async fn jobs(&self) -> Vec<JobRow> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT id, task, state, attempts, max_attempts, job_key, queue_key, last_error
             FROM {} ORDER BY id",
            self.table("job")
        )))
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    pub async fn history(&self) -> Vec<HistoryRow> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT job_id, state, attempt, error FROM {} ORDER BY id",
            self.table("job_history")
        )))
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    pub async fn pending(&self) -> i64 {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE state IN ('scheduled', 'enqueued')",
            self.table("job")
        )))
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    pub async fn deadlocks(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    pub async fn terminate_job_sessions(&self, at_most: usize, older_than: Duration) -> usize {
        let terminated: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM (
                 SELECT pid FROM pg_stat_activity
                 WHERE datname = current_database()
                     AND application_name = $1
                     AND state = 'idle in transaction'
                     AND state_change < now() - make_interval(secs => $2)
                 ORDER BY random()
                 LIMIT $3
             ) AS victims",
        )
        .bind(&self.schema_name)
        .bind(older_than.as_secs_f64())
        .bind(i64::try_from(at_most).unwrap())
        .fetch_all(&self.pool)
        .await
        .unwrap();
        terminated.into_iter().filter(|ok| *ok).count()
    }

    pub async fn lifelines(&self) -> Vec<(i32, String)> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT activity.pid, worker.id::text
             FROM pg_stat_activity AS activity
             JOIN {} AS worker ON activity.application_name = 'jalari lifeline ' || worker.id::text
             WHERE activity.datname = current_database()
             ORDER BY worker.started_at, worker.id",
            self.table("worker")
        )))
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    pub async fn terminate_lifelines(&self, at_most: usize) -> usize {
        let terminated: Vec<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT pg_terminate_backend(pid) FROM (
                 SELECT activity.pid
                 FROM pg_stat_activity AS activity
                 JOIN {} AS worker
                     ON activity.application_name = 'jalari lifeline ' || worker.id::text
                 WHERE activity.datname = current_database()
                 ORDER BY random()
                 LIMIT $1
             ) AS victims",
            self.table("worker")
        )))
        .bind(i64::try_from(at_most).unwrap())
        .fetch_all(&self.pool)
        .await
        .unwrap();
        terminated.into_iter().filter(|ok| *ok).count()
    }

    pub async fn finish(self) {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema_name
        )))
        .execute(&self.pool)
        .await
        .unwrap();
    }
}

pub struct Occupancy {
    holders: Mutex<HashMap<String, Vec<String>>>,
    violations: Mutex<Vec<String>>,
}

pub static OCCUPANCY: LazyLock<Occupancy> = LazyLock::new(|| Occupancy {
    holders: Mutex::new(HashMap::new()),
    violations: Mutex::new(Vec::new()),
});

impl Occupancy {
    pub fn enter(&'static self, keys: Vec<String>, holder: String) -> Seat {
        let mut holders = self.holders.lock().unwrap();
        for key in &keys {
            let current = holders.entry(key.clone()).or_default();
            if !current.is_empty() {
                self.violations.lock().unwrap().push(format!(
                    "{key}: {holder} started while {current:?} still ran"
                ));
            }
            current.push(holder.clone());
        }
        Seat {
            occupancy: self,
            keys,
            holder,
        }
    }

    pub fn violations(&self) -> Vec<String> {
        self.violations.lock().unwrap().clone()
    }

    pub fn busy(&self) -> usize {
        self.holders
            .lock()
            .unwrap()
            .values()
            .filter(|holders| !holders.is_empty())
            .count()
    }
}

pub struct Seat {
    occupancy: &'static Occupancy,
    keys: Vec<String>,
    holder: String,
}

impl Drop for Seat {
    fn drop(&mut self) {
        let mut holders = self.occupancy.holders.lock().unwrap();
        for key in &self.keys {
            if let Some(current) = holders.get_mut(key)
                && let Some(position) = current.iter().position(|h| *h == self.holder)
            {
                current.remove(position);
            }
        }
    }
}

pub struct Ledger {
    events: Mutex<HashMap<String, Vec<(&'static str, Instant)>>>,
}

pub static LEDGER: LazyLock<Ledger> = LazyLock::new(|| Ledger {
    events: Mutex::new(HashMap::new()),
});

impl Ledger {
    pub fn record(&self, label: &str, event: &'static str) {
        self.events
            .lock()
            .unwrap()
            .entry(label.to_owned())
            .or_default()
            .push((event, Instant::now()));
    }

    pub fn events(&self, label: &str) -> Vec<(&'static str, Instant)> {
        self.events
            .lock()
            .unwrap()
            .get(label)
            .cloned()
            .unwrap_or_default()
    }

    pub fn count(&self, label: &str, event: &str) -> usize {
        self.events(label)
            .iter()
            .filter(|(e, _)| *e == event)
            .count()
    }

    pub fn labels(&self) -> Vec<String> {
        self.events.lock().unwrap().keys().cloned().collect()
    }
}

pub async fn wait_until<F, Fut>(description: &str, timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !condition().await {
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn worker_config(poll_interval: Duration) -> WorkerConfig {
    let mut config = WorkerConfig::default();
    config.poll_interval = poll_interval;
    config.shutdown_timeout = Duration::from_secs(10);
    config
}

pub fn fast_retry() -> ExponentialBackoff {
    ExponentialBackoff {
        base: Duration::from_millis(10),
        max: Duration::from_millis(100),
        jitter: 0.5,
    }
}

pub struct RunningWorker {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

impl RunningWorker {
    pub fn spawn(worker: Worker) -> Self {
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));
        Self { shutdown, handle }
    }

    pub fn request_stop(&self) {
        self.shutdown.cancel();
    }

    pub async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(60), self.handle)
            .await
            .expect("worker did not stop within 60 s")
            .unwrap();
    }

    pub async fn crash(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}
