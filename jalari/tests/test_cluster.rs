use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, ClusterConfig, ErrorKind, Job, JobResult, Schema, Worker, WorkerConfig,
};
use serde::{Deserialize, Serialize};

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Serialize, Deserialize)]
struct Noop;

#[jalari::job]
impl Job for Noop {
    const NAME: &'static str = "c_noop";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

struct Fixture {
    pool: PgPool,
    schema_name: String,
}

impl Fixture {
    fn table(&self, name: &str) -> String {
        format!("\"{}\".\"{name}\"", self.schema_name)
    }

    async fn insert_job(&self, label: &str, state: &str, age: &str) -> i64 {
        let id = sqlx::query_scalar(AssertSqlSafe(format!(
            "INSERT INTO {} (task, payload, state, max_attempts, last_error, updated_at)
             VALUES ('c_unknown', 'null', $1, 1, $2, now() - $3::interval)
             RETURNING id",
            self.table("job")
        )))
        .bind(state)
        .bind(label)
        .bind(age)
        .fetch_one(&self.pool)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {} (job_id, state, attempt) VALUES ($1, $2, 1)",
            self.table("job_history")
        )))
        .bind(id)
        .bind(state)
        .execute(&self.pool)
        .await
        .unwrap();
        id
    }

    async fn remaining_jobs(&self) -> Vec<String> {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT last_error FROM {} ORDER BY last_error",
            self.table("job")
        )))
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    async fn history_rows(&self) -> i64 {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {}",
            self.table("job_history")
        )))
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn insert_stale_worker(&self) {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {} (hostname, pid, queues, scheduler, heartbeat_at)
             VALUES ('stale', 1, '[]', FALSE, now() - interval '1 minute')",
            self.table("worker")
        )))
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn stale_workers(&self) -> i64 {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE hostname = 'stale'",
            self.table("worker")
        )))
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn last_housekeeping_at_is_set(&self) -> bool {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT last_housekeeping_at IS NOT NULL FROM {}",
            self.table("config")
        )))
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }
}

async fn wait_until<F: AsyncFn() -> bool>(description: &str, condition: F) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition().await {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_test_{}_{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_config() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await
        .unwrap();
    let schema_name = unique_schema_name();
    jalari::init(pool.clone())
        .schema(Schema::named(&schema_name).unwrap())
        .migrate()
        .await
        .unwrap();
    let fixture = Fixture {
        pool: pool.clone(),
        schema_name: schema_name.clone(),
    };

    check_defaults().await;
    check_invalid_values_are_rejected().await;
    check_heartbeat_must_be_shorter_than_worker_timeout().await;
    check_housekeeping(&fixture).await;

    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}

async fn check_defaults() {
    let cluster = jalari::cluster::get().await.unwrap();
    assert!(!cluster.housekeeping_enabled);
    assert_eq!(cluster.housekeeping_interval, Duration::from_secs(600));
    assert_eq!(cluster.succeeded_retention, DAY);
    assert_eq!(cluster.deleted_retention, DAY * 7);
    assert_eq!(cluster.failed_retention, None);
    assert_eq!(cluster.worker_timeout, Duration::from_secs(300));
}

async fn check_invalid_values_are_rejected() {
    let before = jalari::cluster::get().await.unwrap();
    let changes: [fn(&mut ClusterConfig); 2] = [
        |cluster| cluster.housekeeping_interval = Duration::from_millis(500),
        |cluster| cluster.worker_timeout = Duration::ZERO,
    ];
    for change in changes {
        let err = jalari::cluster::update(change).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidConfigValue);
    }
    assert_eq!(jalari::cluster::get().await.unwrap(), before);

    let saved = jalari::cluster::update(|cluster| {
        cluster.succeeded_retention = DAY * 3 + Duration::from_nanos(1);
    })
    .await
    .unwrap();
    assert_eq!(saved.succeeded_retention, DAY * 3);
    assert_eq!(jalari::cluster::get().await.unwrap(), saved);
}

async fn check_heartbeat_must_be_shorter_than_worker_timeout() {
    jalari::cluster::update(|cluster| cluster.worker_timeout = Duration::from_secs(10))
        .await
        .unwrap();
    let err = Worker::builder()
        .queue("default", 1)
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind, ErrorKind::InvalidHeartbeatInterval);
}

async fn check_housekeeping(fixture: &Fixture) {
    fixture.insert_job("deleted_new", "deleted", "1 day").await;
    fixture.insert_job("deleted_old", "deleted", "8 days").await;
    fixture.insert_job("failed_old", "failed", "40 days").await;
    fixture
        .insert_job("scheduled_old", "scheduled", "90 days")
        .await;
    fixture
        .insert_job("succeeded_new", "succeeded", "1 day")
        .await;
    fixture
        .insert_job("succeeded_old", "succeeded", "4 days")
        .await;
    fixture.insert_stale_worker().await;
    let everything = fixture.remaining_jobs().await;

    let mut settings = WorkerConfig::default();
    settings.heartbeat_interval = Duration::from_secs(1);
    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .queue("default", 1)
        .config(settings)
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.child_token()));

    wait_until("the stale worker to expire", async || {
        fixture.stale_workers().await == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(fixture.remaining_jobs().await, everything);
    assert!(!fixture.last_housekeeping_at_is_set().await);

    jalari::cluster::update(|cluster| cluster.housekeeping_enabled = true)
        .await
        .unwrap();
    wait_until("housekeeping to remove expired jobs", async || {
        fixture.remaining_jobs().await.len() == 4
    })
    .await;
    assert_eq!(
        fixture.remaining_jobs().await,
        [
            "deleted_new",
            "failed_old",
            "scheduled_old",
            "succeeded_new"
        ]
    );
    assert_eq!(fixture.history_rows().await, 4);
    assert!(fixture.last_housekeeping_at_is_set().await);

    jalari::cluster::update(|cluster| {
        cluster.failed_retention = Some(DAY * 30);
        cluster.housekeeping_interval = Duration::from_secs(1);
    })
    .await
    .unwrap();
    wait_until("failed_retention to apply", async || {
        fixture.remaining_jobs().await.len() == 3
    })
    .await;
    assert_eq!(
        fixture.remaining_jobs().await,
        ["deleted_new", "scheduled_old", "succeeded_new"]
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .unwrap()
        .unwrap();
}
