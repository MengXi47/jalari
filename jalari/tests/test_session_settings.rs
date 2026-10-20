use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, Connection, PgConnection, PgPool};
use jalari::{CancellationToken, EnqueueOptions, Job, JobId, JobResult, Schema, Worker};
use serde::{Deserialize, Serialize};

const SESSION_SETTINGS: [(&str, &str, &str); 3] = [
    ("statement_timeout", "1000", "1s"),
    ("idle_in_transaction_session_timeout", "500", "500ms"),
    ("transaction_timeout", "1500", "1500ms"),
];

static KEYED_RUNNING: AtomicUsize = AtomicUsize::new(0);
static KEYED_OVERLAPPED: AtomicBool = AtomicBool::new(false);

#[derive(Serialize, Deserialize)]
struct Keyed;

#[jalari::job(queue = "keyed")]
impl Job for Keyed {
    const NAME: &'static str = "s_keyed";
    const MAX_ATTEMPTS: i32 = 3;

    async fn run(self) -> JobResult {
        if KEYED_RUNNING.fetch_add(1, Ordering::SeqCst) > 0 {
            KEYED_OVERLAPPED.store(true, Ordering::SeqCst);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        KEYED_RUNNING.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_session_settings() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let schema_name = format!("jalari_test_{}_{nanos}", std::process::id());
    let schema = Schema::named(&schema_name).unwrap();
    let mut plain = PgConnection::connect(&url).await.unwrap();
    let server_version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(&mut plain)
        .await
        .unwrap();
    plain.close().await.unwrap();
    let plain = PgPool::connect(&url).await.unwrap();
    jalari::migrate(&plain, &schema).await.unwrap();
    plain.close().await;
    let settings: Vec<_> = SESSION_SETTINGS
        .into_iter()
        .filter(|(name, _, _)| {
            *name != "transaction_timeout" || server_version.parse::<i32>().unwrap() >= 170_000
        })
        .collect();

    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options.options(settings.iter().map(|(name, value, _)| (*name, *value))))
        .await
        .unwrap();
    jalari::init(pool.clone()).schema(schema).await.unwrap();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder().queue("keyed", 2).build().await.unwrap();
    let running = tokio::spawn(worker.run(shutdown.child_token()));

    let mut ids = Vec::new();
    for _ in 0..2 {
        let options = EnqueueOptions::new().queue_key("account:1");
        ids.push(jalari::enqueue_with(&Keyed, options).await.unwrap().id());
    }
    for id in ids {
        assert_eq!(
            finished(&pool, &schema_name, id).await,
            ("succeeded".to_owned(), 1),
            "job {id}"
        );
    }
    assert!(!KEYED_OVERLAPPED.load(Ordering::SeqCst));

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .unwrap()
        .unwrap();

    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await.unwrap());
    }
    for connection in &mut connections {
        for (name, _, shown) in &settings {
            let value: String = sqlx::query_scalar(AssertSqlSafe(format!("SHOW {name}")))
                .fetch_one(&mut **connection)
                .await
                .unwrap();
            assert_eq!(value, *shown, "{name}");
        }
    }
    drop(connections);

    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}

async fn finished(pool: &PgPool, schema_name: &str, id: JobId) -> (String, i32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let row: (String, i32) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state, attempts FROM \"{schema_name}\".job WHERE id = $1"
        )))
        .bind(id.0)
        .fetch_one(pool)
        .await
        .unwrap();
        if row.0 == "succeeded" || row.0 == "failed" || Instant::now() > deadline {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
