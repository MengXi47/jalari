use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{ErrorKind, Job, JobResult, Schema, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Noop;

#[jalari::job]
impl Job for Noop {
    const NAME: &'static str = "pool_noop";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[tokio::test]
async fn test_worker_needs_two_pooled_connections_whatever_its_concurrency() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let schema_name = format!("jalari_pool_{}_{nanos}", std::process::id());
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await
        .unwrap();
    jalari::init(pool.clone())
        .schema(Schema::named(&schema_name).unwrap())
        .migrate()
        .await
        .unwrap();

    let error = Worker::builder()
        .queue("default", 1)
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::InsufficientConnections);

    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}
