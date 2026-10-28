use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{NaiveDateTime, Utc};
use jalari::recurring::Missed;
use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, Interval, IntervalUnit, Job, JobResult, RecurringInfo, Schedule, Schema,
    Worker, WorkerConfig, recurring,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Tock {
    label: String,
}

#[jalari::job(queue = "tocks")]
impl Job for Tock {
    const NAME: &'static str = "m_tock";

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
        format!("\"{}\".{name}", self.schema_name)
    }

    async fn set_column(&self, name: &str, assignment: &str) {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {} SET {assignment} WHERE name = $1",
            self.table("recurring")
        )))
        .bind(name)
        .execute(&self.pool)
        .await
        .unwrap();
    }
}

async fn info(name: &str) -> RecurringInfo {
    recurring::list()
        .await
        .unwrap()
        .into_iter()
        .find(|info| info.name == name)
        .unwrap_or_else(|| panic!("no schedule named {name}"))
}

fn tock(label: &str) -> Tock {
    Tock {
        label: label.to_owned(),
    }
}

fn anchor_now() -> NaiveDateTime {
    Utc::now().naive_utc()
}

fn every(count: u32, unit: IntervalUnit) -> Schedule {
    Schedule::from(Interval::new(count, unit, anchor_now()).unwrap())
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_test_{}_{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_missed() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let schema_name = unique_schema_name();
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .unwrap();
    jalari::init(pool.clone())
        .schema(Schema::named(&schema_name).unwrap())
        .migrate()
        .await
        .unwrap();
    let fixture = Fixture {
        pool: pool.clone(),
        schema_name: schema_name.clone(),
    };

    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    let shutdown = CancellationToken::new();
    let worker = Worker::builder()
        .queue("tocks", 2)
        .config(config)
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.child_token()));

    check_policy_round_trips_and_keeps_the_count(&fixture).await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .unwrap()
        .unwrap();
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}

async fn check_policy_round_trips_and_keeps_the_count(fixture: &Fixture) {
    let hourly = every(1, IntervalUnit::Hours);
    recurring::add_or_update("policy", hourly.clone(), &tock("policy"))
        .await
        .unwrap();
    let created = info("policy").await;
    assert_eq!(created.missed, Missed::Once);
    assert_eq!(created.schedule, Some(hourly.clone()));

    fixture
        .set_column(
            "policy",
            "run_count = 3, next_run_at = next_run_at + interval '2 hours'",
        )
        .await;
    let moved = info("policy").await;
    let skipping = hourly.clone().missed(Missed::Skip);
    recurring::add_or_update("policy", skipping.clone(), &tock("policy"))
        .await
        .unwrap();
    let changed = info("policy").await;
    assert_eq!(changed.missed, Missed::Skip);
    assert_eq!(changed.schedule, Some(skipping));
    assert_eq!(changed.run_count, 3);
    assert_eq!(changed.next_run_at, moved.next_run_at);

    recurring::update_schedule("policy", hourly.clone())
        .await
        .unwrap();
    let reset = info("policy").await;
    assert_eq!(reset.missed, Missed::Once);
    assert_eq!(reset.schedule, Some(hourly));
    assert_eq!(reset.run_count, 3);
    assert_eq!(reset.next_run_at, moved.next_run_at);

    let stored: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT missed FROM {} WHERE name = 'policy'",
        fixture.table("recurring")
    )))
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(stored, "once");
    recurring::remove("policy").await.unwrap();
}
