use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
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

    async fn overdue(&self, name: &str, by: &str) -> DateTime<Utc> {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE {} SET next_run_at = now() - $2::interval WHERE name = $1
             RETURNING next_run_at",
            self.table("recurring")
        )))
        .bind(name)
        .bind(by)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn jobs_for(&self, name: &str) -> usize {
        let count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE job_key = $1",
            self.table("job")
        )))
        .bind(format!("recurring:{name}"))
        .fetch_one(&self.pool)
        .await
        .unwrap();
        usize::try_from(count).unwrap()
    }
}

async fn wait_until<F: AsyncFn() -> bool>(description: &str, condition: F) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition().await {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_next_run(name: &str) -> RecurringInfo {
    wait_until("the scheduler to move the next run", async || {
        info(name)
            .await
            .next_run_at
            .is_none_or(|next_run_at| next_run_at > Utc::now())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    info(name).await
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
    check_once_catches_up_with_one_run(&fixture).await;
    check_skip_enqueues_nothing_for_old_runs(&fixture).await;
    check_late_run_within_the_grace_fires_under_both(&fixture).await;
    check_skip_counts_nothing_toward_max_runs(&fixture).await;
    check_skip_finishes_a_schedule_past_until(&fixture).await;

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

async fn check_once_catches_up_with_one_run(fixture: &Fixture) {
    recurring::add_or_update("once", every(1, IntervalUnit::Minutes), &tock("once"))
        .await
        .unwrap();
    let missed = fixture.overdue("once", "3 minutes 30 seconds").await;
    let caught_up = wait_for_next_run("once").await;
    assert_eq!(fixture.jobs_for("once").await, 1);
    assert_eq!(caught_up.run_count, 1);
    assert_eq!(caught_up.last_run_at, Some(missed));
    assert!(caught_up.next_run_at.unwrap() > Utc::now());
    recurring::remove("once").await.unwrap();
}

async fn check_skip_enqueues_nothing_for_old_runs(fixture: &Fixture) {
    let skipping = every(1, IntervalUnit::Minutes).missed(Missed::Skip);
    recurring::add_or_update("skip", skipping, &tock("skip"))
        .await
        .unwrap();
    fixture.overdue("skip", "3 minutes 30 seconds").await;
    let skipped = wait_for_next_run("skip").await;
    assert_eq!(fixture.jobs_for("skip").await, 0);
    assert_eq!(skipped.run_count, 0);
    assert_eq!(skipped.last_run_at, None);
    assert_eq!(skipped.finished_at, None);
    let next_run_at = skipped.next_run_at.unwrap();
    assert!(next_run_at > Utc::now());
    assert!(next_run_at <= Utc::now() + TimeDelta::minutes(1));
    recurring::remove("skip").await.unwrap();
}

async fn check_late_run_within_the_grace_fires_under_both(fixture: &Fixture) {
    for missed in [Missed::Once, Missed::Skip] {
        let name = format!("late_{missed}");
        let schedule = every(10, IntervalUnit::Seconds).missed(missed);
        recurring::add_or_update(&name, schedule, &tock(&name))
            .await
            .unwrap();
        let late = fixture.overdue(&name, "10 seconds").await;
        let fired = wait_for_next_run(&name).await;
        assert_eq!(fixture.jobs_for(&name).await, 1, "{missed}");
        assert_eq!(fired.run_count, 1, "{missed}");
        assert_eq!(fired.last_run_at, Some(late), "{missed}");
        recurring::pause(&name).await.unwrap();
    }

    let fired = info("late_skip").await;
    recurring::resume("late_skip").await.unwrap();
    fixture.overdue("late_skip", "3 minutes").await;
    let skipped = wait_for_next_run("late_skip").await;
    assert_eq!(fixture.jobs_for("late_skip").await, 1);
    assert_eq!(skipped.run_count, 1);
    assert_eq!(skipped.last_run_at, fired.last_run_at);
    recurring::remove("late_once").await.unwrap();
    recurring::remove("late_skip").await.unwrap();
}

async fn check_skip_counts_nothing_toward_max_runs(fixture: &Fixture) {
    let capped = every(1, IntervalUnit::Minutes)
        .max_runs(1)
        .unwrap()
        .missed(Missed::Skip);
    recurring::add_or_update("capped", capped, &tock("capped"))
        .await
        .unwrap();
    fixture.overdue("capped", "3 minutes 30 seconds").await;
    let skipped = wait_for_next_run("capped").await;
    assert_eq!(skipped.run_count, 0);
    assert_eq!(skipped.finished_at, None);
    assert_eq!(fixture.jobs_for("capped").await, 0);

    fixture.overdue("capped", "5 seconds").await;
    let fired = wait_for_next_run("capped").await;
    assert_eq!(fired.run_count, 1);
    assert!(fired.finished_at.is_some());
    assert_eq!(fired.next_run_at, None);
    assert_eq!(fixture.jobs_for("capped").await, 1);
    recurring::remove("capped").await.unwrap();
}

async fn check_skip_finishes_a_schedule_past_until(fixture: &Fixture) {
    for missed in [Missed::Once, Missed::Skip] {
        let name = format!("ending_{missed}");
        let anchor = anchor_now() - TimeDelta::seconds(30);
        let schedule =
            Schedule::from(Interval::new(1, IntervalUnit::Minutes, anchor).unwrap()).missed(missed);
        recurring::add_or_update(&name, schedule, &tock(&name))
            .await
            .unwrap();
        fixture
            .set_column(&name, "until_at = now() + interval '10 seconds'")
            .await;
        fixture.overdue(&name, "3 minutes").await;
        let ended = wait_for_next_run(&name).await;
        assert!(ended.finished_at.is_some(), "{missed}");
        assert_eq!(ended.next_run_at, None, "{missed}");
        let runs = usize::from(missed == Missed::Once);
        assert_eq!(fixture.jobs_for(&name).await, runs, "{missed}");
        assert_eq!(
            ended.run_count,
            i32::from(missed == Missed::Once),
            "{missed}"
        );
        assert_eq!(
            ended.last_run_at.is_some(),
            missed == Missed::Once,
            "{missed}"
        );
        recurring::remove(&name).await.unwrap();
    }
}
