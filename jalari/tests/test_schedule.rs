use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, TimeDelta, Timelike, Utc};
use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancellationToken, Cron, EnqueueOutcome, ErrorKind, Interval, IntervalUnit, Job, JobResult,
    RecurringInfo, Schedule, Schema, Worker, WorkerConfig, recurring,
};
use serde::{Deserialize, Serialize};

static EXECUTIONS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn record_execution(label: &str) {
    *EXECUTIONS
        .lock()
        .unwrap()
        .entry(label.to_owned())
        .or_default() += 1;
}

fn executions(label: &str) -> u32 {
    EXECUTIONS.lock().unwrap().get(label).copied().unwrap_or(0)
}

#[derive(Serialize, Deserialize)]
struct Beat {
    label: String,
}

#[jalari::job(queue = "beats")]
impl Job for Beat {
    const NAME: &'static str = "s_beat";

    async fn run(self) -> JobResult {
        record_execution(&self.label);
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

    async fn jobs_for(&self, name: &str) -> Vec<DateTime<Utc>> {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT created_at FROM {} WHERE job_key = $1 ORDER BY id",
            self.table("job")
        )))
        .bind(format!("recurring:{name}"))
        .fetch_all(&self.pool)
        .await
        .unwrap()
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

fn beat(label: &str) -> Beat {
    Beat {
        label: label.to_owned(),
    }
}

fn anchor_now() -> NaiveDateTime {
    Utc::now().naive_utc()
}

fn every_second() -> Interval {
    Interval::new(1, IntervalUnit::Seconds, anchor_now()).unwrap()
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_test_{}_{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_schedule() {
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
        .queue("beats", 2)
        .config(config)
        .build()
        .await
        .unwrap();
    let running = tokio::spawn(worker.run(shutdown.child_token()));

    check_interval_fires_aligned_with_its_anchor(&fixture).await;
    check_max_runs_finishes_the_schedule(&fixture).await;
    check_until_finishes_the_schedule(&fixture).await;
    check_finished_schedule_stays_finished_until_changed(&fixture).await;
    check_unchanged_update_keeps_the_count(&fixture).await;
    check_switching_between_cron_and_interval(&fixture).await;
    check_future_anchor_waits(&fixture).await;
    check_invalid_schedule_is_reported(&fixture).await;

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

async fn check_interval_fires_aligned_with_its_anchor(fixture: &Fixture) {
    let given =
        Utc::now().naive_utc().with_nanosecond(123_456_789).unwrap() - TimeDelta::seconds(10);
    let interval = Interval::new(2, IntervalUnit::Seconds, given).unwrap();
    let anchor = interval.anchor();
    assert_eq!(anchor.nanosecond(), 123_456_000);
    recurring::add_or_update("aligned", interval.clone(), &beat("aligned"))
        .await
        .unwrap();
    let created = info("aligned").await;
    assert_eq!(created.schedule, Some(Schedule::from(interval)));
    assert_eq!(created.run_count, 0);
    assert_eq!(created.finished_at, None);
    let first = created.next_run_at.unwrap();
    assert_eq!((first.naive_utc() - anchor).num_milliseconds() % 2000, 0);
    assert!(first > Utc::now() - TimeDelta::seconds(2));

    wait_until("three aligned runs", async || executions("aligned") >= 3).await;
    let fired = info("aligned").await;
    assert!(fired.run_count >= 3);
    assert_eq!(
        (fired.last_run_at.unwrap().naive_utc() - anchor).num_milliseconds() % 2000,
        0
    );
    assert!(fired.next_run_at.unwrap() > fired.last_run_at.unwrap());
    recurring::pause("aligned").await.unwrap();
    assert_eq!(
        fixture.jobs_for("aligned").await.len(),
        usize::try_from(info("aligned").await.run_count).unwrap()
    );
}

async fn check_max_runs_finishes_the_schedule(fixture: &Fixture) {
    let schedule = Schedule::from(every_second()).max_runs(2).unwrap();
    recurring::add_or_update("twice", schedule.clone(), &beat("twice"))
        .await
        .unwrap();
    assert_eq!(info("twice").await.schedule.unwrap().run_limit(), Some(2));
    wait_until("the schedule to finish", async || {
        info("twice").await.finished_at.is_some()
    })
    .await;
    let finished = info("twice").await;
    assert_eq!(finished.run_count, 2);
    assert_eq!(finished.next_run_at, None);
    assert!(finished.enabled);
    wait_until("both runs to execute", async || executions("twice") == 2).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(fixture.jobs_for("twice").await.len(), 2);
    assert_eq!(executions("twice"), 2);

    let outcome = recurring::trigger("twice").await.unwrap();
    assert!(matches!(outcome, EnqueueOutcome::Inserted(_)));
    wait_until("the triggered run", async || executions("twice") == 3).await;
    let triggered = info("twice").await;
    assert_eq!(triggered.run_count, 2);
    assert_eq!(triggered.finished_at, finished.finished_at);
}

async fn check_until_finishes_the_schedule(fixture: &Fixture) {
    let until = Utc::now() + TimeDelta::milliseconds(2500);
    let schedule = Schedule::from(every_second()).until(until);
    recurring::add_or_update("until", schedule, &beat("until"))
        .await
        .unwrap();
    assert_eq!(info("until").await.schedule.unwrap().ends_at(), Some(until));
    wait_until("the schedule to finish", async || {
        info("until").await.finished_at.is_some()
    })
    .await;
    let finished = info("until").await;
    assert!(finished.last_run_at.unwrap() <= until);
    assert!(finished.run_count >= 2, "{finished:?}");
    assert_eq!(finished.next_run_at, None);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        fixture.jobs_for("until").await.len(),
        usize::try_from(finished.run_count).unwrap()
    );

    let past = Schedule::from(every_second()).until(Utc::now() - TimeDelta::seconds(60));
    recurring::add_or_update("expired", past, &beat("expired"))
        .await
        .unwrap();
    let expired = info("expired").await;
    assert!(expired.finished_at.is_some());
    assert_eq!(expired.next_run_at, None);
    assert_eq!(expired.run_count, 0);
    recurring::remove("expired").await.unwrap();
}

async fn check_finished_schedule_stays_finished_until_changed(fixture: &Fixture) {
    let finished = info("twice").await;
    assert!(finished.finished_at.is_some());

    recurring::pause("twice").await.unwrap();
    recurring::resume("twice").await.unwrap();
    let resumed = info("twice").await;
    assert_eq!(resumed.finished_at, finished.finished_at);
    assert_eq!(resumed.next_run_at, None);
    assert!(resumed.enabled);

    recurring::add_or_update("twice", finished.schedule.clone().unwrap(), &beat("twice"))
        .await
        .unwrap();
    let unchanged = info("twice").await;
    assert_eq!(unchanged.finished_at, finished.finished_at);
    assert_eq!(unchanged.run_count, 2);

    let before = executions("twice");
    recurring::update_schedule("twice", Schedule::from(every_second()).max_runs(1).unwrap())
        .await
        .unwrap();
    let restarted = info("twice").await;
    assert_eq!(restarted.finished_at, None);
    assert_eq!(restarted.run_count, 0);
    assert!(restarted.next_run_at.is_some());
    wait_until("one more run", async || executions("twice") == before + 1).await;
    wait_until("the schedule to finish again", async || {
        info("twice").await.finished_at.is_some()
    })
    .await;
    assert_eq!(info("twice").await.run_count, 1);
    assert_eq!(fixture.jobs_for("twice").await.len(), 4);

    recurring::update_schedule("twice", every_second())
        .await
        .unwrap();
    wait_until("the unbounded schedule to run", async || {
        executions("twice") > before + 1
    })
    .await;
    recurring::pause("twice").await.unwrap();
    fixture
        .set_column("twice", "until_at = now() - interval '1 minute'")
        .await;
    recurring::resume("twice").await.unwrap();
    let ended_while_paused = info("twice").await;
    assert!(ended_while_paused.finished_at.is_some());
    assert_eq!(ended_while_paused.next_run_at, None);
    recurring::remove("twice").await.unwrap();
}

async fn check_unchanged_update_keeps_the_count(fixture: &Fixture) {
    let schedule = Schedule::from(every_second()).max_runs(10).unwrap();
    recurring::add_or_update("kept", schedule.clone(), &beat("kept"))
        .await
        .unwrap();
    wait_until("two runs", async || info("kept").await.run_count >= 2).await;
    let before = info("kept").await;

    recurring::add_or_update("kept", schedule.clone(), &beat("kept again"))
        .await
        .unwrap();
    let after_same = info("kept").await;
    assert!(after_same.run_count >= before.run_count);
    assert_eq!(after_same.last_run_at, before.last_run_at);
    wait_until("the new payload to run", async || {
        executions("kept again") >= 1
    })
    .await;

    recurring::update_schedule("kept", schedule.clone())
        .await
        .unwrap();
    assert!(info("kept").await.run_count >= before.run_count);

    recurring::update_schedule(
        "kept",
        schedule.clone().until(Utc::now() + TimeDelta::hours(1)),
    )
    .await
    .unwrap();
    let extended = info("kept").await;
    assert!(extended.run_count >= before.run_count);
    assert_eq!(extended.finished_at, None);
    assert!(extended.next_run_at.unwrap() > before.last_run_at.unwrap());

    recurring::update_schedule(
        "kept",
        Schedule::from(Interval::new(1, IntervalUnit::Seconds, anchor_now()).unwrap())
            .max_runs(10)
            .unwrap(),
    )
    .await
    .unwrap();
    let reset = info("kept").await;
    assert_eq!(reset.run_count, 0);
    assert_eq!(reset.finished_at, None);
    assert!(reset.next_run_at.unwrap() > before.last_run_at.unwrap());

    let nanos = Utc::now().naive_utc().with_nanosecond(123_456_789).unwrap();
    let precise = Schedule::from(Interval::new(1, IntervalUnit::Seconds, nanos).unwrap())
        .until(Utc::now().with_nanosecond(987_654_321).unwrap() + TimeDelta::hours(1));
    recurring::add_or_update("kept", precise.clone(), &beat("kept"))
        .await
        .unwrap();
    wait_until("the precise schedule to run", async || {
        info("kept").await.run_count >= 1
    })
    .await;
    let precise_count = info("kept").await.run_count;
    recurring::add_or_update("kept", precise.clone(), &beat("kept"))
        .await
        .unwrap();
    let same = info("kept").await;
    assert!(same.run_count >= precise_count);
    assert_eq!(same.schedule, Some(precise));
    recurring::remove("kept").await.unwrap();
    let _ = fixture;
}

async fn check_switching_between_cron_and_interval(fixture: &Fixture) {
    recurring::add_or_update("switch", Cron::new("* * * * * *").unwrap(), &beat("switch"))
        .await
        .unwrap();
    wait_until("the cron to run", async || executions("switch") >= 1).await;
    let as_cron = info("switch").await;
    assert!(as_cron.schedule.unwrap().cron().is_some());

    let interval = Interval::new(1, IntervalUnit::Seconds, anchor_now())
        .unwrap()
        .timezone("Asia/Taipei")
        .unwrap();
    recurring::update_schedule("switch", interval.clone())
        .await
        .unwrap();
    let as_interval = info("switch").await;
    let switched = as_interval.schedule.as_ref().unwrap();
    assert_eq!(switched.interval(), Some(&interval));
    assert_eq!(switched.timezone_name(), "Asia/Taipei");
    assert_eq!(as_interval.run_count, 0);
    assert!(as_interval.next_run_at.unwrap() > as_cron.last_run_at.unwrap());
    let before = executions("switch");
    wait_until("the interval to run", async || {
        executions("switch") > before
    })
    .await;

    let yearly = Cron::new("0 0 0 1 1 *").unwrap();
    recurring::update_schedule("switch", yearly.clone())
        .await
        .unwrap();
    let back = info("switch").await;
    assert_eq!(back.schedule, Some(Schedule::from(yearly)));
    assert_eq!(back.run_count, 0);
    assert!(back.next_run_at.unwrap() > Utc::now());
    let stopped = executions("switch");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(executions("switch"), stopped);

    let columns: (
        Option<String>,
        Option<i32>,
        Option<String>,
        Option<NaiveDateTime>,
    ) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT cron, interval_count, interval_unit, anchor FROM {} WHERE name = 'switch'",
        fixture.table("recurring")
    )))
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(columns, (Some("0 0 0 1 1 *".to_owned()), None, None, None));
    recurring::remove("switch").await.unwrap();
}

async fn check_future_anchor_waits(fixture: &Fixture) {
    let given = Utc::now().naive_utc() + TimeDelta::seconds(2);
    let interval = Interval::new(1, IntervalUnit::Hours, given).unwrap();
    let anchor = interval.anchor();
    recurring::add_or_update("later", interval, &beat("later"))
        .await
        .unwrap();
    let created = info("later").await;
    assert_eq!(created.next_run_at.unwrap().naive_utc(), anchor);
    assert_eq!(executions("later"), 0);
    wait_until("the anchor run", async || executions("later") == 1).await;
    let fired = info("later").await;
    assert_eq!(fired.last_run_at.unwrap().naive_utc(), anchor);
    assert_eq!(
        fired.next_run_at.unwrap().naive_utc(),
        anchor + TimeDelta::hours(1)
    );
    assert_eq!(fixture.jobs_for("later").await.len(), 1);
    recurring::remove("later").await.unwrap();
}

async fn check_invalid_schedule_is_reported(fixture: &Fixture) {
    let err = Interval::new(0, IntervalUnit::Days, anchor_now()).unwrap_err();
    assert_eq!(err.kind, ErrorKind::InvalidInterval);
    let err = Schedule::from(every_second()).max_runs(0).unwrap_err();
    assert_eq!(err.kind, ErrorKind::InvalidSchedule);

    let anchor = Utc::now().naive_utc();
    let err = Interval::new(u32::MAX, IntervalUnit::Years, anchor).unwrap_err();
    assert_eq!(err.kind, ErrorKind::InvalidInterval);
    let too_many = Schedule::from(every_second())
        .max_runs(u32::MAX)
        .unwrap_err();
    assert_eq!(too_many.kind, ErrorKind::InvalidSchedule);
    let largest = u32::try_from(i32::MAX).unwrap();
    let interval = Interval::new(largest, IntervalUnit::Years, anchor).unwrap();
    let err = recurring::add_or_update("overflow", interval, &beat("overflow"))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::InvalidInterval);

    recurring::add_or_update("corrupt", every_second(), &beat("corrupt"))
        .await
        .unwrap();
    recurring::pause("corrupt").await.unwrap();
    fixture
        .set_column("corrupt", "timezone = 'Mars/Olympus'")
        .await;
    let corrupt = info("corrupt").await;
    assert_eq!(corrupt.schedule, None);
    assert!(!corrupt.enabled);
    let err = recurring::resume("corrupt").await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::UnknownTimezone);
    recurring::remove("corrupt").await.unwrap();

    for assignment in [
        "interval_unit = 'fortnights'",
        "interval_count = 0",
        "cron = '* * * * *'",
        "max_runs = 0",
        "finished_at = now()",
    ] {
        let err = sqlx::query(AssertSqlSafe(format!(
            "UPDATE {} SET {assignment} WHERE name = 'aligned'",
            fixture.table("recurring")
        )))
        .execute(&fixture.pool)
        .await
        .unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().code().as_deref(),
            Some("23514"),
            "{assignment}"
        );
    }
}
