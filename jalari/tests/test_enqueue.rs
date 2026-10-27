use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeDelta, Utc};
use jalari::admin;
use jalari::sqlx::postgres::{PgListener, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    EnqueueOptions, EnqueueOutcome, ErrorKind, Job, JobId, JobResult, OnConflict, Priority, Schema,
    Worker,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

#[jalari::job]
impl Job for SendEmail {
    const NAME: &'static str = "test_send_email";
    const MAX_ATTEMPTS: i32 = 5;
    const TIMEOUT: Option<Duration> = Some(Duration::from_secs(30));

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Alarm;

#[jalari::job(queue = "alarms")]
impl Job for Alarm {
    const NAME: &'static str = "test_alarm";
    const PRIORITY: Priority = Priority::Urgent;

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Report;

#[jalari::job(cron = "0 0 3 * * *", queue = "reports")]
impl Job for Report {
    const NAME: &'static str = "test_report";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Debug, sqlx::FromRow)]
struct JobRow {
    queue: String,
    task: String,
    state: String,
    payload: Vec<u8>,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    attempts: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct OptionsRow {
    priority: i16,
    max_attempts: i32,
    queue_key_limit: Option<i16>,
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

struct Fixture {
    pool: PgPool,
    schema_name: String,
}

impl Fixture {
    async fn row(&self, id: JobId) -> JobRow {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT queue, task, state, payload, max_attempts, timeout_ms, attempts
             FROM \"{}\".job WHERE id = $1",
            self.schema_name
        )))
        .bind(id.0)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn options(&self, id: JobId) -> OptionsRow {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT priority, max_attempts, queue_key_limit, expires_at, created_at
             FROM \"{}\".job WHERE id = $1",
            self.schema_name
        )))
        .bind(id.0)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn count(&self) -> i64 {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM \"{}\".job",
            self.schema_name
        )))
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("jalari_test_{}_{nanos}", std::process::id())
}

#[tokio::test]
async fn test_init_and_enqueue() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await
        .unwrap();
    let schema_name = unique_schema_name();
    let schema = Schema::named(&schema_name).unwrap();

    let error = jalari::init(pool.clone())
        .schema(schema.clone())
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::SchemaVersionMismatch);

    jalari::init(pool.clone())
        .schema(schema.clone())
        .migrate()
        .await
        .unwrap();

    let error = jalari::init(pool.clone()).schema(schema).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::AlreadyInitialized);

    let fixture = Fixture {
        pool: pool.clone(),
        schema_name: schema_name.clone(),
    };
    check_enqueue_writes_row(&fixture).await;
    check_delay_schedules_job(&fixture).await;
    check_attribute_queue_is_default(&fixture).await;
    check_job_key_keeps_existing(&fixture).await;
    check_job_key_replace(&fixture).await;
    check_concurrent_replaces_all_apply().await;
    check_replace_reports_running_job(&fixture).await;
    check_enqueue_in_follows_transaction(&fixture).await;
    check_enqueue_notifies_queue(&fixture).await;
    check_worker_build_checks_connections().await;
    check_priority_defaults_to_the_job_type(&fixture).await;
    check_enqueue_option_overrides_priority(&fixture).await;
    check_replace_takes_the_new_priority_and_keep_existing_keeps_the_old(&fixture).await;
    check_enqueue_rejects_max_attempts_below_one(&fixture).await;
    check_replace_writes_the_new_max_attempts(&fixture).await;
    check_key_limit_validation(&fixture).await;
    check_expiry_is_stored(&fixture).await;

    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}

fn email(to: &str) -> SendEmail {
    SendEmail { to: to.to_owned() }
}

async fn check_enqueue_writes_row(fixture: &Fixture) {
    let outcome = jalari::enqueue(&email("a@example.com")).await.unwrap();
    let EnqueueOutcome::Inserted(id) = outcome else {
        panic!("{outcome:?}");
    };
    let row = fixture.row(id).await;
    assert_eq!(row.queue, "default");
    assert_eq!(row.task, "test_send_email");
    assert_eq!(row.state, "enqueued");
    assert_eq!(row.payload, br#"{"to":"a@example.com"}"#);
    assert_eq!(row.max_attempts, 5);
    assert_eq!(row.timeout_ms, Some(30_000));
}

async fn check_delay_schedules_job(fixture: &Fixture) {
    let outcome = jalari::enqueue_with(
        &email("b@example.com"),
        EnqueueOptions::new()
            .delay(Duration::from_nanos(3_600_000_000_001))
            .queue("emails"),
    )
    .await
    .unwrap();
    let row = fixture.row(outcome.id()).await;
    assert_eq!(row.state, "scheduled");
    assert_eq!(row.queue, "emails");
}

async fn check_attribute_queue_is_default(fixture: &Fixture) {
    let outcome = jalari::enqueue(&Report).await.unwrap();
    assert_eq!(fixture.row(outcome.id()).await.queue, "reports");
}

async fn check_job_key_keeps_existing(fixture: &Fixture) {
    let options = EnqueueOptions::new().job_key("keep:1");
    let first = jalari::enqueue_with(&email("first@example.com"), options.clone())
        .await
        .unwrap();
    let second = jalari::enqueue_with(&email("second@example.com"), options)
        .await
        .unwrap();
    assert!(matches!(first, EnqueueOutcome::Inserted(_)));
    assert_eq!(second, EnqueueOutcome::Kept(first.id()));
    assert_eq!(
        fixture.row(first.id()).await.payload,
        br#"{"to":"first@example.com"}"#
    );
}

async fn check_job_key_replace(fixture: &Fixture) {
    let options = EnqueueOptions::new()
        .job_key("replace:1")
        .on_conflict(OnConflict::Replace);
    let first = jalari::enqueue_with(&email("old@example.com"), options.clone())
        .await
        .unwrap();
    let second = jalari::enqueue_with(&email("new@example.com"), options)
        .await
        .unwrap();
    assert_eq!(second, EnqueueOutcome::Replaced(first.id()));
    let row = fixture.row(first.id()).await;
    assert_eq!(row.payload, br#"{"to":"new@example.com"}"#);
    assert_eq!(row.attempts, 0);
}

async fn check_concurrent_replaces_all_apply() {
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..50 {
        tasks.spawn(jalari::enqueue_with(
            &email(&format!("{i}@example.com")),
            EnqueueOptions::new()
                .job_key("replace:many")
                .on_conflict(OnConflict::Replace),
        ));
    }
    let mut outcomes = (0, 0);
    while let Some(outcome) = tasks.join_next().await {
        match outcome.unwrap().unwrap() {
            EnqueueOutcome::Inserted(_) => outcomes.0 += 1,
            EnqueueOutcome::Replaced(_) => outcomes.1 += 1,
            other => panic!("a concurrent replace returned {other:?}"),
        }
    }
    assert_eq!(outcomes, (1, 49));
}

async fn check_replace_reports_running_job(fixture: &Fixture) {
    let options = EnqueueOptions::new()
        .job_key("running:1")
        .on_conflict(OnConflict::Replace);
    let first = jalari::enqueue_with(&email("a@example.com"), options.clone())
        .await
        .unwrap();

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE \"{}\".job
         SET state = 'running', attempts = 1, locked_by = gen_random_uuid(), locked_at = now(),
             lock_token = 1
         WHERE id = $1",
        fixture.schema_name
    )))
    .bind(first.id().0)
    .execute(&fixture.pool)
    .await
    .unwrap();

    let second = jalari::enqueue_with(&email("b@example.com"), options.clone())
        .await
        .unwrap();
    assert_eq!(second, EnqueueOutcome::Running(first.id()));

    sqlx::query(AssertSqlSafe(format!(
        "UPDATE \"{}\".job
         SET state = 'enqueued', locked_by = NULL, locked_at = NULL, lock_token = NULL
         WHERE id = $1",
        fixture.schema_name
    )))
    .bind(first.id().0)
    .execute(&fixture.pool)
    .await
    .unwrap();
    let mut holder = fixture.pool.begin().await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM \"{}\".job WHERE id = $1 FOR UPDATE",
        fixture.schema_name
    )))
    .bind(first.id().0)
    .execute(&mut *holder)
    .await
    .unwrap();
    let contended = jalari::enqueue_with(&email("c@example.com"), options)
        .await
        .unwrap_err();
    assert_eq!(contended.kind, ErrorKind::JobKeyContention);
    holder.rollback().await.unwrap();
}

async fn check_enqueue_in_follows_transaction(fixture: &Fixture) {
    let before = fixture.count().await;

    let mut transaction = fixture.pool.begin().await.unwrap();
    jalari::enqueue_in(
        &mut transaction,
        &email("rollback@example.com"),
        EnqueueOptions::new(),
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(fixture.count().await, before);

    let mut transaction = fixture.pool.begin().await.unwrap();
    jalari::enqueue_in(
        &mut transaction,
        &email("commit@example.com"),
        EnqueueOptions::new(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(fixture.count().await, before + 1);
}

async fn check_enqueue_notifies_queue(fixture: &Fixture) {
    let mut listener = PgListener::connect_with(&fixture.pool).await.unwrap();
    listener
        .listen(&format!("{}.job", fixture.schema_name))
        .await
        .unwrap();

    jalari::enqueue_with(
        &email("notify@example.com"),
        EnqueueOptions::new().queue("notify"),
    )
    .await
    .unwrap();

    let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notification.payload(), "notify");
}

async fn check_worker_build_checks_connections() {
    Worker::builder()
        .queue("default", 64)
        .queue("reports", 32)
        .build()
        .await
        .unwrap();
}

async fn info(id: JobId) -> admin::JobInfo {
    admin::job(id).await.unwrap().unwrap().job
}

fn later() -> EnqueueOptions {
    EnqueueOptions::new().delay(Duration::from_secs(3600))
}

async fn check_priority_defaults_to_the_job_type(fixture: &Fixture) {
    let alarm = jalari::enqueue(&Alarm).await.unwrap().id();
    let plain = jalari::enqueue(&email("priority@example.com"))
        .await
        .unwrap()
        .id();
    assert_eq!(fixture.options(alarm).await.priority, 2);
    assert_eq!(fixture.options(plain).await.priority, 0);
    assert_eq!(info(alarm).await.priority, Priority::Urgent);
    assert_eq!(info(plain).await.priority, Priority::Normal);
}

async fn check_enqueue_option_overrides_priority(fixture: &Fixture) {
    let low = jalari::enqueue_with(&Alarm, EnqueueOptions::new().priority(Priority::Low))
        .await
        .unwrap()
        .id();
    let high = jalari::enqueue_with(
        &email("high@example.com"),
        EnqueueOptions::new().priority(Priority::High),
    )
    .await
    .unwrap()
    .id();
    assert_eq!(fixture.options(low).await.priority, -1);
    assert_eq!(fixture.options(high).await.priority, 1);
    assert_eq!(info(low).await.priority, Priority::Low);
}

async fn check_replace_takes_the_new_priority_and_keep_existing_keeps_the_old(fixture: &Fixture) {
    let keyed = |priority: Priority| later().job_key("priority-key").priority(priority);
    let EnqueueOutcome::Inserted(id) =
        jalari::enqueue_with(&email("keyed@example.com"), keyed(Priority::High))
            .await
            .unwrap()
    else {
        panic!("the first keyed enqueue inserts");
    };
    let kept = jalari::enqueue_with(&email("keyed@example.com"), keyed(Priority::Low))
        .await
        .unwrap();
    assert_eq!(kept, EnqueueOutcome::Kept(id));
    assert_eq!(fixture.options(id).await.priority, 1);
    let replaced = jalari::enqueue_with(
        &email("keyed@example.com"),
        keyed(Priority::Urgent).on_conflict(OnConflict::Replace),
    )
    .await
    .unwrap();
    assert_eq!(replaced, EnqueueOutcome::Replaced(id));
    assert_eq!(fixture.options(id).await.priority, 2);
}

async fn check_enqueue_rejects_max_attempts_below_one(fixture: &Fixture) {
    let before = fixture.count().await;
    for max_attempts in [0, -3] {
        let error = jalari::enqueue_with(
            &email("attempts@example.com"),
            EnqueueOptions::new().max_attempts(max_attempts),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidOptions, "{max_attempts}");
        assert!(error.msg.contains("max_attempts"), "{}", error.msg);
    }
    assert_eq!(fixture.count().await, before);
    let id = jalari::enqueue_with(
        &email("attempts@example.com"),
        EnqueueOptions::new().max_attempts(2),
    )
    .await
    .unwrap()
    .id();
    assert_eq!(fixture.options(id).await.max_attempts, 2);
}

async fn check_replace_writes_the_new_max_attempts(fixture: &Fixture) {
    let keyed = |max_attempts: i32| later().job_key("attempts-key").max_attempts(max_attempts);
    let id = jalari::enqueue_with(&email("replace@example.com"), keyed(3))
        .await
        .unwrap()
        .id();
    assert_eq!(fixture.options(id).await.max_attempts, 3);
    let replaced = jalari::enqueue_with(
        &email("replace@example.com"),
        keyed(7).on_conflict(OnConflict::Replace),
    )
    .await
    .unwrap();
    assert_eq!(replaced, EnqueueOutcome::Replaced(id));
    assert_eq!(fixture.options(id).await.max_attempts, 7);
}

async fn check_key_limit_validation(fixture: &Fixture) {
    let before = fixture.count().await;
    for options in [
        later().queue_key("limits").queue_key_limit(0),
        later().queue_key("limits").queue_key_limit(40_000),
        later().queue_key_limit(2),
    ] {
        let error = jalari::enqueue_with(&email("limit@example.com"), options)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidOptions);
        assert!(error.msg.contains("queue_key_limit"), "{}", error.msg);
    }
    assert_eq!(fixture.count().await, before);
    for (limit, stored) in [(1, None), (4, Some(4))] {
        let options = later().queue_key("limits").queue_key_limit(limit);
        let id = jalari::enqueue_with(&email("limit@example.com"), options)
            .await
            .unwrap()
            .id();
        assert_eq!(fixture.options(id).await.queue_key_limit, stored);
        assert_eq!(info(id).await.queue_key_limit, limit);
    }
    let default = jalari::enqueue_with(&email("limit@example.com"), later().queue_key("limits"))
        .await
        .unwrap()
        .id();
    assert_eq!(info(default).await.queue_key_limit, 1);
}

async fn check_expiry_is_stored(fixture: &Fixture) {
    let at =
        DateTime::from_timestamp_micros((Utc::now() + TimeDelta::minutes(5)).timestamp_micros())
            .unwrap();
    let after = Duration::from_secs(120);
    let enqueue = |options: EnqueueOptions| async move {
        jalari::enqueue_with(&email("expiry@example.com"), options)
            .await
            .unwrap()
            .id()
    };

    let absolute = enqueue(later().expires_at(at)).await;
    assert_eq!(fixture.options(absolute).await.expires_at, Some(at));
    assert_eq!(info(absolute).await.expires_at, Some(at));

    let relative = enqueue(later().expire_after(after)).await;
    let row = fixture.options(relative).await;
    let lifetime = (row.expires_at.unwrap() - row.created_at).to_std().unwrap();
    assert!(
        (after..after + Duration::from_secs(1)).contains(&lifetime),
        "{lifetime:?}"
    );

    let replaced_by_absolute = enqueue(later().expire_after(after).expires_at(at)).await;
    assert_eq!(
        fixture.options(replaced_by_absolute).await.expires_at,
        Some(at)
    );
    let replaced_by_relative = enqueue(later().expires_at(at).expire_after(after)).await;
    assert_ne!(
        fixture.options(replaced_by_relative).await.expires_at,
        Some(at)
    );
    assert_eq!(
        fixture.options(enqueue(later()).await).await.expires_at,
        None
    );
}
