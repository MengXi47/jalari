use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::{PgListener, PgPoolOptions};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    EnqueueOptions, EnqueueOutcome, ErrorKind, Job, JobId, JobResult, OnConflict, Schema, Worker,
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

    let mut holder = fixture.pool.begin().await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM \"{}\".job WHERE id = $1 FOR UPDATE",
        fixture.schema_name
    )))
    .bind(first.id().0)
    .execute(&mut *holder)
    .await
    .unwrap();

    let second = jalari::enqueue_with(&email("b@example.com"), options)
        .await
        .unwrap();
    assert_eq!(second, EnqueueOutcome::Running(first.id()));
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
    let error = Worker::builder()
        .queue("default", 7)
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::InsufficientConnections);

    Worker::builder()
        .queue("default", 4)
        .queue("reports", 2)
        .build()
        .await
        .unwrap();
}
