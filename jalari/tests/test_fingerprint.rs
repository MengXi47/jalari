mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use jalari::admin::{self, JobFilter, RetryOutcome, RunNowOutcome, StrandedJobs};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    Cron, EnqueueOptions, EnqueueOutcome, Interval, IntervalUnit, Job, JobId, JobResult,
    OnConflict, Worker, recurring,
};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

use common::{Database, RunningWorker, wait_until, worker_config};

const QUEUE: &str = "fingerprint";
const UNSERVED: &str = "fp_unserved";
const PINNED: &str = "fp_pinned";
const UNPINNED: &str = "fp_unpinned";
const ELSEWHERE: &str = "fp_elsewhere";
const OTHER: &str = "0123456789abcdef0123456789abcdef";
const WAIT: Duration = Duration::from_secs(20);
const ANOTHER_BUILD: &str = "no live worker runs these jobs, enqueued by another build of the task";
const UNKNOWN_TASK: &str = "no live worker knows this task";

static RAN: LazyLock<Mutex<BTreeSet<String>>> = LazyLock::new(|| Mutex::new(BTreeSet::new()));
static WARNINGS: LazyLock<Mutex<Vec<BTreeMap<String, String>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

fn ran(label: &str) -> bool {
    RAN.lock().unwrap().contains(label)
}

#[derive(Default)]
struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

struct CaptureWarnings;

impl Subscriber for CaptureWarnings {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() == Level::WARN
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        WARNINGS.lock().unwrap().push(fields.0);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

fn warned(queue: &str, task: &str, message: &str) -> bool {
    WARNINGS.lock().unwrap().iter().any(|fields| {
        fields.get("queue").map(String::as_str) == Some(queue)
            && fields.get("task").map(String::as_str) == Some(task)
            && fields.get("message").map(String::as_str) == Some(message)
    })
}

#[derive(Serialize, Deserialize)]
struct Pinned {
    label: String,
}

#[jalari::job(queue = "fingerprint")]
impl Job for Pinned {
    const NAME: &'static str = "fp_pinned";

    async fn run(self) -> JobResult {
        RAN.lock().unwrap().insert(self.label);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Unpinned {
    label: String,
}

#[jalari::job(queue = "fingerprint", fingerprint = false)]
impl Job for Unpinned {
    const NAME: &'static str = "fp_unpinned";

    async fn run(self) -> JobResult {
        RAN.lock().unwrap().insert(self.label);
        Ok(())
    }
}

fn pinned(label: &str) -> Pinned {
    Pinned {
        label: label.to_owned(),
    }
}

struct Row<'a> {
    queue: &'a str,
    task: &'a str,
    label: &'a str,
    state: &'a str,
    fingerprint: Option<&'a str>,
    job_key: Option<&'a str>,
    due_in_hours: i32,
}

impl<'a> Row<'a> {
    fn new(task: &'a str, label: &'a str, fingerprint: Option<&'a str>) -> Self {
        Self {
            queue: QUEUE,
            task,
            label,
            state: "enqueued",
            fingerprint,
            job_key: None,
            due_in_hours: 0,
        }
    }

    async fn insert(self, database: &Database) -> JobId {
        let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "INSERT INTO {} (queue, task, payload, state, max_attempts, fingerprint, job_key,
                 run_at)
             VALUES ($1, $2, $3, $4, 3, $5, $6, now() + make_interval(hours => $7))
             RETURNING id",
            database.table("job")
        )))
        .bind(self.queue)
        .bind(self.task)
        .bind(serde_json::to_vec(&serde_json::json!({ "label": self.label })).unwrap())
        .bind(self.state)
        .bind(self.fingerprint)
        .bind(self.job_key)
        .bind(self.due_in_hours)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        JobId(id)
    }
}

async fn fingerprint_of(id: JobId) -> Option<String> {
    admin::job(id).await.unwrap().unwrap().job.fingerprint
}

async fn state_of(id: JobId) -> (String, i32) {
    let job = admin::job(id).await.unwrap().unwrap().job;
    (job.state.as_str().to_owned(), job.attempts)
}

async fn updated_at(id: JobId) -> DateTime<Utc> {
    admin::job(id).await.unwrap().unwrap().job.updated_at
}

type Group = (String, String, Option<String>, i64);

fn group(queue: &str, task: &str, fingerprint: Option<&str>, count: i64) -> Group {
    (
        queue.to_owned(),
        task.to_owned(),
        fingerprint.map(str::to_owned),
        count,
    )
}

async fn stranded() -> Vec<Group> {
    let groups: Vec<StrandedJobs> = admin::stranded().await.unwrap();
    groups
        .into_iter()
        .map(|found| (found.queue, found.task, found.fingerprint, found.count))
        .collect()
}

async fn add_worker_row(database: &Database, tasks: Option<serde_json::Value>) -> String {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {} (hostname, pid, queues, scheduler, tasks)
         VALUES ('old-build', 1, $1, FALSE, $2)
         RETURNING id::text",
        database.table("worker")
    )))
    .bind(serde_json::json!([{ "name": QUEUE, "concurrency": 1 }]))
    .bind(tasks)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn remove_worker_row(database: &Database, id: &str) {
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {} WHERE id = $1::uuid",
        database.table("worker")
    )))
    .bind(id)
    .execute(&database.pool)
    .await
    .unwrap();
}

async fn listening(database: &Database) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
             WHERE application_name = $1 AND query ILIKE 'LISTEN%')",
    )
    .bind(&database.schema_name)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn test_fingerprint() {
    tracing::subscriber::set_global_default(CaptureWarnings).unwrap();
    let Some(database) = Database::start("fingerprint", 12).await else {
        return;
    };

    let registered = jalari::__private::find(PINNED)
        .unwrap()
        .fingerprint
        .expect("the job attribute stamps a fingerprint");
    assert_eq!(registered.len(), 32);
    assert!(
        registered
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert_ne!(registered, OTHER);
    assert_eq!(jalari::__private::find(UNPINNED).unwrap().fingerprint, None);

    let EnqueueOutcome::Inserted(enqueued) = jalari::enqueue(&pinned("enqueued")).await.unwrap()
    else {
        panic!("a plain enqueue inserts");
    };
    assert_eq!(fingerprint_of(enqueued).await.as_deref(), Some(registered));
    let EnqueueOutcome::Inserted(unpinned) = jalari::enqueue(&Unpinned {
        label: "unpinned enqueued".to_owned(),
    })
    .await
    .unwrap() else {
        panic!("a plain enqueue inserts");
    };
    assert_eq!(fingerprint_of(unpinned).await, None);

    let foreign = Row {
        job_key: Some("k"),
        ..Row::new(PINNED, "foreign", Some(OTHER))
    }
    .insert(&database)
    .await;
    let outcome = jalari::enqueue_with(
        &pinned("replaced"),
        EnqueueOptions::new()
            .job_key("k")
            .on_conflict(OnConflict::Replace),
    )
    .await
    .unwrap();
    assert_eq!(outcome, EnqueueOutcome::Replaced(foreign));
    assert_eq!(fingerprint_of(foreign).await.as_deref(), Some(registered));

    Row::new(PINNED, "no fingerprint", None)
        .insert(&database)
        .await;
    Row::new(PINNED, "same build", Some(registered))
        .insert(&database)
        .await;
    let waiting = Row::new(PINNED, "other build", Some(OTHER))
        .insert(&database)
        .await;
    Row::new(UNPINNED, "unpinned other build", Some(OTHER))
        .insert(&database)
        .await;
    Row::new(ELSEWHERE, "unknown task", None)
        .insert(&database)
        .await;
    let unserved = Row {
        queue: UNSERVED,
        ..Row::new(PINNED, "unserved queue", Some(registered))
    }
    .insert(&database)
    .await;

    let worker = Worker::builder()
        .queue(QUEUE, 4)
        .config(worker_config(Duration::from_secs(30)))
        .build()
        .await
        .unwrap();
    let running = RunningWorker::spawn(worker);
    let expected = [
        "enqueued",
        "unpinned enqueued",
        "replaced",
        "no fingerprint",
        "same build",
        "unpinned other build",
    ];
    wait_until("the compatible jobs to run", WAIT, || async {
        expected.iter().all(|label| ran(label))
    })
    .await;
    wait_until("the listener to subscribe", WAIT, || listening(&database)).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!ran("other build"));
    assert_eq!(state_of(waiting).await, ("enqueued".to_owned(), 0));

    let workers = admin::workers().await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(
        workers[0].tasks,
        BTreeMap::from([
            (PINNED.to_owned(), Some(registered.to_owned())),
            (UNPINNED.to_owned(), None),
        ])
    );

    let groups = admin::stranded().await.unwrap();
    assert!(groups.iter().all(|found| found.since <= Utc::now()));
    assert_eq!(
        stranded().await,
        [
            group(QUEUE, ELSEWHERE, None, 1),
            group(QUEUE, PINNED, Some(OTHER), 1),
            group(UNSERVED, PINNED, Some(registered), 1),
        ]
    );
    wait_until("the worker to warn about stranded jobs", WAIT, || async {
        warned(QUEUE, PINNED, ANOTHER_BUILD) && warned(QUEUE, ELSEWHERE, UNKNOWN_TASK)
    })
    .await;
    assert!(
        !WARNINGS
            .lock()
            .unwrap()
            .iter()
            .any(|fields| fields.get("queue").map(String::as_str) == Some(UNSERVED)),
        "a worker only warns about its own queues"
    );

    let old_build = add_worker_row(&database, Some(serde_json::json!({ PINNED: OTHER }))).await;
    assert_eq!(
        stranded().await,
        [
            group(QUEUE, ELSEWHERE, None, 1),
            group(UNSERVED, PINNED, Some(registered), 1),
        ]
    );
    remove_worker_row(&database, &old_build).await;
    let old_release = add_worker_row(&database, None).await;
    let listed = admin::workers().await.unwrap();
    let old = listed
        .iter()
        .find(|worker| worker.id == old_release)
        .expect("the old release worker is listed");
    assert!(old.tasks.is_empty());
    assert_eq!(
        stranded().await,
        [group(UNSERVED, PINNED, Some(registered), 1)]
    );
    remove_worker_row(&database, &old_release).await;

    let later = Row {
        due_in_hours: 1,
        state: "scheduled",
        ..Row::new(PINNED, "run now", Some(OTHER))
    }
    .insert(&database)
    .await;
    assert_eq!(
        admin::run_now(later).await.unwrap(),
        RunNowOutcome::Enqueued
    );
    assert_eq!(fingerprint_of(later).await.as_deref(), Some(OTHER));
    assert!(
        stranded()
            .await
            .contains(&group(QUEUE, PINNED, Some(OTHER), 2))
    );

    let stopped = Row {
        state: "stopped",
        ..Row::new(PINNED, "stopped", Some(OTHER))
    }
    .insert(&database)
    .await;
    let failed = Row {
        state: "failed",
        ..Row::new(PINNED, "failed", Some(OTHER))
    }
    .insert(&database)
    .await;
    let stopped_updated_at = updated_at(stopped).await;
    let released = admin::clear_fingerprint(&JobFilter::new().task(PINNED).queue(QUEUE))
        .await
        .unwrap();
    assert_eq!(released, 3);
    assert_eq!(fingerprint_of(stopped).await, None);
    assert_eq!(updated_at(stopped).await, stopped_updated_at);
    assert_eq!(state_of(stopped).await.0, "stopped");
    assert_eq!(fingerprint_of(failed).await.as_deref(), Some(OTHER));
    assert_eq!(fingerprint_of(unserved).await.as_deref(), Some(registered));
    wait_until("the released jobs to run", WAIT, || async {
        ran("other build") && ran("run now")
    })
    .await;
    assert_eq!(
        stranded().await,
        [
            group(QUEUE, ELSEWHERE, None, 1),
            group(UNSERVED, PINNED, Some(registered), 1),
        ]
    );
    assert_eq!(
        admin::clear_fingerprint(&JobFilter::new().task(PINNED).queue(QUEUE))
            .await
            .unwrap(),
        0
    );

    let every_second = Interval::new(1, IntervalUnit::Seconds, Utc::now().naive_utc()).unwrap();
    recurring::add_or_update("fp_every_second", every_second, &pinned("scheduled"))
        .await
        .unwrap();
    wait_until("the schedule to fire", WAIT, || async { ran("scheduled") }).await;
    recurring::remove("fp_every_second").await.unwrap();
    let fired: Vec<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT fingerprint FROM {} WHERE job_key = 'recurring:fp_every_second'",
        database.table("job")
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert!(!fired.is_empty());
    assert!(
        fired
            .iter()
            .all(|fingerprint| fingerprint.as_deref() == Some(registered))
    );

    recurring::add_or_update(
        "fp_manual",
        Cron::new("0 0 0 1 1 *").unwrap(),
        &pinned("triggered"),
    )
    .await
    .unwrap();
    let EnqueueOutcome::Inserted(triggered) = recurring::trigger("fp_manual").await.unwrap() else {
        panic!("a trigger with nothing pending inserts");
    };
    assert_eq!(fingerprint_of(triggered).await.as_deref(), Some(registered));
    wait_until("the triggered run", WAIT, || async { ran("triggered") }).await;

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (name, cron, timezone, task, payload, queue, max_attempts, managed,
             next_run_at)
         VALUES ('fp_unknown', '0 0 0 1 1 *', 'UTC', $1, 'null', $2, 1, FALSE,
             now() + interval '1 day')",
        database.table("recurring")
    )))
    .bind(ELSEWHERE)
    .bind(QUEUE)
    .execute(&database.pool)
    .await
    .unwrap();
    let EnqueueOutcome::Inserted(elsewhere) = recurring::trigger("fp_unknown").await.unwrap()
    else {
        panic!("a trigger with nothing pending inserts");
    };
    assert_eq!(fingerprint_of(elsewhere).await, None);

    let retried = Row {
        state: "failed",
        ..Row::new(PINNED, "retried", Some(OTHER))
    }
    .insert(&database)
    .await;
    assert_eq!(admin::retry(retried).await.unwrap(), RetryOutcome::Retried);
    assert_eq!(fingerprint_of(retried).await.as_deref(), Some(registered));
    wait_until("the retried job to run", WAIT, || async { ran("retried") }).await;

    let kept = Row {
        state: "failed",
        ..Row::new(ELSEWHERE, "kept", Some(OTHER))
    }
    .insert(&database)
    .await;
    assert_eq!(admin::retry(kept).await.unwrap(), RetryOutcome::Retried);
    assert_eq!(fingerprint_of(kept).await.as_deref(), Some(OTHER));

    let kept_in_batch = Row {
        state: "failed",
        ..Row::new(ELSEWHERE, "kept in batch", Some(OTHER))
    }
    .insert(&database)
    .await;
    let summary = admin::retry_failed(&JobFilter::new().queue(QUEUE))
        .await
        .unwrap();
    assert_eq!(summary.retried, 2);
    assert_eq!(fingerprint_of(failed).await.as_deref(), Some(registered));
    assert_eq!(fingerprint_of(kept_in_batch).await.as_deref(), Some(OTHER));
    wait_until("the batch-retried job to run", WAIT, || async {
        ran("failed")
    })
    .await;

    assert_eq!(
        stranded().await,
        [
            group(QUEUE, ELSEWHERE, None, 2),
            group(QUEUE, ELSEWHERE, Some(OTHER), 2),
            group(UNSERVED, PINNED, Some(registered), 1),
        ]
    );

    running.stop().await;
    database.finish().await;
}
