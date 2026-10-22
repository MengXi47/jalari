mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use chrono::Utc;
use jalari::admin::{self, JobFilter, RetryOutcome, StrandedJobs};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    EnqueueOptions, EnqueueOutcome, Interval, IntervalUnit, Job, JobId, JobResult, OnConflict,
    Worker, recurring,
};
use serde::{Deserialize, Serialize};

use common::{Database, RunningWorker, wait_until, worker_config};

const QUEUE: &str = "fingerprint";
const PINNED: &str = "fp_pinned";
const UNPINNED: &str = "fp_unpinned";
const OTHER: &str = "0123456789abcdef0123456789abcdef";
const WAIT: Duration = Duration::from_secs(20);

static RAN: LazyLock<Mutex<BTreeSet<String>>> = LazyLock::new(|| Mutex::new(BTreeSet::new()));

fn ran(label: &str) -> bool {
    RAN.lock().unwrap().contains(label)
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

async fn insert(
    database: &Database,
    task: &str,
    label: &str,
    state: &str,
    fingerprint: Option<&str>,
    job_key: Option<&str>,
) -> JobId {
    let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {} (queue, task, payload, state, max_attempts, fingerprint, job_key)
         VALUES ($1, $2, $3, $4, 3, $5, $6)
         RETURNING id",
        database.table("job")
    )))
    .bind(QUEUE)
    .bind(task)
    .bind(serde_json::to_vec(&serde_json::json!({ "label": label })).unwrap())
    .bind(state)
    .bind(fingerprint)
    .bind(job_key)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    JobId(id)
}

async fn fingerprint_of(id: JobId) -> Option<String> {
    admin::job(id).await.unwrap().unwrap().job.fingerprint
}

async fn state_of(id: JobId) -> (String, i32) {
    let job = admin::job(id).await.unwrap().unwrap().job;
    (job.state.as_str().to_owned(), job.attempts)
}

fn summary(groups: &[StrandedJobs]) -> Vec<(String, String, Option<String>, i64)> {
    groups
        .iter()
        .map(|group| {
            (
                group.queue.clone(),
                group.task.clone(),
                group.fingerprint.clone(),
                group.count,
            )
        })
        .collect()
}

fn stranded_row(task: &str, fingerprint: Option<&str>) -> (String, String, Option<String>, i64) {
    (
        QUEUE.to_owned(),
        task.to_owned(),
        fingerprint.map(str::to_owned),
        1,
    )
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

#[tokio::test]
async fn test_fingerprint() {
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

    let foreign = insert(
        &database,
        PINNED,
        "foreign",
        "enqueued",
        Some(OTHER),
        Some("k"),
    )
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

    insert(&database, PINNED, "no fingerprint", "enqueued", None, None).await;
    insert(
        &database,
        PINNED,
        "same build",
        "enqueued",
        Some(registered),
        None,
    )
    .await;
    let waiting = insert(
        &database,
        PINNED,
        "other build",
        "enqueued",
        Some(OTHER),
        None,
    )
    .await;
    insert(
        &database,
        UNPINNED,
        "unpinned other build",
        "enqueued",
        Some(OTHER),
        None,
    )
    .await;

    let worker = Worker::builder()
        .queue(QUEUE, 4)
        .config(worker_config(Duration::from_millis(100)))
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
    assert_eq!(summary(&groups), [stranded_row(PINNED, Some(OTHER))]);
    assert!(groups[0].since <= Utc::now());

    let old_build = add_worker_row(&database, Some(serde_json::json!({ PINNED: OTHER }))).await;
    assert!(admin::stranded().await.unwrap().is_empty());
    remove_worker_row(&database, &old_build).await;
    let old_release = add_worker_row(&database, None).await;
    assert!(admin::stranded().await.unwrap().is_empty());
    remove_worker_row(&database, &old_release).await;
    assert_eq!(
        summary(&admin::stranded().await.unwrap()),
        [stranded_row(PINNED, Some(OTHER))]
    );

    let released = admin::clear_fingerprint(&JobFilter::new().task(PINNED))
        .await
        .unwrap();
    assert_eq!(released, 1);
    wait_until("the released job to run", WAIT, || async {
        ran("other build")
    })
    .await;
    assert!(admin::stranded().await.unwrap().is_empty());
    assert_eq!(
        admin::clear_fingerprint(&JobFilter::new().task(PINNED))
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

    let yearly = jalari::Cron::new("0 0 0 1 1 *").unwrap();
    recurring::add_or_update("fp_manual", yearly, &pinned("triggered"))
        .await
        .unwrap();
    let EnqueueOutcome::Inserted(triggered) = recurring::trigger("fp_manual").await.unwrap() else {
        panic!("a trigger with nothing pending inserts");
    };
    assert_eq!(fingerprint_of(triggered).await.as_deref(), Some(registered));
    wait_until("the triggered run", WAIT, || async { ran("triggered") }).await;

    let failed = insert(&database, PINNED, "retried", "failed", Some(OTHER), None).await;
    assert_eq!(admin::retry(failed).await.unwrap(), RetryOutcome::Retried);
    assert_eq!(fingerprint_of(failed).await.as_deref(), Some(registered));
    wait_until("the retried job to run", WAIT, || async { ran("retried") }).await;

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (name, cron, timezone, task, payload, queue, max_attempts, managed,
             next_run_at)
         VALUES ('fp_unknown', '0 0 0 1 1 *', 'UTC', 'fp_elsewhere', 'null', $1, 1, FALSE,
             now() + interval '1 day')",
        database.table("recurring")
    )))
    .bind(QUEUE)
    .execute(&database.pool)
    .await
    .unwrap();
    let EnqueueOutcome::Inserted(elsewhere) = recurring::trigger("fp_unknown").await.unwrap()
    else {
        panic!("a trigger with nothing pending inserts");
    };
    assert_eq!(fingerprint_of(elsewhere).await, None);
    assert_eq!(
        summary(&admin::stranded().await.unwrap()),
        [stranded_row("fp_elsewhere", None)]
    );

    running.stop().await;
    database.finish().await;
}
