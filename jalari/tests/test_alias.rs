mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use common::{Database, RunningWorker, wait_until, worker_config};
use jalari::admin::{self, JobInfo};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{Job, JobId, JobResult, JobState, Worker};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(15);
const QUEUE: &str = "alias";
const FOREIGN_FINGERPRINT: &str = "ffffffffffffffffffffffffffffffff";

static RAN: LazyLock<Mutex<Vec<(String, &'static str)>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static RELEASE: AtomicBool = AtomicBool::new(false);

#[derive(Serialize, Deserialize)]
enum SendWelcome {
    Record(String),
    Block,
}

#[jalari::job(queue = "alias", fingerprint = true)]
impl Job for SendWelcome {
    const NAME: &'static str = "send_welcome";
    const ALIASES: &'static [&'static str] = &["welcome_email"];

    async fn run(self) -> JobResult {
        match self {
            Self::Record(label) => {
                let name = jalari::current_job().expect("inside a job").name();
                RAN.lock().unwrap().push((label, name));
            }
            Self::Block => {
                while !RELEASE.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        Ok(())
    }
}

fn ran_as(label: &str) -> Option<&'static str> {
    RAN.lock()
        .unwrap()
        .iter()
        .find(|(ran, _)| ran == label)
        .map(|(_, name)| *name)
}

fn own_fingerprint() -> &'static str {
    jalari::__private::find("send_welcome")
        .and_then(|registration| registration.fingerprint)
        .expect("send_welcome has a fingerprint")
}

async fn insert(
    database: &Database,
    task: &str,
    job: &SendWelcome,
    fingerprint: Option<&str>,
) -> JobId {
    let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {} (queue, task, payload, state, max_attempts, fingerprint)
         VALUES ($1, $2, $3, 'enqueued', 3, $4)
         RETURNING id",
        database.table("job")
    )))
    .bind(QUEUE)
    .bind(task)
    .bind(serde_json::to_vec(job).unwrap())
    .bind(fingerprint)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    JobId(id)
}

async fn info(id: JobId) -> JobInfo {
    admin::job(id).await.unwrap().unwrap().job
}

async fn wait_state(id: JobId, state: JobState) {
    wait_until(
        &format!("job {id:?} to be {state:?}"),
        WAIT,
        || async move { info(id).await.state == state },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_alias() {
    let Some(database) = Database::start("alias", 8).await else {
        return;
    };
    let worker = Worker::builder()
        .queue(QUEUE, 1)
        .config(worker_config(Duration::from_millis(50)))
        .build()
        .await
        .unwrap();
    let worker = RunningWorker::spawn(worker);

    check_alias_is_served_with_the_fingerprint().await;
    check_row_under_alias_runs_with_canonical_name(&database).await;
    check_alias_row_with_matching_fingerprint_is_claimed(&database).await;
    check_stranded_does_not_list_alias_rows(&database).await;

    worker.stop().await;
    database.finish().await;
}

async fn check_alias_is_served_with_the_fingerprint() {
    wait_until("the worker to register", WAIT, || async {
        !admin::workers().await.unwrap().is_empty()
    })
    .await;
    let workers = admin::workers().await.unwrap();
    let tasks = &workers[0].tasks;
    let fingerprint = Some(own_fingerprint().to_owned());
    assert_eq!(tasks.get("send_welcome"), Some(&fingerprint));
    assert_eq!(tasks.get("welcome_email"), Some(&fingerprint));
}

async fn check_row_under_alias_runs_with_canonical_name(database: &Database) {
    let job = SendWelcome::Record("unstamped".to_owned());
    let id = insert(database, "welcome_email", &job, None).await;
    wait_state(id, JobState::Succeeded).await;
    assert_eq!(ran_as("unstamped"), Some("send_welcome"));
    assert_eq!(info(id).await.task, "welcome_email");
}

async fn check_alias_row_with_matching_fingerprint_is_claimed(database: &Database) {
    let foreign = SendWelcome::Record("foreign build".to_owned());
    let foreign = insert(
        database,
        "welcome_email",
        &foreign,
        Some(FOREIGN_FINGERPRINT),
    )
    .await;
    let own = SendWelcome::Record("same build".to_owned());
    let own = insert(database, "welcome_email", &own, Some(own_fingerprint())).await;
    wait_state(own, JobState::Succeeded).await;
    assert_eq!(ran_as("same build"), Some("send_welcome"));
    assert_eq!(info(foreign).await.state, JobState::Enqueued);
    assert_eq!(ran_as("foreign build"), None);
    jalari::cancel(foreign).await.unwrap();
}

async fn check_stranded_does_not_list_alias_rows(database: &Database) {
    let blocker = jalari::enqueue(&SendWelcome::Block).await.unwrap().id();
    wait_state(blocker, JobState::Running).await;
    let waiting = SendWelcome::Record("behind the blocker".to_owned());
    let waiting = insert(database, "welcome_email", &waiting, Some(own_fingerprint())).await;
    let unknown = insert(database, "nobody_runs_this", &SendWelcome::Block, None).await;

    let stranded = admin::stranded().await.unwrap();
    let tasks: Vec<&str> = stranded.iter().map(|group| group.task.as_str()).collect();
    assert!(tasks.contains(&"nobody_runs_this"), "{tasks:?}");
    assert!(!tasks.contains(&"welcome_email"), "{tasks:?}");
    assert_eq!(info(waiting).await.state, JobState::Enqueued);

    RELEASE.store(true, Ordering::SeqCst);
    wait_state(waiting, JobState::Succeeded).await;
    jalari::cancel(unknown).await.unwrap();
}
