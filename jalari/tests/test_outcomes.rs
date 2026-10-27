mod common;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, RunningWorker, fast_retry, wait_until, worker_config};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{EnqueueOptions, Job, JobError, JobId, JobResult, RetryPolicy, Worker};
use serde::{Deserialize, Serialize};

const SETTLE: Duration = Duration::from_secs(20);

static RUNS: LazyLock<Mutex<HashMap<String, Vec<Run>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone)]
struct Run {
    attempt: i32,
    start: Instant,
    end: Option<Instant>,
}

fn runs(label: &str) -> Vec<Run> {
    RUNS.lock().unwrap().get(label).cloned().unwrap_or_default()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Step {
    Succeed,
    Fail,
    RetryAfter(u64),
}

#[derive(Serialize, Deserialize)]
struct Scripted {
    label: String,
    steps: Vec<Step>,
}

#[jalari::job(queue = "outcomes")]
impl Job for Scripted {
    const NAME: &'static str = "outcomes_scripted";
    const MAX_ATTEMPTS: i32 = 10;

    async fn run(self) -> JobResult {
        perform(self).await
    }
}

async fn perform(scripted: Scripted) -> JobResult {
    let attempt = jalari::current_job().unwrap().attempt();
    let index = {
        let mut all = RUNS.lock().unwrap();
        let runs = all.entry(scripted.label.clone()).or_default();
        runs.push(Run {
            attempt,
            start: Instant::now(),
            end: None,
        });
        runs.len() - 1
    };
    let step = scripted.steps[index.min(scripted.steps.len() - 1)];
    let result = match step {
        Step::Succeed => Ok(()),
        Step::Fail => Err(JobError::new(format!("{} failed", scripted.label))),
        Step::RetryAfter(millis) => {
            Err(JobError::new("rate limited".to_owned()).retry_after(Duration::from_millis(millis)))
        }
    };
    if let Some(run) = RUNS
        .lock()
        .unwrap()
        .get_mut(&scripted.label)
        .and_then(|runs| runs.get_mut(index))
    {
        run.end = Some(Instant::now());
    }
    result
}

fn scripted(label: &str, steps: &[Step]) -> Scripted {
    Scripted {
        label: label.to_owned(),
        steps: steps.to_vec(),
    }
}

async fn spawn<R: RetryPolicy>(queue: &str, retry_policy: R) -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, 4)
            .config(worker_config(Duration::from_millis(50)))
            .retry_policy(retry_policy)
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn enqueue<T: Job>(job: &T, queue: &str, options: EnqueueOptions) -> JobId {
    jalari::enqueue_with(job, options.queue(queue))
        .await
        .unwrap()
        .id()
}

#[derive(Debug, sqlx::FromRow)]
struct Row {
    state: String,
    attempts: i32,
    last_error: Option<String>,
    wait_ms: f64,
}

async fn row(database: &Database, id: JobId) -> Row {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts, last_error,
             (EXTRACT(EPOCH FROM run_at - updated_at) * 1000)::float8 AS wait_ms
         FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn history(database: &Database, id: JobId) -> Vec<(String, i32, Option<String>, bool)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempt, error, duration_ms IS NOT NULL
         FROM {} WHERE job_id = $1 ORDER BY id",
        database.table("job_history")
    )))
    .bind(id.0)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn wait_for_state(database: &Database, id: JobId, state: &str) {
    wait_until(&format!("job {id} to be {state}"), SETTLE, || async {
        row(database, id).await.state == state
    })
    .await;
}

struct Endless;

impl RetryPolicy for Endless {
    fn next_retry(&self, _: i32, _: i32, _: &JobError) -> Option<Duration> {
        Some(Duration::MAX)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_outcomes() {
    let Some(database) = Database::start("outcomes", 40).await else {
        return;
    };

    tokio::join!(
        check_retry_after_sets_the_next_run(&database),
        check_policy_delay_is_capped(&database),
    );

    database.finish().await;
}

async fn check_retry_after_sets_the_next_run(database: &Database) {
    let queue = "outcomes_retry_after";
    let worker = spawn(queue, fast_retry()).await;
    let id = enqueue(
        &scripted("retry_after", &[Step::RetryAfter(2000), Step::Succeed]),
        queue,
        EnqueueOptions::new(),
    )
    .await;

    wait_for_state(database, id, "scheduled").await;
    let waiting = row(database, id).await;
    assert_eq!(waiting.attempts, 1);
    assert_eq!(waiting.last_error.as_deref(), Some("rate limited"));
    assert!(
        (waiting.wait_ms - 2000.0).abs() <= 100.0,
        "run_at is {} ms after the close, expected 2000 ms",
        waiting.wait_ms
    );

    wait_for_state(database, id, "succeeded").await;
    let runs = runs("retry_after");
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[1].attempt, 2);
    let gap = runs[1].start - runs[0].end.unwrap();
    assert!(
        gap >= Duration::from_millis(1900),
        "second run after {gap:?}"
    );
    assert_eq!(
        history(database, id).await,
        vec![
            (
                "scheduled".to_owned(),
                1,
                Some("rate limited".to_owned()),
                true
            ),
            ("succeeded".to_owned(), 2, None, true),
        ]
    );
    worker.stop().await;
}

async fn check_policy_delay_is_capped(database: &Database) {
    let queue = "outcomes_capped";
    let worker = spawn(queue, Endless).await;
    let id = enqueue(
        &scripted("capped", &[Step::Fail]),
        queue,
        EnqueueOptions::new(),
    )
    .await;

    wait_for_state(database, id, "scheduled").await;
    let waiting = row(database, id).await;
    assert_eq!(waiting.attempts, 1);
    let days = waiting.wait_ms / 86_400_000.0;
    assert!(
        (days - 36_500.0).abs() < 1.0,
        "run_at is {days} days after the close, expected 100 years"
    );
    worker.stop().await;
}
