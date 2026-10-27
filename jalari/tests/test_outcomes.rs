mod common;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, RunningWorker, fast_retry, wait_until, worker_config};
use jalari::admin::{RetryOutcome, RunNowOutcome};
use jalari::recurring::{self, Cron};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    EnqueueOptions, EnqueueOutcome, ExponentialBackoff, Job, JobError, JobId, JobMeta,
    JobMiddleware, JobResult, Next, RetryPolicy, Worker,
};
use serde::{Deserialize, Serialize};

const SETTLE: Duration = Duration::from_secs(20);
const SNOOZED: &str = "the job asked to run again later";

static RUNS: LazyLock<Mutex<HashMap<String, Vec<Run>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone)]
struct Run {
    attempt: i32,
    resumed: Option<u64>,
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
    Snooze(u64),
    SaveThenSnooze(u64),
    Busy,
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
    let (resumed, checkpoint) = jalari::resume::<u64>()?;
    let index = {
        let mut all = RUNS.lock().unwrap();
        let runs = all.entry(scripted.label.clone()).or_default();
        runs.push(Run {
            attempt,
            resumed,
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
        Step::Snooze(millis) => jalari::snooze(Duration::from_millis(millis)),
        Step::SaveThenSnooze(millis) => {
            checkpoint.save(&7).await?;
            jalari::snooze(Duration::from_millis(millis))
        }
        Step::Busy => Err(JobError::new("busy".to_owned())),
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

struct Fixed(Duration);

impl RetryPolicy for Fixed {
    fn next_retry(&self, attempt: i32, max_attempts: i32, error: &JobError) -> Option<Duration> {
        (!error.is_permanent() && attempt < max_attempts).then_some(self.0)
    }
}

static QUICK: Fixed = Fixed(Duration::from_millis(50));

#[derive(Serialize, Deserialize)]
struct Picky(Scripted);

#[jalari::job(queue = "outcomes")]
impl Job for Picky {
    const NAME: &'static str = "outcomes_picky";
    const RETRY_POLICY: Option<&'static dyn RetryPolicy> = Some(&QUICK);

    async fn run(self) -> JobResult {
        perform(self.0).await
    }
}

#[derive(Serialize, Deserialize)]
struct Ticker;

#[jalari::job(queue = "outcomes_recurring")]
impl Job for Ticker {
    const NAME: &'static str = "outcomes_ticker";

    async fn run(self) -> JobResult {
        perform(scripted("recurring", &[Step::Snooze(3000), Step::Succeed])).await
    }
}

struct Snoozer;

impl JobMiddleware for Snoozer {
    type Provides = ();

    async fn call(&self, _: &JobMeta, next: Next<'_>) -> JobResult {
        match next.run().await {
            Err(error) if error.msg() == "busy" => jalari::snooze(Duration::from_millis(200)),
            result => result,
        }
    }
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

async fn spawn_snoozer(queue: &str) -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, 4)
            .config(worker_config(Duration::from_millis(50)))
            .retry_policy(fast_retry())
            .middleware(Snoozer)
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
        check_job_retry_policy_overrides_the_worker_policy(&database),
        check_enqueue_max_attempts_overrides_the_job_constant(&database),
        check_snooze_runs_again_without_counting_the_attempt(&database),
        check_snooze_keeps_progress_and_frees_the_queue_key(&database),
        check_snooze_keeps_the_job_key(&database),
        check_run_now_wakes_a_snoozed_job(&database),
        check_middleware_turns_an_error_into_a_snooze(&database),
    );
    check_snoozed_recurring_run_holds_its_schedule(&database).await;

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

async fn check_job_retry_policy_overrides_the_worker_policy(database: &Database) {
    let queue = "outcomes_job_policy";
    let worker = spawn(
        queue,
        ExponentialBackoff {
            base: Duration::from_secs(3600),
            max: Duration::from_secs(3600),
            jitter: 0.0,
        },
    )
    .await;
    let started = Instant::now();
    let id = enqueue(
        &Picky(scripted(
            "job_policy",
            &[Step::Fail, Step::Fail, Step::Succeed],
        )),
        queue,
        EnqueueOptions::new(),
    )
    .await;

    wait_until(
        "the picky job to succeed",
        Duration::from_secs(3),
        || async { row(database, id).await.state == "succeeded" },
    )
    .await;
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(row(database, id).await.attempts, 3);
    worker.stop().await;
}

async fn check_enqueue_max_attempts_overrides_the_job_constant(database: &Database) {
    let queue = "outcomes_max_attempts";
    let worker = spawn(queue, fast_retry()).await;
    let id = enqueue(
        &scripted("max_attempts", &[Step::Fail]),
        queue,
        EnqueueOptions::new().max_attempts(2),
    )
    .await;

    wait_for_state(database, id, "failed").await;
    assert_eq!(row(database, id).await.attempts, 2);
    assert_eq!(runs("max_attempts").len(), 2);

    assert_eq!(
        jalari::admin::retry(id).await.unwrap(),
        RetryOutcome::Retried
    );
    wait_for_state(database, id, "failed").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(row(database, id).await.attempts, 2);
    let attempts: Vec<i32> = runs("max_attempts").iter().map(|run| run.attempt).collect();
    assert_eq!(attempts, [1, 2, 1, 2]);
    worker.stop().await;
}

fn snooze_row(attempt: i32) -> (String, i32, Option<String>, bool) {
    (
        "scheduled".to_owned(),
        attempt,
        Some(SNOOZED.to_owned()),
        true,
    )
}

fn succeeded_row(attempt: i32) -> (String, i32, Option<String>, bool) {
    ("succeeded".to_owned(), attempt, None, true)
}

async fn check_snooze_runs_again_without_counting_the_attempt(database: &Database) {
    let queue = "outcomes_snooze";
    let worker = spawn(queue, fast_retry()).await;
    let id = enqueue(
        &scripted("snooze", &[Step::Snooze(300), Step::Succeed]),
        queue,
        EnqueueOptions::new(),
    )
    .await;

    wait_for_state(database, id, "succeeded").await;
    let job = row(database, id).await;
    assert_eq!(job.attempts, 1);
    assert_eq!(job.last_error, None);
    assert_eq!(
        history(database, id).await,
        vec![snooze_row(0), succeeded_row(1)]
    );
    let runs = runs("snooze");
    assert_eq!(
        runs.iter().map(|run| run.attempt).collect::<Vec<_>>(),
        [1, 1]
    );
    let gap = runs[1].start - runs[0].end.unwrap();
    assert!(
        gap >= Duration::from_millis(290),
        "second run started {gap:?} after the snooze"
    );
    worker.stop().await;
}

async fn check_snooze_keeps_progress_and_frees_the_queue_key(database: &Database) {
    let queue = "outcomes_snooze_progress";
    let worker = spawn(queue, fast_retry()).await;
    let options = || EnqueueOptions::new().queue_key("outcomes_shared");
    let id = enqueue(
        &scripted(
            "snooze_progress",
            &[Step::SaveThenSnooze(2000), Step::Succeed],
        ),
        queue,
        options(),
    )
    .await;
    wait_for_state(database, id, "scheduled").await;
    let sibling = enqueue(
        &scripted("snooze_sibling", &[Step::Succeed]),
        queue,
        options(),
    )
    .await;

    wait_for_state(database, sibling, "succeeded").await;
    wait_for_state(database, id, "succeeded").await;
    let snoozer = runs("snooze_progress");
    let sibling_run = &runs("snooze_sibling")[0];
    assert!(
        sibling_run.end.unwrap() < snoozer[1].start,
        "the sibling with the same queue_key ran while the job was snoozed"
    );
    assert_eq!(snoozer[0].resumed, None);
    assert_eq!(snoozer[1].resumed, Some(7));
    worker.stop().await;
}

async fn check_snooze_keeps_the_job_key(database: &Database) {
    let queue = "outcomes_snooze_key";
    let worker = spawn(queue, fast_retry()).await;
    let options = || EnqueueOptions::new().job_key("outcomes_snooze_key");
    let job = scripted("snooze_key", &[Step::Snooze(2000), Step::Succeed]);
    let id = enqueue(&job, queue, options()).await;
    wait_for_state(database, id, "scheduled").await;

    let again = jalari::enqueue_with(&job, options().queue(queue))
        .await
        .unwrap();
    assert_eq!(again, EnqueueOutcome::Kept(id));
    wait_for_state(database, id, "succeeded").await;
    assert_eq!(runs("snooze_key").len(), 2);
    worker.stop().await;
}

async fn check_run_now_wakes_a_snoozed_job(database: &Database) {
    let queue = "outcomes_run_now";
    let worker = spawn(queue, fast_retry()).await;
    let id = enqueue(
        &scripted("run_now", &[Step::Snooze(3_600_000), Step::Succeed]),
        queue,
        EnqueueOptions::new(),
    )
    .await;
    wait_for_state(database, id, "scheduled").await;

    let woken = Instant::now();
    assert_eq!(
        jalari::admin::run_now(id).await.unwrap(),
        RunNowOutcome::Enqueued
    );
    wait_until(
        "the snoozed job to succeed",
        Duration::from_secs(3),
        || async { row(database, id).await.state == "succeeded" },
    )
    .await;
    assert!(woken.elapsed() < Duration::from_secs(3));
    assert_eq!(row(database, id).await.attempts, 1);
    worker.stop().await;
}

async fn check_middleware_turns_an_error_into_a_snooze(database: &Database) {
    let queue = "outcomes_middleware";
    let worker = spawn_snoozer(queue).await;
    let id = enqueue(
        &scripted(
            "middleware",
            &[Step::Busy, Step::Busy, Step::Busy, Step::Succeed],
        ),
        queue,
        EnqueueOptions::new(),
    )
    .await;

    wait_for_state(database, id, "succeeded").await;
    assert_eq!(row(database, id).await.attempts, 1);
    assert_eq!(
        history(database, id).await,
        vec![
            snooze_row(0),
            snooze_row(0),
            snooze_row(0),
            succeeded_row(1)
        ]
    );
    worker.stop().await;
}

async fn check_snoozed_recurring_run_holds_its_schedule(database: &Database) {
    let worker = RunningWorker::spawn(
        Worker::builder()
            .queue("outcomes_recurring", 2)
            .config(worker_config(Duration::from_millis(50)))
            .retry_policy(fast_retry())
            .build()
            .await
            .unwrap(),
    );
    recurring::add_or_update(
        "outcomes_ticker",
        Cron::new("* * * * * *").unwrap(),
        &Ticker,
    )
    .await
    .unwrap();
    wait_until("the first tick to snooze", SETTLE, || async {
        runs("recurring")
            .first()
            .is_some_and(|run| run.end.is_some())
    })
    .await;

    let snoozed_at = Instant::now();
    while snoozed_at.elapsed() < Duration::from_millis(2000) {
        let (waiting, total): (i64, i64) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT count(*) FILTER (WHERE state IN ('scheduled', 'enqueued', 'running')),
                 count(*)
             FROM {} WHERE task = 'outcomes_ticker'",
            database.table("job")
        )))
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!((waiting, total), (1, 1), "one job row for the schedule");
        let info = recurring::list()
            .await
            .unwrap()
            .into_iter()
            .find(|info| info.name == "outcomes_ticker")
            .unwrap();
        assert_eq!(info.run_count, 1);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(runs("recurring").len(), 1);
    recurring::remove("outcomes_ticker").await.unwrap();
    worker.stop().await;
}
