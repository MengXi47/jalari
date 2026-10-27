mod common;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, fast_retry, wait_until, worker_config};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    CancelOutcome, EnqueueOptions, EnqueueOutcome, ExponentialBackoff, Job, JobError, JobId,
    JobResult, OnConflict, RestartOutcome, RetryPolicy, StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};

const LONG: Duration = Duration::from_secs(30);
const SETTLE: Duration = Duration::from_secs(20);
const SAVE_EVERY: Duration = Duration::from_millis(20);

static PROGRESS: LazyLock<Mutex<HashMap<String, Progress>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Default)]
struct Progress {
    resumed: Vec<Option<u64>>,
    saved: Vec<(u64, Instant)>,
    halted_at: Option<u64>,
}

fn progress(label: &str) -> Progress {
    PROGRESS
        .lock()
        .unwrap()
        .get(label)
        .cloned()
        .unwrap_or_default()
}

fn track(label: &str, update: impl FnOnce(&mut Progress)) {
    update(
        PROGRESS
            .lock()
            .unwrap()
            .entry(label.to_owned())
            .or_default(),
    );
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Counting {
    Forever,
    SavesThenFails(u64),
    ReportsOnly,
}

#[derive(Serialize, Deserialize, Default)]
struct Cursor {
    next: u64,
}

#[derive(Serialize, Deserialize)]
struct Counter {
    label: String,
    counting: Counting,
}

#[jalari::job(queue = "stop_default")]
impl Job for Counter {
    const NAME: &'static str = "stop_counter";
    const MAX_ATTEMPTS: i32 = 3;

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<Cursor>()?;
        track(&self.label, |progress| {
            progress
                .resumed
                .push(saved.as_ref().map(|cursor| cursor.next));
        });
        let mut cursor = saved.unwrap_or_default();
        match self.counting {
            Counting::ReportsOnly => Ok(()),
            Counting::SavesThenFails(next) if cursor.next == 0 => {
                checkpoint.save(&Cursor { next }).await?;
                Err(JobError::new("saved, then failed".to_owned()))
            }
            Counting::SavesThenFails(_) => Ok(()),
            Counting::Forever => loop {
                cursor.next += 1;
                track(&self.label, |progress| {
                    progress.saved.push((cursor.next, Instant::now()));
                });
                if let Err(e) = checkpoint.save(&cursor).await {
                    track(&self.label, |progress| {
                        progress.halted_at = Some(cursor.next)
                    });
                    return Err(e);
                }
                tokio::time::sleep(SAVE_EVERY).await;
            },
        }
    }
}

async fn count(label: &str, counting: Counting, queue: &str, options: EnqueueOptions) -> JobId {
    jalari::enqueue_with(
        &Counter {
            label: label.to_owned(),
            counting,
        },
        options.queue(queue),
    )
    .await
    .unwrap()
    .id()
}

async fn wait_for_resume(label: &str, count: usize) {
    wait_until(
        &format!("{label} to resume {count} times"),
        SETTLE,
        || async { progress(label).resumed.len() >= count },
    )
    .await;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Mode {
    Checkpoints,
    Sleeps,
    Quick(u64),
    FailsFirst,
}

#[derive(Serialize, Deserialize)]
struct Probe {
    label: String,
    mode: Mode,
}

struct Ended(String);

impl Drop for Ended {
    fn drop(&mut self) {
        LEDGER.record(&self.0, "dropped");
    }
}

#[jalari::job(queue = "stop_default")]
impl Job for Probe {
    const NAME: &'static str = "stop_probe";
    const MAX_ATTEMPTS: i32 = 3;

    async fn run(self) -> JobResult {
        let attempt = jalari::current_job().unwrap().attempt();
        LEDGER.record(&self.label, "start");
        LEDGER.record(&format!("{}:{attempt}", self.label), "start");
        let _ended = Ended(self.label.clone());
        let result = match self.mode {
            Mode::Checkpoints => checkpoint_forever().await,
            Mode::Sleeps => {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            }
            Mode::Quick(millis) => {
                tokio::time::sleep(Duration::from_millis(millis)).await;
                Ok(())
            }
            Mode::FailsFirst if attempt == 1 => Err(JobError::new("first attempt".to_owned())),
            Mode::FailsFirst => checkpoint_forever().await,
        };
        LEDGER.record(&self.label, if result.is_ok() { "ok" } else { "err" });
        result
    }
}

async fn checkpoint_forever() -> JobResult {
    loop {
        jalari::checkpoint().await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn spawn(queue: &str, concurrency: usize, stop_timeout: Duration) -> RunningWorker {
    spawn_with(queue, concurrency, stop_timeout, fast_retry()).await
}

async fn spawn_with<R: RetryPolicy>(
    queue: &str,
    concurrency: usize,
    stop_timeout: Duration,
    retry_policy: R,
) -> RunningWorker {
    let mut config = worker_config(Duration::from_millis(50));
    config.stop_timeout = stop_timeout;
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, concurrency)
            .config(config)
            .retry_policy(retry_policy)
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn enqueue(label: &str, mode: Mode, queue: &str, options: EnqueueOptions) -> EnqueueOutcome {
    jalari::enqueue_with(
        &Probe {
            label: label.to_owned(),
            mode,
        },
        options.queue(queue),
    )
    .await
    .unwrap()
}

async fn row(database: &Database, id: JobId) -> (String, i32) {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn history(database: &Database, id: JobId) -> Vec<(String, i32)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempt FROM {} WHERE job_id = $1 ORDER BY id",
        database.table("job_history")
    )))
    .bind(id.0)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn wait_for_state(database: &Database, id: JobId, state: &str) {
    wait_until(&format!("job {id} to be {state}"), SETTLE, || async {
        row(database, id).await.0 == state
    })
    .await;
}

async fn wait_for_start(label: &str, count: usize) {
    wait_until(
        &format!("{label} to start {count} times"),
        SETTLE,
        || async { LEDGER.count(label, "start") >= count },
    )
    .await;
}

async fn settle_stop(id: JobId) -> StopOutcome {
    let deadline = Instant::now() + SETTLE;
    loop {
        let outcome = jalari::stop(id).await.unwrap();
        if outcome != StopOutcome::Requested || Instant::now() >= deadline {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn settle_cancel(id: JobId) -> CancelOutcome {
    let deadline = Instant::now() + SETTLE;
    loop {
        let outcome = jalari::cancel(id).await.unwrap();
        if outcome != CancelOutcome::Requested || Instant::now() >= deadline {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stop_restart_and_cancel() {
    let Some(database) = Database::start("stop", 60).await else {
        return;
    };

    tokio::join!(
        check_stop_on_waiting_job_keeps_it_from_running_until_restart(&database),
        check_stop_on_running_job_ends_it_at_a_checkpoint(&database),
        check_stop_aborts_job_without_checkpoint_after_stop_timeout(&database),
        check_restart_keeps_the_attempt_count(&database),
        check_cancel_deletes_waiting_stopped_and_running_jobs(&database),
        check_ok_returned_while_stop_is_pending_wins(&database),
        check_stopped_job_holds_its_job_key(&database),
        check_stopped_job_releases_its_queue_key(&database),
        check_crashed_job_with_pending_stop_ends_stopped(&database),
        check_progress_survives_stop_and_restart(&database),
        check_progress_survives_a_retry(),
        check_progress_survives_a_crash_minus_a_few_seconds(),
        check_crash_on_the_last_attempt_fails_and_discards_progress(&database),
        check_shutdown_releases_the_job_without_counting_an_attempt(&database),
        check_replace_discards_saved_progress(&database),
    );
    check_flush_from_a_run_taken_over_by_replace_does_not_land(&database).await;

    database.finish().await;
}

#[tokio::test]
async fn test_resume_outside_a_job_returns_nothing_and_saving_does_nothing() {
    let (saved, checkpoint) = jalari::resume::<Cursor>().unwrap();
    assert!(saved.is_none());
    checkpoint.save(&Cursor { next: 1 }).await.unwrap();
    jalari::checkpoint().await.unwrap();
}

async fn check_progress_survives_stop_and_restart(database: &Database) {
    let worker = spawn("progress_restart", 1, LONG).await;
    let id = count(
        "restart_progress",
        Counting::Forever,
        "progress_restart",
        EnqueueOptions::new(),
    )
    .await;
    wait_for_resume("restart_progress", 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    let halted_at = progress("restart_progress")
        .halted_at
        .expect("the stop reached a checkpoint");
    assert_eq!(
        jalari::restart(id).await.unwrap(),
        RestartOutcome::Restarted
    );
    wait_for_resume("restart_progress", 2).await;
    assert_eq!(
        progress("restart_progress").resumed[..2],
        [None, Some(halted_at)],
        "the restarted run must continue from the checkpoint that stopped"
    );
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Requested);
    assert_eq!(settle_cancel(id).await, CancelOutcome::Finished);
    assert_eq!(row(database, id).await, ("deleted".to_owned(), 0));
    worker.stop().await;
}

async fn check_progress_survives_a_retry() {
    let worker = spawn("progress_retry", 1, LONG).await;
    count(
        "retry_progress",
        Counting::SavesThenFails(7),
        "progress_retry",
        EnqueueOptions::new(),
    )
    .await;
    wait_for_resume("retry_progress", 2).await;
    assert_eq!(progress("retry_progress").resumed, [None, Some(7)]);
    worker.stop().await;
}

async fn check_progress_survives_a_crash_minus_a_few_seconds() {
    let crashed = spawn("progress_crash", 1, LONG).await;
    count(
        "crash_progress",
        Counting::Forever,
        "progress_crash",
        EnqueueOptions::new(),
    )
    .await;
    wait_for_resume("crash_progress", 1).await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    crashed.crash().await;
    let crashed_at = Instant::now();

    let worker = spawn("progress_crash", 1, LONG).await;
    wait_for_resume("crash_progress", 2).await;
    let tracked = progress("crash_progress");
    let resumed = tracked.resumed[1].expect("a flush saved progress before the crash");
    let saved_at = tracked
        .saved
        .iter()
        .find(|(next, _)| *next == resumed)
        .map(|(_, at)| *at)
        .expect("the resumed progress was saved by the crashed run");
    assert!(
        crashed_at.duration_since(saved_at) <= Duration::from_secs(7),
        "the crash lost {:?} of progress",
        crashed_at.duration_since(saved_at)
    );
    worker.stop().await;
}

async fn check_crash_on_the_last_attempt_fails_and_discards_progress(database: &Database) {
    let id = count(
        "exhausted_progress",
        Counting::Forever,
        "progress_exhausted",
        EnqueueOptions::new(),
    )
    .await;
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET max_attempts = 1 WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .execute(&database.pool)
    .await
    .unwrap();
    let crashed = spawn("progress_exhausted", 1, LONG).await;
    wait_for_resume("exhausted_progress", 1).await;
    let stored = || async {
        sqlx::query_scalar::<_, bool>(AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE job_id = $1 AND progress IS NOT NULL)",
            database.table("job_control")
        )))
        .bind(id.0)
        .fetch_one(&database.pool)
        .await
        .unwrap()
    };
    wait_until("a flush to store progress", SETTLE, stored).await;
    crashed.crash().await;

    let worker = spawn("progress_exhausted", 1, LONG).await;
    wait_for_state(database, id, "failed").await;
    assert!(
        !stored().await,
        "a job failed by an interrupted last attempt still stores progress"
    );
    worker.stop().await;
}

async fn check_shutdown_releases_the_job_without_counting_an_attempt(database: &Database) {
    let first = spawn("progress_shutdown", 1, LONG).await;
    let id = count(
        "shutdown_progress",
        Counting::Forever,
        "progress_shutdown",
        EnqueueOptions::new(),
    )
    .await;
    wait_for_resume("shutdown_progress", 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    first.stop().await;

    assert_eq!(row(database, id).await, ("enqueued".to_owned(), 0));
    assert_eq!(
        history(database, id).await,
        [("enqueued".to_owned(), 0)],
        "a released attempt must not count"
    );
    let halted_at = progress("shutdown_progress")
        .halted_at
        .expect("the shutdown reached a checkpoint");

    let second = spawn("progress_shutdown", 1, LONG).await;
    wait_for_resume("shutdown_progress", 2).await;
    assert_eq!(progress("shutdown_progress").resumed[1], Some(halted_at));
    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    second.stop().await;
}

async fn check_replace_discards_saved_progress(database: &Database) {
    let hour = Duration::from_secs(3600);
    let worker = spawn_with(
        "progress_replace",
        1,
        LONG,
        ExponentialBackoff {
            base: hour,
            max: hour,
            jitter: 0.0,
        },
    )
    .await;
    let options = || EnqueueOptions::new().job_key("progress-replace");
    let id = count(
        "replace_old",
        Counting::SavesThenFails(5),
        "progress_replace",
        options(),
    )
    .await;
    wait_until("the first attempt to fail", SETTLE, || async {
        row(database, id).await == ("scheduled".to_owned(), 1)
    })
    .await;

    let replaced = jalari::enqueue_with(
        &Counter {
            label: "replace_new".to_owned(),
            counting: Counting::ReportsOnly,
        },
        options()
            .queue("progress_replace")
            .on_conflict(OnConflict::Replace),
    )
    .await
    .unwrap();
    assert_eq!(replaced, EnqueueOutcome::Replaced(id));
    wait_for_state(database, id, "succeeded").await;
    assert_eq!(progress("replace_new").resumed, [None]);
    worker.stop().await;
}

async fn check_flush_from_a_run_taken_over_by_replace_does_not_land(database: &Database) {
    let worker = spawn("progress_zombie", 1, LONG).await;
    let rescuer = spawn("progress_rescuer", 1, LONG).await;
    let options = || EnqueueOptions::new().job_key("progress-zombie");
    let id = count("zombie", Counting::Forever, "progress_zombie", options()).await;
    wait_for_resume("zombie", 1).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(database.terminate_running_lifelines().await, 1);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    worker.crash().await;
    let deadline = Instant::now() + Duration::from_secs(15);
    let replaced = loop {
        let outcome = jalari::enqueue_with(
            &Counter {
                label: "zombie_replacement".to_owned(),
                counting: Counting::ReportsOnly,
            },
            options()
                .queue("progress_zombie")
                .on_conflict(OnConflict::Replace)
                .delay(Duration::from_secs(12)),
        )
        .await
        .unwrap();
        if outcome != EnqueueOutcome::Running(id) || Instant::now() > deadline {
            break outcome;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(replaced, EnqueueOutcome::Replaced(id));
    let worker = spawn("progress_zombie", 1, LONG).await;
    wait_until(
        "the replacement to run",
        Duration::from_secs(30),
        || async { !progress("zombie_replacement").resumed.is_empty() },
    )
    .await;
    assert_eq!(
        progress("zombie_replacement").resumed,
        [None],
        "progress of the interrupted run landed after the replace"
    );
    assert!(
        progress("zombie").saved.len() > 1,
        "the interrupted run kept saving"
    );
    worker.stop().await;
    rescuer.stop().await;
}

async fn check_stop_on_waiting_job_keeps_it_from_running_until_restart(database: &Database) {
    let id = enqueue(
        "waiting",
        Mode::Quick(10),
        "stop_waiting",
        EnqueueOptions::new(),
    )
    .await
    .id();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Stopped);
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Stopped);
    assert_eq!(row(database, id).await, ("stopped".to_owned(), 0));

    let worker = spawn("stop_waiting", 1, LONG).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(LEDGER.count("waiting", "start"), 0, "a stopped job ran");

    assert_eq!(
        jalari::restart(id).await.unwrap(),
        RestartOutcome::Restarted
    );
    wait_for_state(database, id, "succeeded").await;
    assert_eq!(row(database, id).await, ("succeeded".to_owned(), 1));
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Finished);
    assert_eq!(
        jalari::restart(id).await.unwrap(),
        RestartOutcome::NotStopped
    );
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Finished);

    let missing = JobId(i64::MAX);
    assert_eq!(jalari::stop(missing).await.unwrap(), StopOutcome::NotFound);
    assert_eq!(
        jalari::restart(missing).await.unwrap(),
        RestartOutcome::NotFound
    );
    assert_eq!(
        jalari::cancel(missing).await.unwrap(),
        CancelOutcome::NotFound
    );
    worker.stop().await;
}

async fn check_stop_on_running_job_ends_it_at_a_checkpoint(database: &Database) {
    let worker = spawn("stop_running", 1, LONG).await;
    let id = enqueue(
        "running",
        Mode::Checkpoints,
        "stop_running",
        EnqueueOptions::new(),
    )
    .await
    .id();
    wait_for_start("running", 1).await;

    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    assert_eq!(row(database, id).await, ("stopped".to_owned(), 0));
    assert_eq!(
        LEDGER.count("running", "err"),
        1,
        "the checkpoint did not return an error"
    );
    assert_eq!(
        history(database, id).await,
        [("stopped".to_owned(), 0)],
        "a stopped run must not count as an attempt"
    );
    assert_eq!(
        jalari::restart(id).await.unwrap(),
        RestartOutcome::Restarted
    );
    wait_for_start("running", 2).await;
    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    worker.stop().await;
}

async fn check_stop_aborts_job_without_checkpoint_after_stop_timeout(database: &Database) {
    let worker = spawn("stop_abort", 1, Duration::from_millis(300)).await;
    let id = enqueue("abort", Mode::Sleeps, "stop_abort", EnqueueOptions::new())
        .await
        .id();
    wait_for_start("abort", 1).await;

    let requested = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    assert!(
        requested.elapsed() < Duration::from_secs(5),
        "the stop took {:?}",
        requested.elapsed()
    );
    wait_until("the aborted body to be dropped", SETTLE, || async {
        LEDGER.count("abort", "dropped") == 1
    })
    .await;
    assert_eq!(LEDGER.count("abort", "ok"), 0);
    assert_eq!(row(database, id).await, ("stopped".to_owned(), 0));
    worker.stop().await;
}

async fn check_restart_keeps_the_attempt_count(database: &Database) {
    let worker = spawn("stop_restart", 1, LONG).await;
    let id = enqueue(
        "restart",
        Mode::FailsFirst,
        "stop_restart",
        EnqueueOptions::new(),
    )
    .await
    .id();
    wait_for_start("restart:2", 1).await;

    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    assert_eq!(row(database, id).await, ("stopped".to_owned(), 1));
    assert_eq!(
        jalari::restart(id).await.unwrap(),
        RestartOutcome::Restarted
    );
    assert_eq!(
        jalari::restart(id).await.unwrap(),
        RestartOutcome::NotStopped
    );
    wait_for_start("restart:2", 2).await;
    assert_eq!(LEDGER.count("restart:3", "start"), 0);

    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    assert_eq!(row(database, id).await, ("stopped".to_owned(), 1));
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Cancelled);
    assert_eq!(row(database, id).await, ("deleted".to_owned(), 1));
    worker.stop().await;
}

async fn check_cancel_deletes_waiting_stopped_and_running_jobs(database: &Database) {
    let options = || EnqueueOptions::new().job_key("cancel-waiting");
    let waiting = enqueue("cancel_waiting", Mode::Quick(10), "cancel_idle", options())
        .await
        .id();
    assert_eq!(
        jalari::cancel(waiting).await.unwrap(),
        CancelOutcome::Cancelled
    );
    assert_eq!(row(database, waiting).await, ("deleted".to_owned(), 0));
    let again = enqueue("cancel_waiting", Mode::Quick(10), "cancel_idle", options()).await;
    assert!(
        matches!(again, EnqueueOutcome::Inserted(id) if id != waiting),
        "a cancelled job kept its job_key: {again:?}"
    );

    let stopped = enqueue(
        "cancel_stopped",
        Mode::Quick(10),
        "cancel_idle",
        EnqueueOptions::new(),
    )
    .await
    .id();
    assert_eq!(jalari::stop(stopped).await.unwrap(), StopOutcome::Stopped);
    assert_eq!(
        jalari::cancel(stopped).await.unwrap(),
        CancelOutcome::Cancelled
    );
    assert_eq!(row(database, stopped).await, ("deleted".to_owned(), 0));
    assert_eq!(
        jalari::restart(stopped).await.unwrap(),
        RestartOutcome::NotStopped
    );

    let worker = spawn("cancel_running", 1, LONG).await;
    let running = enqueue(
        "cancel_running",
        Mode::Checkpoints,
        "cancel_running",
        EnqueueOptions::new(),
    )
    .await
    .id();
    wait_for_start("cancel_running", 1).await;
    assert_eq!(
        jalari::cancel(running).await.unwrap(),
        CancelOutcome::Requested
    );
    assert_eq!(settle_cancel(running).await, CancelOutcome::Finished);
    assert_eq!(row(database, running).await, ("deleted".to_owned(), 0));
    assert_eq!(LEDGER.count("cancel_running", "err"), 1);
    worker.stop().await;
}

async fn check_ok_returned_while_stop_is_pending_wins(database: &Database) {
    let worker = spawn("stop_ok", 1, LONG).await;
    let id = enqueue("ok", Mode::Quick(600), "stop_ok", EnqueueOptions::new())
        .await
        .id();
    wait_for_start("ok", 1).await;

    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(database, id, "succeeded").await;
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Finished);
    assert_eq!(row(database, id).await, ("succeeded".to_owned(), 1));
    worker.stop().await;
}

async fn check_stopped_job_holds_its_job_key(database: &Database) {
    let options = || EnqueueOptions::new().job_key("stop-held");
    let id = enqueue("held", Mode::Quick(10), "stop_held", options())
        .await
        .id();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Stopped);

    let kept = enqueue("held", Mode::Quick(10), "stop_held", options()).await;
    assert_eq!(kept, EnqueueOutcome::Kept(id));
    let replace = options().on_conflict(OnConflict::Replace);
    let replaced = enqueue("held", Mode::Quick(10), "stop_held", replace).await;
    assert_eq!(replaced, EnqueueOutcome::Kept(id));
    assert_eq!(row(database, id).await, ("stopped".to_owned(), 0));
}

async fn check_stopped_job_releases_its_queue_key(database: &Database) {
    let worker = spawn("stop_siblings", 2, LONG).await;
    let options = || EnqueueOptions::new().queue_key("stop-shared");
    let first = enqueue(
        "sibling_first",
        Mode::Checkpoints,
        "stop_siblings",
        options(),
    )
    .await
    .id();
    wait_for_start("sibling_first", 1).await;
    let second = enqueue(
        "sibling_second",
        Mode::Quick(10),
        "stop_siblings",
        options(),
    )
    .await
    .id();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        LEDGER.count("sibling_second", "start"),
        0,
        "the queue_key did not keep the sibling waiting"
    );

    assert_eq!(settle_stop(first).await, StopOutcome::Stopped);
    wait_for_state(database, second, "succeeded").await;
    assert_eq!(row(database, first).await, ("stopped".to_owned(), 0));
    worker.stop().await;
}

async fn check_crashed_job_with_pending_stop_ends_stopped(database: &Database) {
    let crashed = spawn("stop_crash", 1, LONG).await;
    let id = enqueue("crash", Mode::Sleeps, "stop_crash", EnqueueOptions::new())
        .await
        .id();
    wait_for_start("crash", 1).await;
    crashed.crash().await;

    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    let requested = Instant::now();
    let worker = spawn("stop_crash", 1, LONG).await;
    assert_eq!(settle_stop(id).await, StopOutcome::Stopped);
    assert!(
        requested.elapsed() >= Duration::from_secs(5),
        "the crashed job was stopped {:?} after the request, inside the takeover delay",
        requested.elapsed()
    );
    assert_eq!(
        LEDGER.count("crash", "start"),
        1,
        "the crashed job ran again"
    );
    assert_eq!(row(database, id).await, ("stopped".to_owned(), 0));
    assert_eq!(
        history(database, id).await.last(),
        Some(&("stopped".to_owned(), 0))
    );
    worker.stop().await;
}
