mod common;

use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, fast_retry, wait_until, worker_config};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    CancelOutcome, EnqueueOptions, EnqueueOutcome, Job, JobError, JobId, JobResult, OnConflict,
    RestartOutcome, StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};

const LONG: Duration = Duration::from_secs(30);
const SETTLE: Duration = Duration::from_secs(20);

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
    let mut config = worker_config(Duration::from_millis(50));
    config.stop_timeout = stop_timeout;
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, concurrency)
            .config(config)
            .retry_policy(fast_retry())
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
    );

    database.finish().await;
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
