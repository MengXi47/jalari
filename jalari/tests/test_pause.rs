mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use common::{Database, RunningWorker, wait_until, worker_config};
use jalari::admin::{self, PausedQueue};
use jalari::{EnqueueOptions, Job, JobId, JobResult, JobState, Worker};
use serde::{Deserialize, Serialize};

const POLL_INTERVAL: Duration = Duration::from_millis(200);
const NOTIFIED_ONLY: Duration = Duration::from_secs(60);
const SETTLE: Duration = Duration::from_secs(1);
const RESUME_WITHIN: Duration = Duration::from_secs(1);
const WAIT: Duration = Duration::from_secs(15);

static RELEASE: AtomicBool = AtomicBool::new(false);

#[derive(Serialize, Deserialize)]
enum Pausable {
    Quick,
    Hold,
}

#[jalari::job(queue = "pause_default")]
impl Job for Pausable {
    const NAME: &'static str = "pausable";

    async fn run(self) -> JobResult {
        if let Self::Hold = self {
            while !RELEASE.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(())
    }
}

async fn enqueue(job: Pausable, queue: &str) -> JobId {
    jalari::enqueue_with(&job, EnqueueOptions::new().queue(queue))
        .await
        .unwrap()
        .id()
}

async fn state(id: JobId) -> JobState {
    admin::job(id).await.unwrap().unwrap().job.state
}

async fn wait_state(id: JobId, expected: JobState, timeout: Duration) {
    wait_until(
        &format!("job {id:?} to be {expected:?}"),
        timeout,
        || async move { state(id).await == expected },
    )
    .await;
}

async fn start_worker(queue: &str, concurrency: usize, poll_interval: Duration) -> RunningWorker {
    let worker = Worker::builder()
        .queue(queue, concurrency)
        .config(worker_config(poll_interval))
        .build()
        .await
        .unwrap();
    RunningWorker::spawn(worker)
}

async fn assert_waits(id: JobId) {
    tokio::time::sleep(SETTLE).await;
    assert_eq!(state(id).await, JobState::Enqueued, "{id:?} still waits");
}

fn names(paused: &[PausedQueue]) -> Vec<&str> {
    paused.iter().map(|paused| paused.queue.as_str()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pause() {
    let Some(database) = Database::start("pause", 8).await else {
        return;
    };

    check_pause_and_resume_report_changes().await;
    check_paused_queue_claims_nothing().await;
    check_running_job_finishes_while_paused().await;
    check_pause_survives_worker_restart().await;

    database.finish().await;
}

async fn check_pause_and_resume_report_changes() {
    let before = Utc::now();
    assert!(admin::pause_queue("pause_b").await.unwrap());
    assert!(!admin::pause_queue("pause_b").await.unwrap());
    assert!(admin::pause_queue("pause_a").await.unwrap());

    let paused = admin::paused_queues().await.unwrap();
    assert_eq!(names(&paused), ["pause_a", "pause_b"]);
    assert!(paused[1].paused_at <= paused[0].paused_at);
    assert!(paused.iter().all(|paused| paused.paused_at >= before));
    assert!(paused.iter().all(|paused| paused.paused_at <= Utc::now()));

    assert!(admin::resume_queue("pause_a").await.unwrap());
    assert!(!admin::resume_queue("pause_a").await.unwrap());
    assert!(!admin::resume_queue("pause_never").await.unwrap());
    assert_eq!(names(&admin::paused_queues().await.unwrap()), ["pause_b"]);

    assert!(admin::resume_queue("pause_b").await.unwrap());
    assert!(admin::paused_queues().await.unwrap().is_empty());
}

async fn check_paused_queue_claims_nothing() {
    let queue = "pause_idle";
    assert!(admin::pause_queue(queue).await.unwrap());
    let job = enqueue(Pausable::Quick, queue).await;
    let worker = start_worker(queue, 2, POLL_INTERVAL).await;

    assert_waits(job).await;

    assert!(admin::resume_queue(queue).await.unwrap());
    wait_state(job, JobState::Succeeded, RESUME_WITHIN).await;
    worker.stop().await;
}

async fn check_running_job_finishes_while_paused() {
    let queue = "pause_running";
    let worker = start_worker(queue, 1, NOTIFIED_ONLY).await;
    let held = enqueue(Pausable::Hold, queue).await;
    wait_state(held, JobState::Running, WAIT).await;

    assert!(admin::pause_queue(queue).await.unwrap());
    tokio::time::sleep(SETTLE).await;
    let waiting = enqueue(Pausable::Quick, queue).await;
    RELEASE.store(true, Ordering::SeqCst);
    wait_state(held, JobState::Succeeded, WAIT).await;
    assert_waits(waiting).await;

    assert!(admin::resume_queue(queue).await.unwrap());
    wait_state(waiting, JobState::Succeeded, RESUME_WITHIN).await;
    worker.stop().await;
}

async fn check_pause_survives_worker_restart() {
    let queue = "pause_restart";
    let first = start_worker(queue, 2, POLL_INTERVAL).await;
    assert!(admin::pause_queue(queue).await.unwrap());
    tokio::time::sleep(SETTLE).await;
    let job = enqueue(Pausable::Quick, queue).await;
    assert_waits(job).await;
    first.stop().await;

    let second = start_worker(queue, 2, POLL_INTERVAL).await;
    assert_waits(job).await;

    assert!(admin::resume_queue(queue).await.unwrap());
    wait_state(job, JobState::Succeeded, RESUME_WITHIN).await;
    second.stop().await;
}
