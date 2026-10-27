mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{Database, RunningWorker, wait_until, worker_config};
use jalari::admin::{self, JobFilter, JobInfo};
use jalari::{
    EnqueueOptions, Job, JobError, JobId, JobResult, JobState, Priority, StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

const WAIT: Duration = Duration::from_secs(15);

static RELEASE: AtomicBool = AtomicBool::new(false);

#[derive(Serialize, Deserialize)]
enum Bump {
    Block,
    Succeed,
    Fail,
}

#[jalari::job(queue = "priority")]
impl Job for Bump {
    const NAME: &'static str = "set_priority_bump";

    async fn run(self) -> JobResult {
        match self {
            Self::Block => {
                while !RELEASE.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(())
            }
            Self::Succeed => Ok(()),
            Self::Fail => Err(JobError::permanent(std::io::Error::other("fails"))),
        }
    }
}

async fn enqueue(job: &Bump, options: EnqueueOptions) -> JobId {
    jalari::enqueue_with(job, options).await.unwrap().id()
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

fn tenant(name: &str) -> EnqueueOptions {
    EnqueueOptions::new().context(&json!({ "tenant_id": name }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_priority() {
    let Some(database) = Database::start("priority", 8).await else {
        return;
    };
    let worker = Worker::builder()
        .queue("priority", 4)
        .config(worker_config(Duration::from_millis(50)))
        .build()
        .await
        .unwrap();
    let worker = RunningWorker::spawn(worker);

    check_set_priority_changes_only_waiting_and_stopped_jobs().await;
    check_set_priority_spans_batches().await;

    worker.stop().await;
    database.finish().await;
}

async fn check_set_priority_changes_only_waiting_and_stopped_jobs() {
    let later = || tenant("acme").delay(Duration::from_secs(3600));
    let scheduled = enqueue(&Bump::Succeed, later()).await;
    let enqueued = enqueue(&Bump::Succeed, tenant("acme").queue("unserved")).await;
    let stopped = enqueue(&Bump::Succeed, later()).await;
    assert_eq!(jalari::stop(stopped).await.unwrap(), StopOutcome::Stopped);
    let running = enqueue(&Bump::Block, tenant("acme")).await;
    let succeeded = enqueue(&Bump::Succeed, tenant("acme")).await;
    let failed = enqueue(&Bump::Fail, tenant("acme")).await;
    let other_tenant = enqueue(&Bump::Succeed, tenant("globex").queue("unserved")).await;
    wait_state(running, JobState::Running).await;
    wait_state(succeeded, JobState::Succeeded).await;
    wait_state(failed, JobState::Failed).await;

    let waiting = [scheduled, enqueued, stopped];
    let settled = [running, succeeded, failed, other_tenant];
    let mut updated_at: Vec<DateTime<Utc>> = Vec::new();
    for id in waiting {
        updated_at.push(info(id).await.updated_at);
    }

    let acme = JobFilter::new().context(json!({ "tenant_id": "acme" }));
    assert_eq!(admin::set_priority(&acme, Priority::High).await.unwrap(), 3);
    for (id, before) in waiting.into_iter().zip(&updated_at) {
        let job = info(id).await;
        assert_eq!(job.priority, Priority::High, "{id:?} changed");
        assert_eq!(job.updated_at, *before, "{id:?} keeps updated_at");
    }
    for id in settled {
        assert_eq!(info(id).await.priority, Priority::Normal, "{id:?} kept");
    }

    assert_eq!(admin::set_priority(&acme, Priority::High).await.unwrap(), 0);

    let only_stopped = acme.clone().state(JobState::Stopped);
    assert_eq!(
        admin::set_priority(&only_stopped, Priority::Low)
            .await
            .unwrap(),
        1
    );
    assert_eq!(info(stopped).await.priority, Priority::Low);
    assert_eq!(info(scheduled).await.priority, Priority::High);
    assert_eq!(info(enqueued).await.priority, Priority::High);

    RELEASE.store(true, Ordering::SeqCst);
    wait_state(running, JobState::Succeeded).await;
    assert_eq!(info(running).await.priority, Priority::Normal);
}

async fn check_set_priority_spans_batches() {
    let mut ids = Vec::new();
    for _ in 0..250 {
        ids.push(enqueue(&Bump::Succeed, tenant("bulk").queue("unserved")).await);
    }
    let bulk = JobFilter::new().context(json!({ "tenant_id": "bulk" }));
    assert_eq!(
        admin::set_priority(&bulk, Priority::Urgent).await.unwrap(),
        250
    );
    let urgent = bulk.clone().priority(Priority::Urgent);
    assert_eq!(admin::count(&urgent).await.unwrap(), 250);
    assert_eq!(info(ids[0]).await.priority, Priority::Urgent);
    assert_eq!(info(ids[249]).await.priority, Priority::Urgent);
}
