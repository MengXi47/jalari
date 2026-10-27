mod common;

use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use common::{Database, RunningWorker, fast_retry, wait_until, worker_config};
use jalari::admin::{self, RetryOutcome};
use jalari::{
    EnqueueOptions, EnqueueOutcome, Job, JobError, JobId, JobMeta, JobMiddleware, JobResult,
    JobRun, JobState, Next, OnConflict, Priority, RestartOutcome, StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    label: String,
    job_id: JobId,
    attempt: i32,
    created_at: DateTime<Utc>,
    scheduled_at: DateTime<Utc>,
    priority: Priority,
    last_error: Option<String>,
    parent_id: Option<JobId>,
    root_id: JobId,
}

static IN_JOB: LazyLock<Mutex<Vec<Seen>>> = LazyLock::new(|| Mutex::new(Vec::new()));
static IN_MIDDLEWARE: LazyLock<Mutex<Vec<Seen>>> = LazyLock::new(|| Mutex::new(Vec::new()));

fn seen_in_job(label: &str) -> Vec<Seen> {
    IN_JOB
        .lock()
        .unwrap()
        .iter()
        .filter(|seen| seen.label == label)
        .cloned()
        .collect()
}

#[derive(Serialize, Deserialize, Default)]
struct Probe {
    label: String,
    fail_first: bool,
    always_fail: bool,
    children: u32,
    spawned_child: bool,
    delayed_child: bool,
    permanent_child: bool,
    replace_key: Option<String>,
}

fn probe(label: &str) -> Probe {
    Probe {
        label: label.to_owned(),
        ..Probe::default()
    }
}

fn record(run: &JobRun, label: &str) -> Seen {
    Seen {
        label: label.to_owned(),
        job_id: run.id(),
        attempt: run.attempt(),
        created_at: run.created_at(),
        scheduled_at: run.scheduled_at(),
        priority: run.priority(),
        last_error: run.last_error().map(str::to_owned),
        parent_id: run.parent_id(),
        root_id: run.root_id(),
    }
}

#[jalari::job(queue = "job_run")]
impl Job for Probe {
    const NAME: &'static str = "job_run_probe";
    const MAX_ATTEMPTS: i32 = 3;
    const PRIORITY: Priority = Priority::High;

    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        IN_JOB.lock().unwrap().push(record(&run, &self.label));
        let failure = format!("{} failed on attempt {}", self.label, run.attempt());
        if self.always_fail {
            return Err(JobError::permanent(std::io::Error::other(failure)));
        }
        if self.fail_first && run.attempt() == 1 {
            return Err(JobError::new(failure));
        }
        if self.children > 0 {
            let child = Probe {
                label: format!("{}/child", self.label),
                children: self.children - 1,
                ..Probe::default()
            };
            jalari::enqueue(&child).await?;
        }
        if self.spawned_child {
            let child = probe(&format!("{}/spawned", self.label));
            tokio::spawn(async move { jalari::enqueue(&child).await })
                .await
                .map_err(|e| JobError::new(e.to_string()))??;
        }
        if self.delayed_child {
            let child = probe(&format!("{}/delayed", self.label));
            let options = EnqueueOptions::new().delay(Duration::from_secs(3600));
            jalari::enqueue_with(&child, options).await?;
        }
        if self.permanent_child {
            let child = Probe {
                label: format!("{}/doomed", self.label),
                always_fail: true,
                ..Probe::default()
            };
            jalari::enqueue(&child).await?;
        }
        if let Some(key) = &self.replace_key {
            let options = EnqueueOptions::new()
                .job_key(key)
                .on_conflict(OnConflict::Replace)
                .delay(Duration::from_secs(3600));
            jalari::enqueue_with(&probe("replaced by a job"), options).await?;
        }
        Ok(())
    }
}

struct Recorder;

impl JobMiddleware for Recorder {
    type Provides = ();

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        IN_MIDDLEWARE.lock().unwrap().push(Seen {
            label: String::new(),
            job_id: job.id(),
            attempt: job.attempt(),
            created_at: job.created_at(),
            scheduled_at: job.scheduled_at(),
            priority: job.priority(),
            last_error: job.last_error().map(str::to_owned),
            parent_id: job.parent_id(),
            root_id: job.root_id(),
        });
        next.run().await
    }
}

async fn wait_seen(label: &str, runs: usize) -> Vec<Seen> {
    wait_until(&format!("{runs} run(s) of {label}"), WAIT, || async {
        seen_in_job(label).len() >= runs
    })
    .await;
    seen_in_job(label)
}

async fn wait_state(id: JobId, state: JobState) {
    wait_until(&format!("job {id} to be {state}"), WAIT, || async {
        info(id).await.state == state
    })
    .await;
}

async fn info(id: JobId) -> admin::JobInfo {
    admin::job(id).await.unwrap().unwrap().job
}

async fn lineage(id: JobId) -> (Option<JobId>, Option<JobId>) {
    let job = info(id).await;
    (job.parent_id, job.root_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_run() {
    let Some(database) = Database::start("job_run", 6).await else {
        return;
    };
    let worker = RunningWorker::spawn(
        Worker::builder()
            .queue("job_run", 4)
            .middleware(Recorder)
            .config(worker_config(Duration::from_millis(100)))
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    );

    check_run_metadata_matches_the_row().await;
    check_retry_sees_the_previous_error_and_backoff_time().await;
    check_priority_reaches_job_and_middleware();
    check_child_records_parent_and_root().await;
    check_spawned_task_enqueue_has_no_parent().await;
    check_replace_takes_the_replacer_lineage().await;
    check_retry_and_restart_keep_lineage().await;

    worker.stop().await;
    database.finish().await;
}

async fn check_run_metadata_matches_the_row() {
    let run_at = DateTime::from_timestamp_micros(
        (Utc::now() + TimeDelta::milliseconds(700)).timestamp_micros(),
    )
    .unwrap();
    let id = jalari::enqueue_with(&probe("meta"), EnqueueOptions::new().run_at(run_at))
        .await
        .unwrap()
        .id();
    let seen = wait_seen("meta", 1).await.remove(0);
    let row = info(id).await;
    assert_eq!(seen.job_id, id);
    assert_eq!(seen.attempt, 1);
    assert_eq!(seen.scheduled_at, run_at);
    assert_eq!(seen.created_at, row.created_at);
    assert!(seen.created_at < seen.scheduled_at, "{seen:?}");
    assert_eq!(seen.priority, Priority::High);
    assert_eq!(row.priority, Priority::High);
    assert_eq!(seen.last_error, None);
    assert_eq!(seen.parent_id, None);
    assert_eq!(seen.root_id, id);
}

async fn check_retry_sees_the_previous_error_and_backoff_time() {
    let retried = Probe {
        fail_first: true,
        ..probe("retry")
    };
    let id = jalari::enqueue(&retried).await.unwrap().id();
    wait_state(id, JobState::Succeeded).await;
    let runs = seen_in_job("retry");
    assert_eq!(runs.len(), 2, "{runs:?}");
    let row = info(id).await;
    assert_eq!((runs[0].attempt, runs[1].attempt), (1, 2));
    assert_eq!(runs[0].last_error, None);
    assert_eq!(
        runs[1].last_error.as_deref(),
        Some("retry failed on attempt 1")
    );
    assert!(runs[1].scheduled_at > runs[0].scheduled_at, "{runs:?}");
    assert_eq!(runs[1].scheduled_at, row.run_at);
    assert_eq!(runs[1].created_at, runs[0].created_at);
}

fn check_priority_reaches_job_and_middleware() {
    let in_job = IN_JOB.lock().unwrap().clone();
    let in_middleware = IN_MIDDLEWARE.lock().unwrap().clone();
    assert!(!in_job.is_empty());
    for seen in &in_job {
        let matching = in_middleware
            .iter()
            .find(|meta| meta.job_id == seen.job_id && meta.attempt == seen.attempt)
            .unwrap_or_else(|| panic!("no middleware record for {seen:?}"));
        assert_eq!(
            Seen {
                label: seen.label.clone(),
                ..matching.clone()
            },
            *seen
        );
    }
}

async fn check_child_records_parent_and_root() {
    let tree = Probe {
        children: 2,
        ..probe("tree")
    };
    let a = jalari::enqueue(&tree).await.unwrap().id();
    let c = wait_seen("tree/child/child", 1).await.remove(0);
    let b = seen_in_job("tree/child").remove(0);
    let inside_a = seen_in_job("tree").remove(0);
    assert_eq!((inside_a.parent_id, inside_a.root_id), (None, a));
    assert_eq!((b.parent_id, b.root_id), (Some(a), a));
    assert_eq!((c.parent_id, c.root_id), (Some(b.job_id), a));
    assert_eq!(lineage(a).await, (None, None));
    assert_eq!(lineage(b.job_id).await, (Some(a), Some(a)));
    assert_eq!(lineage(c.job_id).await, (Some(b.job_id), Some(a)));
}

async fn check_spawned_task_enqueue_has_no_parent() {
    let spawner = Probe {
        spawned_child: true,
        ..probe("spawner")
    };
    let parent = jalari::enqueue(&spawner).await.unwrap().id();
    let spawned = wait_seen("spawner/spawned", 1).await.remove(0);
    assert_ne!(spawned.job_id, parent);
    assert_eq!((spawned.parent_id, spawned.root_id), (None, spawned.job_id));
    assert_eq!(lineage(spawned.job_id).await, (None, None));
}

async fn check_replace_takes_the_replacer_lineage() {
    let key = "job-run-lineage";
    let waiting = |label: &str| {
        jalari::enqueue_with(
            &probe(label),
            EnqueueOptions::new()
                .job_key(key)
                .on_conflict(OnConflict::Replace)
                .delay(Duration::from_secs(3600)),
        )
    };
    let id = waiting("top level").await.unwrap().id();
    assert_eq!(lineage(id).await, (None, None));

    let replacer = Probe {
        replace_key: Some(key.to_owned()),
        ..probe("replacer")
    };
    let parent = jalari::enqueue(&replacer).await.unwrap().id();
    wait_state(parent, JobState::Succeeded).await;
    assert_eq!(lineage(id).await, (Some(parent), Some(parent)));

    assert_eq!(
        waiting("top level again").await.unwrap(),
        EnqueueOutcome::Replaced(id)
    );
    assert_eq!(lineage(id).await, (None, None));
}

async fn check_retry_and_restart_keep_lineage() {
    let parent = Probe {
        permanent_child: true,
        delayed_child: true,
        ..probe("keeper")
    };
    let parent = jalari::enqueue(&parent).await.unwrap().id();
    let doomed = wait_seen("keeper/doomed", 1).await.remove(0).job_id;
    wait_state(doomed, JobState::Failed).await;
    assert_eq!(lineage(doomed).await, (Some(parent), Some(parent)));
    assert_eq!(admin::retry(doomed).await.unwrap(), RetryOutcome::Retried);
    wait_seen("keeper/doomed", 2).await;
    wait_state(doomed, JobState::Failed).await;
    assert_eq!(lineage(doomed).await, (Some(parent), Some(parent)));

    let delayed = admin::jobs(&admin::JobFilter::new().parent(parent), 10, None)
        .await
        .unwrap()
        .into_iter()
        .find(|job| job.id != doomed)
        .expect("the delayed child")
        .id;
    assert_eq!(jalari::stop(delayed).await.unwrap(), StopOutcome::Stopped);
    assert_eq!(
        jalari::restart(delayed).await.unwrap(),
        RestartOutcome::Restarted
    );
    assert_eq!(lineage(delayed).await, (Some(parent), Some(parent)));
}
