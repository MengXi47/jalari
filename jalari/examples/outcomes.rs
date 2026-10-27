use std::io;
use std::time::{Duration, Instant};

use jalari::admin::JobFilter;
use jalari::sqlx::types::chrono::{DateTime, Utc};
use jalari::{
    EnqueueOptions, Finished, Job, JobError, JobMeta, JobMiddleware, JobResult, JobState, Next,
};
use serde::{Deserialize, Serialize};

const QUEUE: &str = "outcomes";

struct Report;

impl JobMiddleware for Report {
    type Provides = ();

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        let last = if job.is_last_attempt() {
            ", the last one"
        } else {
            ""
        };
        println!(
            "  -> {} #{} attempt {}/{}{last}",
            job.name(),
            job.id(),
            job.attempt(),
            job.max_attempts()
        );
        next.run().await
    }

    async fn finished(&self, job: &JobMeta, outcome: &Finished) {
        println!("  <- {} #{}: {outcome:?}", job.name(), job.id());
    }
}

#[derive(Serialize, Deserialize)]
struct ChargeCard {
    order: u32,
}

#[jalari::job(name = "charge_card", queue = "outcomes", max_attempts = 3)]
impl Job for ChargeCard {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        if run.attempt() == 1 {
            println!("     the payment API answers 429 and asks to come back in 1 s");
            return Err(JobError::new("rate limited by the payment API".to_owned())
                .retry_after(Duration::from_secs(1)));
        }
        println!(
            "     charged order {} on attempt {}; the previous attempt left {:?}",
            self.order,
            run.attempt(),
            run.last_error()
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct SyncLedger;

#[jalari::job(name = "sync_ledger", queue = "outcomes", max_attempts = 3)]
impl Job for SyncLedger {
    const RETRY_POLICY: Option<&'static dyn jalari::RetryPolicy> =
        Some(&jalari::ExponentialBackoff {
            base: Duration::from_millis(500),
            max: Duration::from_secs(1),
            jitter: 0.0,
        });

    async fn run(self) -> JobResult {
        Err(io::Error::other("the ledger service is down").into())
    }
}

#[derive(Serialize, Deserialize)]
struct DownloadExport {
    export: u32,
}

#[jalari::job(name = "download_export", queue = "outcomes")]
impl Job for DownloadExport {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        let age = (Utc::now() - run.created_at()).num_milliseconds();
        if age < 2000 {
            println!(
                "     export {} is not ready {age} ms after the enqueue, snooze",
                self.export
            );
            return jalari::snooze(Duration::from_millis(700));
        }
        println!(
            "     downloaded export {} on attempt {}, {age} ms after the enqueue",
            self.export,
            run.attempt()
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ShipOrder {
    order: u32,
    cancelled: bool,
}

#[jalari::job(name = "ship_order", queue = "outcomes")]
impl Job for ShipOrder {
    async fn run(self) -> JobResult {
        if self.cancelled {
            return Err(JobError::discard(io::Error::other(format!(
                "order {} was cancelled, nothing to ship",
                self.order
            ))));
        }
        println!("     shipped order {}", self.order);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct SendOneTimeCode {
    user_id: i64,
}

#[jalari::job(name = "send_one_time_code", queue = "outcomes", priority = "high")]
impl Job for SendOneTimeCode {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        let waited = Utc::now() - run.scheduled_at();
        println!(
            "     sent a code to user {} at priority {:?}, {} ms after it became due",
            self.user_id,
            run.priority(),
            waited.num_milliseconds()
        );
        Ok(())
    }
}

async fn wait_until_done(since: DateTime<Utc>) -> Result<(), Box<dyn std::error::Error>> {
    let unfinished = JobFilter::new()
        .queue(QUEUE)
        .created_after(since)
        .state(JobState::Scheduled)
        .state(JobState::Enqueued)
        .state(JobState::Running);
    for _ in 0..300 {
        if jalari::admin::count(&unfinished).await? == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("jobs are still waiting".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;

    let started = Instant::now();
    let since: DateTime<Utc> = jalari::sqlx::query_scalar("SELECT now()")
        .fetch_one(&pool)
        .await?;

    println!("charge_card fails once with retry_after(1 s), then succeeds");
    jalari::enqueue(&ChargeCard { order: 1001 }).await?;
    println!("sync_ledger always fails; its RETRY_POLICY retries after 0.5 s and 1 s");
    jalari::enqueue(&SyncLedger).await?;
    println!("download_export snoozes until its export is ready, on the same attempt");
    jalari::enqueue(&DownloadExport { export: 7 }).await?;
    println!("ship_order discards a cancelled order instead of failing");
    jalari::enqueue(&ShipOrder {
        order: 1002,
        cancelled: true,
    })
    .await?;
    println!("send_one_time_code runs at once and expires in 10 minutes");
    jalari::enqueue_with(
        &SendOneTimeCode { user_id: 7 },
        EnqueueOptions::new().expires_at(Utc::now() + Duration::from_secs(600)),
    )
    .await?;
    let expired = jalari::enqueue_with(
        &SendOneTimeCode { user_id: 8 },
        EnqueueOptions::new()
            .delay(Duration::from_secs(3))
            .expire_after(Duration::from_secs(1)),
    )
    .await?
    .id();
    println!("send_one_time_code #{expired} is due in 3 s but expires after 1 s, so it never runs");

    println!("\nstarting a worker:");
    let mut config = jalari::WorkerConfig::default();
    config.poll_interval = Duration::from_millis(500);
    let shutdown = jalari::CancellationToken::new();
    let worker = jalari::Worker::builder()
        .middleware(Report)
        .queue(QUEUE, 4)
        .scheduler(false)
        .config(config)
        .build()
        .await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    wait_until_done(since).await?;
    shutdown.cancel();
    worker_task.await?;

    println!("\nresults:");
    let filter = JobFilter::new().queue(QUEUE).created_after(since);
    for job in jalari::admin::jobs(&filter, 100, None)
        .await?
        .into_iter()
        .rev()
    {
        println!(
            "  {} #{}: {}, {} attempt(s){}",
            job.task,
            job.id,
            job.state,
            job.attempts,
            job.last_error
                .map(|error| format!(", last_error {error:?}"))
                .unwrap_or_default()
        );
    }

    let window = started.elapsed() + Duration::from_secs(1);
    for queue in jalari::admin::throughput(window).await? {
        if queue.queue == QUEUE {
            println!(
                "throughput of {QUEUE}: {} succeeded, {} failed, {} retried, {} snoozed, {} discarded",
                queue.succeeded, queue.failed, queue.retried, queue.snoozed, queue.discarded
            );
        }
    }
    Ok(())
}
