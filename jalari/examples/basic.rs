use std::io;
use std::time::Duration;

use jalari::sqlx::types::chrono;
use jalari::{EnqueueOptions, Job, JobResult, OnConflict};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

#[jalari::job(queue = "emails")]
impl Job for SendEmail {
    const NAME: &'static str = "send_email";
    const MAX_ATTEMPTS: i32 = 5;
    const TIMEOUT: Option<Duration> = Some(Duration::from_secs(30));

    async fn run(self) -> JobResult {
        if !self.to.contains('@') {
            return Err(jalari::JobError::permanent(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid address {:?}", self.to),
            )));
        }
        if let Some(job) = jalari::current_job() {
            println!(
                "sending {:?} to {} (job {} on {:?}, attempt {}/{})",
                self.subject,
                self.to,
                job.id(),
                job.queue(),
                job.attempt(),
                job.max_attempts()
            );
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct SyncInventory {
    warehouse: String,
    batches: u32,
}

#[jalari::job]
impl Job for SyncInventory {
    const NAME: &'static str = "sync_inventory";

    async fn run(self) -> JobResult {
        for batch in 1..=self.batches {
            tokio::select! {
                () = jalari::shutdown_requested() => {
                    return Err(jalari::JobError::new(format!(
                        "stopped for shutdown before batch {batch}"
                    )));
                }
                () = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            println!(
                "synced batch {batch}/{} of {}",
                self.batches, self.warehouse
            );
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct DailyReport {
    include_failed: bool,
}

#[jalari::job(cron = "0 0 3 * * *", timezone = "Asia/Taipei", queue = "reports")]
impl Job for DailyReport {
    const NAME: &'static str = "daily_report";

    async fn run(self) -> JobResult {
        println!("daily report, include_failed = {}", self.include_failed);
        Ok(())
    }
}

fn email(to: &str, subject: &str) -> SendEmail {
    SendEmail {
        to: to.to_owned(),
        subject: subject.to_owned(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;

    jalari::cluster::update(|cluster| {
        cluster.housekeeping_enabled = true;
        cluster.succeeded_retention = Duration::from_secs(3 * 24 * 60 * 60);
        cluster.failed_retention = Some(Duration::from_secs(30 * 24 * 60 * 60));
    })
    .await?;
    println!("cluster config: {:?}", jalari::cluster::get().await?);

    let mut config = jalari::WorkerConfig::default();
    config.poll_interval = Duration::from_secs(1);
    config.shutdown_timeout = Duration::from_secs(10);
    let shutdown = jalari::CancellationToken::new();
    let worker = jalari::Worker::builder()
        .queue("default", 2)
        .queue("emails", 4)
        .queue("reports", 1)
        .config(config)
        .retry_policy(jalari::ExponentialBackoff {
            base: Duration::from_secs(10),
            max: Duration::from_secs(600),
            jitter: 0.2,
        })
        .build()
        .await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    let outcome = jalari::enqueue(&email("a@example.com", "hi")).await?;
    println!("enqueue -> {outcome:?}");

    let outcome = jalari::enqueue(&email("not-an-address", "bounce")).await?;
    println!("enqueue an invalid address -> {outcome:?}, it fails without retrying");

    let reminder = EnqueueOptions::new()
        .delay(Duration::from_secs(60))
        .job_key("reminder:b@example.com");
    let first = jalari::enqueue_with(&email("b@example.com", "reminder"), reminder.clone()).await?;
    let kept = jalari::enqueue_with(&email("b@example.com", "reminder"), reminder.clone()).await?;
    let replaced = jalari::enqueue_with(
        &email("b@example.com", "updated reminder"),
        reminder.on_conflict(OnConflict::Replace),
    )
    .await?;
    println!("same job_key: {first:?}, then {kept:?}, then with Replace {replaced:?}");

    let outcome = jalari::enqueue_with(
        &email("ops@example.com", "tomorrow's plan"),
        EnqueueOptions::new()
            .run_at(chrono::Utc::now() + Duration::from_secs(12 * 60 * 60))
            .queue("default")
            .timeout(Duration::from_secs(5)),
    )
    .await?;
    println!("scheduled on another queue with its own timeout -> {outcome:?}");

    for batches in [3, 2] {
        jalari::enqueue_with(
            &SyncInventory {
                warehouse: "taipei".to_owned(),
                batches,
            },
            EnqueueOptions::new().queue_key("warehouse:taipei"),
        )
        .await?;
    }
    println!("two inventory syncs share a queue_key, so they run one after the other");

    let mut transaction = pool.begin().await?;
    jalari::enqueue_in(
        &mut transaction,
        &email("c@example.com", "welcome"),
        EnqueueOptions::new().job_key("welcome:c@example.com"),
    )
    .await?;
    transaction.commit().await?;

    jalari::enqueue(&DailyReport {
        include_failed: true,
    })
    .await?;

    jalari::recurring::add_or_update(
        "hourly_digest",
        jalari::Cron::new("0 0 * * * *")?.timezone("Asia/Taipei")?,
        &email("ops@example.com", "hourly digest"),
    )
    .await?;
    jalari::recurring::trigger("hourly_digest").await?;
    jalari::recurring::update_schedule("hourly_digest", jalari::Cron::new("0 */30 * * * *")?)
        .await?;
    jalari::recurring::pause("hourly_digest").await?;
    jalari::recurring::resume("hourly_digest").await?;
    jalari::recurring::trigger(DailyReport::NAME).await?;

    match jalari::recurring::pause(DailyReport::NAME).await {
        Ok(()) => {}
        Err(e) if e.kind == jalari::ErrorKind::RecurringManagedByCode => {
            println!(
                "{} is declared in code, change it with a deploy",
                DailyReport::NAME
            );
        }
        Err(e) => return Err(e.into()),
    }

    for schedule in jalari::recurring::list().await? {
        println!(
            "schedule {} [{} {}] managed={} next={}",
            schedule.name, schedule.cron, schedule.timezone, schedule.managed, schedule.next_run_at
        );
    }
    for worker in jalari::admin::workers().await? {
        println!(
            "worker {}@{} scheduler={} queues={:?}",
            worker.pid, worker.hostname, worker.scheduler, worker.queues
        );
    }

    println!("press Ctrl-C to stop");
    tokio::signal::ctrl_c().await?;
    jalari::recurring::remove("hourly_digest").await?;
    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
