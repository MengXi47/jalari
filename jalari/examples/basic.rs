use std::io;
use std::time::Duration;

use jalari::{Job, JobResult};
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
        println!("sending {:?} to {}", self.subject, self.to);
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
        .await?;

    let cluster = jalari::cluster::update(|cluster| {
        cluster.housekeeping_enabled = true;
        cluster.succeeded_retention = Duration::from_secs(3 * 24 * 60 * 60);
    })
    .await?;
    println!("cluster config: {cluster:?}");

    let mut config = jalari::WorkerConfig::default();
    config.poll_interval = Duration::from_secs(1);
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

    jalari::enqueue(&email("a@example.com", "hi")).await?;

    jalari::enqueue_with(
        &email("b@example.com", "reminder"),
        jalari::EnqueueOptions::new()
            .delay(Duration::from_secs(60))
            .job_key("reminder:b@example.com")
            .on_conflict(jalari::OnConflict::Replace),
    )
    .await?;

    let mut transaction = pool.begin().await?;
    jalari::enqueue_in(
        &mut transaction,
        &email("c@example.com", "welcome"),
        jalari::EnqueueOptions::new().job_key("welcome:c@example.com"),
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

    tokio::signal::ctrl_c().await?;
    jalari::recurring::remove("hourly_digest").await?;
    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
