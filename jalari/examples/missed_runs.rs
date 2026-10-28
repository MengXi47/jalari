use std::time::Duration;

use chrono::{DateTime, Utc};
use jalari::admin::JobFilter;
use jalari::sqlx::PgPool;
use jalari::{Cron, ErrorKind, Job, JobResult, Missed, Schedule};
use serde::{Deserialize, Serialize};

const SCHEDULES: [(&str, Missed); 2] = [
    ("metrics:once", Missed::Once),
    ("metrics:skip", Missed::Skip),
];

#[derive(Serialize, Deserialize)]
struct SampleMetrics {
    schedule: String,
}

#[jalari::job(name = "sample_metrics", queue = "metrics")]
impl Job for SampleMetrics {
    async fn run(self) -> JobResult {
        println!(
            "  [{}] sampled at {}",
            self.schedule,
            Utc::now().format("%H:%M:%S")
        );
        Ok(())
    }
}

fn time(at: Option<DateTime<Utc>>) -> String {
    at.map_or_else(|| "-".to_owned(), |at| at.format("%H:%M:%S").to_string())
}

async fn print_schedules(since: DateTime<Utc>) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = jalari::admin::jobs(
        &JobFilter::new().task("sample_metrics").created_after(since),
        100,
        None,
    )
    .await?;
    for schedule in jalari::recurring::list().await? {
        let Some((name, _)) = SCHEDULES.iter().find(|(name, _)| *name == schedule.name) else {
            continue;
        };
        let mut enqueued = 0;
        for job in &jobs {
            if job.payload_json::<SampleMetrics>()?.schedule == *name {
                enqueued += 1;
            }
        }
        println!(
            "{name}: missed = {}, run_count {}, last_run_at {}, next_run_at {}, jobs enqueued {enqueued}",
            schedule.missed,
            schedule.run_count,
            time(schedule.last_run_at),
            time(schedule.next_run_at),
        );
    }
    Ok(())
}

async fn make_overdue(pool: &PgPool, names: &[&str], late: &str) -> jalari::sqlx::Result<()> {
    let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
    jalari::sqlx::query(
        "UPDATE jalari.recurring SET next_run_at = now() - $2::interval WHERE name = ANY($1)",
    )
    .bind(&names)
    .bind(late)
    .execute(pool)
    .await?;
    Ok(())
}

async fn wait_until_scheduled(names: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..200 {
        let schedules = jalari::recurring::list().await?;
        let pending = schedules.iter().any(|schedule| {
            names.contains(&schedule.name.as_str())
                && schedule
                    .next_run_at
                    .is_some_and(|next_run_at| next_run_at <= Utc::now())
        });
        if !pending {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("the scheduler did not handle the overdue runs".into())
}

async fn wait_for_jobs(since: DateTime<Utc>) -> Result<(), Box<dyn std::error::Error>> {
    let filter = JobFilter::new()
        .task("sample_metrics")
        .created_after(since)
        .state(jalari::JobState::Scheduled)
        .state(jalari::JobState::Enqueued)
        .state(jalari::JobState::Running);
    for _ in 0..200 {
        if jalari::admin::count(&filter).await? == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("the sampled jobs did not finish".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;
    let since = Utc::now();

    println!("== Two hourly schedules, one per missed-run policy (times in UTC)");
    for (name, missed) in SCHEDULES {
        match jalari::recurring::remove(name).await {
            Err(e) if e.kind != ErrorKind::RecurringNotFound => return Err(e.into()),
            _ => {}
        }
        let hourly = Schedule::from(Cron::new("0 0 * * * *")?).missed(missed);
        let job = SampleMetrics {
            schedule: name.to_owned(),
        };
        jalari::recurring::add_or_update(name, hourly, &job).await?;
    }
    print_schedules(since).await?;

    let mut config = jalari::WorkerConfig::default();
    config.poll_interval = Duration::from_millis(500);
    let worker = jalari::Worker::builder()
        .queue("metrics", 2)
        .config(config)
        .build()
        .await?;
    let shutdown = jalari::CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!("\n== An outage: both schedules' next run is now 3 hours overdue");
    let names: Vec<&str> = SCHEDULES.iter().map(|(name, _)| *name).collect();
    make_overdue(&pool, &names, "3 hours").await?;
    wait_until_scheduled(&names).await?;
    wait_for_jobs(since).await?;
    print_schedules(since).await?;
    println!("once enqueued one catch-up run for the three missed hours; skip enqueued none");

    println!("\n== A run 10 seconds late fires under skip too");
    make_overdue(&pool, &["metrics:skip"], "10 seconds").await?;
    wait_until_scheduled(&["metrics:skip"]).await?;
    wait_for_jobs(since).await?;
    print_schedules(since).await?;
    println!("runs less than a minute late always fire, whatever the policy");

    for name in names {
        jalari::recurring::remove(name).await?;
    }
    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
