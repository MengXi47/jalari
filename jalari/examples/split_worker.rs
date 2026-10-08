#[path = "common/jobs.rs"]
mod jobs;

use jalari::Job;
use jobs::{DailyReport, SendEmail};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = std::env::args().all(|arg| arg != "--no-scheduler");

    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool).schema(jalari::Schema::default()).await?;

    let shutdown = jalari::CancellationToken::new();
    let worker = jalari::Worker::builder()
        .queue("emails", 4)
        .queue("reports", 1)
        .scheduler(scheduler)
        .build()
        .await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!(
        "[pid {}] worker for {} and {} started, scheduler = {scheduler}",
        std::process::id(),
        SendEmail::NAME,
        DailyReport::NAME
    );
    for worker in jalari::admin::workers().await? {
        println!(
            "  alive: {}@{} scheduler={}",
            worker.pid, worker.hostname, worker.scheduler
        );
    }

    tokio::signal::ctrl_c().await?;
    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
