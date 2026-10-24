#[path = "common/jobs.rs"]
mod jobs;

use jalari::Job;
use jobs::{DailyReport, Export, SendEmail, VacuumTables};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = std::env::args().all(|arg| arg != "--no-scheduler");

    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool).schema(jalari::Schema::default()).await?;

    let shutdown = jalari::CancellationToken::new();
    let worker = jalari::Worker::builder()
        .queue("emails", 4)
        .queue("reports", 1)
        .queue("exports", 2)
        .queue("maintenance", 1)
        .scheduler(scheduler)
        .build()
        .await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!(
        "[pid {}] worker for {}, {}, {} and {} started, scheduler = {scheduler}",
        std::process::id(),
        SendEmail::NAME,
        DailyReport::NAME,
        Export::NAME,
        VacuumTables::NAME
    );
    let pid = i32::try_from(std::process::id())?;
    let alive = loop {
        let alive = jalari::admin::workers().await?;
        if alive.iter().any(|worker| worker.pid == pid) {
            break alive;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    for worker in alive {
        println!(
            "  alive: {}@{} scheduler={}",
            worker.pid, worker.hostname, worker.scheduler
        );
        for (task, fingerprint) in &worker.tasks {
            match fingerprint {
                Some(fingerprint) => println!("    {task}: fingerprint {fingerprint}"),
                None => println!("    {task}: any build"),
            }
        }
    }

    tokio::signal::ctrl_c().await?;
    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
