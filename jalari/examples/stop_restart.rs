use std::time::Duration;

use jalari::sqlx::PgPool;
use jalari::{CancelOutcome, EnqueueOptions, Job, JobId, JobResult, OnConflict, StopOutcome};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Export {
    rows: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct Cursor {
    next_row: u64,
}

#[jalari::job(queue = "exports")]
impl Job for Export {
    const NAME: &'static str = "export";

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<Cursor>()?;
        let mut cursor = saved.unwrap_or_default();
        if let Some(job) = jalari::current_job() {
            println!(
                "  [export {}] attempt {}/{}, starting at row {} of {}",
                job.id(),
                job.attempt(),
                job.max_attempts(),
                cursor.next_row,
                self.rows
            );
        }
        while cursor.next_row < self.rows {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cursor.next_row += 1;
            checkpoint.save(&cursor).await?;
        }
        println!("  [export] wrote all {} rows", self.rows);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Reindex {
    tables: Vec<String>,
}

#[jalari::job(queue = "exports")]
impl Job for Reindex {
    const NAME: &'static str = "reindex";

    async fn run(self) -> JobResult {
        for table in &self.tables {
            jalari::checkpoint().await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            println!("  [reindex] reindexed {table}");
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct LegacyImport;

#[jalari::job(queue = "exports")]
impl Job for LegacyImport {
    const NAME: &'static str = "legacy_import";

    async fn run(self) -> JobResult {
        println!("  [legacy_import] importing without checkpoints");
        tokio::time::sleep(Duration::from_secs(600)).await;
        Ok(())
    }
}

async fn state(pool: &PgPool, id: JobId) -> Result<String, jalari::sqlx::Error> {
    jalari::sqlx::query_scalar("SELECT state FROM jalari.job WHERE id = $1")
        .bind(id.0)
        .fetch_one(pool)
        .await
}

async fn wait_for_state(
    pool: &PgPool,
    id: JobId,
    wanted: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..600 {
        if state(pool, id).await? == wanted {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("job {id} did not become {wanted}").into())
}

async fn stop_and_wait(id: JobId) -> jalari::Result<StopOutcome> {
    loop {
        match jalari::stop(id).await? {
            StopOutcome::Requested => {
                println!("stop({id}) -> Requested, waiting for the job to reach a checkpoint");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            outcome => {
                println!("stop({id}) -> {outcome:?}");
                return Ok(outcome);
            }
        }
    }
}

async fn cancel_and_wait(id: JobId) -> jalari::Result<CancelOutcome> {
    loop {
        match jalari::cancel(id).await? {
            CancelOutcome::Requested => {
                println!("cancel({id}) -> Requested, waiting for the job to stop");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            outcome => {
                println!("cancel({id}) -> {outcome:?}");
                return Ok(outcome);
            }
        }
    }
}

async fn start_worker() -> jalari::Result<jalari::Worker> {
    let mut config = jalari::WorkerConfig::default();
    config.stop_timeout = Duration::from_secs(2);
    config.shutdown_timeout = Duration::from_secs(5);
    jalari::Worker::builder()
        .queue("exports", 4)
        .scheduler(false)
        .config(config)
        .build()
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;

    let shutdown = jalari::CancellationToken::new();
    let worker = start_worker().await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!("== Stop a waiting job, then cancel it");
    let later = jalari::enqueue_with(
        &Export { rows: 5 },
        EnqueueOptions::new().delay(Duration::from_secs(3600)),
    )
    .await?
    .id();
    stop_and_wait(later).await?;
    println!("restart({later}) -> {:?}", jalari::restart(later).await?);
    cancel_and_wait(later).await?;
    println!("state of {later}: {}", state(&pool, later).await?);

    println!("\n== Stop a running job and restart it from its checkpoint");
    let key = EnqueueOptions::new().job_key("export:daily");
    let export = jalari::enqueue_with(&Export { rows: 30 }, key.clone())
        .await?
        .id();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stop_and_wait(export).await?;
    println!("state of {export}: {}", state(&pool, export).await?);
    let replaced = jalari::enqueue_with(
        &Export { rows: 999 },
        key.clone().on_conflict(OnConflict::Replace),
    )
    .await?;
    println!("Replace while stopped -> {replaced:?}, the stopped job keeps its job_key");
    println!("restart({export}) -> {:?}", jalari::restart(export).await?);
    println!(
        "restart({export}) again -> {:?}",
        jalari::restart(export).await?
    );
    wait_for_state(&pool, export, "succeeded").await?;
    println!("state of {export}: succeeded");
    stop_and_wait(export).await?;

    println!("\n== Stop a job that has no progress to save");
    let reindex = jalari::enqueue(&Reindex {
        tables: [
            "users", "orders", "invoices", "payments", "events", "sessions", "audit", "tags",
        ]
        .map(str::to_owned)
        .to_vec(),
    })
    .await?
    .id();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    stop_and_wait(reindex).await?;
    println!(
        "restart({reindex}) -> {:?}, it starts over",
        jalari::restart(reindex).await?
    );
    wait_for_state(&pool, reindex, "succeeded").await?;
    println!("state of {reindex}: succeeded");

    println!("\n== Cancel a running job");
    let big = jalari::enqueue_with(
        &Export { rows: 10_000 },
        EnqueueOptions::new().job_key("export:big"),
    )
    .await?
    .id();
    tokio::time::sleep(Duration::from_millis(800)).await;
    cancel_and_wait(big).await?;
    println!("state of {big}: {}", state(&pool, big).await?);
    let again = jalari::enqueue_with(
        &Export { rows: 3 },
        EnqueueOptions::new().job_key("export:big"),
    )
    .await?;
    println!("enqueue with the cancelled job's key -> {again:?}");
    wait_for_state(&pool, again.id(), "succeeded").await?;

    println!("\n== Stop a job that never reaches a checkpoint");
    let legacy = jalari::enqueue(&LegacyImport).await?.id();
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop_and_wait(legacy).await?;
    println!("the worker aborted it after stop_timeout (2 s)");
    cancel_and_wait(legacy).await?;

    println!("\n== Shut a worker down mid-run; the next worker resumes the job");
    let resumed = jalari::enqueue(&Export { rows: 25 }).await?.id();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    println!("shutting the worker down");
    shutdown.cancel();
    worker_task.await?;
    println!(
        "state of {resumed} after shutdown: {}",
        state(&pool, resumed).await?
    );
    let shutdown = jalari::CancellationToken::new();
    let worker = start_worker().await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));
    wait_for_state(&pool, resumed, "succeeded").await?;
    println!(
        "state of {resumed}: succeeded; like a stop, the shutdown did not count as an attempt"
    );

    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
