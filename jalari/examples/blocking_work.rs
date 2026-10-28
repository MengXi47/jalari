use std::sync::Mutex;
use std::time::{Duration, Instant};

use jalari::sqlx::PgPool;
use jalari::{EnqueueOptions, Job, JobId, JobResult};
use serde::{Deserialize, Serialize};

static STARTED: Mutex<Option<Instant>> = Mutex::new(None);
static NOTICED: Mutex<Option<Instant>> = Mutex::new(None);

#[derive(Serialize, Deserialize)]
struct Checksum {
    chunks: u32,
}

fn hash_chunk(chunk: u32, sum: u64) -> u64 {
    std::thread::sleep(Duration::from_millis(10));
    sum.wrapping_mul(31).wrapping_add(u64::from(chunk))
}

#[jalari::job(name = "checksum", queue = "blocking")]
impl Job for Checksum {
    async fn run(self) -> JobResult {
        *STARTED.lock().unwrap() = Some(Instant::now());
        let run = jalari::current_job().expect("inside a job");
        let polled = run.clone();
        let chunks = self.chunks;
        let sum = tokio::task::spawn_blocking(move || {
            let mut sum = 0;
            for chunk in 0..chunks {
                if polled.is_interrupted() {
                    *NOTICED.lock().unwrap() = Some(Instant::now());
                    println!(
                        "  [checksum] is_interrupted() before chunk {chunk}; leaving the loop"
                    );
                    return None;
                }
                sum = hash_chunk(chunk, sum);
            }
            Some(sum)
        })
        .await?;
        match sum {
            Some(sum) => {
                println!("  [checksum] all {chunks} chunks hashed: {sum:x}");
                Ok(())
            }
            None => Err(run.interrupted().await),
        }
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!("job {id} did not become {wanted}").into())
}

async fn wait_until_started() -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..600 {
        if STARTED.lock().unwrap().is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("the job did not start".into())
}

fn reset() {
    *STARTED.lock().unwrap() = None;
    *NOTICED.lock().unwrap() = None;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;

    let worker = jalari::Worker::builder()
        .queue("blocking", 2)
        .scheduler(false)
        .build()
        .await?;
    let stop_timeout = jalari::WorkerConfig::default().stop_timeout;
    let shutdown = jalari::CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!("== A blocking loop that runs to the end");
    let short = jalari::enqueue(&Checksum { chunks: 50 }).await?.id();
    wait_for_state(&pool, short, "succeeded").await?;
    println!("state of {short}: succeeded");

    println!("\n== Stop a job whose work runs in spawn_blocking");
    reset();
    let long = jalari::enqueue(&Checksum { chunks: 100_000 }).await?.id();
    wait_until_started().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let asked = Instant::now();
    println!("stop({long}) -> {:?}", jalari::stop(long).await?);
    wait_for_state(&pool, long, "stopped").await?;
    let noticed = NOTICED.lock().unwrap().ok_or("the loop never noticed")?;
    println!(
        "the loop noticed the stop {:.1?} after the request, well inside stop_timeout ({stop_timeout:?})",
        noticed - asked
    );
    println!("stop({long}) again -> {:?}", jalari::stop(long).await?);
    println!("cancel({long}) -> {:?}", jalari::cancel(long).await?);

    println!("\n== Time out a job whose work runs in spawn_blocking");
    reset();
    let options = EnqueueOptions::new()
        .timeout(Duration::from_secs(2))
        .max_attempts(1);
    let timed = jalari::enqueue_with(&Checksum { chunks: 100_000 }, options)
        .await?
        .id();
    wait_for_state(&pool, timed, "failed").await?;
    for _ in 0..100 {
        if NOTICED.lock().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let started = STARTED.lock().unwrap().ok_or("the job never started")?;
    let noticed = NOTICED.lock().unwrap().ok_or("the loop never noticed")?;
    println!(
        "the loop noticed the 2 s timeout {:.1?} after the attempt started",
        noticed - started
    );
    let detail = jalari::admin::job(timed).await?.ok_or("job not found")?;
    println!(
        "state of {timed}: {}, last error {:?}",
        detail.job.state, detail.job.last_error
    );

    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
