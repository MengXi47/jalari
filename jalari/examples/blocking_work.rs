use std::sync::Mutex;
use std::time::{Duration, Instant};

use jalari::sqlx::PgPool;
use jalari::{EnqueueOptions, Job, JobId, JobResult};
use serde::{Deserialize, Serialize};

static STARTED: Mutex<Option<Instant>> = Mutex::new(None);
static NOTICED: Mutex<Option<Instant>> = Mutex::new(None);
static RETURNED: Mutex<Option<Instant>> = Mutex::new(None);

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
        let thread = std::thread::current();
        let mut sum = 0;
        for chunk in 0..self.chunks {
            if run.is_interrupted() {
                *NOTICED.lock().unwrap() = Some(Instant::now());
                println!(
                    "  [checksum on {}] is_interrupted() before chunk {chunk}; leaving the loop",
                    thread.name().unwrap_or("?")
                );
                return Err(run.interrupted().await);
            }
            sum = hash_chunk(chunk, sum);
        }
        println!(
            "  [checksum on {}] all {} chunks hashed: {sum:x}",
            thread.name().unwrap_or("?"),
            self.chunks
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Stubborn {
    millis: u64,
}

#[jalari::job(name = "stubborn", queue = "blocking", max_attempts = 1)]
impl Job for Stubborn {
    async fn run(self) -> JobResult {
        std::thread::sleep(Duration::from_millis(self.millis));
        *RETURNED.lock().unwrap() = Some(Instant::now());
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Lookup {
    key: String,
}

#[jalari::job(name = "lookup", queue = "lookups")]
impl Job for Lookup {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        let polled = run.clone();
        let key = self.key;
        let found = tokio::task::spawn_blocking(move || {
            for _ in 0..50 {
                if polled.is_interrupted() {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Some(key.len())
        })
        .await?;
        match found {
            Some(length) => {
                println!("  [lookup] found a value of length {length} in spawn_blocking");
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

async fn wait_for(
    moment: &Mutex<Option<Instant>>,
    what: &str,
) -> Result<Instant, Box<dyn std::error::Error>> {
    for _ in 0..600 {
        if let Some(at) = *moment.lock().unwrap() {
            return Ok(at);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!("{what} never happened").into())
}

fn reset() {
    *STARTED.lock().unwrap() = None;
    *NOTICED.lock().unwrap() = None;
    *RETURNED.lock().unwrap() = None;
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
        .task_queue("lookups", 50)
        .scheduler(false)
        .build()
        .await?;
    let stop_timeout = jalari::WorkerConfig::default().stop_timeout;
    let shutdown = jalari::CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!("== A blocking loop inside run() that runs to the end");
    let short = jalari::enqueue(&Checksum { chunks: 50 }).await?.id();
    wait_for_state(&pool, short, "succeeded").await?;
    println!("state of {short}: succeeded");

    println!("\n== Stop a blocking loop that polls is_interrupted()");
    reset();
    let long = jalari::enqueue(&Checksum { chunks: 100_000 }).await?.id();
    wait_for(&STARTED, "the start").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let asked = Instant::now();
    println!("stop({long}) -> {:?}", jalari::stop(long).await?);
    wait_for_state(&pool, long, "stopped").await?;
    let noticed = wait_for(&NOTICED, "the stop").await?;
    println!(
        "the loop noticed the stop {:.1?} after the request, well inside stop_timeout ({stop_timeout:?})",
        noticed - asked
    );
    println!("cancel({long}) -> {:?}", jalari::cancel(long).await?);

    println!("\n== Time out a blocking loop that polls is_interrupted()");
    reset();
    let options = EnqueueOptions::new()
        .timeout(Duration::from_secs(2))
        .max_attempts(1);
    let timed = jalari::enqueue_with(&Checksum { chunks: 100_000 }, options)
        .await?
        .id();
    wait_for_state(&pool, timed, "failed").await?;
    let started = wait_for(&STARTED, "the start").await?;
    let noticed = wait_for(&NOTICED, "the timeout").await?;
    println!(
        "the loop noticed the 2 s timeout {:.1?} after the attempt started",
        noticed - started
    );
    let detail = jalari::admin::job(timed).await?.ok_or("job not found")?;
    println!(
        "state of {timed}: {}, last error {:?}",
        detail.job.state, detail.job.last_error
    );

    println!("\n== Time out a job that never checks, while the other slot keeps working");
    reset();
    let enqueued = Instant::now();
    let options = EnqueueOptions::new().timeout(Duration::from_secs(1));
    let stubborn = jalari::enqueue_with(&Stubborn { millis: 4000 }, options)
        .await?
        .id();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let beside = jalari::enqueue(&Checksum { chunks: 50 }).await?.id();
    wait_for_state(&pool, beside, "succeeded").await?;
    println!(
        "a short checksum on the other slot succeeded {:.1?} after the stubborn job was enqueued",
        enqueued.elapsed()
    );
    wait_for_state(&pool, stubborn, "failed").await?;
    println!(
        "the stubborn job was recorded failed {:.1?} after it was enqueued, its code still sleeping",
        enqueued.elapsed()
    );
    let returned = wait_for(&RETURNED, "the stubborn job's return").await?;
    println!(
        "its code returned {:.1?} after it was enqueued; its slot took no job until then",
        returned - enqueued
    );

    println!("\n== Blocking code in a task queue goes to spawn_blocking");
    let lookup = jalari::enqueue(&Lookup {
        key: "customer:42".to_owned(),
    })
    .await?
    .id();
    wait_for_state(&pool, lookup, "succeeded").await?;
    println!("state of {lookup}: succeeded");

    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
