use std::collections::BTreeMap;
use std::process::{Child, Command};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use jalari::sqlx::{PgExecutor, PgPool};
use jalari::{Job, JobError, JobId, JobResult, JobState};
use serde::{Deserialize, Serialize};

const PAGES: i32 = 60;
const PAGE_TIME: Duration = Duration::from_millis(150);
const KILL_AFTER_PAGES: i64 = 40;

static POOL: OnceLock<PgPool> = OnceLock::new();

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Store {
    Save,
    SaveNow,
    SaveIn,
}

impl Store {
    fn name(self) -> &'static str {
        match self {
            Self::Save => "save",
            Self::SaveNow => "save_now",
            Self::SaveIn => "save_in",
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DeliverPages {
    store: Store,
    pages: i32,
}

#[derive(Serialize, Deserialize, Default)]
struct Cursor {
    next_page: i32,
}

async fn deliver<'e>(
    executor: impl PgExecutor<'e>,
    store: Store,
    page: i32,
    attempt: i32,
) -> jalari::sqlx::Result<()> {
    jalari::sqlx::query(
        "INSERT INTO durable_progress_pages (store, page, attempt) VALUES ($1, $2, $3)",
    )
    .bind(store.name())
    .bind(page)
    .bind(attempt)
    .execute(executor)
    .await?;
    Ok(())
}

#[jalari::job(name = "deliver_pages", queue = "pages")]
impl Job for DeliverPages {
    async fn run(self) -> JobResult {
        let pool = POOL
            .get()
            .ok_or_else(|| JobError::new("no pool".to_owned()))?;
        let attempt = jalari::current_job().map_or(0, |run| run.attempt());
        let (saved, checkpoint) = jalari::resume::<Cursor>()?;
        let mut cursor = saved.unwrap_or_default();
        println!(
            "  [{} in pid {}] attempt {attempt} starts at page {}",
            self.store.name(),
            std::process::id(),
            cursor.next_page
        );
        while cursor.next_page < self.pages {
            tokio::time::sleep(PAGE_TIME).await;
            let page = cursor.next_page;
            cursor.next_page += 1;
            match self.store {
                Store::Save => {
                    deliver(pool, self.store, page, attempt).await?;
                    checkpoint.save(&cursor).await?;
                }
                Store::SaveNow => {
                    deliver(pool, self.store, page, attempt).await?;
                    checkpoint.save_now(&cursor).await?;
                }
                Store::SaveIn => {
                    let mut transaction = pool.begin().await?;
                    deliver(&mut *transaction, self.store, page, attempt).await?;
                    checkpoint.save_in(&mut transaction, &cursor).await?;
                    transaction.commit().await?;
                }
            }
        }
        println!(
            "  [{}] all {} pages delivered",
            self.store.name(),
            self.pages
        );
        Ok(())
    }
}

async fn start_worker() -> jalari::Result<jalari::Worker> {
    jalari::Worker::builder()
        .queue("pages", 3)
        .scheduler(false)
        .build()
        .await
}

async fn run_child_worker() -> Result<(), Box<dyn std::error::Error>> {
    let worker = start_worker().await?;
    println!("worker started in pid {}", std::process::id());
    worker.run(jalari::CancellationToken::new()).await;
    Ok(())
}

async fn delivered(pool: &PgPool) -> jalari::sqlx::Result<BTreeMap<String, (i64, i64)>> {
    let rows: Vec<(String, i64, i64)> = jalari::sqlx::query_as(
        "SELECT store, count(*), count(DISTINCT page) FROM durable_progress_pages GROUP BY store",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(store, rows, pages)| (store, (rows, pages)))
        .collect())
}

async fn wait_for_pages(pool: &PgPool, pages: i64) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..600 {
        let counts = delivered(pool).await?;
        if counts.len() == 3 && counts.values().all(|(_, distinct)| *distinct >= pages) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("the jobs did not deliver enough pages".into())
}

async fn wait_until_succeeded(ids: &[(Store, JobId)]) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..600 {
        let mut done = true;
        for (_, id) in ids {
            let detail = jalari::admin::job(*id).await?.ok_or("job not found")?;
            done &= detail.job.state == JobState::Succeeded;
        }
        if done {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("the jobs did not finish".into())
}

fn kill(mut child: Child) -> std::io::Result<()> {
    child.kill()?;
    child.wait()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;
    POOL.set(pool.clone()).ok();
    if std::env::args().nth(1).as_deref() == Some("worker") {
        return run_child_worker().await;
    }

    jalari::sqlx::query(
        "CREATE TABLE IF NOT EXISTS durable_progress_pages (
             store TEXT NOT NULL,
             page INTEGER NOT NULL,
             attempt INTEGER NOT NULL
         )",
    )
    .execute(&pool)
    .await?;
    jalari::sqlx::query("DELETE FROM durable_progress_pages")
        .execute(&pool)
        .await?;

    println!("== Three jobs deliver {PAGES} pages each, storing their cursor three ways");
    let mut ids = Vec::new();
    for store in [Store::Save, Store::SaveNow, Store::SaveIn] {
        let id = jalari::enqueue(&DeliverPages {
            store,
            pages: PAGES,
        })
        .await?
        .id();
        ids.push((store, id));
    }
    let child = Command::new(std::env::current_exe()?)
        .arg("worker")
        .spawn()?;

    wait_for_pages(&pool, KILL_AFTER_PAGES).await?;
    println!("\n== Kill the worker process mid-run (SIGKILL, no shutdown)");
    kill(child)?;
    let killed = Instant::now();
    let counts = delivered(&pool).await?;
    for (store, id) in &ids {
        let detail = jalari::admin::job(*id).await?.ok_or("job not found")?;
        let stored = detail.control.and_then(|control| control.progress);
        let (_, pages) = counts.get(store.name()).copied().unwrap_or_default();
        println!(
            "{:<8} delivered pages 0 to {}, stored cursor {}",
            store.name(),
            pages - 1,
            stored.map_or_else(|| "none".to_owned(), |progress| progress.to_string())
        );
    }

    println!("\n== A new worker takes the jobs over once the dead worker's heartbeat is 7 s old");
    let shutdown = jalari::CancellationToken::new();
    let worker = start_worker().await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));
    wait_until_succeeded(&ids).await?;
    println!(
        "all three jobs succeeded {:.1?} after the kill",
        killed.elapsed()
    );

    println!("\n== Pages delivered twice because the crash lost the cursor");
    for (store, (rows, pages)) in delivered(&pool).await? {
        println!(
            "{store:<8} {rows} deliveries for {pages} pages: {} delivered twice",
            rows - pages
        );
    }

    shutdown.cancel();
    worker_task.await?;
    jalari::sqlx::query("DROP TABLE durable_progress_pages")
        .execute(&pool)
        .await?;
    Ok(())
}
