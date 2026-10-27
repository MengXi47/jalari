use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use jalari::admin::JobFilter;
use jalari::sqlx::types::chrono::{DateTime, Utc};
use jalari::{EnqueueOptions, Job, JobId, JobResult, JobState, Priority};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct RenderPage {
    page: u32,
}

#[jalari::job(name = "render_page", queue = "render")]
impl Job for RenderPage {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        println!("     rendered page {} at {:?}", self.page, run.priority());
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct RenderCover {
    title: String,
}

#[jalari::job(name = "render_cover", queue = "render", priority = "high")]
impl Job for RenderCover {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        println!(
            "     rendered the cover of {:?} at {:?}",
            self.title,
            run.priority()
        );
        Ok(())
    }
}

#[derive(Default)]
struct Running {
    now: usize,
    most: usize,
}

static RUNNING: Mutex<BTreeMap<String, Running>> = Mutex::new(BTreeMap::new());

#[derive(Serialize, Deserialize)]
struct SyncTenant {
    tenant: String,
    batch: u32,
}

#[jalari::job(name = "sync_tenant", queue = "sync")]
impl Job for SyncTenant {
    async fn run(self) -> JobResult {
        let now = {
            let mut running = RUNNING.lock().expect("not poisoned");
            let tenant = running.entry(self.tenant.clone()).or_default();
            tenant.now += 1;
            tenant.most = tenant.most.max(tenant.now);
            tenant.now
        };
        println!(
            "     {} batch {} starts, {now} of {} running",
            self.tenant, self.batch, self.tenant
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut running = RUNNING.lock().expect("not poisoned");
        if let Some(tenant) = running.get_mut(&self.tenant) {
            tenant.now -= 1;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ExportBook {
    title: String,
    chapters: u32,
}

#[jalari::job(name = "export_book", queue = "lineage")]
impl Job for ExportBook {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        println!(
            "     export_book #{}: parent {}, root #{}",
            run.id(),
            parent(run.parent_id()),
            run.root_id()
        );
        let chapters: Vec<_> = (1..=self.chapters)
            .map(|chapter| {
                let job = ExportChapter {
                    title: self.title.clone(),
                    chapter,
                };
                (job, EnqueueOptions::new())
            })
            .collect();
        jalari::enqueue_many(&chapters).await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ExportChapter {
    title: String,
    chapter: u32,
}

#[jalari::job(name = "export_chapter", queue = "lineage")]
impl Job for ExportChapter {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        println!(
            "     export_chapter #{} ({} chapter {}): parent {}, root #{}",
            run.id(),
            self.title,
            self.chapter,
            parent(run.parent_id()),
            run.root_id()
        );
        if self.chapter == 1 {
            jalari::enqueue(&CheckLinks { chapter: 1 }).await?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct CheckLinks {
    chapter: u32,
}

#[jalari::job(name = "check_links", queue = "lineage")]
impl Job for CheckLinks {
    async fn run(self) -> JobResult {
        let run = jalari::current_job().expect("inside a job");
        println!(
            "     check_links #{} (chapter {}): parent {}, root #{}",
            run.id(),
            self.chapter,
            parent(run.parent_id()),
            run.root_id()
        );
        Ok(())
    }
}

fn parent(id: Option<JobId>) -> String {
    id.map_or_else(|| "none".to_owned(), |id| format!("#{id}"))
}

async fn wait_until_done(
    queue: &str,
    since: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let unfinished = JobFilter::new()
        .queue(queue)
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
    Err(format!("jobs of {queue} are still waiting").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;
    let since: DateTime<Utc> = jalari::sqlx::query_scalar("SELECT now()")
        .fetch_one(&pool)
        .await?;

    let shutdown = jalari::CancellationToken::new();
    let worker = jalari::Worker::builder()
        .queue("render", 1)
        .queue("sync", 6)
        .queue("lineage", 2)
        .scheduler(false)
        .build()
        .await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!("== Pause a queue, fill it, reorder it, resume it");
    println!(
        "pause_queue(\"render\") -> {}",
        jalari::admin::pause_queue("render").await?
    );
    for paused in jalari::admin::paused_queues().await? {
        println!("  {} paused since {}", paused.queue, paused.paused_at);
    }

    let pages: Vec<_> = [1, 2, 3, 4, 2]
        .into_iter()
        .map(|page| {
            let options = EnqueueOptions::new().job_key(&format!("page:{page}"));
            (RenderPage { page }, options)
        })
        .collect();
    let outcomes = jalari::enqueue_many(&pages).await?;
    println!("enqueue_many of pages 1, 2, 3, 4 and 2 again -> {outcomes:?}");

    jalari::enqueue(&RenderCover {
        title: "Rust at Sea".to_owned(),
    })
    .await?;
    jalari::enqueue_with(
        &RenderPage { page: 5 },
        EnqueueOptions::new().priority(Priority::Urgent),
    )
    .await?;
    jalari::enqueue_with(
        &RenderPage { page: 6 },
        EnqueueOptions::new().priority(Priority::Low),
    )
    .await?;
    println!(
        "render_cover is high by its attribute, page 5 urgent and page 6 low by their options"
    );

    tokio::time::sleep(Duration::from_secs(1)).await;
    let waiting = JobFilter::new()
        .queue("render")
        .created_after(since)
        .state(JobState::Enqueued);
    println!(
        "one second later {} jobs still wait in the paused queue",
        jalari::admin::count(&waiting).await?
    );

    let low = JobFilter::new()
        .queue("render")
        .created_after(since)
        .priority(Priority::Low);
    let moved = jalari::admin::set_priority(&low, Priority::High).await?;
    println!("set_priority moved {moved} low job to High");

    println!(
        "resume_queue(\"render\") -> {}; one slot runs them by priority, then by age:",
        jalari::admin::resume_queue("render").await?
    );
    wait_until_done("render", since).await?;

    println!("\n== Let up to two jobs of one queue_key run at once");
    let mut syncs = Vec::new();
    for batch in 1..=6 {
        let options = EnqueueOptions::new()
            .queue_key("tenant:acme")
            .queue_key_limit(2);
        syncs.push((
            SyncTenant {
                tenant: "acme".to_owned(),
                batch,
            },
            options,
        ));
    }
    for batch in 1..=3 {
        let options = EnqueueOptions::new().queue_key("tenant:globex");
        syncs.push((
            SyncTenant {
                tenant: "globex".to_owned(),
                batch,
            },
            options,
        ));
    }
    jalari::enqueue_many(&syncs).await?;
    println!("6 acme syncs with queue_key_limit(2) and 3 globex syncs without a limit, 6 slots");
    wait_until_done("sync", since).await?;
    for (tenant, running) in RUNNING.lock().expect("not poisoned").iter() {
        println!("{tenant} ran at most {} at once", running.most);
    }

    println!("\n== Jobs enqueued by jobs");
    let book: JobId = jalari::enqueue(&ExportBook {
        title: "Rust at Sea".to_owned(),
        chapters: 3,
    })
    .await?
    .id();
    wait_until_done("lineage", since).await?;
    let children = jalari::admin::count(&JobFilter::new().parent(book)).await?;
    let tree = jalari::admin::jobs(&JobFilter::new().root(book), 100, None).await?;
    println!(
        "export_book #{book} enqueued {children} jobs itself; its tree holds {}:",
        tree.len()
    );
    for job in tree.iter().rev() {
        println!(
            "  {} #{}: parent {}, root {}",
            job.task,
            job.id,
            parent(job.parent_id),
            job.root_id
                .map_or_else(|| "itself".to_owned(), |id| format!("#{id}"))
        );
    }

    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
