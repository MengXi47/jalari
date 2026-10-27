use std::collections::HashMap;
use std::io;
use std::time::{Duration, Instant};

use jalari::admin::JobFilter;
use jalari::sqlx::PgPool;
use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::types::chrono::{DateTime, Utc};
use jalari::{
    EnqueueOptions, Finished, Job, JobError, JobId, JobMeta, JobMiddleware, JobResult, JobState,
    Next,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Caller {
    tenant: String,
    user_id: i64,
}

struct TenantDb {
    caller: Caller,
    pool: PgPool,
}

struct TracingMiddleware;

impl JobMiddleware for TracingMiddleware {
    type Provides = ();

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        let started = Instant::now();
        println!(
            "  -> {} #{} attempt {}/{} on {:?}, context {}",
            job.name(),
            job.id(),
            job.attempt(),
            job.max_attempts(),
            job.queue(),
            job.raw_context()
                .map_or_else(|| "none".to_owned(), ToString::to_string)
        );
        let result = next.run().await;
        let ending = match &result {
            Ok(()) => "ok".to_owned(),
            Err(e) => format!("error: {}", e.msg()),
        };
        println!(
            "  <- {} #{} after {:?}: {ending}",
            job.name(),
            job.id(),
            started.elapsed()
        );
        result
    }

    async fn finished(&self, job: &JobMeta, outcome: &Finished) {
        let last = if job.is_last_attempt() {
            ", its last attempt"
        } else {
            ""
        };
        println!(
            "  == {} #{} saved as {outcome:?}{last}",
            job.name(),
            job.id()
        );
    }
}

struct TenantMiddleware {
    databases: HashMap<String, PgPool>,
}

impl JobMiddleware for TenantMiddleware {
    type Provides = TenantDb;

    async fn check(&self) -> Result<(), JobError> {
        for (tenant, pool) in &self.databases {
            jalari::sqlx::query("SELECT 1").execute(pool).await?;
            println!("tenant database {tenant:?} is reachable");
        }
        Ok(())
    }

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        let Some(caller) = job.context::<Caller>()? else {
            return Err(JobError::permanent(io::Error::other(
                "the job was enqueued without a caller",
            )));
        };
        let Some(pool) = self.databases.get(&caller.tenant) else {
            return Err(JobError::permanent(io::Error::other(format!(
                "unknown tenant {:?}",
                caller.tenant
            ))));
        };
        let pool = pool.clone();
        next.provide(TenantDb { caller, pool }).run().await
    }
}

#[derive(Serialize, Deserialize)]
struct BuildReport {
    month: String,
}

#[jalari::job(name = "build_report", queue = "reports")]
impl Job for BuildReport {
    type Context = TenantDb;

    async fn run(self, db: &TenantDb) -> JobResult {
        let database: String = jalari::sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&db.pool)
            .await?;
        println!(
            "     building the {} report for tenant {} in database {database}",
            self.month, db.caller.tenant
        );
        let outcome = jalari::enqueue(&NotifyUser {
            message: format!("your {} report is ready", self.month),
        })
        .await?;
        println!(
            "     enqueued notify_user #{}, which inherits this job's context",
            outcome.id()
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct NotifyUser {
    message: String,
}

#[jalari::job(name = "notify_user", queue = "reports")]
impl Job for NotifyUser {
    type Context = TenantDb;

    async fn run(self, db: &TenantDb) -> JobResult {
        println!(
            "     telling user {} of {}: {}",
            db.caller.user_id, db.caller.tenant, self.message
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct PurgeCache;

#[jalari::job(name = "purge_cache", queue = "reports", aliases = ["clear_cache"])]
impl Job for PurgeCache {
    async fn run(self) -> JobResult {
        println!("     purging the cache, no tenant needed");
        Ok(())
    }
}

async fn tenant_pool(url: &str) -> Result<PgPool, jalari::sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5))
        .after_connect(|connection, _| {
            Box::pin(async move {
                jalari::sqlx::query("SET idle_in_transaction_session_timeout = '30s'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
}

async fn wait_until_finished(since: DateTime<Utc>) -> Result<(), Box<dyn std::error::Error>> {
    let unfinished = JobFilter::new()
        .queue("reports")
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
    Err("jobs are still waiting".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&url).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .migrate()
        .await?;

    let since: DateTime<Utc> = jalari::sqlx::query_scalar("SELECT now()")
        .fetch_one(&pool)
        .await?;
    let legacy: i64 = jalari::sqlx::query_scalar(
        "INSERT INTO jalari.job (queue, task, payload, state, max_attempts)
         VALUES ('reports', 'clear_cache', convert_to('null', 'UTF8'), 'enqueued', 10)
         RETURNING id",
    )
    .fetch_one(&pool)
    .await?;
    println!("job {legacy} waits under clear_cache, the name purge_cache had before a rename");

    let databases = HashMap::from([
        ("acme".to_owned(), tenant_pool(&url).await?),
        ("globex".to_owned(), tenant_pool(&url).await?),
    ]);
    let shutdown = jalari::CancellationToken::new();
    let worker = jalari::Worker::builder()
        .middleware(TracingMiddleware)
        .middleware(TenantMiddleware { databases })
        .queue("reports", 2)
        .scheduler(false)
        .build()
        .await?;
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    let mut ids: Vec<(&str, JobId)> = vec![(
        "stored under the alias clear_cache, run by purge_cache",
        JobId(legacy),
    )];

    let acme = Caller {
        tenant: "acme".to_owned(),
        user_id: 7,
    };

    let outcome = jalari::scope(&acme, async {
        let mut transaction = pool.begin().await?;
        let outcome = jalari::enqueue_in(
            &mut transaction,
            &BuildReport {
                month: "2026-09".to_owned(),
            },
            EnqueueOptions::new(),
        )
        .await?;
        transaction.commit().await?;
        Ok::<_, Box<dyn std::error::Error>>(outcome)
    })
    .await?;
    ids.push(("captured from jalari::scope", outcome.id()));

    let globex = Caller {
        tenant: "globex".to_owned(),
        user_id: 42,
    };
    let outcome = jalari::enqueue_with(
        &BuildReport {
            month: "2026-10".to_owned(),
        },
        EnqueueOptions::new().context(&globex),
    )
    .await?;
    ids.push(("set with EnqueueOptions::context", outcome.id()));

    let outcome = jalari::scope(&acme, async {
        jalari::enqueue_with(
            &BuildReport {
                month: "2026-08".to_owned(),
            },
            EnqueueOptions::new().no_context(),
        )
        .await
    })
    .await?;
    ids.push(("no_context, rejected by TenantMiddleware", outcome.id()));

    let initech = Caller {
        tenant: "initech".to_owned(),
        user_id: 1,
    };
    let outcome = jalari::enqueue_with(
        &BuildReport {
            month: "2026-10".to_owned(),
        },
        EnqueueOptions::new().context(&initech),
    )
    .await?;
    ids.push(("unknown tenant, rejected by TenantMiddleware", outcome.id()));

    let outcome = jalari::enqueue(&PurgeCache).await?;
    ids.push((
        "no context needed, only TracingMiddleware wraps it",
        outcome.id(),
    ));

    wait_until_finished(since).await?;
    shutdown.cancel();
    worker_task.await?;

    println!("\nresults:");
    for (label, id) in ids {
        let (state, error): (String, Option<String>) =
            jalari::sqlx::query_as("SELECT state, last_error FROM jalari.job WHERE id = $1")
                .bind(id.0)
                .fetch_one(&pool)
                .await?;
        println!(
            "  job {id} ({label}): {state}{}",
            error.map(|e| format!(", {e}")).unwrap_or_default()
        );
    }
    Ok(())
}
