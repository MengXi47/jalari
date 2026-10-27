mod common;

use std::time::Duration;

use common::{Database, RunningWorker, fast_retry, worker_config};
use jalari::sqlx;
use jalari::{Job, JobResult, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Idle;

#[jalari::job(queue = "lifeline")]
impl Job for Idle {
    const NAME: &'static str = "lifeline_idle";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

async fn spawn_worker() -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue("lifeline", 2)
            .config(worker_config(Duration::from_millis(100)))
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn holds_advisory_lock(database: &Database, pid: i32) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND granted
         )",
    )
    .bind(pid)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifeline() {
    let Some(database) = Database::start("lifeline", 4).await else {
        return;
    };
    check_lifeline_session(&database).await;
    check_terminated_lifeline_registers_again(&database).await;
    database.finish().await;
}

async fn check_lifeline_session(database: &Database) {
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let (pid, worker_id) = database.lifelines().await.remove(0);
    let listed = jalari::admin::workers().await.unwrap();
    assert!(
        listed.iter().any(|worker| worker.id == worker_id),
        "the lifeline's worker {worker_id} is not listed: {listed:?}"
    );
    assert!(holds_advisory_lock(database, pid).await);

    let mut pooled = database.pool.acquire().await.unwrap();
    for setting in [
        "idle_session_timeout",
        "statement_timeout",
        "tcp_user_timeout",
    ] {
        let value: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SHOW {setting}")))
            .fetch_one(&mut *pooled)
            .await
            .unwrap();
        assert_eq!(value, "0", "pooled connections keep their own {setting}");
    }
    drop(pooled);

    worker.stop().await;
    assert!(database.lifelines().await.is_empty());
    assert!(!holds_advisory_lock(database, pid).await);
    assert!(jalari::admin::workers().await.unwrap().is_empty());
}

async fn check_terminated_lifeline_registers_again(database: &Database) {
    let worker = spawn_worker().await;
    common::wait_until(
        "the lifeline to register",
        Duration::from_secs(10),
        || async { database.lifelines().await.len() == 1 },
    )
    .await;
    let (_, old_id) = database.lifelines().await.remove(0);
    assert_eq!(database.terminate_lifelines(1).await, 1);
    common::wait_until(
        "the worker to register again under a new id",
        Duration::from_secs(10),
        || async {
            let lifelines = database.lifelines().await;
            lifelines.len() == 1 && lifelines[0].1 != old_id
        },
    )
    .await;
    common::wait_until(
        "the old worker row to go away",
        Duration::from_secs(10),
        || async {
            !jalari::admin::workers()
                .await
                .unwrap()
                .iter()
                .any(|worker| worker.id == old_id)
        },
    )
    .await;
    worker.stop().await;
    assert!(database.lifelines().await.is_empty());
}
