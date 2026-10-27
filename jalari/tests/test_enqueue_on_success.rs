mod common;

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use common::{Database, LEDGER, Rng, RunningWorker, fast_retry, seed, wait_until, worker_config};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    CancelOutcome, EnqueueOptions, Job, JobError, JobId, JobResult, Priority, RestartOutcome,
    StopOutcome, Worker,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Barrier;

const INTERRUPTED: &str = "the worker stopped during the last attempt";
const REFUSED: &str = "could not enqueue the jobs buffered for success";

static RELEASED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
static FIRST_RUNS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
static CROSSING: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(2));

fn release(label: &str) {
    RELEASED.lock().unwrap().insert(label.to_owned());
}

async fn released(label: &str) {
    while !RELEASED.lock().unwrap().contains(label) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct Spec {
    key: Option<String>,
    queue: Option<String>,
    delay_ms: Option<u64>,
    priority: Option<i8>,
    queue_key: Option<String>,
    expire_after_secs: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
enum Then {
    Succeed,
    FailFirst,
    Fail,
    FailPermanently,
    Sleep,
    Panic,
    Snooze,
    Discard,
    AwaitInterrupt,
    AwaitInterruptOnce,
    OkAfterInterrupt,
    WaitRelease,
    Cross,
}

#[derive(Serialize, Deserialize)]
struct Parent {
    label: String,
    children: Vec<Spec>,
    then: Then,
}

fn priority(level: i8) -> Priority {
    match level {
        -1 => Priority::Low,
        1 => Priority::High,
        2 => Priority::Urgent,
        _ => Priority::Normal,
    }
}

#[jalari::job(queue = "os_parent")]
impl Job for Parent {
    const NAME: &'static str = "os_parent";
    const MAX_ATTEMPTS: i32 = 3;

    async fn run(self) -> JobResult {
        let attempt = jalari::current_job().unwrap().attempt();
        for (index, spec) in self.children.iter().enumerate() {
            let mut options =
                EnqueueOptions::new().queue(spec.queue.as_deref().unwrap_or("os_child"));
            if let Some(key) = &spec.key {
                options = options.job_key(key);
            }
            if let Some(delay_ms) = spec.delay_ms {
                options = options.delay(Duration::from_millis(delay_ms));
            }
            if let Some(level) = spec.priority {
                options = options.priority(priority(level));
            }
            if let Some(queue_key) = &spec.queue_key {
                options = options.queue_key(queue_key).queue_key_limit(3);
            }
            if let Some(secs) = spec.expire_after_secs {
                options = options.expire_after(Duration::from_secs(secs));
            }
            let child = Child {
                label: format!("{}/{attempt}/{index}", self.label),
            };
            jalari::enqueue_on_success_with(&child, options)?;
        }
        LEDGER.record(&self.label, "buffered");
        match self.then {
            Then::Succeed => Ok(()),
            Then::FailFirst if attempt == 1 => Err(JobError::new("first attempt".to_owned())),
            Then::FailFirst => Ok(()),
            Then::Fail => Err(JobError::new("boom".to_owned())),
            Then::FailPermanently => Err(JobError::permanent(std::fmt::Error)),
            Then::Sleep => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(())
            }
            Then::Panic => panic!("parent panicked"),
            Then::Snooze => jalari::snooze(Duration::from_secs(3600)),
            Then::Discard => Err(JobError::discard(std::fmt::Error)),
            Then::AwaitInterrupt => Err(jalari::interrupted().await),
            Then::AwaitInterruptOnce => {
                if FIRST_RUNS.lock().unwrap().insert(self.label.clone()) {
                    return Err(jalari::interrupted().await);
                }
                Ok(())
            }
            Then::OkAfterInterrupt => {
                let _ = jalari::interrupted().await;
                Ok(())
            }
            Then::WaitRelease => {
                released(&self.label).await;
                Ok(())
            }
            Then::Cross => {
                CROSSING.wait().await;
                Ok(())
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Child {
    label: String,
}

#[jalari::job(queue = "os_child", priority = "low", fingerprint = true)]
impl Job for Child {
    const NAME: &'static str = "os_child";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "start");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Chain {
    key: String,
    generation: u32,
}

#[jalari::job(queue = "os_parent")]
impl Job for Chain {
    const NAME: &'static str = "os_chain";

    async fn run(self) -> JobResult {
        if self.generation < 5 {
            let next = Chain {
                key: self.key.clone(),
                generation: self.generation + 1,
            };
            jalari::enqueue_on_success_with(&next, EnqueueOptions::new().job_key(&self.key))?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Root {
    label: String,
}

#[jalari::job(queue = "os_parent")]
impl Job for Root {
    const NAME: &'static str = "os_root";

    async fn run(self) -> JobResult {
        let children = vec![
            Spec {
                priority: Some(2),
                queue: Some("os_idle".to_owned()),
                ..Spec::default()
            },
            Spec {
                queue: Some("os_idle".to_owned()),
                queue_key: Some(format!("{}:shard", self.label)),
                expire_after_secs: Some(3600),
                ..Spec::default()
            },
        ];
        jalari::enqueue_with(
            &Parent {
                label: self.label,
                children,
                then: Then::Succeed,
            },
            EnqueueOptions::new().priority(Priority::High),
        )
        .await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug)]
struct ChildRow {
    label: String,
    state: String,
    run_at: DateTime<Utc>,
    context: Option<Value>,
    fingerprint: Option<String>,
    priority: i16,
    parent_id: Option<i64>,
    root_id: Option<i64>,
    queue_key_limit: Option<i16>,
    expires_at: Option<DateTime<Utc>>,
}

struct Fixture {
    database: Database,
}

impl Fixture {
    fn pool(&self) -> &PgPool {
        &self.database.pool
    }

    async fn children(&self, parent: JobId) -> Vec<ChildRow> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT convert_from(payload, 'UTF8')::jsonb ->> 'label' AS label, state, run_at, context, fingerprint, priority, parent_id, root_id, queue_key_limit,
                 expires_at
             FROM {} WHERE parent_id = $1 ORDER BY id",
            self.database.table("job")
        )))
        .bind(parent.0)
        .fetch_all(self.pool())
        .await
        .unwrap()
    }

    async fn job(&self, id: JobId) -> (String, i32, Option<String>, DateTime<Utc>) {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state, attempts, last_error, updated_at FROM {} WHERE id = $1",
            self.database.table("job")
        )))
        .bind(id.0)
        .fetch_one(self.pool())
        .await
        .unwrap()
    }

    async fn history(&self, id: JobId) -> Vec<(String, i32, Option<String>, DateTime<Utc>)> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT state, attempt, error, created_at FROM {} WHERE job_id = $1 ORDER BY id",
            self.database.table("job_history")
        )))
        .bind(id.0)
        .fetch_all(self.pool())
        .await
        .unwrap()
    }

    async fn wait_state(&self, id: JobId, state: &str, timeout: Duration) {
        wait_until(&format!("job {id} to be {state}"), timeout, || async {
            self.job(id).await.0 == state
        })
        .await;
    }

    async fn rows_with_key(&self, key: &str) -> Vec<(i64, String, Option<i64>)> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT id, state, parent_id FROM {} WHERE job_key = $1 ORDER BY id",
            self.database.table("job")
        )))
        .bind(key)
        .fetch_all(self.pool())
        .await
        .unwrap()
    }
}

fn spec(queue: &str) -> Spec {
    Spec {
        queue: Some(queue.to_owned()),
        ..Spec::default()
    }
}

fn keyed(queue: &str, key: &str) -> Spec {
    Spec {
        key: Some(key.to_owned()),
        ..spec(queue)
    }
}

async fn parent(label: &str, children: Vec<Spec>, then: Then, options: EnqueueOptions) -> JobId {
    jalari::enqueue_with(
        &Parent {
            label: label.to_owned(),
            children,
            then,
        },
        options,
    )
    .await
    .unwrap()
    .id()
}

async fn buffered(label: &str) {
    wait_until(
        &format!("{label} to buffer its children"),
        Duration::from_secs(15),
        || async { LEDGER.count(label, "buffered") > 0 },
    )
    .await;
}

async fn spawn(queue: &str, concurrency: usize, poll_interval: Duration) -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, concurrency)
            .config(worker_config(poll_interval))
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_enqueue_on_success() {
    let Some(database) = Database::start("on_success", 20).await else {
        return;
    };
    let fixture = Fixture { database };
    let parents = spawn("os_parent", 8, Duration::from_millis(100)).await;
    let children = spawn("os_child", 4, Duration::from_secs(30)).await;

    check_children_follow_success(&fixture).await;
    check_children_dropped_on_other_outcomes(&fixture).await;
    check_ok_after_stop_still_inserts(&fixture).await;
    check_child_reuses_parent_job_key(&fixture).await;
    check_child_key_conflict_keeps_existing(&fixture).await;
    check_children_carry_priority_and_lineage(&fixture).await;
    check_crossing_keys_do_not_deadlock(&fixture).await;
    check_uncommitted_key_bounds_the_close(&fixture).await;
    check_stale_close_inserts_no_children(&fixture).await;
    check_abandoned_close_inserts_no_children(&fixture).await;
    check_stop_after_buffering_inserts_nothing(&fixture).await;

    parents.stop().await;
    children.stop().await;
    fixture.database.finish().await;
}

async fn check_children_follow_success(fixture: &Fixture) {
    jalari::enqueue(&Child {
        label: "warm_up".to_owned(),
    })
    .await
    .unwrap();
    wait_until(
        "the child worker to take a job",
        Duration::from_secs(15),
        || async { LEDGER.count("warm_up", "start") > 0 },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let context = json!({ "tenant": "acme" });
    let delayed = Spec {
        delay_ms: Some(2_000),
        ..spec("os_child")
    };
    let id = parent(
        "follow",
        vec![spec("os_child"), spec("os_child"), delayed],
        Then::Succeed,
        EnqueueOptions::new().context(&context),
    )
    .await;
    fixture
        .wait_state(id, "succeeded", Duration::from_secs(15))
        .await;
    let (_, _, _, succeeded_at) = fixture.job(id).await;
    let rows = fixture.children(id).await;
    assert_eq!(
        rows.iter()
            .map(|row| row.label.as_str())
            .collect::<Vec<_>>(),
        ["follow/1/0", "follow/1/1", "follow/1/2"]
    );
    let direct = jalari::enqueue_with(
        &Child {
            label: "direct".to_owned(),
        },
        EnqueueOptions::new().queue("os_idle").no_context(),
    )
    .await
    .unwrap()
    .id();
    let stored: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT fingerprint FROM {} WHERE id = $1",
        fixture.database.table("job")
    )))
    .bind(direct.0)
    .fetch_one(fixture.pool())
    .await
    .unwrap();
    assert!(stored.is_some());
    for row in &rows {
        assert_eq!(row.context.as_ref(), Some(&context), "{row:?}");
        assert_eq!(row.fingerprint, stored, "{row:?}");
        assert_eq!(row.parent_id, Some(id.0));
        assert_eq!(row.root_id, Some(id.0));
    }
    let delay = rows[2].run_at - succeeded_at;
    eprintln!("follow: delayed child runs {delay} after the parent succeeded");
    assert!(
        delay > chrono::Duration::milliseconds(1_900)
            && delay <= chrono::Duration::milliseconds(2_000),
        "{delay}"
    );
    assert_eq!(rows[2].state, "scheduled");
    for label in ["follow/1/0", "follow/1/1"] {
        wait_until(
            &format!("{label} to start"),
            Duration::from_secs(5),
            || async { LEDGER.count(label, "start") > 0 },
        )
        .await;
        eprintln!(
            "follow: {label} started, {} after the parent succeeded",
            Utc::now() - succeeded_at
        );
    }
}

async fn check_children_dropped_on_other_outcomes(fixture: &Fixture) {
    let three = || vec![spec("os_idle"), spec("os_idle"), spec("os_idle")];
    let once = EnqueueOptions::new().max_attempts(1);
    let retried = parent("retried", three(), Then::FailFirst, EnqueueOptions::new()).await;
    let ended = [
        (
            parent("failed", three(), Then::Fail, once.clone()).await,
            "failed",
        ),
        (
            parent(
                "permanent",
                three(),
                Then::FailPermanently,
                EnqueueOptions::new(),
            )
            .await,
            "failed",
        ),
        (
            parent(
                "timeout",
                three(),
                Then::Sleep,
                once.clone().timeout(Duration::from_millis(200)),
            )
            .await,
            "failed",
        ),
        (
            parent("panic", three(), Then::Panic, once.clone()).await,
            "failed",
        ),
        (
            parent("snooze", three(), Then::Snooze, EnqueueOptions::new()).await,
            "scheduled",
        ),
        (
            parent("discard", three(), Then::Discard, EnqueueOptions::new()).await,
            "discarded",
        ),
    ];
    let release = spawn("os_release", 1, Duration::from_millis(100)).await;
    let released_job = parent(
        "release",
        three(),
        Then::AwaitInterrupt,
        EnqueueOptions::new().queue("os_release"),
    )
    .await;
    buffered("release").await;
    release.stop().await;
    for (id, state) in ended.into_iter().chain([(released_job, "enqueued")]) {
        fixture.wait_state(id, state, Duration::from_secs(15)).await;
        assert!(fixture.children(id).await.is_empty(), "{id} ended {state}");
    }
    assert_eq!(fixture.job(released_job).await.1, 0);
    fixture
        .wait_state(retried, "succeeded", Duration::from_secs(15))
        .await;
    let labels: Vec<String> = fixture
        .children(retried)
        .await
        .into_iter()
        .map(|row| row.label)
        .collect();
    assert_eq!(labels, ["retried/2/0", "retried/2/1", "retried/2/2"]);
    for id in [ended_id(fixture, "snooze").await, released_job] {
        assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Cancelled);
    }
}

async fn ended_id(fixture: &Fixture, label: &str) -> JobId {
    let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT id FROM {} WHERE task = 'os_parent'
             AND convert_from(payload, 'UTF8')::jsonb ->> 'label' = $1",
        fixture.database.table("job")
    )))
    .bind(label)
    .fetch_one(fixture.pool())
    .await
    .unwrap();
    JobId(id)
}

async fn check_ok_after_stop_still_inserts(fixture: &Fixture) {
    let id = parent(
        "ok_after_stop",
        vec![spec("os_idle"), spec("os_idle")],
        Then::OkAfterInterrupt,
        EnqueueOptions::new(),
    )
    .await;
    buffered("ok_after_stop").await;
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    fixture
        .wait_state(id, "succeeded", Duration::from_secs(15))
        .await;
    assert_eq!(fixture.children(id).await.len(), 2);
}

async fn check_child_reuses_parent_job_key(fixture: &Fixture) {
    let key = "chain:reuse";
    jalari::enqueue_with(
        &Chain {
            key: key.to_owned(),
            generation: 1,
        },
        EnqueueOptions::new().job_key(key),
    )
    .await
    .unwrap();
    wait_until("five generations", Duration::from_secs(15), || async {
        let rows = fixture.rows_with_key(key).await;
        rows.len() == 5 && rows.iter().all(|(_, state, _)| state == "succeeded")
    })
    .await;
    let rows = fixture.rows_with_key(key).await;
    for pair in rows.windows(2) {
        assert_eq!(pair[1].2, Some(pair[0].0), "{rows:?}");
    }
    assert_eq!(rows[0].2, None);
}

async fn check_child_key_conflict_keeps_existing(fixture: &Fixture) {
    let holder = jalari::enqueue_with(
        &Child {
            label: "holder".to_owned(),
        },
        EnqueueOptions::new().queue("os_idle").job_key("held"),
    )
    .await
    .unwrap()
    .id();
    let id = parent(
        "conflict",
        vec![keyed("os_idle", "held"), spec("os_idle")],
        Then::Succeed,
        EnqueueOptions::new(),
    )
    .await;
    fixture
        .wait_state(id, "succeeded", Duration::from_secs(15))
        .await;
    let labels: Vec<String> = fixture
        .children(id)
        .await
        .into_iter()
        .map(|row| row.label)
        .collect();
    assert_eq!(labels, ["conflict/1/1"]);
    assert_eq!(
        fixture.rows_with_key("held").await,
        [(holder.0, "enqueued".to_owned(), None)]
    );
}

async fn check_children_carry_priority_and_lineage(fixture: &Fixture) {
    let root = jalari::enqueue(&Root {
        label: "lineage".to_owned(),
    })
    .await
    .unwrap()
    .id();
    let parent = ended_id_eventually(fixture, "lineage").await;
    fixture
        .wait_state(parent, "succeeded", Duration::from_secs(15))
        .await;
    let rows = fixture.children(parent).await;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.parent_id, Some(parent.0));
        assert_eq!(row.root_id, Some(root.0));
    }
    assert_eq!(rows[0].priority, 2);
    assert_eq!(rows[1].priority, -1);
    assert_eq!(rows[0].queue_key_limit, None);
    assert_eq!(rows[1].queue_key_limit, Some(3));
    assert_eq!(rows[0].expires_at, None);
    let (_, _, _, succeeded_at) = fixture.job(parent).await;
    let expiry = rows[1].expires_at.unwrap() - succeeded_at;
    assert!(
        (expiry - chrono::Duration::hours(1)).num_seconds().abs() <= 1,
        "{expiry}"
    );
}

async fn ended_id_eventually(fixture: &Fixture, label: &str) -> JobId {
    wait_until(
        &format!("a parent labelled {label}"),
        Duration::from_secs(15),
        || async { LEDGER.count(label, "buffered") > 0 },
    )
    .await;
    ended_id(fixture, label).await
}

async fn check_crossing_keys_do_not_deadlock(fixture: &Fixture) {
    let deadlocks = fixture.database.deadlocks().await;
    let started = Instant::now();
    for round in 0..200 {
        let a = format!("cross:{round}:a");
        let b = format!("cross:{round}:b");
        let first = parent(
            &format!("cross/{round}/ab"),
            vec![keyed("os_idle", &a), keyed("os_idle", &b)],
            Then::Cross,
            EnqueueOptions::new(),
        )
        .await;
        let second = parent(
            &format!("cross/{round}/ba"),
            vec![keyed("os_idle", &b), keyed("os_idle", &a)],
            Then::Cross,
            EnqueueOptions::new(),
        )
        .await;
        for id in [first, second] {
            fixture
                .wait_state(id, "succeeded", Duration::from_secs(15))
                .await;
            assert_eq!(fixture.job(id).await.1, 1, "round {round}: {id} retried");
        }
        let inserted = fixture.children(first).await.len() + fixture.children(second).await.len();
        assert_eq!(inserted, 2, "round {round}");
    }
    eprintln!(
        "crossing: 200 rounds in {:?}, database deadlock count {} -> {}",
        started.elapsed(),
        deadlocks,
        fixture.database.deadlocks().await
    );
}

async fn check_uncommitted_key_bounds_the_close(fixture: &Fixture) {
    let holder_pool = fixture.database.connect("holder", 1).await;
    let mut transaction = holder_pool.begin().await.unwrap();
    jalari::enqueue_in(
        &mut transaction,
        &Child {
            label: "uncommitted".to_owned(),
        },
        EnqueueOptions::new().queue("os_idle").job_key("pending:k"),
    )
    .await
    .unwrap();
    let id = parent(
        "bounded",
        vec![keyed("os_idle", "pending:k")],
        Then::Succeed,
        EnqueueOptions::new(),
    )
    .await;
    buffered("bounded").await;
    let buffered_at = Utc::now();
    wait_until(
        "the refused close to schedule a retry",
        Duration::from_secs(10),
        || async { !fixture.history(id).await.is_empty() },
    )
    .await;
    let history = fixture.history(id).await;
    let refused_after = history[0].3 - buffered_at;
    eprintln!("bounded: refused {refused_after} after buffering: {history:?}");
    assert_eq!(history[0].0, "scheduled");
    assert!(history[0].2.as_deref().unwrap().starts_with(REFUSED));
    assert!(
        refused_after >= chrono::Duration::milliseconds(1_900)
            && refused_after < chrono::Duration::seconds(4),
        "{refused_after}"
    );
    transaction.commit().await.unwrap();
    fixture
        .wait_state(id, "succeeded", Duration::from_secs(15))
        .await;
    assert_eq!(fixture.job(id).await.1, 2);
    assert!(fixture.children(id).await.is_empty());
    assert_eq!(fixture.rows_with_key("pending:k").await.len(), 1);

    let mut transaction = holder_pool.begin().await.unwrap();
    jalari::enqueue_in(
        &mut transaction,
        &Child {
            label: "committed_soon".to_owned(),
        },
        EnqueueOptions::new()
            .queue("os_idle")
            .job_key("pending:soon"),
    )
    .await
    .unwrap();
    let id = parent(
        "bounded_soon",
        vec![keyed("os_idle", "pending:soon")],
        Then::Succeed,
        EnqueueOptions::new(),
    )
    .await;
    buffered("bounded_soon").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    transaction.commit().await.unwrap();
    fixture
        .wait_state(id, "succeeded", Duration::from_secs(15))
        .await;
    let history = fixture.history(id).await;
    assert_eq!(history.len(), 1, "{history:?}");
    assert_eq!(history[0].1, 1);
    assert!(fixture.children(id).await.is_empty());
    holder_pool.close().await;
}

async fn check_stale_close_inserts_no_children(fixture: &Fixture) {
    let id = parent(
        "stale",
        vec![spec("os_idle")],
        Then::WaitRelease,
        EnqueueOptions::new(),
    )
    .await;
    buffered("stale").await;
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET lock_token = lock_token + 1000000 WHERE id = $1",
        fixture.database.table("job")
    )))
    .bind(id.0)
    .execute(fixture.pool())
    .await
    .unwrap();
    release("stale");
    wait_until(
        "the stale run to be released",
        Duration::from_secs(10),
        || async { !fixture.history(id).await.is_empty() },
    )
    .await;
    let history = fixture.history(id).await;
    assert_eq!(history.len(), 1, "{history:?}");
    assert_eq!(history[0].2.as_deref(), Some(INTERRUPTED));
    assert!(fixture.children(id).await.is_empty());
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Cancelled);
}

async fn check_abandoned_close_inserts_no_children(fixture: &Fixture) {
    let id = parent(
        "abandoned",
        vec![spec("os_idle")],
        Then::WaitRelease,
        EnqueueOptions::new().max_attempts(1),
    )
    .await;
    buffered("abandoned").await;
    let holder_pool = fixture.database.connect("row_holder", 1).await;
    let mut holder = holder_pool.begin().await.unwrap();
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "SELECT 1 FROM {} WHERE id = $1 FOR UPDATE",
        fixture.database.table("job")
    )))
    .bind(id.0)
    .execute(&mut *holder)
    .await
    .unwrap();
    release("abandoned");
    for close in 1..=5 {
        let blocked = blocked_by(fixture.pool(), holder_pid, close).await;
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(blocked)
            .fetch_one(fixture.pool())
            .await
            .unwrap();
        assert!(terminated);
    }
    holder.rollback().await.unwrap();
    holder_pool.close().await;
    fixture
        .wait_state(id, "failed", Duration::from_secs(10))
        .await;
    let history: Vec<(String, i32, Option<String>)> = fixture
        .history(id)
        .await
        .into_iter()
        .map(|(state, attempt, error, _)| (state, attempt, error))
        .collect();
    assert_eq!(
        history,
        [("failed".to_owned(), 1, Some(INTERRUPTED.to_owned()))]
    );
    assert!(fixture.children(id).await.is_empty());
}

async fn check_stop_after_buffering_inserts_nothing(fixture: &Fixture) {
    let worker = spawn("os_stop", 50, Duration::from_millis(100)).await;
    let mut rng = Rng::new(seed("stop_after_buffering"));
    let mut ids = Vec::new();
    for round in 0..50 {
        let id = parent(
            &format!("stopped/{round}"),
            vec![spec("os_idle"), spec("os_idle")],
            Then::AwaitInterruptOnce,
            EnqueueOptions::new().queue("os_stop"),
        )
        .await;
        ids.push((round, id, rng.millis(0, 200)));
    }
    let mut stops = Vec::new();
    for (round, id, pause) in ids.clone() {
        stops.push(tokio::spawn(async move {
            buffered(&format!("stopped/{round}")).await;
            tokio::time::sleep(pause).await;
            jalari::stop(id).await.unwrap()
        }));
    }
    for stop in stops {
        assert_eq!(stop.await.unwrap(), StopOutcome::Requested);
    }
    for (round, id, _) in &ids {
        fixture
            .wait_state(*id, "stopped", Duration::from_secs(15))
            .await;
        assert!(fixture.children(*id).await.is_empty(), "round {round}");
    }
    for (_, id, _) in &ids {
        assert_eq!(
            jalari::restart(*id).await.unwrap(),
            RestartOutcome::Restarted
        );
    }
    for (round, id, _) in &ids {
        fixture
            .wait_state(*id, "succeeded", Duration::from_secs(15))
            .await;
        assert_eq!(fixture.children(*id).await.len(), 2, "round {round}");
    }
    worker.stop().await;
}

async fn blocked_by(pool: &PgPool, holder_pid: i32, close: u32) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let blocked: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
        )
        .bind(holder_pid)
        .fetch_optional(pool)
        .await
        .unwrap();
        if let Some(blocked) = blocked {
            return blocked;
        }
        assert!(
            Instant::now() < deadline,
            "close {close} never waited on the row"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
