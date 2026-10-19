mod common;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{
    Database, LEDGER, OCCUPANCY, Rng, RunningWorker, Seat, fast_retry, wait_until, worker_config,
};
use jalari::sqlx::types::chrono::Utc;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    EnqueueOptions, EnqueueOutcome, ErrorKind, Job, JobError, JobId, JobResult, SCHEMA_VERSION,
    Schema, Worker,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;

static ZOMBIE_UNTIL: LazyLock<Mutex<Option<Instant>>> = LazyLock::new(|| Mutex::new(None));

fn enter(prefix: &str, queue_key: Option<&str>) -> (Seat, String) {
    let job = jalari::current_job().unwrap();
    let mut keys = vec![format!("{prefix}:job:{}", job.id())];
    if let Some(queue_key) = queue_key {
        keys.push(format!("{prefix}:qk:{queue_key}"));
    }
    let holder = format!("job {} attempt {}", job.id(), job.attempt());
    (
        OCCUPANCY.enter(keys, holder),
        format!("{prefix}:{}", job.id()),
    )
}

#[derive(Serialize, Deserialize)]
struct Chore {
    queue_key: Option<String>,
    millis: u64,
    fail_first: bool,
    permanent: bool,
}

#[jalari::job(queue = "chores")]
impl Job for Chore {
    const NAME: &'static str = "rm_chore";
    const MAX_ATTEMPTS: i32 = 3;

    async fn run(self) -> JobResult {
        let (_seat, label) = enter("chore", self.queue_key.as_deref());
        let attempt = jalari::current_job().unwrap().attempt();
        LEDGER.record(&label, "start");
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        if self.permanent {
            LEDGER.record(&label, "gave_up");
            return Err(JobError::permanent(io::Error::other("permanent chore")));
        }
        if self.fail_first && attempt == 1 {
            LEDGER.record(&label, "failed");
            return Err(JobError::new("first attempt fails".to_owned()));
        }
        LEDGER.record(&label, "done");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Zombie {
    label: String,
}

#[jalari::job(queue = "fence")]
impl Job for Zombie {
    const NAME: &'static str = "rm_zombie";
    const MAX_ATTEMPTS: i32 = 1;

    async fn run(self) -> JobResult {
        let (seat, _) = enter("fence", Some(&self.label));
        let label = format!("zombie:{}", self.label);
        LEDGER.record(&label, "start");
        tokio::task::spawn_blocking(move || {
            let _seat = seat;
            let started = Instant::now();
            loop {
                let until = *ZOMBIE_UNTIL.lock().unwrap();
                if until.is_some_and(|until| Instant::now() >= until)
                    || started.elapsed() > Duration::from_secs(15)
                {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            LEDGER.record(&label, "end");
        })
        .await
        .unwrap();
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Follower {
    label: String,
}

#[jalari::job(queue = "fence")]
impl Job for Follower {
    const NAME: &'static str = "rm_follower";

    async fn run(self) -> JobResult {
        let (_seat, _) = enter("fence", Some(&self.label));
        let label = format!("follower:{}", self.label);
        LEDGER.record(&label, "start");
        tokio::time::sleep(Duration::from_millis(200)).await;
        LEDGER.record(&label, "done");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Sip {
    millis: u64,
}

#[jalari::job(queue = "sips")]
impl Job for Sip {
    const NAME: &'static str = "rm_sip";

    async fn run(self) -> JobResult {
        let (_seat, label) = enter("sip", None);
        LEDGER.record(&label, "start");
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        LEDGER.record(&label, "done");
        Ok(())
    }
}

fn selected(name: &str) -> bool {
    std::env::var("RACE_ONLY").map_or(true, |only| only == name)
}

async fn eventually<F, Fut>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn execute(pool: &PgPool, sql: String) {
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap();
}

async fn scalar(pool: &PgPool, sql: String) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn install_audit(database: &Database) {
    let schema = &database.schema_name;
    execute(
        &database.pool,
        format!(
            "CREATE TABLE \"{schema}\".race_deleted (
                 id BIGSERIAL PRIMARY KEY,
                 job_id BIGINT NOT NULL,
                 task TEXT NOT NULL,
                 state TEXT NOT NULL
             );
             CREATE FUNCTION \"{schema}\".race_deleted() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 INSERT INTO \"{schema}\".race_deleted (job_id, task, state)
                 VALUES (OLD.id, OLD.task, OLD.state);
                 RETURN OLD;
             END $$;
             CREATE TRIGGER race_deleted AFTER DELETE ON \"{schema}\".job
                 FOR EACH ROW EXECUTE FUNCTION \"{schema}\".race_deleted();
             CREATE TABLE \"{schema}\".race_claim (
                 id BIGSERIAL PRIMARY KEY,
                 gap_ms BIGINT,
                 interval_ms BIGINT NOT NULL
             );
             CREATE FUNCTION \"{schema}\".race_claim() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 INSERT INTO \"{schema}\".race_claim (gap_ms, interval_ms)
                 VALUES (
                     (EXTRACT(EPOCH FROM NEW.last_housekeeping_at - OLD.last_housekeeping_at)
                         * 1000)::bigint,
                     (EXTRACT(EPOCH FROM NEW.housekeeping_interval) * 1000)::bigint
                 );
                 RETURN NEW;
             END $$;
             CREATE TRIGGER race_claim AFTER UPDATE OF last_housekeeping_at ON \"{schema}\".config
                 FOR EACH ROW
                 WHEN (OLD.last_housekeeping_at IS DISTINCT FROM NEW.last_housekeeping_at)
                 EXECUTE FUNCTION \"{schema}\".race_claim();"
        ),
    )
    .await;
}

async fn restore_cluster() {
    jalari::cluster::update(|cluster| {
        cluster.housekeeping_enabled = false;
        cluster.housekeeping_interval = Duration::from_secs(600);
        cluster.succeeded_retention = Duration::from_secs(86_400);
        cluster.deleted_retention = Duration::from_secs(7 * 86_400);
        cluster.failed_retention = None;
        cluster.worker_timeout = Duration::from_secs(300);
    })
    .await
    .unwrap();
}

async fn sweep_everything(enabled: bool) {
    jalari::cluster::update(|cluster| {
        cluster.housekeeping_enabled = enabled;
        cluster.housekeeping_interval = Duration::from_secs(1);
        cluster.succeeded_retention = Duration::ZERO;
        cluster.deleted_retention = Duration::ZERO;
        cluster.failed_retention = Some(Duration::ZERO);
    })
    .await
    .unwrap();
}

async fn spawn_worker(queue: &str, concurrency: usize) -> RunningWorker {
    RunningWorker::spawn(
        Worker::builder()
            .queue(queue, concurrency)
            .config(worker_config(Duration::from_millis(100)))
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

fn occupancy_violations(prefix: &str) -> Vec<String> {
    OCCUPANCY
        .violations()
        .into_iter()
        .filter(|violation| violation.starts_with(prefix))
        .map(|violation| format!("overlap: {violation}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn test_race_maintenance() {
    let Some(database) = Database::start("maint", 12).await else {
        return;
    };
    install_audit(&database).await;
    let seed = common::seed("test_race_maintenance");
    let mut rng = Rng::new(seed);
    let started = Instant::now();
    let mut failures = Vec::new();
    let mut report = |name: &str, violations: Vec<String>, at: Instant| {
        eprintln!(
            "{name}: {} violation(s) in {:?}",
            violations.len(),
            at.elapsed()
        );
        failures.extend(
            violations
                .into_iter()
                .map(|violation| format!("[{name} seed {seed}] {violation}")),
        );
    };

    if selected("sweep_spares_live_jobs") {
        let at = Instant::now();
        let violations = check_sweep_spares_live_jobs(&database, rng.fork(1)).await;
        report("sweep_spares_live_jobs", violations, at);
    }
    if selected("sweep_keeps_queue_key_fence") {
        let at = Instant::now();
        let violations = check_sweep_keeps_queue_key_fence(&database, false).await;
        report("sweep_keeps_queue_key_fence control", violations, at);
        let at = Instant::now();
        let violations = check_sweep_keeps_queue_key_fence(&database, true).await;
        report("sweep_keeps_queue_key_fence", violations, at);
    }
    if selected("cluster_update_keeps_every_change") {
        let at = Instant::now();
        let violations = check_cluster_update_keeps_every_change().await;
        report("cluster_update_keeps_every_change", violations, at);
    }
    if selected("heartbeat_churn") {
        let at = Instant::now();
        let violations = check_heartbeat_churn(&database, rng.fork(2)).await;
        report("heartbeat_churn", violations, at);
    }
    if selected("listed_workers_are_fresh") {
        let at = Instant::now();
        let violations = check_listed_workers_are_fresh(&database).await;
        report("listed_workers_are_fresh", violations, at);
    }
    if selected("concurrent_migrations") {
        let at = Instant::now();
        let violations = check_concurrent_migrations(&database).await;
        report("concurrent_migrations", violations, at);
    }
    if selected("migrate_while_working") {
        let at = Instant::now();
        let violations = check_migrate_while_working(&database, rng.fork(3)).await;
        report("migrate_while_working", violations, at);
    }

    eprintln!("test_race_maintenance finished in {:?}", started.elapsed());
    database.finish().await;
    assert!(
        failures.is_empty(),
        "{} violation(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

struct Tracked {
    id: JobId,
    fail_first: bool,
    permanent: bool,
}

async fn check_sweep_spares_live_jobs(database: &Database, mut rng: Rng) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let deleted_before = scalar(
        &database.pool,
        format!(
            "SELECT coalesce(max(id), 0) FROM {}",
            database.table("race_deleted")
        ),
    )
    .await;
    let claims_before = scalar(
        &database.pool,
        format!(
            "SELECT coalesce(max(id), 0) FROM {}",
            database.table("race_claim")
        ),
    )
    .await;
    sweep_everything(true).await;
    let mut workers = Vec::new();
    for _ in 0..3 {
        workers.push(spawn_worker("chores", 2).await);
    }

    let deadline = Instant::now() + Duration::from_secs(6 * scale);
    let producers: Vec<_> = (0..3)
        .map(|producer| {
            let mut rng = rng.fork(producer);
            tokio::spawn(async move {
                let mut tracked = Vec::new();
                while Instant::now() < deadline {
                    let queue_key = rng.chance(25).then(|| format!("hk-qk-{}", rng.below(3)));
                    let mut options = EnqueueOptions::new();
                    if let Some(queue_key) = &queue_key {
                        options = options.queue_key(queue_key);
                    }
                    if rng.chance(25) {
                        options = options.job_key(&format!("hk-jk-{}", rng.below(4)));
                    }
                    let chore = Chore {
                        queue_key,
                        millis: rng.below(120),
                        fail_first: rng.chance(20),
                        permanent: rng.chance(5),
                    };
                    let (fail_first, permanent) = (chore.fail_first, chore.permanent);
                    if let EnqueueOutcome::Inserted(id) =
                        jalari::enqueue_with(&chore, options).await.unwrap()
                    {
                        tracked.push(Tracked {
                            id,
                            fail_first,
                            permanent,
                        });
                    }
                    tokio::time::sleep(rng.millis(0, 60)).await;
                }
                tracked
            })
        })
        .collect();
    let mut tracked = Vec::new();
    for producer in producers {
        tracked.extend(producer.await.unwrap());
    }

    let resolved = eventually(Duration::from_secs(45 * scale), || {
        let tracked = &tracked;
        async move {
            tracked.iter().all(|job| {
                let label = format!("chore:{}", job.id);
                LEDGER.count(&label, "done") + LEDGER.count(&label, "gave_up") > 0
            })
        }
    })
    .await;
    if !resolved {
        violations.push("not every enqueued chore finished within the time limit".to_owned());
    }
    let swept = eventually(Duration::from_secs(5), || async {
        scalar(
            &database.pool,
            format!(
                "SELECT count(*) FROM {} WHERE task = 'rm_chore'",
                database.table("job")
            ),
        )
        .await
            == 0
    })
    .await;
    if !swept {
        let rows: Vec<common::JobRow> = database
            .jobs()
            .await
            .into_iter()
            .filter(|job| job.task == "rm_chore")
            .collect();
        violations.push(format!(
            "finished chores were not swept with zero retention: {rows:?}"
        ));
    }
    for worker in workers {
        worker.stop().await;
    }
    restore_cluster().await;

    for job in &tracked {
        let label = format!("chore:{}", job.id);
        let starts = LEDGER.count(&label, "start");
        let done = LEDGER.count(&label, "done");
        let gave_up = LEDGER.count(&label, "gave_up");
        let expected = if job.permanent { (0, 1) } else { (1, 0) };
        if (done, gave_up) != expected {
            violations.push(format!(
                "job {} (fail_first {}, permanent {}) finished {done} time(s) and gave up {gave_up} \
                 time(s), expected {expected:?}",
                job.id, job.fail_first, job.permanent
            ));
        }
        if starts > 3 {
            violations.push(format!(
                "job {} started {starts} attempts with MAX_ATTEMPTS 3",
                job.id
            ));
        }
        if job.fail_first && !job.permanent && starts < 2 {
            violations.push(format!(
                "job {} failed its first attempt but started only {starts} time(s)",
                job.id
            ));
        }
    }

    let live_deleted: Vec<(i64, String)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job_id, state FROM {} WHERE id > $1 AND state IN ('scheduled', 'enqueued')",
        database.table("race_deleted")
    )))
    .bind(deleted_before)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    for (job_id, state) in live_deleted {
        violations.push(format!(
            "housekeeping deleted job {job_id} while it was {state} (docs/guide.md:543-544)"
        ));
    }
    let deleted = scalar(
        &database.pool,
        format!(
            "SELECT count(*) FROM {} WHERE id > {deleted_before}",
            database.table("race_deleted")
        ),
    )
    .await;
    let claims: Vec<(Option<i64>, i64)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT gap_ms, interval_ms FROM {} WHERE id > $1 ORDER BY id",
        database.table("race_claim")
    )))
    .bind(claims_before)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    for (gap_ms, interval_ms) in &claims {
        if let Some(gap_ms) = gap_ms
            && gap_ms < interval_ms
        {
            violations.push(format!(
                "housekeeping was claimed {gap_ms} ms after the previous claim with an interval \
                 of {interval_ms} ms (docs/guide.md:543)"
            ));
        }
    }
    let orphans = scalar(
        &database.pool,
        format!(
            "SELECT count(*) FROM {} AS history
             WHERE NOT EXISTS (SELECT 1 FROM {} AS job WHERE job.id = history.job_id)",
            database.table("job_history"),
            database.table("job")
        ),
    )
    .await;
    if orphans > 0 {
        violations.push(format!("{orphans} history row(s) outlived their job"));
    }
    eprintln!(
        "sweep_spares_live_jobs: {} jobs tracked, {deleted} rows swept, {} claims",
        tracked.len(),
        claims.len()
    );
    if deleted == 0 || claims.len() < 2 {
        violations.push(format!(
            "housekeeping barely ran: {deleted} deletions, {} claims",
            claims.len()
        ));
    }
    violations.extend(occupancy_violations("chore:"));
    violations
}

async fn job_state(database: &Database, id: JobId) -> Option<String> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT state FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_optional(&database.pool)
    .await
    .unwrap()
}

async fn check_sweep_keeps_queue_key_fence(database: &Database, sweep: bool) -> Vec<String> {
    let mut violations = Vec::new();
    let label = format!("fence-{}", if sweep { "sweep" } else { "control" });
    sweep_everything(sweep).await;
    let workers = [
        spawn_worker("fence", 1).await,
        spawn_worker("fence", 1).await,
    ];
    *ZOMBIE_UNTIL.lock().unwrap() = None;

    let zombie = jalari::enqueue_with(
        &Zombie {
            label: label.clone(),
        },
        EnqueueOptions::new().queue_key(&label),
    )
    .await
    .unwrap()
    .id();
    let zombie_label = format!("zombie:{label}");
    wait_until("the zombie to start", Duration::from_secs(10), || async {
        LEDGER.count(&zombie_label, "start") == 1
    })
    .await;
    let zombie_started = LEDGER.events(&zombie_label)[0].1;
    tokio::time::sleep_until((zombie_started + Duration::from_millis(1700)).into()).await;
    let killed = eventually(Duration::from_secs(3), || async {
        database
            .terminate_job_sessions(1, Duration::from_millis(200))
            .await
            == 1
    })
    .await;
    let lost = Instant::now();
    *ZOMBIE_UNTIL.lock().unwrap() = Some(lost + Duration::from_millis(2800));
    if !killed {
        violations.push("could not end the zombie's session".to_owned());
    }

    wait_until(
        "the interrupted zombie to fail",
        Duration::from_secs(15),
        || async {
            job_state(database, zombie)
                .await
                .is_none_or(|state| state == "failed")
        },
    )
    .await;
    let failed_seen = Instant::now();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let zombie_row = job_state(database, zombie).await;
    jalari::enqueue_with(
        &Follower {
            label: label.clone(),
        },
        EnqueueOptions::new().queue_key(&label),
    )
    .await
    .unwrap();
    let follower_label = format!("follower:{label}");
    wait_until(
        "the follower to finish",
        Duration::from_secs(20),
        || async { LEDGER.count(&follower_label, "done") == 1 },
    )
    .await;
    wait_until("the zombie run to end", Duration::from_secs(15), || async {
        LEDGER.count(&zombie_label, "end") == 1
    })
    .await;
    for worker in workers {
        worker.stop().await;
    }
    restore_cluster().await;

    let follower_started = LEDGER.events(&follower_label)[0].1;
    let zombie_ended = LEDGER
        .events(&zombie_label)
        .iter()
        .find(|(event, _)| *event == "end")
        .map(|(_, at)| *at);
    let since_lost = |at: Instant| at.saturating_duration_since(lost);
    let timeline = format!(
        "zombie job {zombie} lost its session at L, was found failed at L+{:?}, its row was \
         {zombie_row:?} when the follower was enqueued, the follower started at L+{:?}, the \
         zombie run ended at L+{:?}",
        since_lost(failed_seen),
        since_lost(follower_started),
        zombie_ended.map(since_lost)
    );
    eprintln!("{label}: {timeline}");
    if zombie_ended.is_none_or(|ended| ended > lost + Duration::from_secs(3)) {
        eprintln!("{label}: the zombie outlived the 3 s abort bound, overlap is inconclusive");
    }
    let waited = follower_started.saturating_duration_since(failed_seen);
    if waited + Duration::from_millis(500) < Duration::from_secs(6) {
        violations.push(format!(
            "a job with the interrupted job's queue_key started {waited:?} after the interruption \
             was recorded, expected at least 6 s (docs/guide.md:324-326); {timeline}"
        ));
    }
    violations.extend(
        occupancy_violations("fence:qk:")
            .into_iter()
            .filter(|violation| violation.contains(&label))
            .map(|violation| format!("{violation}; {timeline}")),
    );
    violations
}

async fn check_cluster_update_keeps_every_change() -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    restore_cluster().await;
    let before = jalari::cluster::get().await.unwrap();
    let increments = 24 * u32::try_from(scale).unwrap();
    let stop = Arc::new(AtomicBool::new(false));

    let readers: Vec<_> = (0..3)
        .map(|_| {
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut seen = Vec::new();
                while !stop.load(Ordering::SeqCst) {
                    seen.push(jalari::cluster::get().await.unwrap().succeeded_retention);
                }
                seen
            })
        })
        .collect();
    let writers: Vec<_> = (0..increments)
        .map(|_| {
            tokio::spawn(async move {
                jalari::cluster::update(|cluster| {
                    cluster.succeeded_retention += Duration::from_secs(1);
                })
                .await
                .map(|saved| saved.succeeded_retention)
            })
        })
        .collect();
    let invalid: Vec<_> = (0..8)
        .map(|_| {
            tokio::spawn(async move {
                jalari::cluster::update(|cluster| {
                    cluster.succeeded_retention += Duration::from_secs(100_000);
                    cluster.worker_timeout = Duration::ZERO;
                })
                .await
            })
        })
        .collect();
    let togglers: Vec<_> = (0..4u64)
        .map(|n| {
            tokio::spawn(async move {
                jalari::cluster::update(|cluster| {
                    cluster.housekeeping_interval = Duration::from_secs(600 + n);
                })
                .await
                .map(|saved| saved.succeeded_retention)
            })
        })
        .collect();

    let mut saved = Vec::new();
    for writer in writers {
        match writer.await.unwrap() {
            Ok(retention) => saved.push(retention),
            Err(e) => violations.push(format!("an increment failed: {e}")),
        }
    }
    for task in invalid {
        match task.await.unwrap() {
            Err(e) if e.kind == ErrorKind::InvalidConfigValue => {}
            other => violations.push(format!(
                "an update with worker_timeout 0 returned {other:?}, expected InvalidConfigValue"
            )),
        }
    }
    for toggler in togglers {
        if let Err(e) = toggler.await.unwrap() {
            violations.push(format!("an unrelated update failed: {e}"));
        }
    }
    stop.store(true, Ordering::SeqCst);
    for reader in readers {
        let seen = reader.await.unwrap();
        if let Some(pair) = seen.windows(2).find(|pair| pair[1] < pair[0]) {
            violations.push(format!(
                "a reader saw succeeded_retention go back from {:?} to {:?}",
                pair[0], pair[1]
            ));
        }
        if let Some(bad) = seen
            .iter()
            .find(|retention| **retention >= before.succeeded_retention + Duration::from_secs(1000))
        {
            violations.push(format!(
                "a reader saw {bad:?}, the value of a rejected update"
            ));
        }
    }

    let after = jalari::cluster::get().await.unwrap();
    let expected = before.succeeded_retention + Duration::from_secs(u64::from(increments));
    if after.succeeded_retention != expected {
        violations.push(format!(
            "after {increments} concurrent increments succeeded_retention is {:?}, expected \
             {expected:?} (jalari/src/cluster/api.rs:21-22)",
            after.succeeded_retention
        ));
    }
    let mut saved_sorted = saved.clone();
    saved_sorted.sort();
    saved_sorted.dedup();
    let expected_saved: Vec<Duration> = (1..=increments)
        .map(|k| before.succeeded_retention + Duration::from_secs(u64::from(k)))
        .collect();
    if saved_sorted != expected_saved {
        violations.push(format!(
            "increments returned {saved:?}, expected each of {expected_saved:?} exactly once"
        ));
    }
    if after.worker_timeout != before.worker_timeout {
        violations.push(format!(
            "worker_timeout changed from {:?} to {:?} although every change to it was rejected",
            before.worker_timeout, after.worker_timeout
        ));
    }
    if !(600..604).contains(&after.housekeeping_interval.as_secs()) {
        violations.push(format!(
            "housekeeping_interval is {:?}, expected one of the values written",
            after.housekeeping_interval
        ));
    }
    restore_cluster().await;
    violations
}

struct Snapshot {
    requested: Instant,
    rows: Vec<(String, String, bool)>,
}

enum Churn {
    Started(String, Instant),
    Stopped(String, Instant),
    Aged(String, Instant),
}

async fn check_heartbeat_churn(database: &Database, mut rng: Rng) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    jalari::cluster::update(|cluster| cluster.worker_timeout = Duration::from_secs(1))
        .await
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut snapshots = Vec::new();
            while !stop.load(Ordering::SeqCst) {
                let requested = Instant::now();
                let fresh_after = Utc::now().timestamp() - 1800;
                let rows = jalari::admin::workers()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|worker| {
                        let queue = worker
                            .queues
                            .first()
                            .map_or_else(String::new, |queue| queue.name.clone());
                        (
                            worker.id,
                            queue,
                            worker.heartbeat_at.timestamp() > fresh_after,
                        )
                    })
                    .collect();
                snapshots.push(Snapshot { requested, rows });
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            snapshots
        })
    };

    let mut alive: Vec<(String, RunningWorker)> = Vec::new();
    let mut churn = Vec::new();
    let mut next = 0;
    let deadline = Instant::now() + Duration::from_secs(8 * scale);
    while Instant::now() < deadline {
        let roll = rng.below(100);
        if alive.len() < 2 || (roll < 35 && alive.len() < 4) {
            let queue = format!("hb{next}");
            next += 1;
            alive.push((queue.clone(), spawn_worker(&queue, 1).await));
            churn.push(Churn::Started(queue, Instant::now()));
        } else if roll < 65 {
            let index = usize::try_from(rng.below(alive.len() as u64)).unwrap();
            let (queue, worker) = alive.swap_remove(index);
            worker.stop().await;
            let stopped = Instant::now();
            let listed = jalari::admin::workers()
                .await
                .unwrap()
                .into_iter()
                .any(|worker| worker.queues.iter().any(|q| q.name == queue));
            if listed {
                violations.push(format!(
                    "worker {queue} was still listed right after its graceful stop returned \
                     (jalari/src/admin/workers.rs:67-68)"
                ));
            }
            churn.push(Churn::Stopped(queue, stopped));
        } else {
            let index = usize::try_from(rng.below(alive.len() as u64)).unwrap();
            let queue = alive[index].0.clone();
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {} SET heartbeat_at = now() - interval '1 hour'
                 WHERE queues->0->>'name' = $1",
                database.table("worker")
            )))
            .bind(&queue)
            .execute(&database.pool)
            .await
            .unwrap();
            churn.push(Churn::Aged(queue, Instant::now()));
        }
        tokio::time::sleep(rng.millis(100, 600)).await;
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let ended = Instant::now();
    for (queue, worker) in alive {
        worker.stop().await;
        churn.push(Churn::Stopped(queue, Instant::now()));
    }
    stop.store(true, Ordering::SeqCst);
    let snapshots = sampler.await.unwrap();
    let leftover = jalari::admin::workers().await.unwrap();
    if !leftover.is_empty() {
        violations.push(format!(
            "workers still listed after every worker stopped: {leftover:?}"
        ));
    }
    restore_cluster().await;

    let mut stopped_at: HashMap<&str, Instant> = HashMap::new();
    let mut started_at: HashMap<&str, Instant> = HashMap::new();
    for event in &churn {
        match event {
            Churn::Started(queue, at) => {
                started_at.insert(queue, *at);
            }
            Churn::Stopped(queue, at) => {
                stopped_at.insert(queue, *at);
            }
            Churn::Aged(..) => {}
        }
    }
    let alive_until = |queue: &str| stopped_at.get(queue).copied().unwrap_or(ended);
    let listed_fresh = |queue: &str, from: Instant, to: Instant| {
        snapshots.iter().any(|snapshot| {
            snapshot.requested >= from
                && snapshot.requested <= to
                && snapshot
                    .rows
                    .iter()
                    .any(|(_, name, fresh)| name == queue && *fresh)
        })
    };
    for snapshot in &snapshots {
        let mut seen = HashSet::new();
        for (id, queue, _) in &snapshot.rows {
            if !seen.insert(queue) {
                violations.push(format!("worker {queue} was listed twice (id {id})"));
            }
            if let Some(stopped) = stopped_at.get(queue.as_str())
                && snapshot.requested > *stopped
            {
                violations.push(format!(
                    "worker {queue} was listed {:?} after its graceful stop returned",
                    snapshot.requested - *stopped
                ));
            }
        }
    }
    let window = Duration::from_millis(2500);
    for event in &churn {
        let (queue, at, missing) = match event {
            Churn::Started(queue, at) => (queue, at, "was not listed after starting"),
            Churn::Aged(queue, at) => (queue, at, "did not re-register after missing heartbeats"),
            Churn::Stopped(..) => continue,
        };
        if alive_until(queue) > *at + window && !listed_fresh(queue, *at, *at + window) {
            violations.push(format!("worker {queue} {missing} within {window:?}"));
        }
    }
    let mut ids: HashMap<&str, HashSet<&str>> = HashMap::new();
    for snapshot in &snapshots {
        for (id, queue, _) in &snapshot.rows {
            ids.entry(queue).or_default().insert(id);
        }
    }
    let reregistered = ids.values().filter(|ids| ids.len() > 1).count();
    let aged = churn
        .iter()
        .filter(|event| matches!(event, Churn::Aged(..)))
        .count();
    eprintln!(
        "heartbeat_churn: {} workers, {aged} aged, {reregistered} re-registered with a new id, {} \
         snapshots",
        started_at.len(),
        snapshots.len()
    );
    violations
}

async fn check_listed_workers_are_fresh(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    jalari::cluster::update(|cluster| cluster.worker_timeout = Duration::from_secs(1))
        .await
        .unwrap();
    execute(
        &database.pool,
        format!(
            "INSERT INTO {} (hostname, pid, queues, scheduler, started_at, heartbeat_at)
             VALUES ('killed-host', 999999, '[{{\"name\": \"ghost\", \"concurrency\": 1}}]', TRUE,
                 now() - interval '2 hours', now() - interval '1 hour')",
            database.table("worker")
        ),
    )
    .await;
    let is_ghost = |worker: &jalari::admin::WorkerInfo| worker.hostname == "killed-host";
    let listed = jalari::admin::workers().await.unwrap();
    if listed.iter().any(is_ghost) {
        violations.push(format!(
            "with no live worker, admin::workers() lists a worker whose last heartbeat was 1 hour \
             ago while worker_timeout is 1 s (jalari/src/admin/workers.rs:64-65, \
             docs/guide.md:541): {listed:?}"
        ));
    }
    let worker = spawn_worker("ghostbuster", 1).await;
    let cleaned = eventually(Duration::from_secs(3), || async {
        !jalari::admin::workers().await.unwrap().iter().any(is_ghost)
    })
    .await;
    if !cleaned {
        violations.push("a live worker's heartbeat did not remove the stale worker".to_owned());
    }
    worker.stop().await;
    execute(
        &database.pool,
        format!(
            "DELETE FROM {} WHERE hostname = 'killed-host'",
            database.table("worker")
        ),
    )
    .await;
    restore_cluster().await;
    violations
}

async fn creating_transactions(pool: &PgPool, schema: &str, prefix: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(DISTINCT c.xmin::text) FROM pg_class AS c
         JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname LIKE $2 || '%'",
    )
    .bind(schema)
    .bind(prefix)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn migrate_together(
    pools: &[PgPool; 2],
    schemas: Vec<(Schema, Option<i32>)>,
) -> Vec<(String, jalari::Result<()>)> {
    let barrier = Arc::new(Barrier::new(schemas.len()));
    let tasks: Vec<_> = schemas
        .into_iter()
        .enumerate()
        .map(|(index, (schema, version))| {
            let pool = pools[(index / 2) % 2].clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let result = match version {
                    Some(version) => jalari::migrate_to(&pool, &schema, version).await,
                    None => jalari::migrate(&pool, &schema).await,
                };
                (
                    format!("{}.{} to {version:?}", schema.name(), schema.prefix()),
                    result,
                )
            })
        })
        .collect();
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    results
}

async fn check_concurrent_migrations(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let pools = [
        database.connect("mig1", 3).await,
        database.connect("mig2", 3).await,
    ];
    let base = format!("{}_m", database.schema_name);
    let mut created = Vec::new();

    let same = format!("{base}same");
    created.push(same.clone());
    let schema = Schema::named(&same).unwrap();
    let results = migrate_together(&pools, vec![(schema.clone(), None); 12]).await;
    for (call, result) in results {
        if let Err(e) = result {
            violations.push(format!("concurrent migrate {call} failed: {e}"));
        }
    }
    let version = jalari::schema_version(&pools[0], &schema).await.unwrap();
    if version != Some(SCHEMA_VERSION) {
        violations.push(format!(
            "after concurrent migrate the version is {version:?}, expected {SCHEMA_VERSION}"
        ));
    }
    let creators = creating_transactions(&pools[0], &same, "").await;
    if creators != 1 {
        violations.push(format!(
            "the tables of {same} were created by {creators} transactions, expected exactly one"
        ));
    }
    let tables = scalar(
        &pools[0],
        format!("SELECT count(*) FROM pg_tables WHERE schemaname = '{same}'"),
    )
    .await;
    let rows = scalar(
        &pools[0],
        format!(
            "SELECT (SELECT count(*) FROM \"{same}\".config)
                 + (SELECT count(*) FROM \"{same}\".schema_version)"
        ),
    )
    .await;
    if tables != 6 || rows != 2 {
        violations.push(format!(
            "{same} has {tables} tables and {rows} config and version rows, expected 6 and 2"
        ));
    }

    let mixed = format!("{base}mixed");
    created.push(mixed.clone());
    let schema = Schema::named(&mixed).unwrap();
    let calls = (0..12)
        .map(|n| (schema.clone(), (n % 2 == 0).then_some(1)))
        .collect();
    for (call, result) in migrate_together(&pools, calls).await {
        if let Err(e) = result {
            violations.push(format!("concurrent {call} failed: {e}"));
        }
    }
    let version = jalari::schema_version(&pools[0], &schema).await.unwrap();
    let context_columns = scalar(
        &pools[0],
        format!(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema = '{mixed}' AND column_name = 'context'"
        ),
    )
    .await;
    if version != Some(SCHEMA_VERSION) || context_columns != 2 {
        violations.push(format!(
            "mixed migrate and migrate_to(1) left version {version:?} with {context_columns} \
             context columns, expected {SCHEMA_VERSION} and 2 \
             (jalari/src/storage/migration.rs:81-82)"
        ));
    }
    for version in [0, SCHEMA_VERSION + 1] {
        match jalari::migrate_to(&pools[0], &schema, version).await {
            Err(e) if e.kind == ErrorKind::UnknownSchemaVersion => {}
            other => violations.push(format!(
                "migrate_to({version}) returned {other:?}, expected UnknownSchemaVersion"
            )),
        }
    }

    let rounds = 3 * scale;
    let mut failed_rounds = 0;
    for round in 0..rounds {
        let shared = format!("{base}p{round}");
        created.push(shared.clone());
        let calls = (0..12)
            .map(|n| {
                let prefix = if n % 2 == 0 { "a_" } else { "b_" };
                (Schema::prefixed(&shared, prefix).unwrap(), None)
            })
            .collect();
        let failures: Vec<String> = migrate_together(&pools, calls)
            .await
            .into_iter()
            .filter_map(|(call, result)| {
                result
                    .err()
                    .map(|e| format!("{call}: {:?}: {}", e.kind, e.msg))
            })
            .collect();
        if !failures.is_empty() {
            failed_rounds += 1;
            violations.push(format!(
                "round {round}: migrating prefixes a_ and b_ into the new schema {shared} at the \
                 same time failed {} call(s), although concurrent migrations are documented as \
                 safe (docs/guide.md:82-83, jalari/src/storage/migration.rs:56-57) and separate \
                 prefixes as independent installations (docs/guide.md:55-56): {failures:?}",
                failures.len()
            ));
        }
        for prefix in ["a_", "b_"] {
            let schema = Schema::prefixed(&shared, prefix).unwrap();
            let retried = jalari::migrate(&pools[0], &schema).await;
            let version = jalari::schema_version(&pools[0], &schema).await.unwrap();
            let creators = creating_transactions(&pools[0], &shared, prefix).await;
            if retried.is_err() || version != Some(SCHEMA_VERSION) || creators != 1 {
                violations.push(format!(
                    "round {round}: prefix {prefix} in {shared} after a retry: {retried:?}, \
                     version {version:?}, created by {creators} transactions"
                ));
            }
        }
    }
    eprintln!("concurrent_migrations: {failed_rounds} of {rounds} two-prefix rounds failed");

    for name in created {
        execute(
            &pools[0],
            format!("DROP SCHEMA IF EXISTS \"{name}\" CASCADE"),
        )
        .await;
    }
    for pool in pools {
        pool.close().await;
    }
    violations
}

async fn check_migrate_while_working(database: &Database, mut rng: Rng) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let pools = [
        database.connect("mig3", 3).await,
        database.connect("mig4", 3).await,
    ];
    let schema = Schema::named(&database.schema_name).unwrap();
    let workers = [spawn_worker("sips", 2).await, spawn_worker("sips", 2).await];
    let stop = Arc::new(AtomicBool::new(false));

    let migrators: Vec<_> = (0..6)
        .map(|n| {
            let pool = pools[n % 2].clone();
            let schema = schema.clone();
            let stop = Arc::clone(&stop);
            let mut rng = rng.fork(n as u64);
            tokio::spawn(async move {
                let mut calls = 0;
                let mut errors = Vec::new();
                while !stop.load(Ordering::SeqCst) {
                    let result = if rng.chance(50) {
                        jalari::migrate(&pool, &schema).await
                    } else {
                        jalari::migrate_to(&pool, &schema, 1).await
                    };
                    calls += 1;
                    if let Err(e) = result {
                        errors.push(e.to_string());
                    }
                    tokio::time::sleep(rng.millis(0, 40)).await;
                }
                (calls, errors)
            })
        })
        .collect();

    let mut ids = Vec::new();
    for _ in 0..40 * scale {
        ids.push(
            jalari::enqueue(&Sip {
                millis: rng.below(40),
            })
            .await
            .unwrap()
            .id(),
        );
        tokio::time::sleep(rng.millis(0, 25)).await;
    }
    let finished = eventually(Duration::from_secs(30 * scale), || {
        let ids = &ids;
        async move {
            ids.iter()
                .all(|id| LEDGER.count(&format!("sip:{id}"), "done") > 0)
        }
    })
    .await;
    stop.store(true, Ordering::SeqCst);
    let mut calls = 0;
    for migrator in migrators {
        let (count, errors) = migrator.await.unwrap();
        calls += count;
        for error in errors {
            violations.push(format!("migrate on the live schema failed: {error}"));
        }
    }
    for worker in workers {
        worker.stop().await;
    }
    if !finished {
        violations.push("not every job finished while migrations ran".to_owned());
    }
    for id in &ids {
        let done = LEDGER.count(&format!("sip:{id}"), "done");
        if done != 1 {
            violations.push(format!("job {id} finished {done} times, expected once"));
        }
    }
    let version = jalari::schema_version(&pools[0], &schema).await.unwrap();
    if version != Some(SCHEMA_VERSION) {
        violations.push(format!(
            "the live schema is at version {version:?} after migrate_to(1), expected \
             {SCHEMA_VERSION} (jalari/src/storage/migration.rs:81-82)"
        ));
    }
    eprintln!(
        "migrate_while_working: {} jobs, {calls} migrations",
        ids.len()
    );
    for pool in pools {
        pool.close().await;
    }
    violations.extend(occupancy_violations("sip:"));
    violations
}
