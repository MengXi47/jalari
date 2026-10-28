mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::Duration;

use common::{Database, RunningWorker, fast_retry, wait_until};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{Job, JobError, JobId, JobResult, StopOutcome, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const QUEUE: &str = "step";
const SETTLED: Duration = Duration::from_secs(15);
const LOST: &str = "the worker no longer owns the job";

struct Tables {
    pool: PgPool,
    schema: String,
    job_control: String,
    job_step: String,
}

static TABLES: OnceLock<Tables> = OnceLock::new();

static OPEN: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

static REACHED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

static COUNTS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static SEEN: LazyLock<Mutex<HashMap<String, Vec<String>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn open(gate: &str) {
    OPEN.lock().unwrap().insert(gate.to_owned());
}

fn reach(gate: &str) {
    REACHED.lock().unwrap().insert(gate.to_owned());
}

fn reached(gate: &str) -> bool {
    REACHED.lock().unwrap().contains(gate)
}

async fn pass(gate: &str) {
    reach(gate);
    while !OPEN.lock().unwrap().contains(gate) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn count(label: &str) -> usize {
    let mut counts = COUNTS.lock().unwrap();
    let count = counts.entry(label.to_owned()).or_default();
    *count += 1;
    *count
}

fn counted(label: &str) -> usize {
    COUNTS.lock().unwrap().get(label).copied().unwrap_or(0)
}

fn see(label: &str, seen: String) {
    SEEN.lock()
        .unwrap()
        .entry(label.to_owned())
        .or_default()
        .push(seen);
}

fn seen(label: &str) -> Vec<String> {
    SEEN.lock().unwrap().get(label).cloned().unwrap_or_default()
}

fn outcome<T: std::fmt::Debug>(result: &Result<T, JobError>) -> String {
    match result {
        Ok(value) => format!("ok {value:?}"),
        Err(e) if e.is_permanent() => format!("permanent {}", e.msg()),
        Err(e) if e.is_interruption() => format!("interruption {}", e.msg()),
        Err(e) => format!("error {}", e.msg()),
    }
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
struct StepRow {
    key: String,
    state: String,
    nonce: String,
    starts: i32,
    attempt: i32,
    output: Option<Value>,
    error: Option<String>,
}

async fn step_rows(id: JobId) -> Vec<StepRow> {
    let tables = TABLES.get().unwrap();
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT key, state, nonce::text AS nonce, starts, attempt, output, error
         FROM {} WHERE job_id = $1 ORDER BY key",
        tables.job_step
    )))
    .bind(id.0)
    .fetch_all(&tables.pool)
    .await
    .unwrap()
}

async fn record_done(id: JobId, key: &str, output: Value) {
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (job_id, key, state, attempt, lease, output, finished_at)
         VALUES ($1, $2, 'done', 1, 0, $3, now())",
        tables.job_step
    )))
    .bind(id.0)
    .bind(key)
    .bind(output)
    .execute(&tables.pool)
    .await
    .unwrap();
}

async fn bump_lease(id: JobId) {
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET lease = lease + 1 WHERE job_id = $1",
        tables.job_control
    )))
    .bind(id.0)
    .execute(&tables.pool)
    .await
    .unwrap();
}

#[derive(Serialize, Deserialize)]
struct Spawned {
    label: String,
}

#[jalari::job(queue = "step")]
impl Job for Spawned {
    const NAME: &'static str = "step_spawned";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        let result = tokio::spawn(async move {
            jalari::step("spawned", |_| async {
                count(&label);
                Ok(())
            })
            .await
        })
        .await?;
        see(&self.label, outcome(&result));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Keys {
    label: String,
}

#[jalari::job(queue = "step")]
impl Job for Keys {
    const NAME: &'static str = "step_keys";

    async fn run(self) -> JobResult {
        let label = &self.label;
        let empty = jalari::step("", |_| async { Ok(()) }).await;
        see(label, format!("empty: {}", outcome(&empty)));
        let long = "k".repeat(256);
        let long = jalari::step(&long, |_| async { Ok(()) }).await;
        see(label, format!("long: {}", outcome(&long)));
        let longest = "k".repeat(255);
        let longest = jalari::step(&longest, |run| async move { Ok(run.starts()) }).await;
        see(label, format!("longest: {}", outcome(&longest)));

        let (first, second) = tokio::join!(
            jalari::step("twin", |_| async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(1)
            }),
            jalari::step("twin", |_| async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(2)
            }),
        );
        see(
            label,
            format!("twin: {} / {}", outcome(&first), outcome(&second)),
        );

        let failed = jalari::step("again", |run| async move {
            Err::<i32, _>(JobError::new(format!("failed at start {}", run.starts())))
        })
        .await;
        see(label, format!("again: {}", outcome(&failed)));
        let retried = jalari::step("again", |run| async move { Ok(run.starts()) }).await;
        see(label, format!("again: {}", outcome(&retried)));
        let reused = jalari::step("again", |run| async move { Ok(run.starts()) }).await;
        see(label, format!("again: {}", outcome(&reused)));

        let dropped = tokio::time::timeout(
            Duration::from_millis(500),
            jalari::step("dropped", |_| async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(0)
            }),
        )
        .await;
        see(label, format!("dropped: timed out {}", dropped.is_err()));
        let after_drop = jalari::step("dropped", |run| async move { Ok(run.starts()) }).await;
        see(label, format!("dropped: {}", outcome(&after_drop)));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Recorded {
    label: String,
}

#[jalari::job(queue = "step")]
impl Job for Recorded {
    const NAME: &'static str = "step_recorded";

    async fn run(self) -> JobResult {
        pass(&self.label).await;
        let label = self.label.clone();
        let first = jalari::step("recorded", |_| async move {
            count(&label);
            Ok(7)
        })
        .await;
        see(&self.label, outcome(&first));
        let again = jalari::step("recorded", |_| async { Ok(8) }).await;
        see(&self.label, outcome(&again));
        let mistyped = jalari::step("mistyped", |_| async { Ok(9) }).await;
        see(&self.label, outcome(&mistyped));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct StopFirst {
    label: String,
}

#[jalari::job(queue = "step")]
impl Job for StopFirst {
    const NAME: &'static str = "step_stop_first";

    async fn run(self) -> JobResult {
        reach(&self.label);
        let _ = jalari::interrupted().await;
        let label = self.label.clone();
        let result = jalari::step("after_stop", |_| async move {
            count(&label);
            Ok(())
        })
        .await;
        see(&self.label, outcome(&result));
        result
    }
}

#[derive(Serialize, Deserialize)]
struct ZombieStart {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for ZombieStart {
    const NAME: &'static str = "step_zombie_start";

    async fn run(self) -> JobResult {
        if count(&format!("{}:runs", self.label)) > 1 {
            return Ok(());
        }
        pass(&self.label).await;
        let label = self.label.clone();
        let result = jalari::step("effect", |_| async move {
            count(&label);
            Ok(())
        })
        .await;
        see(&self.label, outcome(&result));
        tokio::time::sleep(Duration::from_millis(500)).await;
        reach(&format!("{}:continued", self.label));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Replayed {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for Replayed {
    const NAME: &'static str = "step_replayed";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        let charge = jalari::step("charge", |run| async move {
            count(&label);
            Ok(format!("charge {}", run.idempotency_key()))
        })
        .await?;
        see(&self.label, charge);
        pass(&self.label).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct CutShort {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for CutShort {
    const NAME: &'static str = "step_cut_short";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        let starts = jalari::step("effect", |run| async move {
            count(&label);
            see(&label, format!("start {}", run.starts()));
            if run.starts() == 1 {
                pass(&label).await;
            }
            Ok(run.starts())
        })
        .await?;
        see(&self.label, format!("returned {starts}"));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ZombieFinish {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for ZombieFinish {
    const NAME: &'static str = "step_zombie_finish";

    async fn run(self) -> JobResult {
        let run_number = count(&format!("{}:runs", self.label));
        let label = self.label.clone();
        let result = jalari::step("late", |_| async move {
            if run_number == 1 {
                pass(&label).await;
            }
            Ok(run_number)
        })
        .await;
        see(&self.label, outcome(&result));
        if run_number == 1 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            reach(&format!("{}:continued", self.label));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct BadOutput {
    label: String,
    kind: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for BadOutput {
    const NAME: &'static str = "step_bad_output";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        let result = match self.kind.as_str() {
            "large" => {
                let large = "x".repeat(1024 * 1024 - 1);
                jalari::step("bad", |_| async move {
                    count(&label);
                    Ok(large)
                })
                .await
                .map(drop)
            }
            "nul" => jalari::step("bad", |_| async move {
                count(&label);
                Ok("a\0b".to_owned())
            })
            .await
            .map(drop),
            _ => jalari::step("bad", |_| async move {
                count(&label);
                Ok(f64::NAN)
            })
            .await
            .map(drop),
        };
        see(&self.label, outcome(&result));
        result
    }
}

#[derive(Serialize, Deserialize)]
struct LargestOutput {
    label: String,
}

#[jalari::job(queue = "step")]
impl Job for LargestOutput {
    const NAME: &'static str = "step_largest_output";

    async fn run(self) -> JobResult {
        let largest =
            jalari::step("largest", |_| async { Ok("x".repeat(1024 * 1024 - 2)) }).await?;
        see(&self.label, format!("{} bytes", largest.len()));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct PassThrough {
    label: String,
    kind: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for PassThrough {
    const NAME: &'static str = "step_pass_through";

    async fn run(self) -> JobResult {
        let kind = self.kind.clone();
        let result = jalari::step("passed", |_| async move {
            if kind == "discard" {
                Err(JobError::discard(std::io::Error::other("no longer needed")))
            } else {
                jalari::snooze(Duration::from_secs(3600))
            }
        })
        .await;
        see(&self.label, outcome(&result));
        result
    }
}

#[derive(Serialize, Deserialize)]
struct Flaky {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for Flaky {
    const NAME: &'static str = "step_flaky";

    async fn run(self) -> JobResult {
        let run = jalari::current_job().unwrap();
        if run.attempt() > 1 {
            let rows = step_rows(run.id()).await;
            see(&self.label, format!("{:?}", rows[0].error));
        }
        jalari::step("flaky", |step| async move {
            if step.starts() == 1 {
                return Err(JobError::new("flaky once".to_owned()));
            }
            Ok(step.starts())
        })
        .await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Retried {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for Retried {
    const NAME: &'static str = "step_retried";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        jalari::step("retried", |_| async move {
            count(&label);
            Ok(1)
        })
        .await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Child {
    n: u32,
}

#[jalari::job(queue = "step_children")]
impl Job for Child {
    const NAME: &'static str = "step_child";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct OnSuccess {
    label: String,
}

#[jalari::job(queue = "step")]
impl Job for OnSuccess {
    const NAME: &'static str = "step_on_success";

    async fn run(self) -> JobResult {
        let inside = jalari::step("child", |_| async {
            let enqueued = jalari::enqueue_on_success(&Child { n: 1 });
            Ok(format!("{:?}", enqueued.map_err(|e| (e.kind, e.msg))))
        })
        .await?;
        see(&self.label, inside);
        let (beside, stepped) = tokio::join!(
            async { jalari::enqueue_on_success(&Child { n: 2 }) },
            jalari::step("beside", |_| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            }),
        );
        beside?;
        stepped?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Replaceable {
    label: String,
    round: u32,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for Replaceable {
    const NAME: &'static str = "step_replaceable";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        let key = jalari::step("charge", |run| async move {
            count(&label);
            Ok(run.idempotency_key().to_owned())
        })
        .await?;
        see(&self.label, format!("round {} key {key}", self.round));
        if self.round == 1 {
            return Err(JobError::new("parked".to_owned()).retry_after(Duration::from_secs(3600)));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct FailsLast {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 1)]
impl Job for FailsLast {
    const NAME: &'static str = "step_fails_last";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        let output = jalari::step("effect", |_| async move {
            count(&label);
            Ok("happened once".to_owned())
        })
        .await?;
        see(&self.label, output);
        if count(&format!("{}:runs", self.label)) == 1 {
            return Err(JobError::new("the last attempt fails".to_owned()));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Clearable {
    label: String,
}

#[jalari::job(queue = "step", max_attempts = 3)]
impl Job for Clearable {
    const NAME: &'static str = "step_clearable";

    async fn run(self) -> JobResult {
        let label = self.label.clone();
        let key = jalari::step("sent", |run| async move {
            count(&label);
            Ok(run.idempotency_key().to_owned())
        })
        .await?;
        see(&self.label, key);
        if count(&format!("{}:runs", self.label)) == 1 {
            pass(&self.label).await;
            return Err(JobError::new("parked".to_owned()).retry_after(Duration::from_secs(3600)));
        }
        Ok(())
    }
}

async fn spawn_worker() -> RunningWorker {
    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    RunningWorker::spawn(
        Worker::builder()
            .queue(QUEUE, 4)
            .config(config)
            .retry_policy(fast_retry())
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    )
}

async fn state(id: JobId) -> String {
    jalari::admin::job(id)
        .await
        .unwrap()
        .unwrap()
        .job
        .state
        .as_str()
        .to_owned()
}

async fn enqueue<T: Job>(job: &T) -> JobId {
    jalari::enqueue(job).await.unwrap().id()
}

async fn wait_for_state(id: JobId, wanted: &str) {
    wait_until(&format!("job {id} to be {wanted}"), SETTLED, || async {
        state(id).await == wanted
    })
    .await;
}

async fn wait_for_gate(gate: &str) {
    wait_until(&format!("{gate} to be reached"), SETTLED, || async {
        reached(gate)
    })
    .await;
}

async fn check_outside_a_job_is_refused() {
    let outside = jalari::step("outside", |_| async { Ok(()) }).await;
    assert_eq!(
        outside.unwrap_err().msg(),
        "step \"outside\": jalari::step works only in a running job's own task, not in tasks \
         it spawns"
    );
    let label = "spawned";
    let id = enqueue(&Spawned {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "succeeded").await;
    let [refused] = &seen(label)[..] else {
        panic!("{:?}", seen(label));
    };
    assert!(
        refused.starts_with("permanent step \"spawned\""),
        "{refused}"
    );
    assert!(refused.contains("not in tasks it spawns"), "{refused}");
    assert_eq!(counted(label), 0);
    assert_eq!(step_rows(id).await, []);
}

async fn check_key_rules() {
    let label = "keys";
    let id = enqueue(&Keys {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "succeeded").await;
    let seen = seen(label);
    let long = "k".repeat(256);
    assert_eq!(
        seen,
        [
            "empty: permanent step \"\": a key must be 1 to 255 bytes long".to_owned(),
            format!("long: permanent step {long:?}: a key must be 1 to 255 bytes long"),
            "longest: ok 1".to_owned(),
            "twin: ok 1 / permanent step \"twin\" is already running in this run; give \
             concurrent steps different keys"
                .to_owned(),
            "again: error failed at start 1".to_owned(),
            "again: ok 2".to_owned(),
            "again: permanent step \"again\" already succeeded in this run; a key can succeed \
             once per run"
                .to_owned(),
            "dropped: timed out true".to_owned(),
            "dropped: ok 2".to_owned(),
        ]
    );
    let rows = step_rows(id).await;
    let starts: Vec<(&str, i32)> = rows
        .iter()
        .map(|row| (row.key.as_str(), row.starts))
        .collect();
    assert_eq!(
        starts,
        [
            ("again", 2),
            ("dropped", 2),
            ("k".repeat(255).as_str(), 1),
            ("twin", 1)
        ]
    );
    assert!(rows.iter().all(|row| row.attempt == 1));
    assert!(
        rows.iter()
            .all(|row| row.state == "done" && row.error.is_none())
    );
}

async fn check_recorded_step_returns_its_output() {
    let label = "recorded";
    let id = enqueue(&Recorded {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    record_done(id, "recorded", json!(42)).await;
    record_done(id, "mistyped", json!("not a number")).await;
    open(label);
    wait_for_state(id, "succeeded").await;
    let seen = seen(label);
    assert_eq!(
        seen[..2],
        [
            "ok 42",
            "permanent step \"recorded\" already succeeded in this run; a key can succeed once per run"
        ]
    );
    assert!(
        seen[2].starts_with(
            "permanent step \"mistyped\": the recorded output does not fit the step's type"
        ),
        "{seen:?}"
    );
    assert_eq!(counted(label), 0);
    let rows = step_rows(id).await;
    assert!(
        rows.iter()
            .all(|row| row.starts == 1 && row.state == "done")
    );
}

async fn check_a_step_is_a_stop_point() {
    let label = "stop_first";
    let id = enqueue(&StopFirst {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(id, "stopped").await;
    assert_eq!(seen(label), ["interruption the job was asked to stop"]);
    assert_eq!(counted(label), 0);
    assert_eq!(step_rows(id).await, []);
    assert_eq!(
        jalari::cancel(id).await.unwrap(),
        jalari::CancelOutcome::Cancelled
    );
}

async fn check_a_zombie_cannot_start_a_step() {
    let label = "zombie_start";
    let id = enqueue(&ZombieStart {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    bump_lease(id).await;
    open(label);
    wait_for_state(id, "succeeded").await;
    assert_eq!(seen(label), [format!("interruption {LOST}")]);
    assert_eq!(counted(label), 0, "the zombie ran the step's closure");
    assert!(
        !reached("zombie_start:continued"),
        "a run that ignored the lost error kept running past its next await"
    );
    assert_eq!(step_rows(id).await, []);
}

async fn check_a_done_step_is_not_rerun_after_a_crash(worker: RunningWorker) -> RunningWorker {
    let label = "replayed";
    let id = enqueue(&Replayed {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    worker.crash().await;
    open(label);
    let worker = spawn_worker().await;
    wait_for_state(id, "succeeded").await;
    let [first, second] = &seen(label)[..] else {
        panic!("{:?}", seen(label));
    };
    assert_eq!(first, second, "the replay returned another output");
    assert_eq!(counted(label), 1, "the done step ran again");
    let [row] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_eq!(first, &format!("charge {}", row.nonce));
    assert_eq!((row.state.as_str(), row.starts), ("done", 1));
    worker
}

async fn check_a_crash_before_done_reruns_the_step(worker: RunningWorker) -> RunningWorker {
    let label = "cut_short";
    let id = enqueue(&CutShort {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    worker.crash().await;
    let worker = spawn_worker().await;
    wait_for_state(id, "succeeded").await;
    assert_eq!(seen(label), ["start 1", "start 2", "returned 2"]);
    assert_eq!(counted(label), 2);
    let [row] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_eq!(
        (row.state.as_str(), row.starts, row.attempt, &row.output),
        ("done", 2, 2, &Some(json!(2)))
    );
    worker
}

async fn check_a_zombie_cannot_finish_a_step() {
    let label = "zombie_finish";
    let id = enqueue(&ZombieFinish {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    bump_lease(id).await;
    open(label);
    wait_for_state(id, "succeeded").await;
    assert_eq!(
        seen(label),
        [format!("interruption {LOST}"), "ok 2".to_owned()]
    );
    assert!(
        !reached("zombie_finish:continued"),
        "a run that ignored the lost error kept running past its next await"
    );
    let [row] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_eq!(
        (row.state.as_str(), row.starts, &row.output),
        ("done", 2, &Some(json!(2)))
    );
    let succeeded = jalari::admin::job(id)
        .await
        .unwrap()
        .unwrap()
        .history
        .iter()
        .filter(|entry| entry.state == jalari::JobState::Succeeded)
        .count();
    assert_eq!(succeeded, 1);
}

async fn check_a_bad_output_leaves_the_step_started() {
    for (kind, reason) in [
        (
            "large",
            "the output is 1048577 bytes of JSON, above the limit of 1048576",
        ),
        (
            "nul",
            "the output contains the character \\u0000, which PostgreSQL cannot store",
        ),
        (
            "nan",
            "the output does not read back as the step's type: invalid type: null, expected f64",
        ),
    ] {
        let label = format!("bad_{kind}");
        let id = enqueue(&BadOutput {
            label: label.clone(),
            kind: kind.to_owned(),
        })
        .await;
        wait_for_state(id, "failed").await;
        let msg = format!("step \"bad\": {reason}");
        assert_eq!(seen(&label), [format!("permanent {msg}")], "{kind}");
        assert_eq!(counted(&label), 1, "{kind}");
        let [row] = &step_rows(id).await[..] else {
            panic!("one step row expected");
        };
        assert_eq!(
            (row.state.as_str(), &row.output, row.error.as_deref()),
            ("started", &None, Some(msg.as_str())),
            "{kind}"
        );
    }
    let label = "largest";
    let id = enqueue(&LargestOutput {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "succeeded").await;
    assert_eq!(seen(label), ["1048574 bytes"]);
}

async fn check_closure_errors_pass_through() {
    for (kind, ended, recorded, last_error, attempts) in [
        (
            "discard",
            "discarded",
            "no longer needed",
            Some("no longer needed"),
            1,
        ),
        ("snooze", "scheduled", "snoozed for 3600s", None, 0),
    ] {
        let label = format!("pass_{kind}");
        let id = enqueue(&PassThrough {
            label: label.clone(),
            kind: kind.to_owned(),
        })
        .await;
        wait_for_state(id, ended).await;
        let [row] = &step_rows(id).await[..] else {
            panic!("one step row expected");
        };
        assert_eq!(
            (row.state.as_str(), row.error.as_deref()),
            ("started", Some(recorded)),
            "{kind}"
        );
        let detail = jalari::admin::job(id).await.unwrap().unwrap();
        assert_eq!(detail.job.last_error.as_deref(), last_error, "{kind}");
        assert_eq!(detail.job.attempts, attempts, "{kind}");
    }
}

async fn check_a_retry_clears_the_recorded_error() {
    let label = "flaky";
    let id = enqueue(&Flaky {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "succeeded").await;
    assert_eq!(seen(label), ["Some(\"flaky once\")"]);
    let [row] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_eq!(
        (row.state.as_str(), row.starts, row.attempt, &row.error),
        ("done", 2, 2, &None)
    );
}

async fn check_the_finish_survives_a_dropped_connection() {
    let tables = TABLES.get().unwrap();
    let schema = &tables.schema;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE SEQUENCE \"{schema}\".finish_kills;
         CREATE FUNCTION \"{schema}\".kill_finish() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF strpos(current_query(), 'finished_at = statement_timestamp()') > 0 THEN
                 IF nextval('\"{schema}\".finish_kills') = 1 THEN
                     PERFORM pg_terminate_backend(pg_backend_pid());
                 END IF;
             END IF;
             RETURN NULL;
         END $$;
         CREATE TRIGGER kill_finish BEFORE UPDATE ON {job_step}
             FOR EACH STATEMENT EXECUTE FUNCTION \"{schema}\".kill_finish();",
        job_step = tables.job_step,
    )))
    .execute(&tables.pool)
    .await
    .unwrap();
    let label = "retried";
    let id = enqueue(&Retried {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "succeeded").await;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP TRIGGER kill_finish ON {};",
        tables.job_step
    )))
    .execute(&tables.pool)
    .await
    .unwrap();
    let kills: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT last_value FROM \"{schema}\".finish_kills"
    )))
    .fetch_one(&tables.pool)
    .await
    .unwrap();
    assert_eq!(kills, 2, "the first finish must have been cut off");
    assert_eq!(counted(label), 1);
    let [row] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_eq!((row.state.as_str(), row.starts), ("done", 1));
    let detail = jalari::admin::job(id).await.unwrap().unwrap();
    assert_eq!(detail.job.attempts, 1);
}

async fn check_enqueue_on_success_is_refused_inside_a_step() {
    let label = "on_success";
    let id = enqueue(&OnSuccess {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "succeeded").await;
    assert_eq!(
        seen(label),
        [
            "Err((NotInJob, \"step_child: enqueue_on_success does not work inside jalari::step; \
          call it after the step returns\"))"
        ]
    );
    let children: Vec<u32> = jalari::admin::jobs(
        &jalari::admin::JobFilter::new().task("step_child"),
        10,
        None,
    )
    .await
    .unwrap()
    .iter()
    .map(|child| child.payload_json::<Child>().unwrap().n)
    .collect();
    assert_eq!(children, [2]);
}

async fn check_replace_discards_the_steps() {
    let label = "replaceable";
    let options = || {
        jalari::EnqueueOptions::new()
            .job_key("step_replaceable")
            .on_conflict(jalari::OnConflict::Replace)
    };
    let id = jalari::enqueue_with(
        &Replaceable {
            label: label.to_owned(),
            round: 1,
        },
        options(),
    )
    .await
    .unwrap()
    .id();
    wait_for_state(id, "scheduled").await;
    let [first] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    let replaced = jalari::enqueue_with(
        &Replaceable {
            label: label.to_owned(),
            round: 2,
        },
        options(),
    )
    .await
    .unwrap();
    assert_eq!(replaced, jalari::EnqueueOutcome::Replaced(id));
    assert_eq!(step_rows(id).await, []);
    wait_for_state(id, "succeeded").await;
    let [second] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_ne!(first.nonce, second.nonce);
    assert_eq!(
        seen(label),
        [
            format!("round 1 key {}", first.nonce),
            format!("round 2 key {}", second.nonce)
        ]
    );
    assert_eq!(counted(label), 2);
    assert_eq!(second.starts, 1);
}

async fn check_a_retried_failure_keeps_its_steps() {
    let label = "fails_last";
    let id = enqueue(&FailsLast {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "failed").await;
    let [row] = &step_rows(id).await[..] else {
        panic!("a failed job keeps its step");
    };
    assert_eq!(row.state, "done");
    assert_eq!(
        jalari::admin::retry(id).await.unwrap(),
        jalari::admin::RetryOutcome::Retried
    );
    wait_for_state(id, "succeeded").await;
    assert_eq!(seen(label), ["happened once", "happened once"]);
    assert_eq!(counted(label), 1, "the retried job ran its done step again");
}

async fn check_steps_are_listed_and_cleared() {
    let label = "clearable";
    let id = enqueue(&Clearable {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    let detail = jalari::admin::job(id).await.unwrap().unwrap();
    let [listed] = &detail.steps[..] else {
        panic!("{:?}", detail.steps);
    };
    let [row] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_eq!(listed.key, "sent");
    assert_eq!(listed.state, "done");
    assert_eq!((listed.starts, listed.attempt), (1, 1));
    assert_eq!(listed.output, Some(json!(row.nonce)));
    assert_eq!(listed.error, None);
    assert!(listed.finished_at.unwrap() >= listed.started_at);
    assert_eq!(
        jalari::admin::clear_steps(id).await.unwrap(),
        jalari::admin::ClearStepsOutcome::Running
    );
    assert_eq!(step_rows(id).await.len(), 1);
    open(label);
    wait_for_state(id, "scheduled").await;
    assert_eq!(
        jalari::admin::clear_steps(id).await.unwrap(),
        jalari::admin::ClearStepsOutcome::Cleared(1)
    );
    assert_eq!(step_rows(id).await, []);
    assert!(
        jalari::admin::job(id)
            .await
            .unwrap()
            .unwrap()
            .steps
            .is_empty()
    );
    assert_eq!(
        jalari::admin::clear_steps(JobId(i64::MAX)).await.unwrap(),
        jalari::admin::ClearStepsOutcome::NotFound
    );
    assert_eq!(
        jalari::admin::run_now(id).await.unwrap(),
        jalari::admin::RunNowOutcome::Enqueued
    );
    wait_for_state(id, "succeeded").await;
    let [again] = &step_rows(id).await[..] else {
        panic!("one step row expected");
    };
    assert_ne!(again.nonce, row.nonce);
    assert_eq!(seen(label), [row.nonce.clone(), again.nonce.clone()]);
    assert_eq!(counted(label), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_step() {
    let Some(database) = Database::start("step", 16).await else {
        return;
    };
    let _ = TABLES.set(Tables {
        pool: database.pool.clone(),
        schema: database.schema_name.clone(),
        job_control: database.table("job_control"),
        job_step: database.table("job_step"),
    });
    let worker = spawn_worker().await;
    check_outside_a_job_is_refused().await;
    check_key_rules().await;
    check_recorded_step_returns_its_output().await;
    check_a_step_is_a_stop_point().await;
    check_a_zombie_cannot_start_a_step().await;
    let worker = check_a_done_step_is_not_rerun_after_a_crash(worker).await;
    let worker = check_a_crash_before_done_reruns_the_step(worker).await;
    check_a_zombie_cannot_finish_a_step().await;
    check_a_bad_output_leaves_the_step_started().await;
    check_closure_errors_pass_through().await;
    check_a_retry_clears_the_recorded_error().await;
    check_the_finish_survives_a_dropped_connection().await;
    check_enqueue_on_success_is_refused_inside_a_step().await;
    check_replace_discards_the_steps().await;
    check_a_retried_failure_keeps_its_steps().await;
    check_steps_are_listed_and_cleared().await;
    worker.stop().await;
    database.finish().await;
}
