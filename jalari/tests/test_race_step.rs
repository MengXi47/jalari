mod common;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use common::{Database, Rng, RunningWorker, fast_retry, wait_until};
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{Job, JobId, JobResult, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const QUEUE: &str = "race_step";
const KEYS: [&str; 4] = ["reserve", "charge", "ship", "notify"];
const SETTLED: Duration = Duration::from_secs(120);

struct Execution {
    started: Instant,
    ended: Option<Instant>,
}

type StepId = (i64, &'static str);

type Outputs = Vec<(Instant, String)>;

static EXECUTIONS: LazyLock<Mutex<HashMap<StepId, Vec<Execution>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static RETURNED: LazyLock<Mutex<HashMap<StepId, Outputs>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct Running {
    step: StepId,
    index: usize,
}

impl Running {
    fn enter(step: StepId) -> Self {
        let mut executions = EXECUTIONS.lock().unwrap();
        let runs = executions.entry(step).or_default();
        runs.push(Execution {
            started: Instant::now(),
            ended: None,
        });
        Self {
            step,
            index: runs.len() - 1,
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(runs) = EXECUTIONS.lock().unwrap().get_mut(&self.step) {
            runs[self.index].ended = Some(Instant::now());
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Order {
    effect_ms: Vec<u64>,
}

#[jalari::job(queue = "race_step", max_attempts = 100)]
impl Job for Order {
    const NAME: &'static str = "race_step_order";

    async fn run(self) -> JobResult {
        let id = jalari::current_job().unwrap().id().0;
        for (key, effect_ms) in KEYS.into_iter().zip(self.effect_ms) {
            let output = jalari::step(key, |run| async move {
                let _running = Running::enter((id, key));
                tokio::time::sleep(Duration::from_millis(effect_ms)).await;
                Ok(format!(
                    "{id} {key} start {} key {}",
                    run.starts(),
                    run.idempotency_key()
                ))
            })
            .await?;
            RETURNED
                .lock()
                .unwrap()
                .entry((id, key))
                .or_default()
                .push((Instant::now(), output));
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

async fn unfinished(database: &Database, ids: &[JobId]) -> i64 {
    let ids: Vec<i64> = ids.iter().map(|id| id.0).collect();
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE id = ANY($1) AND state <> 'succeeded'",
        database.table("job")
    )))
    .bind(&ids)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn end_a_busy_lifeline(database: &Database) -> usize {
    let ended: Vec<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT pg_terminate_backend(activity.pid)
         FROM pg_stat_activity AS activity
         JOIN (
             SELECT locked_by FROM (SELECT DISTINCT locked_by FROM {} WHERE state = 'running')
                 AS busy
             ORDER BY random()
             LIMIT 1
         ) AS owner ON activity.application_name = 'jalari lifeline ' || owner.locked_by::text
         WHERE activity.datname = current_database()",
        database.table("job")
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap();
    ended.into_iter().filter(|ok| *ok).count()
}

#[derive(sqlx::FromRow)]
struct StepRow {
    job_id: i64,
    key: String,
    state: String,
    starts: i32,
    output: Option<Value>,
}

#[derive(Default)]
struct Overlaps {
    reruns: usize,
    replays: usize,
    overlapped: Vec<Duration>,
    shortest_gap: Option<Duration>,
}

impl Overlaps {
    fn measure(&mut self, executions: &[Execution], now: Instant) {
        let mut ordered: Vec<&Execution> = executions.iter().collect();
        ordered.sort_by_key(|execution| execution.started);
        for pair in ordered.windows(2) {
            self.reruns += 1;
            let earlier_ended = pair[0].ended.unwrap_or(now);
            match pair[1].started.checked_duration_since(earlier_ended) {
                Some(gap) => {
                    self.shortest_gap =
                        Some(self.shortest_gap.map_or(gap, |shortest| shortest.min(gap)));
                }
                None => self
                    .overlapped
                    .push(earlier_ended.duration_since(pair[1].started)),
            }
        }
    }
}

fn audit(row: &StepRow, step: StepId, overlaps: &mut Overlaps, now: Instant) -> Vec<String> {
    let mut violations = Vec::new();
    let executions = EXECUTIONS.lock().unwrap();
    let executions = executions.get(&step).map_or(&[][..], Vec::as_slice);
    let returned = RETURNED
        .lock()
        .unwrap()
        .get(&step)
        .cloned()
        .unwrap_or_default();
    let label = format!("job {} step {}", step.0, step.1);
    if row.state != "done" {
        violations.push(format!("{label}: the record is {}", row.state));
    }
    if executions.is_empty() {
        violations.push(format!("{label}: the closure never ran"));
    }
    let executed = i32::try_from(executions.len()).unwrap();
    if executed > row.starts {
        violations.push(format!(
            "{label}: the closure ran {executed} times for {} recorded starts",
            row.starts
        ));
    }
    let stored = row.output.as_ref().and_then(Value::as_str);
    for (_, output) in &returned {
        if Some(output.as_str()) != stored {
            violations.push(format!(
                "{label}: a run got {output:?} while {stored:?} is recorded"
            ));
        }
    }
    if let Some(first_done) = returned.iter().map(|(at, _)| *at).min() {
        let late = executions
            .iter()
            .filter(|execution| execution.started > first_done)
            .count();
        if late > 0 {
            violations.push(format!(
                "{label}: the closure ran {late} times after a run had got its output"
            ));
        }
    }
    overlaps.measure(executions, now);
    overlaps.replays += returned.len().saturating_sub(1);
    violations
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_race_step() {
    let Some(database) = Database::start("race_step", 40).await else {
        return;
    };
    let seed = common::seed("test_race_step");
    let mut rng = Rng::new(seed);
    let scale = common::scale();
    let total_jobs = usize::try_from(60 * scale).unwrap();
    let chaos = Duration::from_secs(12 * scale);
    let workers = vec![
        spawn_worker().await,
        spawn_worker().await,
        spawn_worker().await,
    ];

    let started = Instant::now();
    let mut ids = Vec::new();
    let mut kills = 0;
    while started.elapsed() < chaos {
        tokio::time::sleep(rng.millis(200, 1200)).await;
        for _ in 0..=rng.below(3) {
            if ids.len() < total_jobs {
                let effect_ms = KEYS.iter().map(|_| 50 + rng.below(750)).collect();
                ids.push(jalari::enqueue(&Order { effect_ms }).await.unwrap().id());
            }
        }
        if rng.chance(60) {
            kills += end_a_busy_lifeline(&database).await;
        }
    }
    while ids.len() < total_jobs {
        let effect_ms = KEYS.iter().map(|_| 50 + rng.below(750)).collect();
        ids.push(jalari::enqueue(&Order { effect_ms }).await.unwrap().id());
    }
    wait_until("every order to succeed", SETTLED, || async {
        unfinished(&database, &ids).await == 0
    })
    .await;
    for worker in workers {
        worker.stop().await;
    }

    let rows: Vec<StepRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job_id, key, state, starts, output FROM {} WHERE job_id = ANY($1)",
        database.table("job_step")
    )))
    .bind(ids.iter().map(|id| id.0).collect::<Vec<_>>())
    .fetch_all(&database.pool)
    .await
    .unwrap();
    let now = Instant::now();
    let mut overlaps = Overlaps::default();
    let mut violations = Vec::new();
    for id in &ids {
        for key in KEYS {
            let Some(row) = rows.iter().find(|row| row.job_id == id.0 && row.key == key) else {
                violations.push(format!("job {id} step {key}: no record"));
                continue;
            };
            violations.extend(audit(row, (id.0, key), &mut overlaps, now));
        }
    }
    let longest_overlap = overlaps.overlapped.iter().max().copied();
    eprintln!(
        "test_race_step: {} orders, {} steps, {kills} lifelines ended, {} steps ran again, \
         {} reruns started while an earlier execution still ran (longest {longest_overlap:?}), \
         shortest gap between an execution and its rerun {:?}, {} done steps replayed",
        ids.len(),
        ids.len() * KEYS.len(),
        overlaps.reruns,
        overlaps.overlapped.len(),
        overlaps.shortest_gap,
        overlaps.replays,
    );
    database.finish().await;
    assert!(
        violations.is_empty(),
        "{} violations (seed {seed}):\n{}",
        violations.len(),
        violations.join("\n")
    );
    assert!(kills > 0, "no lifeline was ended (seed {seed})");
}
