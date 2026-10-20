mod common;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{
    Database, LEDGER, OCCUPANCY, Rng, RunningWorker, fast_retry, wait_until, worker_config,
};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{Cron, EnqueueOutcome, ErrorKind, Job, JobId, JobResult, RecurringInfo, Worker};
use serde::{Deserialize, Serialize};

static GATE_OPEN: AtomicBool = AtomicBool::new(false);

const CRONS: [&str; 3] = ["*/1 * * * * *", "*/2 * * * * *", "*/3 * * * * *"];
const TIMEZONES: [&str; 6] = [
    "UTC",
    "Asia/Taipei",
    "Europe/Berlin",
    "America/New_York",
    "Asia/Kathmandu",
    "Australia/Adelaide",
];

#[derive(Serialize, Deserialize, Default)]
struct EverySecond;

#[jalari::job(cron = "*/1 * * * * *", queue = "decl")]
impl Job for EverySecond {
    const NAME: &'static str = "rs_every_second";

    async fn run(self) -> JobResult {
        let job = jalari::current_job().unwrap();
        let _seat = OCCUPANCY.enter(
            vec![format!("decl:job:{}", job.id())],
            format!("job {} attempt {}", job.id(), job.attempt()),
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct EveryTwo;

#[jalari::job(cron = "*/2 * * * * *", queue = "decl")]
impl Job for EveryTwo {
    const NAME: &'static str = "rs_every_two";

    async fn run(self) -> JobResult {
        let job = jalari::current_job().unwrap();
        let _seat = OCCUPANCY.enter(
            vec![format!("decl:job:{}", job.id())],
            format!("job {} attempt {}", job.id(), job.attempt()),
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Pulse {
    op: u64,
    millis: u64,
}

#[jalari::job(queue = "pulse")]
impl Job for Pulse {
    const NAME: &'static str = "rs_pulse";

    async fn run(self) -> JobResult {
        let job = jalari::current_job().unwrap();
        let _seat = OCCUPANCY.enter(
            vec![format!("pulse:job:{}", job.id())],
            format!("job {} op {} attempt {}", job.id(), self.op, job.attempt()),
        );
        LEDGER.record(&format!("pulse:{}", job.id()), "done");
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Gate;

#[jalari::job(queue = "gate")]
impl Job for Gate {
    const NAME: &'static str = "rs_gate";

    async fn run(self) -> JobResult {
        let label = format!("gate:{}", jalari::current_job().unwrap().id());
        LEDGER.record(&label, "start");
        while !GATE_OPEN.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        LEDGER.record(&label, "done");
        Ok(())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct AuditRow {
    op: String,
    name: String,
    old_enabled: Option<bool>,
    old_next: Option<i64>,
    new_next: Option<i64>,
    old_last: Option<i64>,
    new_last: Option<i64>,
    cron: String,
    at: i64,
    txid: i64,
}

impl AuditRow {
    fn fired(&self) -> bool {
        self.op == "UPDATE" && self.new_last != self.old_last
    }
}

fn selected(name: &str) -> bool {
    std::env::var("RACE_ONLY").map_or(true, |only| only == name)
}

fn period_ms(cron: &str) -> Option<i64> {
    match cron {
        "*/1 * * * * *" | "* * * * * *" => Some(1000),
        "*/2 * * * * *" => Some(2000),
        "*/3 * * * * *" => Some(3000),
        _ => None,
    }
}

fn cron(expression: &str, timezone: &str) -> Cron {
    Cron::new(expression).unwrap().timezone(timezone).unwrap()
}

fn job_key(name: &str) -> String {
    format!("recurring:{name}")
}

async fn install_audit(database: &Database) {
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE \"{schema}\".race_recurring (
             seq BIGSERIAL PRIMARY KEY,
             op TEXT NOT NULL,
             name TEXT NOT NULL,
             old_enabled BOOLEAN,
             old_next BIGINT,
             new_next BIGINT,
             old_last BIGINT,
             new_last BIGINT,
             cron TEXT NOT NULL,
             at BIGINT NOT NULL,
             txid BIGINT NOT NULL
         );
         CREATE FUNCTION \"{schema}\".race_ms(value TIMESTAMPTZ) RETURNS BIGINT
             LANGUAGE sql IMMUTABLE AS $$ SELECT (EXTRACT(EPOCH FROM value) * 1000)::bigint $$;
         CREATE FUNCTION \"{schema}\".race_recurring() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO \"{schema}\".race_recurring
                 (op, name, old_enabled, old_next, new_next, old_last, new_last, cron, at, txid)
             VALUES (
                 TG_OP,
                 coalesce(NEW.name, OLD.name),
                 OLD.enabled,
                 \"{schema}\".race_ms(OLD.next_run_at),
                 \"{schema}\".race_ms(NEW.next_run_at),
                 \"{schema}\".race_ms(OLD.last_run_at),
                 \"{schema}\".race_ms(NEW.last_run_at),
                 coalesce(NEW.cron, OLD.cron),
                 \"{schema}\".race_ms(now()),
                 txid_current()
             );
             RETURN NULL;
         END $$;
         CREATE TRIGGER race_recurring AFTER INSERT OR UPDATE OR DELETE
             ON \"{schema}\".recurring
             FOR EACH ROW EXECUTE FUNCTION \"{schema}\".race_recurring();
         CREATE TABLE \"{schema}\".race_job (
             job_id BIGINT PRIMARY KEY,
             job_key TEXT,
             at BIGINT NOT NULL,
             txid BIGINT NOT NULL
         );
         CREATE FUNCTION \"{schema}\".race_job() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO \"{schema}\".race_job (job_id, job_key, at, txid)
             VALUES (NEW.id, NEW.job_key, \"{schema}\".race_ms(clock_timestamp()), txid_current());
             RETURN NULL;
         END $$;
         CREATE TRIGGER race_job AFTER INSERT ON \"{schema}\".job
             FOR EACH ROW EXECUTE FUNCTION \"{schema}\".race_job();"
    )))
    .execute(&database.pool)
    .await
    .unwrap();
}

async fn audit(database: &Database, after: i64) -> Vec<AuditRow> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT op, name, old_enabled, old_next, new_next, old_last, new_last, cron, at, txid
         FROM {} WHERE seq > $1 ORDER BY seq",
        database.table("race_recurring")
    )))
    .bind(after)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn audit_mark(database: &Database) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT coalesce(max(seq), 0) FROM {}",
        database.table("race_recurring")
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn db_now(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn keyed_jobs(database: &Database, name: &str) -> Vec<(i64, i64, i64)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job_id, at, txid FROM {} WHERE job_key = $1 ORDER BY job_id",
        database.table("race_job")
    )))
    .bind(job_key(name))
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

async fn job_key_of(database: &Database, id: JobId) -> Option<String> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT job_key FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_optional(&database.pool)
    .await
    .unwrap()
    .flatten()
}

async fn job_state(database: &Database, id: JobId) -> String {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT state FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn build_scheduler() -> Worker {
    Worker::builder()
        .queue("decl", 1)
        .queue("pulse", 1)
        .config(worker_config(Duration::from_millis(200)))
        .retry_policy(fast_retry())
        .build()
        .await
        .unwrap()
}

async fn start_schedulers(count: usize) -> Vec<RunningWorker> {
    let mut built = Vec::new();
    for _ in 0..count {
        built.push(build_scheduler().await);
    }
    built.into_iter().map(RunningWorker::spawn).collect()
}

fn check_fires(rows: &[AuditRow]) -> Vec<String> {
    let mut violations = Vec::new();
    let mut fired: HashMap<&str, i64> = HashMap::new();
    for row in rows {
        if let (Some(next), Some(last)) = (row.new_next, row.new_last)
            && next <= last
        {
            violations.push(format!(
                "{} would run again at {next} after its last run {last}: {row:?}",
                row.name
            ));
        }
        if !row.fired() {
            continue;
        }
        let Some(last) = row.new_last else {
            violations.push(format!("{} lost its last_run_at: {row:?}", row.name));
            continue;
        };
        if row.old_last.is_some_and(|old| last < old) {
            violations.push(format!("{} last_run_at moved back: {row:?}", row.name));
        }
        if Some(last) != row.old_next {
            violations.push(format!(
                "{} fired for {last} although it was due at {:?}: {row:?}",
                row.name, row.old_next
            ));
        }
        if row.old_enabled != Some(true) {
            violations.push(format!("{} fired while paused: {row:?}", row.name));
        }
        if let Some(previous) = fired.get(row.name.as_str())
            && last <= *previous
        {
            violations.push(format!(
                "{} fired for {last} after already firing for {previous} (docs/guide.md:239): \
                 {row:?}",
                row.name
            ));
        }
        let next = row.new_next.unwrap_or(i64::MIN);
        if next <= row.at {
            violations.push(format!(
                "{} fired at {} but its next run {next} is not in the future, so it would catch up \
                 more than once (docs/guide.md:238): {row:?}",
                row.name, row.at
            ));
        }
        if let Some(period) = period_ms(&row.cron) {
            if last % period != 0 {
                violations.push(format!(
                    "{} fired for {last}, which is not a tick of {:?}: {row:?}",
                    row.name, row.cron
                ));
            }
            if next != last + period && row.at < last + period {
                violations.push(format!(
                    "{} fired on time for {last} ({} ms late) but skipped to {next}: {row:?}",
                    row.name,
                    row.at - last
                ));
            }
        }
        fired.insert(&row.name, last);
    }
    violations
}

async fn check_jobs_come_from_fires(
    database: &Database,
    rows: &[AuditRow],
    names: &[String],
    triggered: &HashSet<i64>,
) -> Vec<String> {
    let mut violations = Vec::new();
    for name in names {
        let fires: HashSet<i64> = rows
            .iter()
            .filter(|row| row.fired() && row.name == *name)
            .map(|row| row.txid)
            .collect();
        let mut per_fire: HashMap<i64, Vec<i64>> = HashMap::new();
        for (job_id, _, txid) in keyed_jobs(database, name).await {
            if fires.contains(&txid) {
                per_fire.entry(txid).or_default().push(job_id);
            } else if !triggered.contains(&job_id) {
                violations.push(format!(
                    "job {job_id} with job_key {} came from neither a fire nor a trigger",
                    job_key(name)
                ));
            }
        }
        for (txid, jobs) in per_fire {
            if jobs.len() > 1 {
                violations.push(format!(
                    "one fire of {name} (transaction {txid}) inserted jobs {jobs:?}"
                ));
            }
        }
    }
    violations
}

fn occupancy_violations() -> Vec<String> {
    OCCUPANCY
        .violations()
        .into_iter()
        .map(|violation| format!("overlap: {violation}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn test_race_scheduler() {
    let Some(database) = Database::start("sched", 18).await else {
        return;
    };
    install_audit(&database).await;
    let seed = common::seed("test_race_scheduler");
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

    if selected("declared_sync_converges") {
        let at = Instant::now();
        let violations = check_declared_sync_converges(&database).await;
        report("declared_sync_converges", violations, at);
    }
    if selected("ticks_fire_once") {
        let at = Instant::now();
        let violations = check_ticks_fire_once(&database).await;
        report("ticks_fire_once", violations, at);
    }
    if selected("scheduler_restarts") {
        let at = Instant::now();
        let violations = check_scheduler_restarts(&database, rng.fork(1)).await;
        report("scheduler_restarts", violations, at);
    }
    if selected("runtime_storm") {
        let at = Instant::now();
        let violations = check_runtime_storm(&database, rng.fork(2)).await;
        report("runtime_storm", violations, at);
    }
    if selected("trigger_kept_exactly_when_pending") {
        let at = Instant::now();
        let violations = check_trigger_kept_exactly_when_pending(&database).await;
        report("trigger_kept_exactly_when_pending", violations, at);
    }
    report("occupancy", occupancy_violations(), Instant::now());

    eprintln!("test_race_scheduler finished in {:?}", started.elapsed());
    database.finish().await;
    assert!(
        failures.is_empty(),
        "{} violation(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

async fn check_declared_sync_converges(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let mark = audit_mark(database).await;
    let workers = start_schedulers(3).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let schedules = jalari::recurring::list().await.unwrap();
    for worker in workers {
        worker.stop().await;
    }

    let rows = audit(database, mark).await;
    for (name, expression) in [
        ("rs_every_second", "*/1 * * * * *"),
        ("rs_every_two", "*/2 * * * * *"),
    ] {
        let matching: Vec<&RecurringInfo> = schedules.iter().filter(|s| s.name == name).collect();
        let converged = matching.len() == 1
            && matching[0].managed
            && matching[0].enabled
            && matching[0].schedule == Some(jalari::Schedule::from(cron(expression, "UTC")))
            && matching[0].task == name;
        if !converged {
            violations.push(format!(
                "after 3 schedulers started together, {name} is {matching:?}, expected one \
                 enabled managed schedule with cron {expression:?}"
            ));
        }
        let inserts = rows
            .iter()
            .filter(|row| row.name == name && row.op == "INSERT")
            .count();
        let fires = rows
            .iter()
            .filter(|row| row.name == name && row.fired())
            .count();
        if inserts > 1 {
            violations.push(format!("{name} was inserted {inserts} times"));
        }
        if fires == 0 {
            violations.push(format!("{name} never fired while 3 schedulers ran"));
        }
        for row in rows
            .iter()
            .filter(|row| row.name == name && row.op == "UPDATE" && !row.fired())
        {
            if row.new_next != row.old_next {
                violations.push(format!(
                    "syncing the unchanged declaration of {name} moved its next run: {row:?}"
                ));
            }
        }
    }
    violations.extend(check_fires(&rows));
    violations
}

async fn check_ticks_fire_once(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let mark = audit_mark(database).await;
    let names: Vec<String> = ["tick-a", "tick-b", "tick-p"]
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    for (index, name) in names.iter().enumerate() {
        let expression = if index == 1 { CRONS[1] } else { CRONS[0] };
        let millis = if index == 0 { 1500 } else { 50 };
        jalari::recurring::add_or_update(
            name,
            cron(expression, "UTC"),
            &Pulse {
                op: index as u64,
                millis,
            },
        )
        .await
        .unwrap();
    }
    let workers = start_schedulers(3).await;
    let duration = Duration::from_secs(9 * scale);
    let begun = Instant::now();
    tokio::time::sleep(duration / 3).await;
    jalari::recurring::pause("tick-p").await.unwrap();
    let paused_at = db_now(&database.pool).await;
    tokio::time::sleep(duration / 3).await;
    let resumed_at = db_now(&database.pool).await;
    jalari::recurring::resume("tick-p").await.unwrap();
    tokio::time::sleep_until((begun + duration).into()).await;
    let stopped_at = db_now(&database.pool).await;
    for worker in workers {
        worker.stop().await;
    }
    for name in &names {
        jalari::recurring::remove(name).await.unwrap();
    }

    let rows = audit(database, mark).await;
    violations.extend(check_fires(&rows));
    violations.extend(check_jobs_come_from_fires(database, &rows, &names, &HashSet::new()).await);
    for row in rows
        .iter()
        .filter(|row| row.fired() && row.name == "tick-p")
    {
        if row.at >= paused_at && row.at < resumed_at {
            violations.push(format!(
                "tick-p fired {} ms after pause returned and before resume was called: {row:?}",
                row.at - paused_at
            ));
        }
    }
    for name in &names {
        let fires: Vec<&AuditRow> = rows
            .iter()
            .filter(|row| row.fired() && row.name == *name)
            .collect();
        let last = fires.last().and_then(|row| row.new_last).unwrap_or(0);
        if stopped_at - last > 3000 {
            violations.push(format!(
                "{name} stopped firing: its last run was for {last}, {} ms before the schedulers \
                 stopped",
                stopped_at - last
            ));
        }
        let jobs = keyed_jobs(database, name).await.len();
        eprintln!(
            "ticks_fire_once: {name} fired {} times, {jobs} jobs",
            fires.len()
        );
    }
    violations
}

async fn check_scheduler_restarts(database: &Database, mut rng: Rng) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let mark = audit_mark(database).await;
    let names: Vec<String> = ["restart-a", "restart-b"]
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    jalari::recurring::add_or_update(
        &names[0],
        cron(CRONS[0], "UTC"),
        &Pulse { op: 0, millis: 20 },
    )
    .await
    .unwrap();
    jalari::recurring::add_or_update(
        &names[1],
        cron(CRONS[1], "Asia/Kathmandu"),
        &Pulse { op: 1, millis: 20 },
    )
    .await
    .unwrap();

    let mut running = start_schedulers(2).await;
    let mut outages: Vec<(i64, i64)> = Vec::new();
    let mut crashes = 0;
    let deadline = Instant::now() + Duration::from_secs(12 * scale);
    while Instant::now() < deadline {
        let roll = rng.below(100);
        if running.is_empty() || (roll < 30 && running.len() < 3) {
            running.push(RunningWorker::spawn(build_scheduler().await));
        } else if roll < 50 {
            let index = usize::try_from(rng.below(running.len() as u64)).unwrap();
            running.swap_remove(index).stop().await;
        } else if roll < 75 {
            let index = usize::try_from(rng.below(running.len() as u64)).unwrap();
            running.swap_remove(index).crash().await;
            crashes += 1;
        } else if roll < 85 {
            for worker in running.drain(..) {
                if rng.chance(50) {
                    worker.crash().await;
                    crashes += 1;
                } else {
                    worker.stop().await;
                }
            }
            let down = db_now(&database.pool).await;
            tokio::time::sleep(rng.millis(2200, 3500)).await;
            let up = db_now(&database.pool).await;
            outages.push((down, up));
            running.push(RunningWorker::spawn(build_scheduler().await));
        }
        tokio::time::sleep(rng.millis(200, 900)).await;
    }
    if running.is_empty() {
        running.push(RunningWorker::spawn(build_scheduler().await));
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let stopped_at = db_now(&database.pool).await;
    for worker in running {
        worker.stop().await;
    }
    for name in &names {
        jalari::recurring::remove(name).await.unwrap();
    }

    let rows = audit(database, mark).await;
    violations.extend(check_fires(&rows));
    violations.extend(check_jobs_come_from_fires(database, &rows, &names, &HashSet::new()).await);
    let mut watched = names.clone();
    watched.extend(["rs_every_second".to_owned(), "rs_every_two".to_owned()]);
    for name in &watched {
        let fires: Vec<&AuditRow> = rows
            .iter()
            .filter(|row| row.fired() && row.name == *name)
            .collect();
        for (down, up) in &outages {
            let inside: Vec<i64> = fires
                .iter()
                .filter_map(|row| row.new_last)
                .filter(|last| last >= down && last < up)
                .collect();
            if inside.len() > 1 {
                violations.push(format!(
                    "{name} enqueued {} runs for times inside the outage {down}..{up}: {inside:?}, \
                     expected one catch-up (docs/guide.md:238)",
                    inside.len()
                ));
            }
        }
        let last = fires.last().and_then(|row| row.new_last).unwrap_or(0);
        if stopped_at - last > 3500 {
            violations.push(format!(
                "{name} did not fire after the last restart: last run for {last}, schedulers \
                 stopped at {stopped_at}"
            ));
        }
    }
    eprintln!(
        "scheduler_restarts: {} outages, {crashes} crashes, {} fires",
        outages.len(),
        rows.iter().filter(|row| row.fired()).count()
    );
    violations
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Add,
    Update,
    Pause,
    Resume,
    Remove,
    Trigger,
}

#[derive(Debug, Clone)]
struct Done {
    name: String,
    kind: Kind,
    cron: String,
    timezone: String,
    start: Instant,
    end: Instant,
    outcome: Result<Option<EnqueueOutcome>, ErrorKind>,
}

impl Done {
    fn exists_after(&self) -> Option<bool> {
        match (&self.outcome, self.kind) {
            (Err(ErrorKind::RecurringNotFound), _) | (Ok(_), Kind::Remove) => Some(false),
            (Ok(_), _) => Some(true),
            (Err(_), _) => None,
        }
    }
}

fn last_candidates<'a>(ops: &[&'a Done]) -> Vec<&'a Done> {
    ops.iter()
        .filter(|op| !ops.iter().any(|other| other.start > op.end))
        .copied()
        .collect()
}

fn check_final_state(name: &str, ops: &[Done], last: Option<&RecurringInfo>) -> Vec<String> {
    let mut violations = Vec::new();
    let touching: Vec<&Done> = ops
        .iter()
        .filter(|op| op.name == name && op.exists_after().is_some())
        .collect();
    let candidates = last_candidates(&touching);
    let exists = last.is_some();
    if !candidates
        .iter()
        .any(|op| op.exists_after() == Some(exists))
    {
        violations.push(format!(
            "{name} final existence {exists} matches none of the possibly-last operations \
             {candidates:?}"
        ));
    }
    let Some(info) = last else {
        return violations;
    };
    let writers: Vec<&Done> = ops
        .iter()
        .filter(|op| {
            op.name == name
                && op.outcome.is_ok()
                && matches!(op.kind, Kind::Add | Kind::Update | Kind::Remove)
        })
        .collect();
    let writers = last_candidates(&writers);
    if !writers.iter().any(|op| {
        matches!(op.kind, Kind::Add | Kind::Update)
            && info.schedule == Some(jalari::Schedule::from(cron(&op.cron, &op.timezone)))
    }) {
        violations.push(format!(
            "{name} ended with schedule {:?} which no possibly-last add_or_update or \
             update_schedule set: {writers:?}",
            info.schedule
        ));
    }
    let switches: Vec<&Done> = ops
        .iter()
        .filter(|op| {
            op.name == name
                && op.outcome.is_ok()
                && matches!(
                    op.kind,
                    Kind::Add | Kind::Remove | Kind::Pause | Kind::Resume
                )
        })
        .collect();
    let switches = last_candidates(&switches);
    if !switches.iter().any(|op| match op.kind {
        Kind::Pause => !info.enabled,
        Kind::Resume => info.enabled,
        _ => true,
    }) {
        violations.push(format!(
            "{name} ended with enabled {} which no possibly-last pause or resume explains: \
             {switches:?}",
            info.enabled
        ));
    }
    violations
}

async fn run_operation(name: String, kind: Kind, rng: &mut Rng, op: u64) -> Done {
    let expression = (*rng.pick(&CRONS)).to_owned();
    let timezone = (*rng.pick(&TIMEZONES)).to_owned();
    let millis = if rng.chance(30) { 1200 } else { rng.below(200) };
    let start = Instant::now();
    let outcome = match kind {
        Kind::Add => jalari::recurring::add_or_update(
            &name,
            cron(&expression, &timezone),
            &Pulse { op, millis },
        )
        .await
        .map(|()| None),
        Kind::Update => jalari::recurring::update_schedule(&name, cron(&expression, &timezone))
            .await
            .map(|()| None),
        Kind::Pause => jalari::recurring::pause(&name).await.map(|()| None),
        Kind::Resume => jalari::recurring::resume(&name).await.map(|()| None),
        Kind::Remove => jalari::recurring::remove(&name).await.map(|()| None),
        Kind::Trigger => jalari::recurring::trigger(&name).await.map(Some),
    };
    Done {
        name,
        kind,
        cron: expression,
        timezone,
        start,
        end: Instant::now(),
        outcome: outcome.map_err(|e| e.kind),
    }
}

async fn check_runtime_storm(database: &Database, mut rng: Rng) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let mark = audit_mark(database).await;
    let names: Vec<String> = (0..4).map(|n| format!("storm-{n}")).collect();
    for name in &names {
        jalari::recurring::add_or_update(name, cron(CRONS[0], "UTC"), &Pulse { op: 0, millis: 0 })
            .await
            .unwrap();
    }
    let workers = start_schedulers(3).await;
    let deadline = Instant::now() + Duration::from_secs(7 * scale);
    let tasks: Vec<_> = (0..4u64)
        .map(|task| {
            let mut rng = rng.fork(task);
            let names = names.clone();
            tokio::spawn(async move {
                let mut done = Vec::new();
                let mut round = 0;
                while Instant::now() < deadline {
                    let name = rng.pick(&names).clone();
                    let kind = *rng.pick(&[
                        Kind::Add,
                        Kind::Add,
                        Kind::Add,
                        Kind::Update,
                        Kind::Update,
                        Kind::Pause,
                        Kind::Resume,
                        Kind::Resume,
                        Kind::Remove,
                        Kind::Trigger,
                        Kind::Trigger,
                        Kind::Trigger,
                    ]);
                    done.push(run_operation(name, kind, &mut rng, task * 10_000 + round).await);
                    round += 1;
                    tokio::time::sleep(rng.millis(0, 120)).await;
                }
                done
            })
        })
        .collect();
    let mut ops = Vec::new();
    for task in tasks {
        ops.extend(task.await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let schedules = jalari::recurring::list().await.unwrap();
    let mut removed_at = 0;
    for name in &names {
        match jalari::recurring::remove(name).await {
            Ok(()) => {}
            Err(e) if e.kind == ErrorKind::RecurringNotFound => {}
            Err(e) => violations.push(format!("final remove of {name} failed: {e}")),
        }
        removed_at = db_now(&database.pool).await;
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;
    for worker in workers {
        worker.stop().await;
    }

    for op in &ops {
        let allowed = match (&op.outcome, op.kind) {
            (Ok(_), _) => true,
            (Err(ErrorKind::RecurringNotFound), kind) => kind != Kind::Add,
            (Err(_), _) => false,
        };
        if !allowed {
            violations.push(format!("undocumented error: {op:?}"));
        }
    }
    for name in &names {
        let last = schedules.iter().find(|info| info.name == *name);
        violations.extend(check_final_state(name, &ops, last));
    }

    let mut inserted = HashSet::new();
    for op in &ops {
        let Ok(Some(outcome)) = &op.outcome else {
            continue;
        };
        let key = job_key_of(database, outcome.id()).await;
        if key.as_deref() != Some(job_key(&op.name).as_str()) {
            violations.push(format!(
                "trigger of {} returned {outcome:?} but that job has job_key {key:?}",
                op.name
            ));
        }
        match outcome {
            EnqueueOutcome::Inserted(id) => {
                if !inserted.insert(id.0) {
                    violations.push(format!("two triggers both inserted job {id}"));
                }
            }
            EnqueueOutcome::Kept(_) => {}
            other => violations.push(format!("trigger returned {other:?}")),
        }
    }

    let rows = audit(database, mark).await;
    violations.extend(check_fires(&rows));
    violations.extend(check_jobs_come_from_fires(database, &rows, &names, &inserted).await);
    for row in rows
        .iter()
        .filter(|row| row.fired() && names.contains(&row.name) && row.at >= removed_at)
    {
        violations.push(format!("{} fired after it was removed: {row:?}", row.name));
    }
    for name in &names {
        for (job_id, at, _) in keyed_jobs(database, name).await {
            if at >= removed_at {
                violations.push(format!(
                    "job {job_id} for {name} was inserted {} ms after the schedule was removed",
                    at - removed_at
                ));
            }
        }
    }
    let triggers = ops.iter().filter(|op| op.kind == Kind::Trigger).count();
    let kept = ops
        .iter()
        .filter(|op| matches!(op.outcome, Ok(Some(EnqueueOutcome::Kept(_)))))
        .count();
    let not_found = ops.iter().filter(|op| op.outcome.is_err()).count();
    eprintln!(
        "runtime_storm: {} operations, {triggers} triggers ({} inserted, {kept} kept), \
         {not_found} not found, {} fires",
        ops.len(),
        inserted.len(),
        rows.iter().filter(|row| row.fired()).count()
    );
    violations
}

async fn trigger_together(name: &str, count: usize) -> Vec<jalari::Result<EnqueueOutcome>> {
    let tasks: Vec<_> = (0..count)
        .map(|_| {
            let name = name.to_owned();
            tokio::spawn(async move { jalari::recurring::trigger(&name).await })
        })
        .collect();
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    outcomes
}

fn expect_one_inserted(
    phase: &str,
    outcomes: &[jalari::Result<EnqueueOutcome>],
    previous: Option<JobId>,
) -> (Vec<String>, Option<JobId>) {
    let inserted: Vec<JobId> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            Ok(EnqueueOutcome::Inserted(id)) => Some(*id),
            _ => None,
        })
        .collect();
    let mut violations = Vec::new();
    let Some(id) = inserted.first().copied() else {
        return (
            vec![format!("{phase}: no trigger inserted a job: {outcomes:?}")],
            None,
        );
    };
    if inserted.len() != 1 || Some(id) == previous {
        violations.push(format!(
            "{phase}: expected exactly one new job, got {outcomes:?}"
        ));
    }
    for outcome in outcomes {
        match outcome {
            Ok(EnqueueOutcome::Inserted(_)) => {}
            Ok(EnqueueOutcome::Kept(kept)) if *kept == id => {}
            other => violations.push(format!(
                "{phase}: while job {id} was waiting a trigger returned {other:?}, expected \
                 Kept({id}) (jalari/src/recurring/api.rs:144-145)"
            )),
        }
    }
    (violations, Some(id))
}

async fn check_trigger_kept_exactly_when_pending(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let name = "gated";
    GATE_OPEN.store(false, Ordering::SeqCst);
    jalari::recurring::add_or_update(name, cron("0 0 0 1 1 *", "UTC"), &Gate)
        .await
        .unwrap();

    let outcomes = trigger_together(name, 8).await;
    let (found, first) = expect_one_inserted("waiting", &outcomes, None);
    violations.extend(found);
    let Some(first) = first else {
        return violations;
    };

    let worker = RunningWorker::spawn(
        Worker::builder()
            .queue("gate", 1)
            .config(worker_config(Duration::from_millis(100)))
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    );
    wait_until(
        "the gated job to start",
        Duration::from_secs(10),
        || async { LEDGER.count(&format!("gate:{first}"), "start") == 1 },
    )
    .await;
    for outcome in trigger_together(name, 8).await {
        if !matches!(outcome, Ok(EnqueueOutcome::Kept(id)) if id == first) {
            violations.push(format!(
                "running: while job {first} ran a trigger returned {outcome:?}, expected \
                 Kept({first}) (jalari/src/recurring/api.rs:144-145)"
            ));
        }
    }

    GATE_OPEN.store(true, Ordering::SeqCst);
    wait_until(
        "the gated job to succeed",
        Duration::from_secs(10),
        || async { job_state(database, first).await == "succeeded" },
    )
    .await;
    let outcomes = trigger_together(name, 8).await;
    let (found, second) = expect_one_inserted("after success", &outcomes, Some(first));
    violations.extend(found);
    if let Some(second) = second {
        wait_until(
            "the second job to succeed",
            Duration::from_secs(10),
            || async { job_state(database, second).await == "succeeded" },
        )
        .await;
    }

    jalari::recurring::pause(name).await.unwrap();
    let outcomes = trigger_together(name, 8).await;
    let (found, third) = expect_one_inserted("paused", &outcomes, second);
    violations.extend(found);
    if let Some(third) = third {
        wait_until(
            "the third job to succeed",
            Duration::from_secs(10),
            || async { job_state(database, third).await == "succeeded" },
        )
        .await;
    }
    jalari::recurring::resume(name).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let results = Arc::new(Mutex::new(Vec::new()));
    let hammers: Vec<_> = (0..6)
        .map(|_| {
            let stop = Arc::clone(&stop);
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let outcome = jalari::recurring::trigger(name).await;
                    results.lock().unwrap().push(outcome);
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let remover = tokio::spawn(async move { jalari::recurring::remove(name).await });
    let removed = remover.await.unwrap();
    let after_remove = jalari::recurring::trigger(name).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::SeqCst);
    for hammer in hammers {
        hammer.await.unwrap();
    }
    if let Err(e) = removed {
        violations.push(format!("remove while triggering failed: {e}"));
    }
    if !matches!(&after_remove, Err(e) if e.kind == ErrorKind::RecurringNotFound) {
        violations.push(format!(
            "trigger after remove returned {after_remove:?}, expected RecurringNotFound"
        ));
    }
    let results = std::mem::take(&mut *results.lock().unwrap());
    let mut inserted = HashSet::new();
    for outcome in &results {
        match outcome {
            Ok(EnqueueOutcome::Inserted(id)) => {
                inserted.insert(*id);
            }
            Ok(EnqueueOutcome::Kept(_)) => {}
            Err(e) if e.kind == ErrorKind::RecurringNotFound => {}
            other => violations.push(format!("hammered trigger returned {other:?}")),
        }
    }
    for outcome in results.iter().flatten() {
        let key = job_key_of(database, outcome.id()).await;
        if key.as_deref() != Some("recurring:gated") {
            violations.push(format!(
                "trigger returned {outcome:?} for a job with job_key {key:?}"
            ));
        }
    }
    let created = keyed_jobs(database, name).await.len();
    let expected = inserted.len() + [Some(first), second, third].iter().flatten().count();
    if created != expected {
        violations.push(format!(
            "{created} jobs carry job_key recurring:gated but triggers reported {expected} \
             inserts"
        ));
    }
    eprintln!(
        "trigger_kept_exactly_when_pending: hammer made {} triggers, {} inserted",
        results.len(),
        inserted.len()
    );
    worker.stop().await;
    violations
}
