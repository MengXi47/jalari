mod common;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use common::{Database, Rng, RunningWorker, fast_retry, worker_config};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{
    Cron, EnqueueOutcome, ErrorKind, Interval, IntervalUnit, Job, JobResult, RecurringInfo,
    Schedule, Worker,
};
use serde::{Deserialize, Serialize};

static GATE_OPEN: AtomicBool = AtomicBool::new(false);

#[derive(Serialize, Deserialize)]
struct Beat {
    millis: u64,
}

#[jalari::job(queue = "beat")]
impl Job for Beat {
    const NAME: &'static str = "rsch_beat";

    async fn run(self) -> JobResult {
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Gate;

#[jalari::job(queue = "gate")]
impl Job for Gate {
    const NAME: &'static str = "rsch_gate";

    async fn run(self) -> JobResult {
        while !GATE_OPEN.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct AuditRow {
    op: String,
    name: String,
    old_enabled: Option<bool>,
    new_enabled: Option<bool>,
    old_next: Option<i64>,
    new_next: Option<i64>,
    old_last: Option<i64>,
    new_last: Option<i64>,
    old_runs: Option<i32>,
    new_runs: Option<i32>,
    old_finished: Option<i64>,
    new_finished: Option<i64>,
    old_rule: Option<String>,
    new_rule: Option<String>,
    old_ends: Option<String>,
    new_ends: Option<String>,
    cron: Option<String>,
    timezone: String,
    interval_count: Option<i32>,
    interval_unit: Option<String>,
    anchor: Option<NaiveDateTime>,
    until_at: Option<i64>,
    max_runs: Option<i32>,
    at: i64,
    txid: i64,
}

impl AuditRow {
    fn fired(&self) -> bool {
        self.op == "UPDATE" && self.new_last != self.old_last
    }

    fn rhythm(&self) -> Option<(i64, i64)> {
        if let Some(expression) = &self.cron {
            let period = match expression.as_str() {
                "*/1 * * * * *" | "* * * * * *" => 1000,
                "*/2 * * * * *" => 2000,
                "*/3 * * * * *" => 3000,
                _ => return None,
            };
            return Some((period, 0));
        }
        let seconds = match self.interval_unit.as_deref()? {
            "seconds" => 1,
            "minutes" => 60,
            "hours" => 3600,
            _ => return None,
        };
        let period = i64::from(self.interval_count?) * seconds * 1000;
        Some((period, anchor_ms(self.anchor?, &self.timezone)))
    }

    fn ended_by_until(&self, expected: Option<i64>) -> bool {
        self.until_at
            .is_some_and(|until| expected.is_none_or(|expected| expected > until))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Add,
    Update,
}

#[derive(Debug, Clone)]
struct Write {
    kind: Kind,
    schedule: Schedule,
    start: Instant,
    end: Instant,
    outcome: Result<(), ErrorKind>,
}

fn selected(name: &str) -> bool {
    std::env::var("RACE_ONLY").map_or(true, |only| only == name)
}

fn job_key(name: &str) -> String {
    format!("recurring:{name}")
}

fn local(text: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").unwrap()
}

fn interval(count: u32, anchor: &str, timezone: &str) -> Interval {
    Interval::new(count, IntervalUnit::Seconds, local(anchor))
        .unwrap()
        .timezone(timezone)
        .unwrap()
}

fn cron(expression: &str, timezone: &str) -> Cron {
    Cron::new(expression).unwrap().timezone(timezone).unwrap()
}

fn anchor_ms(anchor: NaiveDateTime, timezone: &str) -> i64 {
    timezone
        .parse::<Tz>()
        .unwrap()
        .from_local_datetime(&anchor)
        .single()
        .unwrap()
        .timestamp_millis()
}

fn first_after(base: i64, (period, anchor): (i64, i64)) -> i64 {
    if base < anchor {
        anchor
    } else {
        anchor + ((base - anchor) / period + 1) * period
    }
}

fn instant(millis: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis).unwrap()
}

async fn install_audit(database: &Database) {
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE \"{schema}\".race_recurring (
             seq BIGSERIAL PRIMARY KEY,
             op TEXT NOT NULL,
             name TEXT NOT NULL,
             old_enabled BOOLEAN,
             new_enabled BOOLEAN,
             old_next BIGINT,
             new_next BIGINT,
             old_last BIGINT,
             new_last BIGINT,
             old_runs INTEGER,
             new_runs INTEGER,
             old_finished BIGINT,
             new_finished BIGINT,
             old_rule TEXT,
             new_rule TEXT,
             old_ends TEXT,
             new_ends TEXT,
             cron TEXT,
             timezone TEXT NOT NULL,
             interval_count INTEGER,
             interval_unit TEXT,
             anchor TIMESTAMP,
             until_at BIGINT,
             max_runs INTEGER,
             at BIGINT NOT NULL,
             txid BIGINT NOT NULL
         );
         CREATE FUNCTION \"{schema}\".race_ms(value TIMESTAMPTZ) RETURNS BIGINT
             LANGUAGE sql IMMUTABLE
             AS $$ SELECT floor(EXTRACT(EPOCH FROM value) * 1000)::bigint $$;
         CREATE FUNCTION \"{schema}\".race_rule(value \"{schema}\".recurring) RETURNS TEXT
             LANGUAGE sql IMMUTABLE AS $$ SELECT concat_ws('|',
                 coalesce(value.cron, '-'),
                 value.timezone,
                 coalesce(value.interval_count::text, '-'),
                 coalesce(value.interval_unit, '-'),
                 coalesce(value.anchor::text, '-')) $$;
         CREATE FUNCTION \"{schema}\".race_ends(value \"{schema}\".recurring) RETURNS TEXT
             LANGUAGE sql IMMUTABLE AS $$ SELECT concat_ws('|',
                 coalesce(EXTRACT(EPOCH FROM value.until_at)::text, '-'),
                 coalesce(value.max_runs::text, '-')) $$;
         CREATE FUNCTION \"{schema}\".race_recurring() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE
             latest \"{schema}\".recurring;
         BEGIN
             IF TG_OP = 'DELETE' THEN latest := OLD; ELSE latest := NEW; END IF;
             INSERT INTO \"{schema}\".race_recurring (
                 op, name, old_enabled, new_enabled, old_next, new_next, old_last, new_last,
                 old_runs, new_runs, old_finished, new_finished, old_rule, new_rule, old_ends,
                 new_ends, cron, timezone, interval_count, interval_unit, anchor, until_at, max_runs, at, txid
             ) VALUES (
                 TG_OP,
                 latest.name,
                 OLD.enabled,
                 NEW.enabled,
                 \"{schema}\".race_ms(OLD.next_run_at),
                 \"{schema}\".race_ms(NEW.next_run_at),
                 \"{schema}\".race_ms(OLD.last_run_at),
                 \"{schema}\".race_ms(NEW.last_run_at),
                 OLD.run_count,
                 NEW.run_count,
                 \"{schema}\".race_ms(OLD.finished_at),
                 \"{schema}\".race_ms(NEW.finished_at),
                 CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE \"{schema}\".race_rule(OLD) END,
                 CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE \"{schema}\".race_rule(NEW) END,
                 CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE \"{schema}\".race_ends(OLD) END,
                 CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE \"{schema}\".race_ends(NEW) END,
                 latest.cron,
                 latest.timezone,
                 latest.interval_count,
                 latest.interval_unit,
                 latest.anchor,
                 \"{schema}\".race_ms(latest.until_at),
                 latest.max_runs,
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
        "SELECT op, name, old_enabled, new_enabled, old_next, new_next, old_last, new_last,
             old_runs, new_runs, old_finished, new_finished, old_rule, new_rule, old_ends,
             new_ends, cron, timezone,
             interval_count, interval_unit, anchor, until_at, max_runs, at, txid
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
    sqlx::query_scalar("SELECT floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn keyed_jobs(database: &Database, name: &str) -> usize {
    let count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE job_key = $1",
        database.table("race_job")
    )))
    .bind(job_key(name))
    .fetch_one(&database.pool)
    .await
    .unwrap();
    usize::try_from(count).unwrap()
}

async fn info(name: &str) -> Option<RecurringInfo> {
    jalari::recurring::list()
        .await
        .unwrap()
        .into_iter()
        .find(|info| info.name == name)
}

async fn finished(name: &str) -> bool {
    info(name)
        .await
        .is_some_and(|info| info.finished_at.is_some())
}

async fn wait_for<F, Fut>(timeout: Duration, mut condition: F) -> bool
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn start_schedulers(count: usize) -> Vec<RunningWorker> {
    let mut built = Vec::new();
    for _ in 0..count {
        built.push(
            Worker::builder()
                .queue("beat", 2)
                .queue("gate", 1)
                .config(worker_config(Duration::from_millis(200)))
                .retry_policy(fast_retry())
                .build()
                .await
                .unwrap(),
        );
    }
    built.into_iter().map(RunningWorker::spawn).collect()
}

async fn stop_all(workers: Vec<RunningWorker>) {
    for worker in workers {
        worker.stop().await;
    }
}

async fn remove_all(names: &[&str]) {
    for name in names {
        jalari::recurring::remove(name).await.unwrap();
    }
}

fn fired_count(rows: &[AuditRow], name: &str) -> usize {
    rows.iter()
        .filter(|row| row.fired() && row.name == name)
        .count()
}

fn counted_fires(rows: &[AuditRow], name: &str) -> i32 {
    rows.iter()
        .filter(|row| row.fired() && row.name == name)
        .map(|row| row.new_runs.unwrap_or(0) - row.old_runs.unwrap_or(0))
        .sum()
}

fn check_rows(rows: &[AuditRow]) -> Vec<String> {
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
        if row.op != "DELETE" && row.new_next.is_none() != row.new_finished.is_some() {
            violations.push(format!(
                "{} has next_run_at {:?} with finished_at {:?}: {row:?}",
                row.name, row.new_next, row.new_finished
            ));
        }
        if row.fired() {
            violations.extend(check_fire(row, fired.get(row.name.as_str()).copied()));
            if let Some(last) = row.new_last {
                fired.insert(&row.name, last);
            }
        } else if row.op == "UPDATE" {
            violations.extend(check_change(row));
        } else if row.op == "INSERT" {
            violations.extend(check_planned(row, row.at, 0, "creating"));
        }
    }
    violations
}

fn check_fire(row: &AuditRow, previous: Option<i64>) -> Vec<String> {
    let mut violations = Vec::new();
    let name = &row.name;
    let Some(last) = row.new_last else {
        return vec![format!("{name} lost its last_run_at: {row:?}")];
    };
    if row.old_next != Some(last) {
        violations.push(format!(
            "{name} fired for {last} although it was due at {:?}: {row:?}",
            row.old_next
        ));
    }
    if row.old_enabled != Some(true) {
        violations.push(format!("{name} fired while paused: {row:?}"));
    }
    if row.old_finished.is_some() {
        violations.push(format!("{name} fired after it had finished: {row:?}"));
    }
    if row.old_rule != row.new_rule || row.old_ends != row.new_ends {
        violations.push(format!("{name} changed its schedule while firing: {row:?}"));
    }
    if let Some(previous) = previous
        && last <= previous
    {
        violations.push(format!(
            "{name} fired for {last} after already firing for {previous}: {row:?}"
        ));
    }
    let old_runs = row.old_runs.unwrap_or(0);
    let new_runs = row.new_runs.unwrap_or(0);
    if new_runs != old_runs && new_runs != old_runs + 1 {
        violations.push(format!(
            "{name} run_count went from {old_runs} to {new_runs} in one fire: {row:?}"
        ));
    }
    let rhythm = row.rhythm();
    if let Some((period, anchor)) = rhythm
        && (last - anchor).rem_euclid(period) != 0
    {
        violations.push(format!(
            "{name} fired for {last}, which is not a round of period {period} from {anchor}: \
             {row:?}"
        ));
    }
    let expected = rhythm.map(|rhythm| first_after(row.at.max(last), rhythm));
    let runs_left = row.max_runs.is_none_or(|max_runs| new_runs < max_runs);
    match row.new_next {
        Some(next) => {
            if next <= row.at {
                violations.push(format!(
                    "{name} fired at {} but its next run {next} is not in the future: {row:?}",
                    row.at
                ));
            }
            if let Some(expected) = expected
                && next != expected
            {
                violations.push(format!(
                    "{name} fired at {} for {last} and planned {next}, expected the first round \
                     after both, {expected}: {row:?}",
                    row.at
                ));
            }
            if !runs_left {
                violations.push(format!(
                    "{name} is still scheduled after {new_runs} of {:?} runs: {row:?}",
                    row.max_runs
                ));
            }
            if row.until_at.is_some_and(|until| next > until) {
                violations.push(format!(
                    "{name} planned {next} after its end {:?}: {row:?}",
                    row.until_at
                ));
            }
        }
        None => {
            if runs_left && !row.ended_by_until(expected) {
                violations.push(format!(
                    "{name} finished after {new_runs} runs although {:?} were allowed and the \
                     next round {expected:?} is not after its end {:?}: {row:?}",
                    row.max_runs, row.until_at
                ));
            }
        }
    }
    violations
}

fn check_planned(row: &AuditRow, base: i64, runs: i32, action: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let name = &row.name;
    if row.new_runs != Some(runs) {
        violations.push(format!(
            "{action} the schedule of {name} left run_count at {:?}, expected {runs}: {row:?}",
            row.new_runs
        ));
    }
    let expected = row.rhythm().map(|rhythm| first_after(base, rhythm));
    let runs_left = row.max_runs.is_none_or(|max_runs| runs < max_runs);
    match row.new_next {
        Some(next) => {
            if !runs_left {
                violations.push(format!(
                    "{action} the schedule of {name} planned {next} although {runs} of {:?} runs \
                     are used: {row:?}",
                    row.max_runs
                ));
            }
            if next <= base {
                violations.push(format!(
                    "{action} the schedule of {name} planned {next}, not after {base}: {row:?}"
                ));
            }
            if let Some(expected) = expected
                && next != expected
            {
                violations.push(format!(
                    "{action} the schedule of {name} planned {next}, expected {expected}: {row:?}"
                ));
            }
            if row.until_at.is_some_and(|until| next > until) {
                violations.push(format!(
                    "{action} the schedule of {name} planned {next} after its end: {row:?}"
                ));
            }
        }
        None => {
            if runs_left && !row.ended_by_until(expected) {
                violations.push(format!(
                    "{action} the schedule of {name} finished it after {runs} of {:?} runs \
                     although its next round {expected:?} is not after its end {:?}: {row:?}",
                    row.max_runs, row.until_at
                ));
            }
        }
    }
    violations
}

fn check_change(row: &AuditRow) -> Vec<String> {
    let base = row.old_last.map_or(row.at, |last| last.max(row.at));
    if row.old_rule != row.new_rule {
        return check_planned(row, base, 0, "changing");
    }
    if row.old_ends != row.new_ends {
        return check_planned(
            row,
            base,
            row.old_runs.unwrap_or(0),
            "changing only the end conditions of",
        );
    }
    let mut violations = Vec::new();
    let name = &row.name;
    if row.new_runs != row.old_runs {
        violations.push(format!(
            "an update that kept the schedule of {name} moved run_count from {:?} to {:?}: \
             {row:?}",
            row.old_runs, row.new_runs
        ));
    }
    if row.old_enabled == row.new_enabled {
        if row.new_next != row.old_next || row.new_finished != row.old_finished {
            violations.push(format!(
                "an update that kept the schedule of {name} moved next_run_at {:?} -> {:?} or \
                 finished_at {:?} -> {:?}: {row:?}",
                row.old_next, row.new_next, row.old_finished, row.new_finished
            ));
        }
    } else if row.old_finished.is_some()
        && (row.new_finished != row.old_finished || row.new_next.is_some())
    {
        violations.push(format!(
            "pausing or resuming the finished schedule {name} revived it: {row:?}"
        ));
    }
    violations
}

async fn check_jobs(
    database: &Database,
    rows: &[AuditRow],
    names: &[&str],
    triggered: &HashSet<i64>,
) -> Vec<String> {
    let mut violations = Vec::new();
    let jobs: Vec<(i64, Option<String>, i64)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job_id, job_key, txid FROM {} ORDER BY job_id",
        database.table("race_job")
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap();
    let mut by_transaction: HashMap<i64, Vec<(i64, Option<String>)>> = HashMap::new();
    for (job_id, key, txid) in &jobs {
        by_transaction
            .entry(*txid)
            .or_default()
            .push((*job_id, key.clone()));
    }
    let mut fires: HashMap<i64, &str> = HashMap::new();
    for row in rows
        .iter()
        .filter(|row| row.fired() && names.contains(&row.name.as_str()))
    {
        fires.insert(row.txid, &row.name);
        let inserted = by_transaction.get(&row.txid).map_or(&[][..], Vec::as_slice);
        if inserted.len() > 1 {
            violations.push(format!(
                "one fire of {} for {:?} inserted jobs {inserted:?}",
                row.name, row.new_last
            ));
        }
        for (job_id, key) in inserted {
            if key.as_deref() != Some(job_key(&row.name).as_str()) {
                violations.push(format!(
                    "the fire of {} inserted job {job_id} with job_key {key:?}",
                    row.name
                ));
            }
        }
        let counted = row.new_runs.unwrap_or(0) - row.old_runs.unwrap_or(0);
        if usize::try_from(counted).ok() != Some(inserted.len()) {
            violations.push(format!(
                "the fire of {} for {:?} inserted {} job(s) but run_count went from {:?} to {:?}",
                row.name,
                row.new_last,
                inserted.len(),
                row.old_runs,
                row.new_runs
            ));
        }
    }
    for (job_id, key, txid) in &jobs {
        let Some(name) = names
            .iter()
            .find(|name| key.as_deref() == Some(job_key(name).as_str()))
        else {
            continue;
        };
        if fires.get(txid) != Some(name) && !triggered.contains(job_id) {
            violations.push(format!(
                "job {job_id} with job_key {key:?} came from neither a fire nor a trigger"
            ));
        }
    }
    violations
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_race_schedule() {
    let Some(database) = Database::start("schedule", 30).await else {
        return;
    };
    install_audit(&database).await;
    let seed = common::seed("test_race_schedule");
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

    if selected("interval_fires_once") {
        let at = Instant::now();
        let violations = check_interval_fires_once(&database).await;
        report("interval_fires_once", violations, at);
    }
    if selected("max_runs_stops_exactly") {
        let at = Instant::now();
        let violations = check_max_runs_stops_exactly(&database).await;
        report("max_runs_stops_exactly", violations, at);
    }
    if selected("until_stops") {
        let at = Instant::now();
        let violations = check_until_stops(&database).await;
        report("until_stops", violations, at);
    }
    if selected("kept_rounds_do_not_count") {
        let at = Instant::now();
        let violations = check_kept_rounds_do_not_count(&database).await;
        report("kept_rounds_do_not_count", violations, at);
    }
    if selected("switch_between_cron_and_interval_under_fire") {
        let at = Instant::now();
        let violations =
            check_switch_between_cron_and_interval_under_fire(&database, rng.fork(1)).await;
        report(
            "switch_between_cron_and_interval_under_fire",
            violations,
            at,
        );
    }
    if selected("concurrent_add_or_update_keeps_count") {
        let at = Instant::now();
        let violations = check_concurrent_add_or_update_keeps_count(&database, rng.fork(2)).await;
        report("concurrent_add_or_update_keeps_count", violations, at);
    }
    if selected("sub_microsecond_schedule_keeps_count") {
        let at = Instant::now();
        let violations = check_sub_microsecond_schedule_keeps_count(&database).await;
        report("sub_microsecond_schedule_keeps_count", violations, at);
    }

    eprintln!("test_race_schedule finished in {:?}", started.elapsed());
    database.finish().await;
    assert!(
        failures.is_empty(),
        "{} violation(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

async fn check_interval_fires_once(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let mark = audit_mark(database).await;
    let plans: [(&str, Schedule, u64, i64); 5] = [
        (
            "iv-1-utc",
            interval(1, "2026-01-01 00:00:00", "UTC").into(),
            20,
            1,
        ),
        (
            "iv-2-ktm",
            interval(2, "2025-12-31 23:59:01", "Asia/Kathmandu").into(),
            20,
            2,
        ),
        (
            "iv-3-adl",
            interval(3, "2026-01-01 00:00:07", "Australia/Adelaide").into(),
            20,
            3,
        ),
        (
            "iv-1-slow",
            interval(1, "2025-06-01 12:00:00", "America/New_York").into(),
            1500,
            1,
        ),
        (
            "cron-2-tpe",
            cron("*/2 * * * * *", "Asia/Taipei").into(),
            20,
            2,
        ),
    ];
    let names: Vec<&str> = plans.iter().map(|(name, ..)| *name).collect();
    for (name, schedule, millis, _) in &plans {
        jalari::recurring::add_or_update(name, schedule.clone(), &Beat { millis: *millis })
            .await
            .unwrap();
    }
    let workers = start_schedulers(4).await;
    let duration = Duration::from_secs(6 * scale);
    tokio::time::sleep(duration).await;
    let schedules = jalari::recurring::list().await.unwrap();
    stop_all(workers).await;
    remove_all(&names).await;

    let rows = audit(database, mark).await;
    violations.extend(check_rows(&rows));
    violations.extend(check_jobs(database, &rows, &names, &HashSet::new()).await);
    for (name, schedule, _, period) in &plans {
        let fires = fired_count(&rows, name);
        let minimum =
            usize::try_from(i64::try_from(duration.as_secs()).unwrap() / period - 2).unwrap_or(0);
        if fires < minimum {
            violations.push(format!(
                "{name} fired {fires} times in {duration:?}, expected at least {minimum}"
            ));
        }
        match schedules.iter().find(|info| info.name == *name) {
            Some(info) if info.schedule.as_ref() == Some(schedule) => {}
            other => violations.push(format!(
                "{name} reads back as {other:?}, expected schedule {schedule}"
            )),
        }
        eprintln!(
            "interval_fires_once: {name} fired {fires} times, {} counted, {} jobs",
            counted_fires(&rows, name),
            keyed_jobs(database, name).await
        );
    }
    violations
}

async fn check_max_runs_stops_exactly(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let mark = audit_mark(database).await;
    let plans: [(&str, u32); 3] = [("mr-1", 1), ("mr-2", 2), ("mr-3", 3)];
    let names: Vec<&str> = plans.iter().map(|(name, _)| *name).collect();
    for (name, max_runs) in plans {
        let schedule = Schedule::from(interval(1, "2026-01-01 00:00:00", "UTC"))
            .max_runs(max_runs)
            .unwrap();
        jalari::recurring::add_or_update(name, schedule, &Beat { millis: 0 })
            .await
            .unwrap();
    }
    let workers = start_schedulers(3).await;
    let all_finished = wait_for(Duration::from_secs(15), || async {
        for name in ["mr-1", "mr-2", "mr-3"] {
            if !finished(name).await {
                return false;
            }
        }
        true
    })
    .await;
    if !all_finished {
        violations.push(format!(
            "the max_runs schedules did not all finish within 15 s: {:?}",
            jalari::recurring::list().await.unwrap()
        ));
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    for (name, max_runs) in plans {
        let jobs = keyed_jobs(database, name).await;
        if jobs != max_runs as usize {
            violations.push(format!(
                "{name} with max_runs {max_runs} enqueued {jobs} jobs"
            ));
        }
        let info = info(name).await.unwrap();
        if info.run_count != i32::try_from(max_runs).unwrap()
            || info.next_run_at.is_some()
            || info.finished_at.is_none()
        {
            violations.push(format!(
                "{name} with max_runs {max_runs} ended as {info:?}, expected run_count \
                 {max_runs}, no next run and a finished_at"
            ));
        }
    }

    let before = info("mr-2").await.unwrap();
    let mut triggered = HashSet::new();
    match jalari::recurring::trigger("mr-2").await {
        Ok(EnqueueOutcome::Inserted(id)) => {
            triggered.insert(id.0);
        }
        other => violations.push(format!(
            "triggering the finished mr-2 returned {other:?}, expected a new job"
        )),
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after_trigger = info("mr-2").await.unwrap();
    if after_trigger.run_count != before.run_count
        || after_trigger.finished_at != before.finished_at
        || after_trigger.next_run_at.is_some()
    {
        violations.push(format!(
            "triggering the finished mr-2 changed it from {before:?} to {after_trigger:?}"
        ));
    }
    jalari::recurring::pause("mr-2").await.unwrap();
    jalari::recurring::resume("mr-2").await.unwrap();
    let after_resume = info("mr-2").await.unwrap();
    if !after_resume.enabled
        || after_resume.run_count != before.run_count
        || after_resume.finished_at != before.finished_at
        || after_resume.next_run_at.is_some()
    {
        violations.push(format!(
            "pausing and resuming the finished mr-2 changed it from {before:?} to \
             {after_resume:?}"
        ));
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let jobs = keyed_jobs(database, "mr-2").await;
    if jobs != 3 {
        violations.push(format!(
            "mr-2 has {jobs} jobs after 2 runs and 1 trigger, expected 3"
        ));
    }

    let rule = Schedule::from(interval(1, "2026-01-01 00:00:00", "UTC"));
    jalari::recurring::update_schedule("mr-2", rule.clone().max_runs(4).unwrap())
        .await
        .unwrap();
    let raised = info("mr-2").await.unwrap();
    if raised.run_count != 2 || raised.next_run_at.is_none() || raised.finished_at.is_some() {
        violations.push(format!(
            "raising max_runs of the finished mr-2 from 2 to 4 left it as {raised:?}, expected \
             run_count 2 and a next run"
        ));
    }
    jalari::recurring::update_schedule("mr-3", rule.max_runs(2).unwrap())
        .await
        .unwrap();
    let lowered = info("mr-3").await.unwrap();
    if lowered.run_count != 3 || lowered.next_run_at.is_some() || lowered.finished_at.is_none() {
        violations.push(format!(
            "lowering max_runs of the finished mr-3 from 3 to 2 left it as {lowered:?}, expected \
             run_count 3 and finished"
        ));
    }
    let revived = wait_for(Duration::from_secs(10), || finished("mr-2")).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let ended = info("mr-2").await.unwrap();
    let jobs = keyed_jobs(database, "mr-2").await;
    if !revived || ended.run_count != 4 || ended.next_run_at.is_some() || jobs != 5 {
        violations.push(format!(
            "after raising max_runs to 4 mr-2 became {ended:?} with {jobs} jobs, expected \
             run_count 4, finished, and 5 jobs counting the trigger"
        ));
    }
    stop_all(workers).await;
    remove_all(&names).await;

    let rows = audit(database, mark).await;
    violations.extend(check_rows(&rows));
    violations.extend(check_jobs(database, &rows, &names, &triggered).await);
    for (name, max_runs) in plans {
        let counted = counted_fires(&rows, name);
        let expected = if name == "mr-2" { 4 } else { max_runs };
        if counted != i32::try_from(expected).unwrap() {
            violations.push(format!(
                "{name} counted {counted} fires in the audit, expected {expected}"
            ));
        }
        eprintln!(
            "max_runs_stops_exactly: {name} fired {} times, {counted} counted",
            fired_count(&rows, name)
        );
    }
    violations
}

async fn check_until_stops(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let mark = audit_mark(database).await;
    let now = db_now(&database.pool).await;
    let whole = now - now.rem_euclid(1000) + 4000;
    let plans: [(&str, i64); 2] = [("until-whole", whole), ("until-half", whole - 1500)];
    let names: Vec<&str> = plans.iter().map(|(name, _)| *name).collect();
    for (name, until) in plans {
        let schedule =
            Schedule::from(interval(1, "2026-01-01 00:00:00", "Asia/Taipei")).until(instant(until));
        jalari::recurring::add_or_update(name, schedule, &Beat { millis: 0 })
            .await
            .unwrap();
    }
    let workers = start_schedulers(3).await;
    let all_finished = wait_for(Duration::from_secs(12), || async {
        finished("until-whole").await && finished("until-half").await
    })
    .await;
    if !all_finished {
        violations.push(format!(
            "the until schedules did not finish within 12 s: {:?}",
            jalari::recurring::list().await.unwrap()
        ));
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut infos = Vec::new();
    for (name, _) in plans {
        infos.push(info(name).await.unwrap());
    }
    stop_all(workers).await;
    remove_all(&names).await;

    let rows = audit(database, mark).await;
    violations.extend(check_rows(&rows));
    violations.extend(check_jobs(database, &rows, &names, &HashSet::new()).await);
    for ((name, until), info) in plans.iter().zip(&infos) {
        let until_at = instant(*until);
        let last = info.last_run_at;
        if info.finished_at.is_none()
            || info.next_run_at.is_some()
            || last.is_none_or(|last| last > until_at)
            || last.is_some_and(|last| last < until_at - chrono::TimeDelta::seconds(2))
        {
            violations.push(format!(
                "{name} with until {until_at} ended as {info:?}, expected it finished with its \
                 last run within 2 s before the end"
            ));
        }
        for row in rows.iter().filter(|row| row.fired() && row.name == *name) {
            if row.new_last.is_some_and(|last| last > *until) {
                violations.push(format!(
                    "{name} fired for {:?} after its end {until}: {row:?}",
                    row.new_last
                ));
            }
        }
        let jobs = keyed_jobs(database, name).await;
        let counted = counted_fires(&rows, name);
        if usize::try_from(counted).ok() != Some(jobs) || info.run_count != counted {
            violations.push(format!(
                "{name} has {jobs} jobs, {counted} counted fires and run_count {}",
                info.run_count
            ));
        }
        eprintln!(
            "until_stops: {name} fired {} times, {jobs} jobs, last run {last:?}, end {until_at}",
            fired_count(&rows, name)
        );
    }
    violations
}

async fn check_kept_rounds_do_not_count(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let mark = audit_mark(database).await;
    let name = "kept-gate";
    GATE_OPEN.store(false, Ordering::SeqCst);
    let schedule = Schedule::from(interval(1, "2026-01-01 00:00:00", "Europe/Berlin"))
        .max_runs(3)
        .unwrap();
    jalari::recurring::add_or_update(name, schedule, &Gate)
        .await
        .unwrap();
    let workers = start_schedulers(3).await;
    tokio::time::sleep(Duration::from_secs(4 * scale)).await;
    let held = info(name).await.unwrap();
    let held_jobs = keyed_jobs(database, name).await;
    let held_fires = fired_count(&audit(database, mark).await, name);
    if held.run_count != 1 || held.finished_at.is_some() || held.next_run_at.is_none() {
        violations.push(format!(
            "while the first run was still pending {name} became {held:?}, expected run_count 1 \
             and not finished"
        ));
    }
    if held_jobs != 1 {
        violations.push(format!(
            "while the first run was still pending {name} enqueued {held_jobs} jobs"
        ));
    }
    if held_fires < 3 {
        violations.push(format!(
            "{name} fired only {held_fires} rounds while held, so kept rounds were not exercised"
        ));
    }
    GATE_OPEN.store(true, Ordering::SeqCst);
    let done = wait_for(Duration::from_secs(15), || finished(name)).await;
    let last = info(name).await.unwrap();
    if !done || last.run_count != 3 || last.next_run_at.is_some() {
        violations.push(format!(
            "after the gate opened {name} became {last:?}, expected run_count 3 and finished"
        ));
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let jobs = keyed_jobs(database, name).await;
    if jobs != 3 {
        violations.push(format!("{name} with max_runs 3 enqueued {jobs} jobs"));
    }
    stop_all(workers).await;
    remove_all(&[name]).await;
    GATE_OPEN.store(false, Ordering::SeqCst);

    let rows = audit(database, mark).await;
    violations.extend(check_rows(&rows));
    violations.extend(check_jobs(database, &rows, &[name], &HashSet::new()).await);
    eprintln!(
        "kept_rounds_do_not_count: {held_fires} fires while held, {} fires in total, {jobs} jobs",
        fired_count(&rows, name)
    );
    violations
}

fn switch_candidates() -> Vec<Schedule> {
    let rules: [Schedule; 4] = [
        cron("*/1 * * * * *", "UTC").into(),
        cron("*/1 * * * * *", "Asia/Taipei").into(),
        interval(1, "2026-01-01 00:00:00", "UTC").into(),
        interval(2, "2026-01-01 00:00:01", "Asia/Kathmandu").into(),
    ];
    let mut candidates = Vec::new();
    for rule in rules {
        candidates.push(rule.clone().max_runs(1).unwrap());
        candidates.push(rule.clone().max_runs(3).unwrap());
        candidates.push(rule);
    }
    candidates
}

fn last_candidates<'a>(writes: &[&'a Write]) -> Vec<&'a Write> {
    writes
        .iter()
        .filter(|write| !writes.iter().any(|other| other.start > write.end))
        .copied()
        .collect()
}

async fn run_write(name: &str, kind: Kind, schedule: Schedule) -> Write {
    let start = Instant::now();
    let outcome = match kind {
        Kind::Add => {
            jalari::recurring::add_or_update(name, schedule.clone(), &Beat { millis: 0 }).await
        }
        Kind::Update => jalari::recurring::update_schedule(name, schedule.clone()).await,
    };
    Write {
        kind,
        schedule,
        start,
        end: Instant::now(),
        outcome: outcome.map_err(|e| e.kind),
    }
}

async fn check_switch_between_cron_and_interval_under_fire(
    database: &Database,
    mut rng: Rng,
) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let mark = audit_mark(database).await;
    let name = "switch";
    let candidates = switch_candidates();
    let mut writes = vec![run_write(name, Kind::Add, candidates[2].clone()).await];
    let workers = start_schedulers(3).await;
    let deadline = Instant::now() + Duration::from_secs(5 * scale);
    let tasks: Vec<_> = (0..3u64)
        .map(|task| {
            let mut rng = rng.fork(task);
            let candidates = candidates.clone();
            tokio::spawn(async move {
                let mut done: Vec<Write> = Vec::new();
                while Instant::now() < deadline {
                    let repeat = done.last().filter(|_| rng.chance(35));
                    let schedule = match repeat {
                        Some(previous) => previous.schedule.clone(),
                        None => rng.pick(&candidates).clone(),
                    };
                    let kind = if rng.chance(50) {
                        Kind::Add
                    } else {
                        Kind::Update
                    };
                    done.push(run_write(name, kind, schedule).await);
                    tokio::time::sleep(rng.millis(30, 400)).await;
                }
                done
            })
        })
        .collect();
    for task in tasks {
        writes.extend(task.await.unwrap());
    }
    let counting = wait_for(Duration::from_secs(5), || async {
        info(name).await.is_some_and(|info| info.run_count > 0)
    })
    .await;
    let last = info(name).await;
    if let Some(before) = &last
        && let Some(schedule) = &before.schedule
    {
        jalari::recurring::add_or_update(name, schedule.clone(), &Beat { millis: 0 })
            .await
            .unwrap();
        jalari::recurring::update_schedule(name, schedule.clone())
            .await
            .unwrap();
        let after = info(name).await.unwrap();
        if !counting
            || after.run_count < before.run_count
            || (before.finished_at.is_some() && after.finished_at != before.finished_at)
        {
            violations.push(format!(
                "repeating the current schedule of {name} turned {before:?} into {after:?}"
            ));
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop_all(workers).await;
    remove_all(&[name]).await;

    for write in &writes {
        if write.outcome.is_err() {
            violations.push(format!("a write failed: {write:?}"));
        }
    }
    let succeeded: Vec<&Write> = writes.iter().filter(|w| w.outcome.is_ok()).collect();
    let possibly_last = last_candidates(&succeeded);
    match &last {
        Some(info)
            if possibly_last
                .iter()
                .any(|w| info.schedule.as_ref() == Some(&w.schedule)) => {}
        other => violations.push(format!(
            "{name} ended as {other:?}, which no possibly-last write set: {possibly_last:?}"
        )),
    }

    let rows = audit(database, mark).await;
    violations.extend(check_rows(&rows));
    violations.extend(check_jobs(database, &rows, &[name], &HashSet::new()).await);
    let updates: Vec<&AuditRow> = rows
        .iter()
        .filter(|row| row.op == "UPDATE" && !row.fired())
        .collect();
    let rule_changes = updates
        .iter()
        .filter(|row| row.old_rule != row.new_rule)
        .count();
    let end_changes: Vec<&&AuditRow> = updates
        .iter()
        .filter(|row| row.old_rule == row.new_rule && row.old_ends != row.new_ends)
        .collect();
    let end_changes_counting = end_changes
        .iter()
        .filter(|row| row.old_runs.unwrap_or(0) > 0)
        .count();
    let revived = end_changes
        .iter()
        .filter(|row| row.old_finished.is_some() && row.new_next.is_some())
        .count();
    let repeats = updates.len() - rule_changes - end_changes.len();
    let finishes = rows
        .iter()
        .filter(|row| row.fired() && row.new_next.is_none())
        .count();
    eprintln!(
        "switch_between_cron_and_interval_under_fire: {} writes ({} add_or_update), \
         {rule_changes} rule changes, {} end-only changes ({end_changes_counting} with runs, \
         {revived} revived), {repeats} repeats, {} fires, {finishes} finishes",
        writes.len(),
        writes.iter().filter(|w| w.kind == Kind::Add).count(),
        end_changes.len(),
        fired_count(&rows, name)
    );
    violations
}

async fn check_concurrent_add_or_update_keeps_count(
    database: &Database,
    mut rng: Rng,
) -> Vec<String> {
    let mut violations = Vec::new();
    let scale = common::scale();
    let mark = audit_mark(database).await;
    let name = "same";
    let schedule = Schedule::from(interval(1, "2026-01-01 00:00:00", "Europe/Berlin"))
        .until(Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap())
        .max_runs(1000)
        .unwrap();
    jalari::recurring::add_or_update(name, schedule.clone(), &Beat { millis: 0 })
        .await
        .unwrap();
    let workers = start_schedulers(3).await;
    let duration = Duration::from_secs(5 * scale);
    let deadline = Instant::now() + duration;
    let tasks: Vec<_> = (0..8u64)
        .map(|task| {
            let mut rng = rng.fork(task);
            let schedule = schedule.clone();
            tokio::spawn(async move {
                let mut failed = Vec::new();
                let mut calls = 0;
                while Instant::now() < deadline {
                    if let Err(e) = jalari::recurring::add_or_update(
                        name,
                        schedule.clone(),
                        &Beat { millis: 0 },
                    )
                    .await
                    {
                        failed.push(e.to_string());
                    }
                    calls += 1;
                    tokio::time::sleep(rng.millis(0, 40)).await;
                }
                (calls, failed)
            })
        })
        .collect();
    let mut calls = 0;
    for task in tasks {
        let (made, failed) = task.await.unwrap();
        calls += made;
        violations.extend(
            failed
                .into_iter()
                .map(|e| format!("add_or_update failed: {e}")),
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let last = info(name).await.unwrap();
    stop_all(workers).await;
    remove_all(&[name]).await;

    let rows = audit(database, mark).await;
    violations.extend(check_rows(&rows));
    violations.extend(check_jobs(database, &rows, &[name], &HashSet::new()).await);
    for row in rows.iter().filter(|row| {
        row.op == "UPDATE" && (row.old_rule != row.new_rule || row.old_ends != row.new_ends)
    }) {
        violations.push(format!(
            "an identical add_or_update changed the stored schedule: {row:?}"
        ));
    }
    let counted = counted_fires(&rows, name);
    let minimum = i32::try_from(duration.as_secs()).unwrap() - 2;
    if last.schedule.as_ref() != Some(&schedule)
        || last.run_count != counted
        || last.run_count < minimum
    {
        violations.push(format!(
            "after {calls} identical add_or_update calls {name} is {last:?}, expected schedule \
             {schedule} and run_count {counted} (at least {minimum})"
        ));
    }
    eprintln!(
        "concurrent_add_or_update_keeps_count: {calls} calls, {} fires, run_count {}",
        fired_count(&rows, name),
        last.run_count
    );
    violations
}

async fn check_sub_microsecond_schedule_keeps_count(database: &Database) -> Vec<String> {
    let mut violations = Vec::new();
    let mark = audit_mark(database).await;
    let until = Utc
        .with_ymd_and_hms(2030, 1, 1, 0, 0, 0)
        .unwrap()
        .with_nanosecond(123_456_789)
        .unwrap();
    let plans: [(&str, Schedule); 2] = [
        (
            "nanos-until",
            Schedule::from(interval(1, "2026-01-01 00:00:00", "UTC")).until(until),
        ),
        (
            "nanos-anchor",
            Schedule::from(interval(1, "2026-01-01 00:00:00.000000500", "UTC")),
        ),
    ];
    let names: Vec<&str> = plans.iter().map(|(name, _)| *name).collect();
    for (name, schedule) in &plans {
        jalari::recurring::add_or_update(name, schedule.clone(), &Beat { millis: 0 })
            .await
            .unwrap();
    }
    let workers = start_schedulers(2).await;
    let counting = wait_for(Duration::from_secs(10), || async {
        for name in ["nanos-until", "nanos-anchor"] {
            if info(name).await.is_none_or(|info| info.run_count < 2) {
                return false;
            }
        }
        true
    })
    .await;
    if !counting {
        violations.push("the sub-microsecond schedules did not run twice in 10 s".to_owned());
    }
    for (name, schedule) in &plans {
        let before = info(name).await.unwrap();
        jalari::recurring::add_or_update(name, schedule.clone(), &Beat { millis: 0 })
            .await
            .unwrap();
        let after = info(name).await.unwrap();
        if after.run_count < before.run_count {
            violations.push(format!(
                "repeating add_or_update with the same schedule {schedule} reset run_count of \
                 {name} from {} to {}",
                before.run_count, after.run_count
            ));
        }
        if after.schedule.as_ref() != Some(schedule) {
            violations.push(format!(
                "{name} was set to {schedule:?} but reads back as {:?}",
                after.schedule
            ));
        }
    }
    stop_all(workers).await;
    remove_all(&names).await;

    let rows = audit(database, mark).await;
    violations.extend(check_rows(&rows));
    violations
}
