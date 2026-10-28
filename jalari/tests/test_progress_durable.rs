mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::Duration;

use common::{Database, RunningWorker, fast_retry, wait_until};
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{Job, JobError, JobId, JobResult, StopOutcome, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const QUEUE: &str = "progress_durable";
const SETTLED: Duration = Duration::from_secs(15);
const LOST: &str = "the worker no longer owns the job";
const FLUSH_STATEMENT: &str = "AS batch";
const SAVE_NOW_STATEMENT: &str = "AS present";

struct Tables {
    pool: PgPool,
    schema: String,
    job_control: String,
    stall: String,
}

static TABLES: OnceLock<Tables> = OnceLock::new();

static OPEN: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

static RESUMED: LazyLock<Mutex<HashMap<String, Vec<Option<String>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static REACHED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

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

fn resumed(label: &str) -> Vec<Option<String>> {
    RESUMED
        .lock()
        .unwrap()
        .get(label)
        .cloned()
        .unwrap_or_default()
}

static ERRORS: LazyLock<Mutex<HashMap<String, JobError>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn record_error(label: &str, error: JobError) {
    ERRORS.lock().unwrap().insert(label.to_owned(), error);
}

fn recorded_error(label: &str) -> Option<JobError> {
    ERRORS.lock().unwrap().get(label).cloned()
}

fn record_resumed(label: &str, saved: Option<String>) {
    RESUMED
        .lock()
        .unwrap()
        .entry(label.to_owned())
        .or_default()
        .push(saved);
}

static LEASES: LazyLock<Mutex<HashMap<String, Vec<Option<i64>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn leases(label: &str) -> Vec<Option<i64>> {
    LEASES
        .lock()
        .unwrap()
        .get(label)
        .cloned()
        .unwrap_or_default()
}

async fn delete_control_row(job_id: JobId) {
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {} WHERE job_id = $1",
        tables.job_control
    )))
    .bind(job_id.0)
    .execute(&tables.pool)
    .await
    .unwrap();
}

#[derive(Serialize, Deserialize)]
struct Leases {
    label: String,
    drop_control_row: bool,
}

#[jalari::job(queue = "progress_durable", max_attempts = 3)]
impl Job for Leases {
    const NAME: &'static str = "progress_leases";

    async fn run(self) -> JobResult {
        let run = jalari::current_job().unwrap();
        let lease = jalari::admin::job(run.id())
            .await?
            .and_then(|detail| detail.control)
            .map(|control| control.lease);
        LEASES
            .lock()
            .unwrap()
            .entry(self.label.clone())
            .or_default()
            .push(lease);
        if run.attempt() > 1 {
            return Ok(());
        }
        if self.drop_control_row {
            delete_control_row(run.id()).await;
        }
        Err(JobError::new("the first run fails".to_owned()))
    }
}

#[derive(Serialize, Deserialize)]
struct Overtaken {
    label: String,
}

#[jalari::job(queue = "progress_durable", max_attempts = 3)]
impl Job for Overtaken {
    const NAME: &'static str = "progress_overtaken";

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<String>()?;
        RESUMED
            .lock()
            .unwrap()
            .entry(self.label.clone())
            .or_default()
            .push(saved);
        if jalari::current_job().unwrap().attempt() > 1 {
            return Ok(());
        }
        checkpoint.save(&"older".to_owned()).await?;
        pass(&self.label).await;
        Err(JobError::new("the first run fails".to_owned()))
    }
}

#[derive(Serialize, Deserialize)]
struct Durable {
    label: String,
}

#[jalari::job(queue = "progress_durable", max_attempts = 3)]
impl Job for Durable {
    const NAME: &'static str = "progress_durable_crash";

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<String>()?;
        record_resumed(&self.label, saved);
        if jalari::current_job().unwrap().attempt() > 1 {
            return Ok(());
        }
        checkpoint.save_now(&"durable".to_owned()).await?;
        reach(&self.label);
        tokio::time::sleep(Duration::from_secs(600)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Zombie {
    label: String,
}

#[jalari::job(queue = "progress_durable", max_attempts = 3)]
impl Job for Zombie {
    const NAME: &'static str = "progress_zombie";

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<String>()?;
        record_resumed(&self.label, saved);
        if resumed(&self.label).len() > 1 {
            return Ok(());
        }
        pass(&self.label).await;
        if let Err(e) = checkpoint.save_now(&"zombie".to_owned()).await {
            record_error(&self.label, e);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        reach(&format!("{}:continued", self.label));
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Leftover {
    label: String,
}

#[jalari::job(queue = "progress_durable", max_attempts = 3)]
impl Job for Leftover {
    const NAME: &'static str = "progress_leftover";

    async fn run(self) -> JobResult {
        let (_, checkpoint) = jalari::resume::<String>()?;
        if jalari::current_job().unwrap().attempt() == 1 {
            let label = self.label.clone();
            tokio::spawn(async move {
                pass(&format!("{label}:go")).await;
                let written = checkpoint.save_now(&"stale".to_owned()).await;
                record_error(
                    &label,
                    written
                        .err()
                        .unwrap_or_else(|| JobError::new("stored".to_owned())),
                );
            });
            return Err(JobError::new("the first run fails".to_owned()));
        }
        checkpoint.save_now(&"fresh".to_owned()).await?;
        pass(&format!("{}:fresh", self.label)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Regress {
    label: String,
}

#[jalari::job(queue = "progress_durable")]
impl Job for Regress {
    const NAME: &'static str = "progress_regress";

    async fn run(self) -> JobResult {
        let (_, checkpoint) = jalari::resume::<String>()?;
        checkpoint.save(&"A".to_owned()).await?;
        pass(&format!("{}:saved", self.label)).await;
        checkpoint.save_now(&"C".to_owned()).await?;
        pass(&format!("{}:now", self.label)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct LostSave {
    label: String,
}

#[jalari::job(queue = "progress_durable")]
impl Job for LostSave {
    const NAME: &'static str = "progress_lost_save";

    async fn run(self) -> JobResult {
        let (_, checkpoint) = jalari::resume::<String>()?;
        let label = self.label.clone();
        let durable = checkpoint.clone();
        tokio::spawn(async move {
            let written = durable.save_now(&"C".to_owned()).await;
            reach(&format!("{label}:now"));
            if let Err(e) = written {
                record_error(&label, e);
            }
        });
        tokio::time::sleep(Duration::from_millis(1100)).await;
        checkpoint.save(&"D".to_owned()).await?;
        pass(&format!("{}:saved", self.label)).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct MissingRow {
    label: String,
}

#[jalari::job(queue = "progress_durable")]
impl Job for MissingRow {
    const NAME: &'static str = "progress_missing_row";

    async fn run(self) -> JobResult {
        let (_, checkpoint) = jalari::resume::<String>()?;
        delete_control_row(jalari::current_job().unwrap().id()).await;
        if let Err(e) = checkpoint.save_now(&"nowhere".to_owned()).await {
            record_error(&self.label, e);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct StopNow {
    label: String,
}

#[jalari::job(queue = "progress_durable")]
impl Job for StopNow {
    const NAME: &'static str = "progress_stop_now";

    async fn run(self) -> JobResult {
        let (_, checkpoint) = jalari::resume::<u32>()?;
        for count in 0.. {
            if let Err(e) = checkpoint.save_now(&count).await {
                LEASES
                    .lock()
                    .unwrap()
                    .insert(self.label.clone(), vec![Some(i64::from(count))]);
                return Err(e);
            }
            reach(&self.label);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Parked;

#[jalari::job(queue = "progress_durable")]
impl Job for Parked {
    const NAME: &'static str = "progress_parked";

    async fn run(self) -> JobResult {
        Err(JobError::new("parked for an hour".to_owned()).retry_after(Duration::from_secs(3600)))
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

async fn stored(id: JobId) -> Option<Value> {
    let tables = TABLES.get().unwrap();
    sqlx::query_scalar::<_, Option<Value>>(AssertSqlSafe(format!(
        "SELECT progress FROM {} WHERE job_id = $1",
        tables.job_control
    )))
    .bind(id.0)
    .fetch_optional(&tables.pool)
    .await
    .unwrap()
    .flatten()
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

async fn install_stall() {
    let tables = TABLES.get().unwrap();
    let schema = &tables.schema;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {stall} (marker text NOT NULL, seconds float8 NOT NULL);
         CREATE FUNCTION \"{schema}\".stall_statement() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE
             wait float8;
         BEGIN
             SELECT max(seconds) INTO wait FROM {stall}
             WHERE strpos(current_query(), marker) > 0;
             IF wait IS NOT NULL THEN
                 PERFORM pg_sleep(wait);
             END IF;
             RETURN NULL;
         END $$;
         CREATE TRIGGER stall_statement BEFORE UPDATE ON {job_control}
             FOR EACH STATEMENT EXECUTE FUNCTION \"{schema}\".stall_statement();",
        stall = tables.stall,
        job_control = tables.job_control,
    )))
    .execute(&tables.pool)
    .await
    .unwrap();
}

async fn stall(marker: &str, seconds: f64) {
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (marker, seconds) VALUES ($1, $2)",
        tables.stall
    )))
    .bind(marker)
    .bind(seconds)
    .execute(&tables.pool)
    .await
    .unwrap();
}

async fn unstall() {
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!("DELETE FROM {}", tables.stall)))
        .execute(&tables.pool)
        .await
        .unwrap();
}

async fn stalled(marker: &str) -> bool {
    let tables = TABLES.get().unwrap();
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_stat_activity
             WHERE datname = current_database() AND application_name = $1
                 AND wait_event = 'PgSleep' AND strpos(query, $2) > 0
         )",
    )
    .bind(&tables.schema)
    .bind(marker)
    .fetch_one(&tables.pool)
    .await
    .unwrap()
}

async fn run_leases(label: &str, drop_control_row: bool) -> Vec<Option<i64>> {
    let id = jalari::enqueue(&Leases {
        label: label.to_owned(),
        drop_control_row,
    })
    .await
    .unwrap()
    .id();
    wait_until(&format!("{label} to succeed"), SETTLED, || async {
        state(id).await == "succeeded"
    })
    .await;
    leases(label)
}

async fn check_each_run_has_its_own_lease() {
    let seen = run_leases("unique", false).await;
    let [Some(first), Some(second)] = seen[..] else {
        panic!("every run must see its control row, saw {seen:?}");
    };
    assert!(
        second > first,
        "the second run must hold a newer lease than the first: {seen:?}"
    );
}

async fn check_close_without_a_row_still_fences() {
    let seen = run_leases("rowless", true).await;
    let [Some(first), Some(second)] = seen[..] else {
        panic!("every run must see its control row, saw {seen:?}");
    };
    assert_ne!(
        first, second,
        "a run whose control row vanished must not share its lease with the next run"
    );
}

async fn check_close_keeps_a_newer_stored_copy() {
    let label = "overtaken";
    let id = jalari::enqueue(&Overtaken {
        label: label.to_owned(),
    })
    .await
    .unwrap()
    .id();
    wait_until("the first run to save", SETTLED, || async {
        reached(label)
    })
    .await;
    let tables = TABLES.get().unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET progress = '\"newer\"', progress_seq = progress_seq + 100
         WHERE job_id = $1",
        tables.job_control
    )))
    .bind(id.0)
    .execute(&tables.pool)
    .await
    .unwrap();
    open(label);
    wait_until("the second run to succeed", SETTLED, || async {
        state(id).await == "succeeded"
    })
    .await;
    assert_eq!(resumed(label), [None, Some("newer".to_owned())]);
}

async fn check_save_now_survives_a_crash(worker: RunningWorker) -> RunningWorker {
    let label = "durable";
    let id = enqueue(&Durable {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    worker.crash().await;
    let worker = spawn_worker().await;
    wait_for_state(id, "succeeded").await;
    assert_eq!(resumed(label), [None, Some("durable".to_owned())]);
    worker
}

async fn check_fenced_out_save_now_ends_the_run() {
    let label = "zombie";
    let id = enqueue(&Zombie {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    bump_lease(id).await;
    open(label);
    wait_until("the zombie's save_now to fail", SETTLED, || async {
        recorded_error(label).is_some()
    })
    .await;
    let error = recorded_error(label).unwrap();
    assert_eq!(error.msg(), LOST);
    assert!(error.is_interruption());
    wait_for_state(id, "succeeded").await;
    assert!(
        !reached("zombie:continued"),
        "a run that ignored the lost error kept running past its next await"
    );
    assert_eq!(resumed(label), [None, None]);
}

async fn check_leftover_save_now_is_rejected() {
    let label = "leftover";
    let id = enqueue(&Leftover {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate("leftover:fresh").await;
    open("leftover:go");
    wait_until("the leftover save_now to answer", SETTLED, || async {
        recorded_error(label).is_some()
    })
    .await;
    assert_eq!(recorded_error(label).unwrap().msg(), LOST);
    assert_eq!(stored(id).await, Some(json!("fresh")));
    open("leftover:fresh");
    wait_for_state(id, "succeeded").await;
}

async fn check_delayed_flush_does_not_overwrite_save_now() {
    let label = "regress";
    let id = enqueue(&Regress {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate("regress:saved").await;
    stall(FLUSH_STATEMENT, 1.5).await;
    wait_until("the flush to stall", SETTLED, || async {
        stalled(FLUSH_STATEMENT).await
    })
    .await;
    open("regress:saved");
    wait_for_gate("regress:now").await;
    unstall().await;
    wait_until("the stalled flush to finish", SETTLED, || async {
        !stalled(FLUSH_STATEMENT).await
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(stored(id).await, Some(json!("C")));
    open("regress:now");
    wait_for_state(id, "succeeded").await;
}

async fn check_save_during_save_now_is_flushed() {
    let label = "lost_save";
    stall(SAVE_NOW_STATEMENT, 2.0).await;
    let id = enqueue(&LostSave {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate("lost_save:now").await;
    unstall().await;
    assert!(
        recorded_error(label).is_none(),
        "{:?}",
        recorded_error(label)
    );
    wait_until("the later save to reach the database", SETTLED, || async {
        stored(id).await == Some(json!("D"))
    })
    .await;
    open("lost_save:saved");
    wait_for_state(id, "succeeded").await;
}

async fn check_save_now_without_a_control_row_is_permanent() {
    let label = "missing_row";
    let id = enqueue(&MissingRow {
        label: label.to_owned(),
    })
    .await;
    wait_for_state(id, "succeeded").await;
    let error = recorded_error(label).unwrap();
    assert!(error.is_permanent(), "{error:?}");
    assert!(error.msg().contains("has no control row"), "{error:?}");
}

async fn check_save_now_is_a_stop_point() {
    let label = "stop_now";
    let id = enqueue(&StopNow {
        label: label.to_owned(),
    })
    .await;
    wait_for_gate(label).await;
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(id, "stopped").await;
    let [Some(last)] = leases(label)[..] else {
        panic!("the stop was not seen by save_now");
    };
    assert_eq!(stored(id).await, Some(json!(last)));
    assert_eq!(
        jalari::cancel(id).await.unwrap(),
        jalari::CancelOutcome::Cancelled
    );
}

async fn control_rows(job_ids: &[JobId]) -> i64 {
    let tables = TABLES.get().unwrap();
    let job_ids: Vec<i64> = job_ids.iter().map(|id| id.0).collect();
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {} WHERE job_id = ANY($1)",
        tables.job_control
    )))
    .bind(&job_ids)
    .fetch_one(&tables.pool)
    .await
    .unwrap()
}

async fn check_housekeeping_removes_control_rows_with_their_jobs() {
    let finished = run_leases("housekept", false).await;
    assert_eq!(finished.len(), 2);
    assert!(finished.iter().all(Option::is_some));
    let finished_id: Vec<JobId> = jalari::admin::jobs(
        &jalari::admin::JobFilter::new().state(jalari::JobState::Succeeded),
        100,
        None,
    )
    .await
    .unwrap()
    .into_iter()
    .map(|job| job.id)
    .collect();
    let parked = jalari::enqueue(&Parked).await.unwrap().id();
    wait_until("the parked job to wait for its retry", SETTLED, || async {
        state(parked).await == "scheduled"
    })
    .await;
    assert_eq!(
        control_rows(&finished_id).await,
        i64::try_from(finished_id.len()).unwrap()
    );
    jalari::cluster::update(|cluster| {
        cluster.housekeeping_enabled = true;
        cluster.housekeeping_interval = Duration::from_secs(1);
        cluster.succeeded_retention = Duration::ZERO;
    })
    .await
    .unwrap();
    wait_until(
        "housekeeping to remove the finished jobs",
        SETTLED,
        || async {
            let mut gone = true;
            for id in &finished_id {
                gone &= jalari::admin::job(*id).await.unwrap().is_none();
            }
            gone
        },
    )
    .await;
    jalari::cluster::update(|cluster| cluster.housekeeping_enabled = false)
        .await
        .unwrap();
    assert_eq!(control_rows(&finished_id).await, 0);
    assert_eq!(control_rows(&[parked]).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_progress_durable() {
    let Some(database) = Database::start("progress_durable", 16).await else {
        return;
    };
    let _ = TABLES.set(Tables {
        pool: database.pool.clone(),
        schema: database.schema_name.clone(),
        job_control: database.table("job_control"),
        stall: database.table("stall"),
    });
    install_stall().await;
    let worker = spawn_worker().await;
    check_each_run_has_its_own_lease().await;
    check_close_without_a_row_still_fences().await;
    check_close_keeps_a_newer_stored_copy().await;
    let worker = check_save_now_survives_a_crash(worker).await;
    check_fenced_out_save_now_ends_the_run().await;
    check_leftover_save_now_is_rejected().await;
    check_delayed_flush_does_not_overwrite_save_now().await;
    check_save_during_save_now_is_flushed().await;
    check_save_now_without_a_control_row_is_permanent().await;
    check_save_now_is_a_stop_point().await;
    check_housekeeping_removes_control_rows_with_their_jobs().await;
    worker.stop().await;
    database.finish().await;
}
