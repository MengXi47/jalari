mod common;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use common::{Database, LEDGER, RunningWorker, wait_until};
use jalari::admin::JobDetail;
use jalari::sqlx::PgPool;
use jalari::{
    CancelOutcome, CancellationToken, Cron, EnqueueOptions, Finished, Job, JobError, JobId,
    JobMeta, JobMiddleware, JobResult, JobState, Next, StopOutcome, Worker, WorkerConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

const CHILD_ENV: &str = "JALARI_ISOLATION_CHILD";
const ISO: &str = "iso";
const SOLO: &str = "solo";
const OTHER: &str = "other";
const TASKS: &str = "tasks";
const QUIET: &str = "quiet";
const CROWD: &str = "crowd";
const LONG_QUEUE: &str = "a_very_long_queue_name";
const DOWN: &str = "down";
const CRASH: &str = "crash";
const CRON_QUEUE: &str = "cron_spin";
const CRON_SLOTS: usize = 2;
const CRON_WINDOW: Duration = Duration::from_secs(5);
const HELD_FIRE_BACKOFF: Duration = Duration::from_millis(100);
const SLOT_TARGET: &str = "jalari::worker::slot";
const WATCHDOG: &str = "the worker's runtime";
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const STOP_TIMEOUT: Duration = Duration::from_secs(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const TIMEOUT: Duration = Duration::from_secs(1);
const ABORT_WAIT: Duration = Duration::from_secs(1);
const CONTROL_LATENCY: Duration = Duration::from_millis(2500);
const SETTLE: Duration = Duration::from_millis(500);
const BLOCK: Duration = Duration::from_millis(1500);
const LONG_BLOCK: Duration = Duration::from_secs(4);
const BYSTANDER: Duration = Duration::from_millis(500);
const TICKER_GAP: Duration = Duration::from_millis(300);
const TICK: Duration = Duration::from_millis(50);
const PULSE_EVERY: Duration = Duration::from_millis(200);
const HELD_REPORT: Duration = Duration::from_secs(10);
const WATCHDOG_BLOCK: Duration = Duration::from_secs(12);
const OVERSUBSCRIBED_SLACK: Duration = Duration::from_secs(2);
const THREAD_EXIT: Duration = Duration::from_secs(1);
const START: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(10);
const POLL_LIMIT: Duration = Duration::from_secs(30);
const STOPPED: &str = "the job was asked to stop";
const NO_CHECKPOINT: &str = "aborted because it reached no checkpoint within 1s";
const INTERRUPTED: &str = "the worker stopped during the last attempt";
const TIMED_OUT_HALT: &str = "the job timed out";
const EXPLODED: &str = "panicked: job exploded";
const NESTED: &str = "panicked: Cannot start a runtime from within a runtime.";
const BLOCKING_ON_CURRENT_THREAD: &str =
    "panicked: can call blocking only when running on the multi-threaded runtime";

static POOL: OnceLock<PgPool> = OnceLock::new();
static RELEASE_ALL: AtomicBool = AtomicBool::new(false);
static RELEASED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
static THREADS: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NOTES: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static EVENTS: LazyLock<Mutex<Vec<Captured>>> = LazyLock::new(|| Mutex::new(Vec::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flavor {
    TwoThreads,
    CurrentThread,
    DefaultThreads,
    OneThread,
    AwaitedOnCurrentThread,
}

impl Flavor {
    fn tag(self) -> &'static str {
        match self {
            Flavor::TwoThreads => "isolation_mt",
            Flavor::CurrentThread => "isolation_ct",
            Flavor::DefaultThreads => "isolation_default",
            Flavor::OneThread => "isolation_one",
            Flavor::AwaitedOnCurrentThread => "isolation_awaited",
        }
    }

    fn is_current_thread(self) -> bool {
        matches!(self, Flavor::CurrentThread | Flavor::AwaitedOnCurrentThread)
    }

    fn runs_every_check(self) -> bool {
        matches!(self, Flavor::TwoThreads | Flavor::CurrentThread)
    }
}

#[derive(Debug, Clone)]
struct Captured {
    at: Instant,
    level: Level,
    target: String,
    message: String,
    fields: HashMap<String, String>,
}

struct Fields(HashMap<String, String>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

struct Capture;

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= Level::DEBUG && metadata.target().starts_with("jalari")
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields(HashMap::new());
        event.record(&mut fields);
        let message = fields.0.remove("message").unwrap_or_default();
        EVENTS.lock().unwrap().push(Captured {
            at: Instant::now(),
            level: *event.metadata().level(),
            target: event.metadata().target().to_owned(),
            message,
            fields: fields.0,
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

fn slot_events(job_id: JobId) -> Vec<Captured> {
    let id = job_id.0.to_string();
    EVENTS
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.target == SLOT_TARGET && event.fields.get("job_id") == Some(&id))
        .cloned()
        .collect()
}

fn held_reports(job_id: JobId) -> Vec<(Level, String)> {
    slot_events(job_id)
        .into_iter()
        .filter(|event| event.level <= Level::INFO)
        .map(|event| (event.level, event.message))
        .collect()
}

fn assert_never_held(job_id: JobId) {
    assert_eq!(held_reports(job_id), [], "job {job_id} was reported held");
}

fn watchdog_lines() -> Vec<String> {
    EVENTS
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.message.starts_with(WATCHDOG))
        .map(|event| event.message.clone())
        .collect()
}

fn started_threads() -> Vec<String> {
    EVENTS
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.target == SLOT_TARGET && event.message == "slot thread started")
        .filter_map(|event| event.fields.get("thread").cloned())
        .collect()
}

struct ReleaseAllOnDrop;

impl Drop for ReleaseAllOnDrop {
    fn drop(&mut self) {
        RELEASE_ALL.store(true, Ordering::SeqCst);
    }
}

fn release(label: &str) {
    RELEASED.lock().unwrap().insert(label.to_owned());
}

fn is_released(label: &str) -> bool {
    RELEASE_ALL.load(Ordering::SeqCst) || RELEASED.lock().unwrap().contains(label)
}

fn note(label: &str, value: String) {
    NOTES.lock().unwrap().insert(label.to_owned(), value);
}

fn noted(label: &str) -> Option<String> {
    NOTES.lock().unwrap().get(label).cloned()
}

fn thread_of(label: &str) -> String {
    THREADS
        .lock()
        .unwrap()
        .get(label)
        .cloned()
        .unwrap_or_default()
}

fn current_thread_name() -> String {
    std::thread::current().name().unwrap_or_default().to_owned()
}

fn at(label: &str, event: &str) -> Option<Instant> {
    LEDGER
        .events(label)
        .into_iter()
        .find(|(name, _)| *name == event)
        .map(|(_, at)| at)
}

struct LastLabel(RefCell<Option<String>>);

impl Drop for LastLabel {
    fn drop(&mut self) {
        if let Some(label) = self.0.borrow_mut().take() {
            LEDGER.record(&label, "thread exited");
        }
    }
}

thread_local! {
    static LAST_LABEL: LastLabel = const { LastLabel(RefCell::new(None)) };
}

struct Living(String);

impl Living {
    fn enter(label: &str) -> Self {
        THREADS
            .lock()
            .unwrap()
            .insert(label.to_owned(), current_thread_name());
        LAST_LABEL.with(|last| *last.0.borrow_mut() = Some(label.to_owned()));
        LEDGER.record(label, "start");
        Self(label.to_owned())
    }
}

impl Drop for Living {
    fn drop(&mut self) {
        LEDGER.record(&self.0, "end");
    }
}

fn spin(duration: Duration) {
    let started = Instant::now();
    while started.elapsed() < duration {
        std::hint::spin_loop();
    }
}

fn wall_clock() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn pool() -> &'static PgPool {
    POOL.get().unwrap()
}

fn poll_until_interrupted(run: &jalari::JobRun, label: &str) -> bool {
    let started = Instant::now();
    while started.elapsed() < POLL_LIMIT {
        if run.is_interrupted() {
            LEDGER.record(label, "noticed");
            return false;
        }
        std::thread::sleep(POLL);
    }
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Style {
    AsyncIo,
    AsyncSleep,
    Spin { millis: u64 },
    ClockedSpin { millis: u64 },
    ThreadSleep { millis: u64 },
    InlinePoll,
    SpawnBlockingPoll,
    BlockInPlace,
    BlockInPlacePoll,
    Mixed { spin_millis: u64 },
    AsyncChild,
    BlockingChild,
    PanicEarly,
    PanicLate,
    UntilReleased,
    LongAsyncSleep,
    NestedBlockOn,
    StdThreads,
    Progress,
    ProgressLoop,
    TaskId,
    Named,
}

#[derive(Serialize, Deserialize)]
struct Styled {
    label: String,
    style: Style,
}

#[jalari::job(queue = "iso", max_attempts = 1)]
impl Job for Styled {
    const NAME: &'static str = "isolation_styled";

    async fn run(self) -> JobResult {
        let _living = Living::enter(&self.label);
        run_style(self.style, &self.label).await
    }
}

async fn run_style(style: Style, label: &str) -> JobResult {
    match style {
        Style::AsyncIo => {
            let one: i32 = jalari::sqlx::query_scalar("SELECT 1")
                .fetch_one(pool())
                .await?;
            jalari::sqlx::query("SELECT pg_sleep(0.2)")
                .execute(pool())
                .await?;
            let two: i32 = jalari::sqlx::query_scalar("SELECT 2")
                .fetch_one(pool())
                .await?;
            assert_eq!((one, two), (1, 2));
            Ok(())
        }
        Style::AsyncSleep => {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(())
        }
        Style::Spin { millis } => {
            spin(Duration::from_millis(millis));
            Ok(())
        }
        Style::ClockedSpin { millis } => {
            note(&format!("{label}:from"), wall_clock().to_string());
            spin(Duration::from_millis(millis));
            note(&format!("{label}:to"), wall_clock().to_string());
            Ok(())
        }
        Style::ThreadSleep { millis } => {
            std::thread::sleep(Duration::from_millis(millis));
            Ok(())
        }
        Style::InlinePoll => {
            let run = jalari::current_job().expect("current_job() inside the loop");
            if poll_until_interrupted(&run, label) {
                return Ok(());
            }
            let error = run.interrupted().await;
            note(label, error.msg().to_owned());
            Err(error)
        }
        Style::SpawnBlockingPoll => {
            let run = jalari::current_job().unwrap();
            let polled = run.clone();
            let owned = label.to_owned();
            let finished =
                tokio::task::spawn_blocking(move || poll_until_interrupted(&polled, &owned))
                    .await?;
            if finished {
                return Ok(());
            }
            Err(run.interrupted().await)
        }
        Style::BlockInPlace => {
            tokio::task::block_in_place(|| std::thread::sleep(BLOCK));
            Ok(())
        }
        Style::BlockInPlacePoll => {
            let run = jalari::current_job().unwrap();
            let finished = tokio::task::block_in_place(|| poll_until_interrupted(&run, label));
            if finished {
                return Ok(());
            }
            Err(run.interrupted().await)
        }
        Style::Mixed { spin_millis } => {
            tokio::time::sleep(Duration::from_millis(100)).await;
            spin(Duration::from_millis(spin_millis));
            let one: i32 = jalari::sqlx::query_scalar("SELECT 1")
                .fetch_one(pool())
                .await?;
            assert_eq!(one, 1);
            Ok(())
        }
        Style::AsyncChild => {
            let (saw_job, thread) = tokio::spawn(async {
                let one: i32 = jalari::sqlx::query_scalar("SELECT 1")
                    .fetch_one(pool())
                    .await
                    .unwrap();
                assert_eq!(one, 1);
                (jalari::current_job().is_some(), current_thread_name())
            })
            .await?;
            note(label, thread);
            if saw_job {
                return Err(JobError::new("the child saw the job".to_owned()));
            }
            Ok(())
        }
        Style::BlockingChild => {
            tokio::spawn(async { std::thread::sleep(BLOCK) }).await?;
            Ok(())
        }
        Style::PanicEarly => panic!("job exploded"),
        Style::PanicLate => {
            tokio::time::sleep(Duration::from_millis(10)).await;
            panic!("job exploded")
        }
        Style::UntilReleased => {
            while !is_released(label) {
                std::thread::sleep(POLL);
            }
            Ok(())
        }
        Style::LongAsyncSleep => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        }
        Style::NestedBlockOn => {
            tokio::runtime::Handle::current().block_on(async {});
            Ok(())
        }
        Style::StdThreads => {
            let threads: Vec<_> = (0..2)
                .map(|_| {
                    std::thread::spawn(|| {
                        std::thread::sleep(BLOCK);
                        tokio::runtime::Handle::try_current().is_err()
                    })
                })
                .collect();
            for thread in threads {
                if !thread.join().unwrap() {
                    return Err(JobError::new("a std thread saw a runtime".to_owned()));
                }
            }
            Ok(())
        }
        Style::Progress => run_progress(label).await,
        Style::ProgressLoop => {
            let (_, checkpoint) = jalari::resume::<u32>()?;
            for value in 1.. {
                note(label, value.to_string());
                checkpoint.save(&value).await?;
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(())
        }
        Style::TaskId => {
            note(label, tokio::task::try_id().is_some().to_string());
            Ok(())
        }
        Style::Named => Ok(()),
    }
}

async fn run_progress(label: &str) -> JobResult {
    let (_, checkpoint) = jalari::resume::<u32>()?;
    let charged: String = jalari::step("charge", |_| async { Ok("charge-1".to_owned()) }).await?;
    let receipt: bool = jalari::step("receipt", |_| async { Ok(true) }).await?;
    assert_eq!((charged.as_str(), receipt), ("charge-1", true));
    checkpoint.save_now(&1).await?;
    for value in 2..=4 {
        checkpoint.save(&value).await?;
        jalari::checkpoint().await?;
    }
    jalari::enqueue_on_success(&Styled {
        label: format!("{label}:child"),
        style: Style::Named,
    })?;
    spin(Duration::from_millis(300));
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Bystander {
    label: String,
}

#[jalari::job(queue = "other")]
impl Job for Bystander {
    const NAME: &'static str = "isolation_bystander";

    async fn run(self) -> JobResult {
        LEDGER.record(&self.label, "ran");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct CallBlocked {
    label: String,
}

#[jalari::job(queue = "iso", max_attempts = 1)]
impl Job for CallBlocked {
    const NAME: &'static str = "isolation_call_blocked";

    async fn run(self) -> JobResult {
        let _living = Living::enter(&self.label);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct HookBlocked {
    label: String,
}

#[jalari::job(queue = "iso", max_attempts = 1)]
impl Job for HookBlocked {
    const NAME: &'static str = "isolation_hook_blocked";

    async fn run(self) -> JobResult {
        let _living = Living::enter(&self.label);
        Ok(())
    }
}

struct Hooks;

impl JobMiddleware for Hooks {
    type Provides = ();

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        if job.name() == CallBlocked::NAME {
            let label = job.id().0.to_string();
            note(&label, current_thread_name());
            std::thread::sleep(BLOCK);
        }
        next.run().await
    }

    async fn finished(&self, job: &JobMeta, _outcome: &Finished) {
        if job.name() == HookBlocked::NAME {
            let label = job.id().0.to_string();
            LEDGER.record(&label, "hook start");
            std::thread::sleep(BLOCK);
            LEDGER.record(&label, "hook end");
        }
    }
}

struct Pulse {
    stop: CancellationToken,
    ticker: JoinHandle<Duration>,
    feeder: JoinHandle<Vec<(String, Instant)>>,
}

#[derive(Debug)]
struct Measured {
    widest_tick: Duration,
    slowest_bystander: Duration,
}

impl Pulse {
    fn start(name: &str) -> Self {
        let stop = CancellationToken::new();
        let ticker = tokio::spawn({
            let stop = stop.clone();
            async move {
                let mut last = Instant::now();
                let mut widest = Duration::ZERO;
                loop {
                    tokio::select! {
                        () = stop.cancelled() => return widest,
                        () = tokio::time::sleep(TICK) => {}
                    }
                    widest = widest.max(last.elapsed());
                    last = Instant::now();
                }
            }
        });
        let name = name.to_owned();
        let feeder = tokio::spawn({
            let stop = stop.clone();
            async move {
                let mut sent = Vec::new();
                for n in 0.. {
                    let label = format!("bystander:{name}:{n}");
                    let enqueued = Instant::now();
                    jalari::enqueue(&Bystander {
                        label: label.clone(),
                    })
                    .await
                    .unwrap();
                    sent.push((label, enqueued));
                    tokio::select! {
                        () = stop.cancelled() => break,
                        () = tokio::time::sleep(PULSE_EVERY) => {}
                    }
                }
                sent
            }
        });
        Self {
            stop,
            ticker,
            feeder,
        }
    }

    async fn finish(self) -> Measured {
        self.stop.cancel();
        let widest_tick = self.ticker.await.unwrap();
        let sent = self.feeder.await.unwrap();
        wait_until("every bystander to run", START, || async {
            sent.iter()
                .all(|(label, _)| LEDGER.count(label, "ran") == 1)
        })
        .await;
        let slowest_bystander = sent
            .iter()
            .map(|(label, enqueued)| {
                at(label, "ran")
                    .unwrap()
                    .saturating_duration_since(*enqueued)
            })
            .max()
            .unwrap_or_default();
        Measured {
            widest_tick,
            slowest_bystander,
        }
    }

    async fn assert_unaffected(self, scenario: &str, slack: Duration) {
        let measured = self.finish().await;
        eprintln!("{scenario}: {measured:?}");
        assert!(
            measured.widest_tick <= TICKER_GAP + slack,
            "{scenario}: the caller runtime stalled for {:?}",
            measured.widest_tick
        );
        assert!(
            measured.slowest_bystander <= BYSTANDER + slack,
            "{scenario}: a bystander job waited {:?}",
            measured.slowest_bystander
        );
    }
}

async fn detail(id: JobId) -> JobDetail {
    jalari::admin::job(id).await.unwrap().unwrap()
}

async fn wait_for_state(id: JobId, state: JobState, since: Instant, within: Duration) {
    let deadline = since + within;
    loop {
        let detail = detail(id).await;
        let seen = detail.job.state;
        if seen == state {
            return;
        }
        if Instant::now() >= deadline {
            let events: Vec<(i128, Level, String)> = slot_events(id)
                .into_iter()
                .map(|event| {
                    let offset = if event.at >= since {
                        i128::try_from(event.at.duration_since(since).as_millis()).unwrap()
                    } else {
                        -i128::try_from(since.duration_since(event.at).as_millis()).unwrap()
                    };
                    (offset, event.level, event.message)
                })
                .collect();
            panic!(
                "job {id} is {seen:?}, not {state:?}, {within:?} after the action\njob: {:?}\n\
                 history: {:?}\nslot events (ms after the action): {events:?}",
                detail.job, detail.history
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn last_error(id: JobId) -> Option<String> {
    detail(id)
        .await
        .history
        .last()
        .and_then(|entry| entry.error.clone())
}

async fn enqueue(
    label: &str,
    style: Style,
    queue: &str,
    timeout: Option<Duration>,
) -> (JobId, Instant) {
    let mut options = EnqueueOptions::new().queue(queue);
    if let Some(timeout) = timeout {
        options = options.timeout(timeout);
    }
    let enqueued = Instant::now();
    let job = Styled {
        label: label.to_owned(),
        style,
    };
    let id = jalari::enqueue_with(&job, options).await.unwrap().id();
    (id, enqueued)
}

async fn wait_started(label: &str) -> Instant {
    wait_until(&format!("{label} to start"), START, || async {
        LEDGER.count(label, "start") >= 1
    })
    .await;
    at(label, "start").unwrap()
}

async fn wait_ended(label: &str, within: Duration) {
    wait_until(&format!("{label} to end"), within, || async {
        LEDGER.count(label, "end") >= 1
    })
    .await;
}

fn assert_ran_on(label: &str, queue: &str) {
    let thread = thread_of(label);
    match queue {
        TASKS => assert!(
            !thread.starts_with(&format!("{TASKS}:")),
            "{label} ran on {thread}"
        ),
        _ => assert!(
            thread.starts_with(&format!("{queue}:")),
            "{label} ran on {thread}, not on a {queue} slot thread"
        ),
    }
}

fn max_overlap(labels: &[String]) -> usize {
    let mut edges: Vec<(Instant, i32)> = Vec::new();
    for label in labels {
        if let Some(start) = at(label, "start") {
            edges.push((start, 1));
            edges.push((at(label, "end").unwrap_or_else(Instant::now), -1));
        }
    }
    edges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut live = 0;
    let mut widest = 0;
    for (_, change) in edges {
        live += change;
        widest = widest.max(live);
    }
    usize::try_from(widest).unwrap()
}

fn worker_config(shutdown_timeout: Duration) -> WorkerConfig {
    let mut config = WorkerConfig::default();
    config.poll_interval = POLL_INTERVAL;
    config.stop_timeout = STOP_TIMEOUT;
    config.shutdown_timeout = shutdown_timeout;
    config
}

fn crowd_size() -> usize {
    std::thread::available_parallelism().map_or(4, usize::from) * 2
}

async fn build_main_worker() -> Worker {
    Worker::builder()
        .queue(ISO, 2)
        .queue(SOLO, 1)
        .queue(OTHER, 2)
        .queue(QUIET, 2)
        .queue(CROWD, crowd_size())
        .queue(LONG_QUEUE, 1)
        .task_queue(TASKS, 1)
        .middleware(Hooks)
        .config(worker_config(Duration::from_secs(10)))
        .scheduler(false)
        .build()
        .await
        .unwrap()
}

#[test]
fn test_isolation() {
    if std::env::var("DATABASE_URL").is_err() {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    }
    for driver in [
        "test_isolation_on_two_threads",
        "test_isolation_on_current_thread",
        "test_isolation_on_default_threads",
        "test_isolation_on_one_thread",
        "test_isolation_awaited_on_current_thread",
    ] {
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                driver,
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ENV, "1")
            .status()
            .unwrap();
        assert!(status.success(), "{driver} failed: {status}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "runs only inside the process test_isolation spawns"]
async fn test_isolation_on_two_threads() {
    run_catalogue(Flavor::TwoThreads).await;
}

#[tokio::test]
#[ignore = "runs only inside the process test_isolation spawns"]
async fn test_isolation_on_current_thread() {
    run_catalogue(Flavor::CurrentThread).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "runs only inside the process test_isolation spawns"]
async fn test_isolation_on_default_threads() {
    run_catalogue(Flavor::DefaultThreads).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "runs only inside the process test_isolation spawns"]
async fn test_isolation_on_one_thread() {
    run_catalogue(Flavor::OneThread).await;
}

#[tokio::test]
#[ignore = "runs only inside the process test_isolation spawns"]
async fn test_isolation_awaited_on_current_thread() {
    run_catalogue(Flavor::AwaitedOnCurrentThread).await;
}

const CORE_CHECKS: [&str; 7] = [
    "slot_threads_are_named",
    "jobs_that_succeed",
    "cooperative_loops_stop_on_time",
    "panics_leave_the_thread_serving",
    "never_returning_body_is_recorded_on_time",
    "cron_under_blocking_bodies",
    "shutdown_with_a_body_that_never_yields",
];

fn selected(check: &str) -> bool {
    std::env::var("ISOLATION_ONLY").map_or(true, |only| only.split(',').any(|name| name == check))
}

async fn run_catalogue(flavor: Flavor) {
    if std::env::var(CHILD_ENV).is_err() {
        return;
    }
    let _release = ReleaseAllOnDrop;
    tracing::subscriber::set_global_default(Capture).unwrap();
    let Some(database) = Database::start(flavor.tag(), 16).await else {
        return;
    };
    POOL.set(database.pool.clone()).unwrap();
    let worker = build_main_worker().await;
    if flavor == Flavor::AwaitedOnCurrentThread {
        let shutdown = CancellationToken::new();
        let checks = async {
            wait_registered(&database).await;
            run_checks(flavor, &database).await;
            shutdown.cancel();
        };
        tokio::join!(worker.run(shutdown.clone()), checks);
    } else {
        let running = RunningWorker::spawn(worker);
        wait_registered(&database).await;
        run_checks(flavor, &database).await;
        running.stop().await;
    }
    database.finish().await;
}

async fn wait_registered(database: &Database) {
    wait_until("the worker's lifeline to register", START, || async {
        database.lifelines().await.len() == 1
    })
    .await;
    let (id, enqueued) = enqueue("warm_up", Style::Named, ISO, None).await;
    wait_for_state(id, JobState::Succeeded, enqueued, START).await;
}

async fn run_checks(flavor: Flavor, database: &Database) {
    let wanted = |check: &str| {
        selected(check) && (flavor.runs_every_check() || CORE_CHECKS.contains(&check))
    };
    if wanted("slot_threads_are_named") {
        check_slot_threads_are_named().await;
    }
    if wanted("jobs_that_succeed") {
        check_jobs_that_succeed(flavor).await;
    }
    if wanted("documented_runtime_limits") {
        check_documented_runtime_limits(flavor).await;
    }
    if wanted("cooperative_loops_stop_on_time") {
        check_cooperative_loops_stop_on_time().await;
    }
    if wanted("spawn_blocking_recipe_on_both_placements") {
        check_spawn_blocking_recipe_on_both_placements().await;
    }
    if wanted("block_in_place") {
        check_block_in_place(flavor).await;
    }
    if wanted("mixed_body_with_a_timeout") {
        check_mixed_body_with_a_timeout().await;
    }
    if wanted("panics_leave_the_thread_serving") {
        check_panics_leave_the_thread_serving().await;
    }
    if wanted("nested_block_on_panics") {
        check_nested_block_on_panics().await;
    }
    if wanted("never_returning_body_is_recorded_on_time") {
        check_never_returning_body_is_recorded_on_time().await;
    }
    if wanted("stops_on_bodies_that_never_check") {
        check_stops_on_bodies_that_never_check().await;
    }
    if wanted("held_slots_keep_capacity_honest") {
        check_held_slots_keep_capacity_honest().await;
    }
    if wanted("progress_is_the_same_on_both_placements") {
        check_progress_is_the_same_on_both_placements().await;
    }
    if wanted("progress_survives_a_stop") {
        check_progress_survives_a_stop().await;
    }
    if wanted("middleware_call_blocks_only_its_slot") {
        check_middleware_call_blocks_only_its_slot().await;
    }
    if wanted("finished_hook_runs_after_the_outcome") {
        check_finished_hook_runs_after_the_outcome(flavor).await;
    }
    if wanted("task_ids") {
        check_task_ids().await;
    }
    if wanted("more_blocking_bodies_than_cores") {
        check_more_blocking_bodies_than_cores().await;
    }
    if wanted("long_hold_reports_and_watchdog") {
        check_long_hold_reports_and_watchdog(database).await;
    }
    if flavor == Flavor::TwoThreads && wanted("task_queue_holds_too") {
        check_task_queue_holds_too().await;
    }
    if wanted("cron_under_blocking_bodies") {
        check_cron_under_blocking_bodies(database).await;
    }
    if wanted("stop_and_lost_holds") {
        check_stop_and_lost_holds(database).await;
    }
    if wanted("shutdown_with_a_body_that_never_yields") {
        check_shutdown_with_a_body_that_never_yields().await;
    }
    if wanted("dropped_worker_leaves_nothing") {
        check_dropped_worker_leaves_nothing(database).await;
    }
}

async fn check_slot_threads_are_named() {
    let mut expected: Vec<String> = [
        "iso:0", "iso:1", "solo:0", "other:0", "other:1", "quiet:0", "quiet:1",
    ]
    .map(str::to_owned)
    .to_vec();
    expected.extend((0..crowd_size()).map(|index| format!("{CROWD}:{index}")));
    expected.push("a_very_long_q:0".to_owned());
    expected.sort();
    wait_until("every slot thread to start", START, || async {
        started_threads().len() >= expected.len()
    })
    .await;
    let mut started = started_threads();
    started.sort();
    assert_eq!(started, expected);
    assert!(started.iter().all(|name| name.len() <= 15));
}

struct Succeeds {
    label: &'static str,
    style: Style,
    queue: &'static str,
    within: Duration,
}

async fn check_jobs_that_succeed(flavor: Flavor) {
    let block = u64::try_from(BLOCK.as_millis()).unwrap();
    let cases = [
        Succeeds {
            label: "async_io",
            style: Style::AsyncIo,
            queue: ISO,
            within: Duration::from_millis(200) + SETTLE,
        },
        Succeeds {
            label: "async_io_task",
            style: Style::AsyncIo,
            queue: TASKS,
            within: Duration::from_millis(200) + SETTLE,
        },
        Succeeds {
            label: "async_sleep",
            style: Style::AsyncSleep,
            queue: ISO,
            within: Duration::from_millis(300) + SETTLE,
        },
        Succeeds {
            label: "spin",
            style: Style::Spin { millis: block },
            queue: ISO,
            within: BLOCK + SETTLE,
        },
        Succeeds {
            label: "thread_sleep",
            style: Style::ThreadSleep { millis: block },
            queue: ISO,
            within: BLOCK + SETTLE,
        },
        Succeeds {
            label: "mixed",
            style: Style::Mixed { spin_millis: block },
            queue: ISO,
            within: Duration::from_millis(100) + BLOCK + SETTLE,
        },
        Succeeds {
            label: "async_child",
            style: Style::AsyncChild,
            queue: ISO,
            within: SETTLE,
        },
        Succeeds {
            label: "std_threads",
            style: Style::StdThreads,
            queue: ISO,
            within: BLOCK + SETTLE,
        },
    ];
    for case in cases {
        let label = format!("{}:{flavor:?}", case.label);
        let pulse = Pulse::start(&label);
        let (id, enqueued) = enqueue(&label, case.style, case.queue, None).await;
        wait_for_state(id, JobState::Succeeded, enqueued, case.within).await;
        eprintln!("{label}: succeeded {:?} after enqueue", enqueued.elapsed());
        pulse.assert_unaffected(&label, Duration::ZERO).await;
        assert_ran_on(&label, case.queue);
        assert_never_held(id);
    }
    let child = noted(&format!("async_child:{flavor:?}")).unwrap();
    assert!(
        !child.starts_with("iso:"),
        "the spawned child ran on {child}"
    );
}

async fn check_documented_runtime_limits(flavor: Flavor) {
    let block = u64::try_from(BLOCK.as_millis()).unwrap();
    for (label, style, queue) in [
        ("task_spin", Style::Spin { millis: block }, TASKS),
        ("blocking_child", Style::BlockingChild, ISO),
    ] {
        let label = format!("{label}:{flavor:?}");
        let pulse = Pulse::start(&label);
        let (id, enqueued) = enqueue(&label, style, queue, None).await;
        wait_for_state(id, JobState::Succeeded, enqueued, BLOCK + SETTLE).await;
        let measured = pulse.finish().await;
        eprintln!("{label}: blocking code on the worker's runtime, {measured:?}");
        if flavor.is_current_thread() {
            assert!(
                measured.widest_tick >= BLOCK / 2,
                "{label}: code on the worker's runtime did not block it: {measured:?}"
            );
        }
    }
}

async fn check_cooperative_loops_stop_on_time() {
    let label = "inline_stop";
    let pulse = Pulse::start(label);
    let (id, _) = enqueue(label, Style::InlinePoll, ISO, None).await;
    wait_started(label).await;
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(id, JobState::Stopped, asked, CONTROL_LATENCY).await;
    assert_eq!(detail(id).await.job.attempts, 0);
    assert_eq!(last_error(id).await.as_deref(), Some(STOPPED));
    assert_eq!(noted(label).as_deref(), Some(STOPPED));
    assert_never_held(id);

    let label = "inline_cancel";
    let (id, _) = enqueue(label, Style::InlinePoll, ISO, None).await;
    wait_started(label).await;
    let asked = Instant::now();
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Requested);
    wait_for_state(id, JobState::Deleted, asked, CONTROL_LATENCY).await;
    assert_never_held(id);

    let label = "inline_timeout";
    let (id, _) = enqueue(label, Style::InlinePoll, ISO, Some(TIMEOUT)).await;
    let started = wait_started(label).await;
    wait_for_state(id, JobState::Failed, started, TIMEOUT + SETTLE).await;
    let noticed = at(label, "noticed")
        .unwrap()
        .saturating_duration_since(started);
    eprintln!("inline_timeout: the loop saw its timeout {noticed:?} after it started");
    assert!(
        noticed >= TIMEOUT && noticed < TIMEOUT + POLL * 10,
        "{noticed:?}"
    );
    assert_eq!(
        last_error(id).await,
        Some(format!("timed out after {TIMEOUT:?}"))
    );
    assert_eq!(noted(label).as_deref(), Some(TIMED_OUT_HALT));
    assert_never_held(id);
    pulse
        .assert_unaffected("cooperative loops", Duration::ZERO)
        .await;
}

async fn check_spawn_blocking_recipe_on_both_placements() {
    let pulse = Pulse::start("spawn_blocking");
    for queue in [ISO, TASKS] {
        let label = format!("spawn_blocking_stop:{queue}");
        let (id, _) = enqueue(&label, Style::SpawnBlockingPoll, queue, None).await;
        wait_started(&label).await;
        let asked = Instant::now();
        assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
        wait_for_state(id, JobState::Stopped, asked, CONTROL_LATENCY).await;
        assert_eq!(last_error(id).await.as_deref(), Some(STOPPED));
        assert_never_held(id);

        let label = format!("spawn_blocking_timeout:{queue}");
        let (id, _) = enqueue(&label, Style::SpawnBlockingPoll, queue, Some(TIMEOUT)).await;
        let started = wait_started(&label).await;
        wait_for_state(id, JobState::Failed, started, TIMEOUT + SETTLE).await;
        assert_eq!(
            last_error(id).await,
            Some(format!("timed out after {TIMEOUT:?}"))
        );
        assert_never_held(id);
    }
    pulse
        .assert_unaffected("spawn_blocking recipe", Duration::ZERO)
        .await;
}

async fn check_block_in_place(flavor: Flavor) {
    let pulse = Pulse::start("block_in_place");
    let label = format!("block_in_place:{flavor:?}");
    let (id, enqueued) = enqueue(&label, Style::BlockInPlace, ISO, None).await;
    wait_for_state(id, JobState::Succeeded, enqueued, BLOCK + SETTLE).await;
    assert_ran_on(&label, ISO);

    let label = format!("block_in_place_stop:{flavor:?}");
    let (id, _) = enqueue(&label, Style::BlockInPlacePoll, ISO, None).await;
    wait_started(&label).await;
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(id, JobState::Stopped, asked, CONTROL_LATENCY).await;
    assert_eq!(last_error(id).await.as_deref(), Some(STOPPED));
    pulse
        .assert_unaffected("block_in_place", Duration::ZERO)
        .await;

    let label = format!("block_in_place_task:{flavor:?}");
    let (id, enqueued) = enqueue(&label, Style::BlockInPlace, TASKS, None).await;
    match flavor.is_current_thread() {
        false => {
            wait_for_state(id, JobState::Succeeded, enqueued, BLOCK + SETTLE).await;
        }
        true => {
            wait_for_state(id, JobState::Failed, enqueued, SETTLE).await;
            assert_eq!(
                last_error(id).await.as_deref(),
                Some(BLOCKING_ON_CURRENT_THREAD)
            );
        }
    }
}

async fn check_mixed_body_with_a_timeout() {
    let pulse = Pulse::start("mixed_timeout");
    let label = "mixed_timeout";
    let spin_millis = u64::try_from((TIMEOUT + ABORT_WAIT + SETTLE).as_millis()).unwrap();
    let (id, _) = enqueue(label, Style::Mixed { spin_millis }, ISO, Some(TIMEOUT)).await;
    let started = wait_started(label).await;
    wait_for_state(id, JobState::Failed, started, TIMEOUT + ABORT_WAIT + SETTLE).await;
    assert_eq!(
        last_error(id).await,
        Some(format!("timed out after {TIMEOUT:?}"))
    );
    wait_ended(label, START).await;
    wait_until("the held body's report", SETTLE, || async {
        held_reports(id).len() == 2
    })
    .await;
    let reports = held_reports(id);
    assert_eq!(reports[0].0, Level::WARN);
    assert_eq!(
        reports[0].1,
        format!(
            "job {id} did not stop within 1s after it timed out; slot {} takes no job until it \
             returns",
            thread_of(label)
        )
    );
    assert_eq!(reports[1].0, Level::INFO);
    assert!(
        reports[1].1.starts_with(&format!("job {id} returned ")),
        "{reports:?}"
    );
    pulse
        .assert_unaffected("mixed with a timeout", Duration::ZERO)
        .await;
}

async fn check_panics_leave_the_thread_serving() {
    let pulse = Pulse::start("panics");
    for (label, style) in [
        ("panic_early", Style::PanicEarly),
        ("panic_late", Style::PanicLate),
    ] {
        let (id, enqueued) = enqueue(label, style, SOLO, None).await;
        wait_for_state(id, JobState::Failed, enqueued, SETTLE).await;
        assert_eq!(last_error(id).await.as_deref(), Some(EXPLODED));
        let next = format!("{label}:next");
        let (id, enqueued) = enqueue(&next, Style::Named, SOLO, None).await;
        wait_for_state(id, JobState::Succeeded, enqueued, SETTLE).await;
        assert_eq!(thread_of(label), "solo:0");
        assert_eq!(thread_of(&next), "solo:0");

        let label = format!("{label}:task");
        let (id, enqueued) = enqueue(&label, style, TASKS, None).await;
        wait_for_state(id, JobState::Failed, enqueued, SETTLE).await;
        assert_eq!(last_error(id).await.as_deref(), Some(EXPLODED));
    }
    pulse.assert_unaffected("panics", Duration::ZERO).await;
}

async fn check_nested_block_on_panics() {
    for queue in [SOLO, TASKS] {
        let label = format!("nested_block_on:{queue}");
        let (id, enqueued) = enqueue(&label, Style::NestedBlockOn, queue, None).await;
        wait_for_state(id, JobState::Failed, enqueued, SETTLE).await;
        let error = last_error(id).await.unwrap();
        assert!(error.starts_with(NESTED), "{error}");
    }
    let (id, enqueued) = enqueue("nested_block_on:next", Style::Named, SOLO, None).await;
    wait_for_state(id, JobState::Succeeded, enqueued, SETTLE).await;
    assert_eq!(thread_of("nested_block_on:next"), "solo:0");
}

async fn check_never_returning_body_is_recorded_on_time() {
    let pulse = Pulse::start("never_returns");
    let label = "never_returns";
    let (id, _) = enqueue(label, Style::UntilReleased, ISO, Some(TIMEOUT)).await;
    let started = wait_started(label).await;
    wait_for_state(id, JobState::Failed, started, TIMEOUT + ABORT_WAIT + SETTLE).await;
    assert_eq!(
        last_error(id).await,
        Some(format!("timed out after {TIMEOUT:?}"))
    );
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        LEDGER.count(label, "end"),
        0,
        "the body did not keep running"
    );
    pulse
        .assert_unaffected("never returns", Duration::ZERO)
        .await;
    release(label);
    wait_ended(label, SETTLE).await;
}

async fn check_stops_on_bodies_that_never_check() {
    let pulse = Pulse::start("stops");
    let long = u64::try_from(LONG_BLOCK.as_millis()).unwrap();
    for (label, style) in [
        ("stop_spin", Style::Spin { millis: long }),
        ("stop_thread_sleep", Style::ThreadSleep { millis: long }),
    ] {
        let (id, _) = enqueue(label, style, ISO, None).await;
        wait_started(label).await;
        let asked = Instant::now();
        assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
        wait_for_state(
            id,
            JobState::Stopped,
            asked,
            CONTROL_LATENCY + STOP_TIMEOUT + ABORT_WAIT + SETTLE,
        )
        .await;
        assert_eq!(last_error(id).await.as_deref(), Some(NO_CHECKPOINT));
        let warned = held_reports(id);
        assert!(
            warned
                .first()
                .is_some_and(|(level, message)| *level == Level::WARN
                    && message.contains("after it reached stop_timeout")),
            "{warned:?}"
        );
        wait_ended(label, LONG_BLOCK).await;
    }
    let label = "cancel_spin";
    let (id, _) = enqueue(label, Style::Spin { millis: long }, ISO, None).await;
    wait_started(label).await;
    let asked = Instant::now();
    assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Requested);
    wait_for_state(
        id,
        JobState::Deleted,
        asked,
        CONTROL_LATENCY + STOP_TIMEOUT + ABORT_WAIT + SETTLE,
    )
    .await;
    wait_ended(label, LONG_BLOCK).await;

    let label = "stop_async_sleep";
    let (id, _) = enqueue(label, Style::LongAsyncSleep, ISO, None).await;
    wait_started(label).await;
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(
        id,
        JobState::Stopped,
        asked,
        CONTROL_LATENCY + STOP_TIMEOUT + SETTLE,
    )
    .await;
    assert_eq!(last_error(id).await.as_deref(), Some(NO_CHECKPOINT));
    assert_eq!(LEDGER.count(label, "end"), 1, "the async body was dropped");
    assert_never_held(id);
    pulse.assert_unaffected("stops", Duration::ZERO).await;
}

async fn check_held_slots_keep_capacity_honest() {
    let pulse = Pulse::start("capacity");
    let mut labels: Vec<String> = Vec::new();
    let first = "held_first".to_owned();
    let (id, _) = enqueue(&first, Style::UntilReleased, ISO, Some(TIMEOUT)).await;
    let started = wait_started(&first).await;
    wait_for_state(id, JobState::Failed, started, TIMEOUT + ABORT_WAIT + SETTLE).await;
    labels.push(first.clone());
    for n in 0..3 {
        let label = format!("half_capacity:{n}");
        let (id, enqueued) = enqueue(&label, Style::Named, ISO, None).await;
        wait_for_state(id, JobState::Succeeded, enqueued, BYSTANDER).await;
        assert_ne!(thread_of(&label), thread_of(&first));
        labels.push(label);
    }
    let second = "held_second".to_owned();
    let (id, _) = enqueue(&second, Style::UntilReleased, ISO, Some(TIMEOUT)).await;
    let started = wait_started(&second).await;
    wait_for_state(id, JobState::Failed, started, TIMEOUT + ABORT_WAIT + SETTLE).await;
    labels.push(second.clone());
    let mut waiting = Vec::new();
    for n in 0..2 {
        let label = format!("no_capacity:{n}");
        waiting.push(enqueue(&label, Style::Named, ISO, None).await.0);
        labels.push(label);
    }
    tokio::time::sleep(ABORT_WAIT + SETTLE).await;
    for id in &waiting {
        assert_eq!(detail(*id).await.job.state, JobState::Enqueued);
    }
    release(&first);
    release(&second);
    let released = Instant::now();
    for id in waiting {
        wait_for_state(id, JobState::Succeeded, released, SETTLE).await;
    }
    assert_eq!(max_overlap(&labels), 2);
    pulse.assert_unaffected("capacity", Duration::ZERO).await;
}

async fn check_progress_is_the_same_on_both_placements() {
    let mut shapes = Vec::new();
    for queue in [ISO, TASKS] {
        let label = format!("progress:{queue}");
        let (id, enqueued) = enqueue(&label, Style::Progress, queue, None).await;
        wait_for_state(id, JobState::Succeeded, enqueued, SETTLE * 2).await;
        let finished = detail(id).await;
        let children = jalari::admin::jobs(&jalari::admin::JobFilter::default(), 1000, None)
            .await
            .unwrap()
            .into_iter()
            .filter(|job| job.parent_id == Some(id))
            .collect::<Vec<_>>();
        assert_eq!(children.len(), 1, "{label}");
        wait_for_state(children[0].id, JobState::Succeeded, Instant::now(), SETTLE).await;
        let steps: Vec<(String, String)> = finished
            .steps
            .iter()
            .map(|step| (step.key.clone(), step.state.clone()))
            .collect();
        shapes.push((
            finished.job.state,
            steps,
            finished.control.and_then(|control| control.progress),
        ));
    }
    assert_eq!(
        shapes[0],
        (
            JobState::Succeeded,
            vec![
                ("charge".to_owned(), "done".to_owned()),
                ("receipt".to_owned(), "done".to_owned())
            ],
            None
        )
    );
    assert_eq!(shapes[0], shapes[1]);
}

async fn check_progress_survives_a_stop() {
    let label = "progress_loop";
    let (id, _) = enqueue(label, Style::ProgressLoop, ISO, None).await;
    wait_started(label).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(id, JobState::Stopped, asked, CONTROL_LATENCY).await;
    let saved: u32 = noted(label).unwrap().parse().unwrap();
    assert!(saved >= 2, "{saved}");
    assert_eq!(
        detail(id)
            .await
            .control
            .and_then(|control| control.progress),
        Some(json!(saved))
    );
}

async fn check_middleware_call_blocks_only_its_slot() {
    let pulse = Pulse::start("middleware_call");
    let label = "call_blocked";
    let enqueued = Instant::now();
    let id = jalari::enqueue(&CallBlocked {
        label: label.to_owned(),
    })
    .await
    .unwrap()
    .id();
    wait_for_state(id, JobState::Succeeded, enqueued, BLOCK + SETTLE).await;
    let call_thread = noted(&id.0.to_string()).unwrap();
    assert!(call_thread.starts_with("iso:"), "{call_thread}");
    assert_eq!(thread_of(label), call_thread);
    pulse
        .assert_unaffected("middleware call", Duration::ZERO)
        .await;
}

async fn check_finished_hook_runs_after_the_outcome(flavor: Flavor) {
    let pulse = Pulse::start("finished_hook");
    let id = jalari::enqueue(&HookBlocked {
        label: "hook_blocked".to_owned(),
    })
    .await
    .unwrap()
    .id();
    let hook = id.0.to_string();
    wait_until("the finished hook to start", START, || async {
        LEDGER.count(&hook, "hook start") == 1
    })
    .await;
    assert_eq!(detail(id).await.job.state, JobState::Succeeded);
    wait_until("the finished hook to end", BLOCK + SETTLE, || async {
        LEDGER.count(&hook, "hook end") == 1
    })
    .await;
    let measured = pulse.finish().await;
    eprintln!("finished_hook: a blocking hook on the worker's runtime, {measured:?}");
    if flavor.is_current_thread() {
        assert!(measured.widest_tick >= BLOCK / 2, "{measured:?}");
    }
}

async fn check_task_ids() {
    for queue in [ISO, TASKS] {
        let label = format!("task_id:{queue}");
        let (id, enqueued) = enqueue(&label, Style::TaskId, queue, None).await;
        wait_for_state(id, JobState::Succeeded, enqueued, SETTLE).await;
        let has_id = queue == TASKS;
        assert_eq!(noted(&label), Some(has_id.to_string()), "{label}");
    }
}

async fn check_more_blocking_bodies_than_cores() {
    let pulse = Pulse::start("oversubscribed");
    let spin_millis = u64::try_from((TIMEOUT * 2).as_millis()).unwrap();
    let mut jobs = Vec::new();
    for n in 0..crowd_size() {
        let label = format!("crowd:{n}");
        let (id, enqueued) = enqueue(
            &label,
            Style::Spin {
                millis: spin_millis,
            },
            CROWD,
            Some(TIMEOUT),
        )
        .await;
        jobs.push((id, enqueued, label));
    }
    for (id, enqueued, _) in &jobs {
        wait_for_state(
            *id,
            JobState::Failed,
            *enqueued,
            TIMEOUT + ABORT_WAIT + SETTLE + OVERSUBSCRIBED_SLACK,
        )
        .await;
        assert_eq!(
            last_error(*id).await,
            Some(format!("timed out after {TIMEOUT:?}"))
        );
    }
    for (_, _, label) in &jobs {
        wait_ended(label, START).await;
    }
    pulse
        .assert_unaffected("more blocking bodies than cores", OVERSUBSCRIBED_SLACK)
        .await;
}

async fn check_long_hold_reports_and_watchdog(database: &Database) {
    let registered = database.lifelines().await;
    let pulse = Pulse::start("long_hold");
    let watchdog_millis = u64::try_from(WATCHDOG_BLOCK.as_millis()).unwrap();
    let mut quiet = Vec::new();
    for n in 0..2 {
        let label = format!("quiet:{n}");
        let (id, enqueued) = enqueue(
            &label,
            Style::ThreadSleep {
                millis: watchdog_millis,
            },
            QUIET,
            None,
        )
        .await;
        quiet.push((id, enqueued, label));
    }
    let held = "long_hold";
    let (id, _) = enqueue(held, Style::UntilReleased, SOLO, Some(TIMEOUT)).await;
    let (waiting, _) = enqueue("long_hold:next", Style::Named, SOLO, None).await;
    let started = wait_started(held).await;
    wait_for_state(id, JobState::Failed, started, TIMEOUT + ABORT_WAIT + SETTLE).await;
    wait_until(
        "the held-slot error",
        TIMEOUT + HELD_REPORT + SETTLE * 2,
        || async {
            held_reports(id)
                .iter()
                .any(|(level, _)| *level == Level::ERROR)
        },
    )
    .await;
    assert_eq!(detail(waiting).await.job.state, JobState::Enqueued);
    let starts: Vec<Instant> = quiet
        .iter()
        .map(|(_, _, label)| at(label, "start").unwrap())
        .collect();
    let apart = starts[1]
        .saturating_duration_since(starts[0])
        .max(starts[0].saturating_duration_since(starts[1]));
    assert!(apart < Duration::from_secs(1), "{apart:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;
    release(held);
    let released = Instant::now();
    wait_for_state(waiting, JobState::Succeeded, released, SETTLE).await;
    assert_eq!(thread_of("long_hold:next"), "solo:0");
    let reports = held_reports(id);
    eprintln!("long_hold: {reports:?}");
    assert_eq!(reports.len(), 3, "{reports:?}");
    assert_eq!(
        reports[0],
        (
            Level::WARN,
            format!(
                "job {id} did not stop within 1s after it timed out; slot solo:0 takes no job \
                 until it returns"
            )
        )
    );
    assert_eq!(
        reports[1],
        (
            Level::ERROR,
            format!(
                "job {id} still runs 10 s after it timed out; slot solo:0 takes no job until it \
                 returns"
            )
        )
    );
    assert_eq!(
        reports[2],
        (
            Level::INFO,
            format!("job {id} returned 11 s after it timed out; slot solo:0 takes jobs again")
        )
    );
    let warned = slot_events(id)
        .into_iter()
        .find(|event| event.level == Level::WARN)
        .unwrap();
    assert_eq!(warned.fields.get("queue").map(String::as_str), Some(SOLO));
    assert_eq!(warned.fields.get("slot").map(String::as_str), Some("0"));
    assert_eq!(
        warned.fields.get("cause").map(String::as_str),
        Some("timed out")
    );
    for (id, enqueued, _) in &quiet {
        wait_for_state(*id, JobState::Succeeded, *enqueued, WATCHDOG_BLOCK + SETTLE).await;
    }
    assert_eq!(watchdog_lines(), Vec::<String>::new());
    assert_eq!(database.lifelines().await, registered);
    pulse.assert_unaffected("long hold", Duration::ZERO).await;
}

async fn check_task_queue_holds_too() {
    let label = "task_hold";
    let (id, _) = enqueue(label, Style::UntilReleased, TASKS, Some(TIMEOUT)).await;
    let (waiting, _) = enqueue("task_hold:next", Style::Named, TASKS, None).await;
    let started = wait_started(label).await;
    wait_for_state(id, JobState::Failed, started, TIMEOUT + ABORT_WAIT + SETTLE).await;
    wait_until("the held task warning", SETTLE, || async {
        !held_reports(id).is_empty()
    })
    .await;
    assert_eq!(
        held_reports(id)[0],
        (
            Level::WARN,
            format!(
                "job {id} did not stop within 1s after it timed out; slot tasks:0 takes no job \
                 until it returns"
            )
        )
    );
    assert_eq!(detail(waiting).await.job.state, JobState::Enqueued);
    release(label);
    let released = Instant::now();
    wait_for_state(waiting, JobState::Succeeded, released, SETTLE).await;
}

async fn check_stop_and_lost_holds(database: &Database) {
    let label = "stop_hold";
    let (id, _) = enqueue(label, Style::UntilReleased, SOLO, None).await;
    wait_started(label).await;
    let asked = Instant::now();
    assert_eq!(jalari::stop(id).await.unwrap(), StopOutcome::Requested);
    wait_for_state(
        id,
        JobState::Stopped,
        asked,
        CONTROL_LATENCY + STOP_TIMEOUT + ABORT_WAIT + SETTLE,
    )
    .await;
    wait_until("the stop hold warning", SETTLE, || async {
        !held_reports(id).is_empty()
    })
    .await;
    assert!(
        held_reports(id)[0]
            .1
            .contains("after it reached stop_timeout; slot solo:0"),
        "{:?}",
        held_reports(id)
    );
    release(label);
    wait_until("the stop hold to end", SETTLE, || async {
        held_reports(id).len() == 2
    })
    .await;
    assert_eq!(held_reports(id)[1].0, Level::INFO);

    let label = "lost_hold";
    let (id, _) = enqueue(label, Style::UntilReleased, SOLO, None).await;
    wait_started(label).await;
    assert_eq!(database.terminate_lifelines(1).await, 1);
    wait_until(
        "the lost hold warning",
        CONTROL_LATENCY + ABORT_WAIT,
        || async { !held_reports(id).is_empty() },
    )
    .await;
    let warned = held_reports(id);
    assert!(
        warned[0].1.contains("after it was lost; slot solo:0"),
        "{warned:?}"
    );
    assert_eq!(LEDGER.count(label, "start"), 1);
    release(label);
    wait_until("the lost hold to end", SETTLE, || async {
        held_reports(id).len() == 2
    })
    .await;
    wait_until("the lifeline to register again", START, || async {
        database.lifelines().await.len() == 1
    })
    .await;
    let _ = jalari::cancel(id).await.unwrap();
}

async fn check_shutdown_with_a_body_that_never_yields() {
    let worker = RunningWorker::spawn(
        Worker::builder()
            .queue(DOWN, 2)
            .config(worker_config(SHUTDOWN_TIMEOUT))
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    );
    let stuck = "down_stuck";
    let cooperative = "down_cooperative";
    let stuck_id = jalari::enqueue_with(
        &Styled {
            label: stuck.to_owned(),
            style: Style::UntilReleased,
        },
        EnqueueOptions::new().queue(DOWN).max_attempts(3),
    )
    .await
    .unwrap()
    .id();
    let (cooperative_id, _) = enqueue(cooperative, Style::InlinePoll, DOWN, None).await;
    wait_started(stuck).await;
    wait_started(cooperative).await;
    let asked = Instant::now();
    worker.stop().await;
    let returned = Instant::now();
    let took = returned.saturating_duration_since(asked);
    eprintln!("shutdown: Worker::run returned {took:?} after the request");
    assert!(took <= SHUTDOWN_TIMEOUT + ABORT_WAIT + SETTLE, "{took:?}");
    let cooperative_row = detail(cooperative_id).await.job;
    assert_eq!(
        (cooperative_row.state, cooperative_row.attempts),
        (JobState::Enqueued, 0)
    );
    let stuck_row = detail(stuck_id).await.job;
    assert_eq!(stuck_row.state, JobState::Scheduled);
    assert_eq!(stuck_row.last_error.as_deref(), Some(INTERRUPTED));
    let exited = at(cooperative, "thread exited");
    assert!(
        exited.is_some_and(|exited| exited <= returned),
        "the idle slot thread was not joined: {exited:?}"
    );
    assert_eq!(LEDGER.count(stuck, "thread exited"), 0);
    let left = EVENTS
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.message.starts_with("left thread down:"))
        .map(|event| event.message.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        left,
        [format!(
            "left thread {} running job {stuck_id} at shutdown",
            thread_of(stuck)
        )]
    );
    release(stuck);
    wait_until("the left thread to exit", THREAD_EXIT, || async {
        LEDGER.count(stuck, "thread exited") == 1
    })
    .await;
    for id in [stuck_id, cooperative_id] {
        assert_eq!(jalari::cancel(id).await.unwrap(), CancelOutcome::Cancelled);
    }
}

async fn check_dropped_worker_leaves_nothing(database: &Database) {
    let before = database.lifelines().await.len();
    let worker = RunningWorker::spawn(
        Worker::builder()
            .queue(CRASH, 2)
            .config(worker_config(SHUTDOWN_TIMEOUT))
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    );
    wait_until("the crash worker to register", START, || async {
        database.lifelines().await.len() == before + 1
    })
    .await;
    let stuck = "crash_stuck";
    let idle = "crash_idle";
    let (stuck_id, _) = enqueue(stuck, Style::UntilReleased, CRASH, None).await;
    let (idle_id, enqueued) = enqueue(idle, Style::Named, CRASH, None).await;
    wait_started(stuck).await;
    wait_for_state(idle_id, JobState::Succeeded, enqueued, SETTLE).await;
    worker.crash().await;
    wait_until("the idle thread to exit", THREAD_EXIT, || async {
        LEDGER.count(idle, "thread exited") == 1
    })
    .await;
    wait_until("the crashed worker's lifeline to close", START, || async {
        database.lifelines().await.len() == before
    })
    .await;
    assert_eq!(LEDGER.count(stuck, "thread exited"), 0);
    release(stuck);
    wait_until("the stuck thread to exit", THREAD_EXIT, || async {
        LEDGER.count(stuck, "thread exited") == 1
    })
    .await;
    let _ = jalari::cancel(stuck_id).await.unwrap();
}

async fn check_cron_under_blocking_bodies(database: &Database) {
    let spin_millis = u64::try_from((CRON_WINDOW + Duration::from_secs(1)).as_millis()).unwrap();
    let fire_bound = (HELD_FIRE_BACKOFF + SETTLE).as_secs_f64();
    for schedulers in [1, 2] {
        let name = format!("cron_blocked_{schedulers}");
        let key = format!("recurring:{name}");
        let mut workers = Vec::new();
        for _ in 0..schedulers {
            workers.push(RunningWorker::spawn(
                Worker::builder()
                    .queue(CRON_QUEUE, CRON_SLOTS)
                    .config(worker_config(SHUTDOWN_TIMEOUT))
                    .build()
                    .await
                    .unwrap(),
            ));
        }
        let pulse = Pulse::start(&name);
        jalari::recurring::add_or_update(
            &name,
            Cron::new("* * * * * *").unwrap(),
            &Styled {
                label: name.clone(),
                style: Style::Named,
            },
        )
        .await
        .unwrap();
        wait_until(&format!("{name}'s first run"), START, || async {
            !cron_runs(database, &key).await.is_empty()
        })
        .await;
        let spins: Vec<String> = (0..schedulers * CRON_SLOTS)
            .map(|n| format!("{name}:spin:{n}"))
            .collect();
        for label in &spins {
            enqueue(
                label,
                Style::ClockedSpin {
                    millis: spin_millis,
                },
                CRON_QUEUE,
                None,
            )
            .await;
        }
        for label in &spins {
            wait_ended(label, CRON_WINDOW * 3).await;
        }
        jalari::recurring::remove(&name).await.unwrap();
        let clock = |suffix: &str| -> Vec<f64> {
            spins
                .iter()
                .map(|label| {
                    noted(&format!("{label}:{suffix}"))
                        .unwrap()
                        .parse()
                        .unwrap()
                })
                .collect()
        };
        let blocked_from = clock("from").into_iter().fold(f64::MIN, f64::max);
        let blocked_to = clock("to").into_iter().fold(f64::MAX, f64::min);
        let runs = cron_runs(database, &key).await;
        let mut seconds: Vec<i64> = runs.iter().map(|run| run.floor() as i64).collect();
        seconds.dedup();
        assert_eq!(
            seconds.len(),
            runs.len(),
            "{name}: a tick was enqueued twice: {runs:?}"
        );
        let first_tick = blocked_from.ceil() as i64;
        let last_tick = (blocked_to - fire_bound).floor() as i64;
        let ticks = usize::try_from(last_tick - first_tick + 1).unwrap_or(0);
        let mut latest = 0.0_f64;
        for tick in first_tick..=last_tick {
            let fired: Vec<f64> = runs
                .iter()
                .map(|run| run - tick as f64)
                .filter(|late| (0.0..1.0).contains(late))
                .collect();
            assert_eq!(
                fired.len(),
                1,
                "{name}: tick {tick} while every slot spun: {runs:?}"
            );
            latest = latest.max(fired[0]);
        }
        eprintln!(
            "{name}: every slot spun for {:.2} s; {ticks} ticks in it each enqueued once, the \
             latest {latest:.3} s after its tick",
            blocked_to - blocked_from
        );
        assert!(
            ticks >= 5,
            "{name}: only {ticks} ticks while every slot spun"
        );
        assert!(
            latest <= fire_bound,
            "{name}: a run was enqueued {latest} s after its tick"
        );
        pulse.assert_unaffected(&name, Duration::ZERO).await;
        for label in &spins {
            assert_ran_on(label, CRON_QUEUE);
        }
        for worker in workers {
            worker.stop().await;
        }
    }
}

async fn cron_runs(database: &Database, key: &str) -> Vec<f64> {
    jalari::sqlx::query_scalar(jalari::sqlx::AssertSqlSafe(format!(
        "SELECT EXTRACT(EPOCH FROM created_at)::float8 FROM {} WHERE job_key = $1
         ORDER BY created_at",
        database.table("job")
    )))
    .bind(key)
    .fetch_all(pool())
    .await
    .unwrap()
}
