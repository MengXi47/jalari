use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::job::SNOOZED;
use crate::storage::interval;
use crate::{Command, Error, ErrorKind, JobId, JobState, Priority, Result};

const JSON_CODEC: &str = "json";
const MAX_PAGE: u32 = 1000;

pub(crate) const JOB_COLUMNS: &str = "id, queue, task, state, codec, payload, job_key, queue_key, \
     priority, attempts, max_attempts, run_at, timeout_ms, last_error, context, fingerprint, \
     locked_by::text AS worker_id, locked_at AS running_since, command, created_at, updated_at, \
     parent_id, root_id, coalesce(queue_key_limit, 1::smallint) AS queue_key_limit, expires_at";

pub(crate) const FILTER_SQL: &str = "($1::text[] IS NULL OR state = ANY($1))
    AND ($2::text IS NULL OR queue = $2)
    AND ($3::text IS NULL OR task = $3)
    AND ($4::text IS NULL OR job_key = $4)
    AND ($5::text IS NULL OR starts_with(job_key, $5))
    AND ($6::timestamptz IS NULL OR created_at >= $6)
    AND ($7::timestamptz IS NULL OR created_at < $7)
    AND ($8::jsonb IS NULL OR context @> $8)
    AND ($9::smallint[] IS NULL OR priority = ANY($9))
    AND ($10::bigint IS NULL OR parent_id = $10)
    AND ($11::bigint IS NULL OR root_id = $11 OR id = $11)";

macro_rules! bind_filter {
    ($query:expr, $filter:expr) => {
        $query
            .bind($filter.state_names())
            .bind(&$filter.queue)
            .bind(&$filter.task)
            .bind(&$filter.job_key)
            .bind(&$filter.job_key_prefix)
            .bind($filter.created_after)
            .bind($filter.created_before)
            .bind(&$filter.context)
            .bind($filter.priority_levels())
            .bind($filter.parent.map(|id| id.0))
            .bind($filter.root.map(|id| id.0))
    };
}

pub(crate) use bind_filter;

/// Which jobs [`jobs`], [`count`] and [`retry_failed`](super::retry_failed) look at.
///
/// Every condition is optional and all set conditions must hold.
///
/// # Examples
///
/// ```rust
/// let filter = jalari::admin::JobFilter::new()
///     .state(jalari::JobState::Failed)
///     .queue("emails")
///     .context(serde_json::json!({ "tenant_id": "acme" }));
/// ```
#[derive(Debug, Clone, Default)]
pub struct JobFilter {
    pub(crate) states: Vec<JobState>,
    pub(crate) queue: Option<String>,
    pub(crate) task: Option<String>,
    pub(crate) job_key: Option<String>,
    pub(crate) job_key_prefix: Option<String>,
    pub(crate) created_after: Option<DateTime<Utc>>,
    pub(crate) created_before: Option<DateTime<Utc>>,
    pub(crate) context: Option<Value>,
    pub(crate) priorities: Vec<Priority>,
    pub(crate) parent: Option<JobId>,
    pub(crate) root: Option<JobId>,
}

impl JobFilter {
    /// A filter that matches every job.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a state to match; a job matches when it is in any of the added states.
    pub fn state(mut self, state: JobState) -> Self {
        if !self.states.contains(&state) {
            self.states.push(state);
        }
        self
    }

    /// Matches only jobs in `queue`.
    pub fn queue(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_owned());
        self
    }

    /// Matches only jobs of the task with this [`Job::NAME`](crate::Job::NAME).
    pub fn task(mut self, task: &str) -> Self {
        self.task = Some(task.to_owned());
        self
    }

    /// Matches only the job with exactly this `job_key`.
    pub fn job_key(mut self, job_key: &str) -> Self {
        self.job_key = Some(job_key.to_owned());
        self
    }

    /// Matches only jobs whose `job_key` starts with `prefix`.
    pub fn job_key_prefix(mut self, prefix: &str) -> Self {
        self.job_key_prefix = Some(prefix.to_owned());
        self
    }

    /// Matches only jobs created at or after `at`.
    pub fn created_after(mut self, at: DateTime<Utc>) -> Self {
        self.created_after = Some(at);
        self
    }

    /// Matches only jobs created before `at`.
    pub fn created_before(mut self, at: DateTime<Utc>) -> Self {
        self.created_before = Some(at);
        self
    }

    /// Matches only jobs whose stored context contains `context`, with PostgreSQL's `@>`.
    ///
    /// `json!({ "tenant_id": "acme" })` matches a context object that has that key with that
    /// value, whatever else it holds. Migration 6 indexes the context column for this.
    pub fn context(mut self, context: Value) -> Self {
        self.context = Some(context);
        self
    }

    /// Adds a priority to match; a job matches when it has any of the added priorities.
    pub fn priority(mut self, priority: Priority) -> Self {
        if !self.priorities.contains(&priority) {
            self.priorities.push(priority);
        }
        self
    }

    /// Matches only jobs enqueued by the running job with this id.
    pub fn parent(mut self, parent: JobId) -> Self {
        self.parent = Some(parent);
        self
    }

    /// Matches the job with this id and every job enqueued below it, at any depth.
    pub fn root(mut self, root: JobId) -> Self {
        self.root = Some(root);
        self
    }

    pub(crate) fn priority_levels(&self) -> Option<Vec<i16>> {
        (!self.priorities.is_empty()).then(|| {
            self.priorities
                .iter()
                .map(|priority| priority.level())
                .collect()
        })
    }

    pub(crate) fn state_names(&self) -> Option<Vec<&'static str>> {
        (!self.states.is_empty()).then(|| self.states.iter().map(|state| state.as_str()).collect())
    }

    pub(crate) fn only_failed(&self) -> Self {
        Self {
            states: vec![JobState::Failed],
            ..self.clone()
        }
    }
}

/// One job as stored, with its payload still encoded.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInfo {
    /// Primary key.
    pub id: JobId,
    /// Queue the job waits in.
    pub queue: String,
    /// [`Job::NAME`](crate::Job::NAME) of the job.
    pub task: String,
    /// Current state.
    pub state: JobState,
    /// How the payload is encoded; `json` for jobs enqueued by this crate.
    pub codec: String,
    /// The encoded job; decode a `json` one with [`payload_json`](Self::payload_json).
    pub payload: Vec<u8>,
    /// Deduplication key, if any.
    pub job_key: Option<String>,
    /// Exclusion key, if any.
    pub queue_key: Option<String>,
    /// How many jobs sharing the `queue_key` may run at once; 1 unless set with
    /// [`EnqueueOptions::queue_key_limit`](crate::EnqueueOptions::queue_key_limit).
    pub queue_key_limit: u16,
    /// Which due jobs of the queue run before this one.
    pub priority: Priority,
    /// Attempts started so far.
    pub attempts: i32,
    /// Attempts allowed.
    pub max_attempts: i32,
    /// When the job became or becomes due; a worker taking the job does not change it.
    pub run_at: DateTime<Utc>,
    /// Per-attempt time limit, if any.
    pub timeout: Option<Duration>,
    /// Error of the last attempt that failed, kept until a later attempt succeeds.
    pub last_error: Option<String>,
    /// Context stored when the job was enqueued.
    pub context: Option<Value>,
    /// Fingerprint of the build that enqueued the job; only a worker of that build runs it.
    ///
    /// `None` for a job enqueued without one: before migration 7, by a task declared without
    /// `fingerprint = true`, by a process that does not link the task, or after
    /// [`clear_fingerprint`](super::clear_fingerprint). Any worker that runs the task takes
    /// such a job.
    pub fingerprint: Option<String>,
    /// Id of the worker running the job, as listed by [`workers`](super::workers); `None`
    /// unless the job is [`Running`](JobState::Running).
    pub worker_id: Option<String>,
    /// When the current attempt started; `None` unless the job is running.
    pub running_since: Option<DateTime<Utc>>,
    /// A [`stop`](crate::stop) or [`cancel`](crate::cancel) the running job has not acted on
    /// yet.
    pub pending: Option<Command>,
    /// When the job was enqueued.
    pub created_at: DateTime<Utc>,
    /// When the state last changed.
    pub updated_at: DateTime<Utc>,
    /// Id of the job that was running when this one was enqueued, if any.
    pub parent_id: Option<JobId>,
    /// Id of the first job of the chain this job belongs to; `None` for a job enqueued outside
    /// a job, which is its own root.
    pub root_id: Option<JobId>,
    /// When the job is discarded if it is still waiting, if set.
    pub expires_at: Option<DateTime<Utc>>,
}

impl JobInfo {
    /// Decodes a `json` payload into the job type it was enqueued as.
    ///
    /// # Errors
    ///
    /// Returns an error if the codec is not `json` or the bytes do not deserialize into `T`
    /// ([`PayloadDecodeFailed`](ErrorKind::PayloadDecodeFailed)).
    pub fn payload_json<T: DeserializeOwned>(&self) -> Result<T> {
        if self.codec != JSON_CODEC {
            return Err(Error::new(
                ErrorKind::PayloadDecodeFailed,
                format!("job {}: codec {:?} is not json", self.id, self.codec),
            ));
        }
        serde_json::from_slice(&self.payload).map_err(|err| {
            Error::new(
                ErrorKind::PayloadDecodeFailed,
                format!("job {}: {err}", self.id),
            )
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct JobRow {
    id: i64,
    queue: String,
    task: String,
    state: String,
    codec: String,
    payload: Vec<u8>,
    job_key: Option<String>,
    queue_key: Option<String>,
    priority: i16,
    attempts: i32,
    max_attempts: i32,
    run_at: DateTime<Utc>,
    timeout_ms: Option<i32>,
    last_error: Option<String>,
    context: Option<Value>,
    fingerprint: Option<String>,
    worker_id: Option<String>,
    running_since: Option<DateTime<Utc>>,
    command: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    parent_id: Option<i64>,
    root_id: Option<i64>,
    queue_key_limit: i16,
    expires_at: Option<DateTime<Utc>>,
}

impl JobRow {
    fn into_info(self) -> Result<JobInfo> {
        Ok(JobInfo {
            id: JobId(self.id),
            queue: self.queue,
            task: self.task,
            state: JobState::parse(&self.state)?,
            codec: self.codec,
            payload: self.payload,
            job_key: self.job_key,
            queue_key: self.queue_key,
            queue_key_limit: u16::try_from(self.queue_key_limit).unwrap_or(1),
            priority: Priority::from_level(self.priority),
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            timeout: self
                .timeout_ms
                .map(|ms| Duration::from_millis(u64::try_from(ms).unwrap_or(0))),
            last_error: self.last_error,
            context: self.context,
            fingerprint: self.fingerprint,
            worker_id: self.worker_id,
            running_since: self.running_since,
            pending: self.command.as_deref().and_then(Command::parse),
            created_at: self.created_at,
            updated_at: self.updated_at,
            parent_id: self.parent_id.map(JobId),
            root_id: self.root_id.map(JobId),
            expires_at: self.expires_at,
        })
    }
}

/// One entry of a job's history: a state it entered and when.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
    /// State the job entered.
    pub state: JobState,
    /// Attempt number at that time; `0` before the first.
    pub attempt: i32,
    /// Error or note recorded with the change.
    pub error: Option<String>,
    /// How long the attempt ran, when the entry closes one.
    pub duration: Option<Duration>,
    /// When the change happened.
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct HistoryRow {
    state: String,
    attempt: i32,
    error: Option<String>,
    duration_ms: Option<i64>,
    created_at: DateTime<Utc>,
}

/// Progress that [`checkpoint`](crate::checkpoint)s saved for a job, and its lease.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlInfo {
    /// Progress saved at the job's last checkpoint, kept while it waits or is stopped.
    pub progress: Option<Value>,
    /// Incremented when a worker claims the job, when a run ends, and each time the job changes
    /// hands otherwise, so every run holds a lease of its own.
    pub lease: i64,
    /// When the control row last changed.
    pub updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ControlRow {
    progress: Option<Value>,
    lease: i64,
    updated_at: DateTime<Utc>,
}

/// One [`step`](crate::step) a job recorded, as listed in [`JobDetail::steps`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepInfo {
    /// The key the step was started with.
    pub key: String,
    /// `started` while the step has not succeeded, `done` once its output is recorded.
    pub state: String,
    /// How often the step started without being recorded as done; see
    /// [`StepRun::starts`](crate::StepRun::starts).
    pub starts: i32,
    /// The job attempt that last started the step.
    pub attempt: i32,
    /// When the step first started.
    pub started_at: DateTime<Utc>,
    /// When the step's output was recorded; `None` while it is started.
    pub finished_at: Option<DateTime<Utc>>,
    /// Error of the last execution that failed, cut to 1 KiB; cleared when the step succeeds.
    pub error: Option<String>,
    /// The recorded output as JSON; `None` while the step is started.
    pub output: Option<Value>,
}

#[derive(sqlx::FromRow)]
struct StepRow {
    key: String,
    state: String,
    starts: i32,
    attempt: i32,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    error: Option<String>,
    output: Option<Value>,
}

/// One job with its history and control state, as returned by [`job`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDetail {
    /// The job itself.
    pub job: JobInfo,
    /// Every state change, oldest first.
    pub history: Vec<HistoryEntry>,
    /// Saved progress and lease; `None` until a worker first claims the job.
    pub control: Option<ControlInfo>,
    /// The [`step`](crate::step)s the job recorded, in the order they first started.
    pub steps: Vec<StepInfo>,
}

/// Result of [`clear_steps`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearStepsOutcome {
    /// The job's recorded steps were deleted; the number is how many.
    Cleared(u64),
    /// The job is running, so its steps were kept.
    Running,
    /// No job has this id, or housekeeping already removed it.
    NotFound,
}

/// Number of jobs in one queue and state, as returned by [`stats`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCount {
    /// Queue name.
    pub queue: String,
    /// State the counted jobs are in.
    pub state: JobState,
    /// How many.
    pub count: i64,
}

/// What one queue's jobs did within a window, as returned by [`throughput`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Throughput {
    /// Queue name.
    pub queue: String,
    /// Attempts that succeeded.
    pub succeeded: i64,
    /// Jobs that failed for good.
    pub failed: i64,
    /// Attempts that ran, failed and were scheduled again; interrupted attempts are not
    /// counted, since they never finished.
    pub retried: i64,
    /// Attempts that ran and asked to run again later; they are not in `retried`.
    pub snoozed: i64,
    /// Jobs that ended discarded, by `JobError::discard` or by expiry; they are not in
    /// `failed`.
    pub discarded: i64,
    /// Attempts that ran and were stopped.
    pub stopped: i64,
    /// Jobs that were cancelled.
    pub cancelled: i64,
    /// Mean duration of the attempts that finished, succeeded or not.
    pub average_duration: Option<Duration>,
    /// Longest attempt that finished.
    pub max_duration: Option<Duration>,
}

#[derive(sqlx::FromRow)]
struct ThroughputRow {
    queue: String,
    succeeded: i64,
    failed: i64,
    retried: i64,
    snoozed: i64,
    discarded: i64,
    stopped: i64,
    cancelled: i64,
    average_ms: Option<f64>,
    max_ms: Option<i64>,
}

/// Counts the jobs in every queue by state.
///
/// Queues and states with no jobs are absent. Finished jobs stay counted until housekeeping
/// removes them.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// for count in jalari::admin::stats().await? {
///     println!("{} {}: {}", count.queue, count.state, count.count);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn stats() -> Result<Vec<JobCount>> {
    let config = config()?;
    let rows: Vec<(String, String, i64)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT queue, state, count(*) FROM {job} GROUP BY queue, state ORDER BY queue, state",
        job = config.schema.table("job"),
    )))
    .fetch_all(&config.pool.pool())
    .await?;
    rows.into_iter()
        .map(|(queue, state, count)| {
            Ok(JobCount {
                queue,
                state: JobState::parse(&state)?,
                count,
            })
        })
        .collect()
}

/// Counts the jobs matching `filter`.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// let failed = jalari::admin::JobFilter::new().state(jalari::JobState::Failed);
/// println!("{} failed jobs", jalari::admin::count(&failed).await?);
/// # Ok(())
/// # }
/// ```
pub async fn count(filter: &JobFilter) -> Result<i64> {
    let config = config()?;
    let query = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {job} WHERE {FILTER_SQL}",
        job = config.schema.table("job"),
    )));
    let count = bind_filter!(query, filter)
        .fetch_one(&config.pool.pool())
        .await?;
    Ok(count)
}

/// Lists up to `limit` jobs matching `filter`, newest first.
///
/// Pass the id of the last job of a page as `before` to get the next page; the pages stay
/// consistent while jobs are added or removed, because they are cut by id rather than by
/// position. `limit` is capped at 1000. An empty result means there are no more jobs.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// let filter = jalari::admin::JobFilter::new()
///     .context(serde_json::json!({ "tenant_id": "acme" }));
/// let mut before = None;
/// loop {
///     let page = jalari::admin::jobs(&filter, 100, before).await?;
///     let Some(last) = page.last() else { break };
///     before = Some(last.id);
///     for job in &page {
///         println!("{} {} {}", job.id, job.task, job.state);
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub async fn jobs(filter: &JobFilter, limit: u32, before: Option<JobId>) -> Result<Vec<JobInfo>> {
    let config = config()?;
    let query = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT {JOB_COLUMNS} FROM {job}
         WHERE {FILTER_SQL} AND ($12::bigint IS NULL OR id < $12)
         ORDER BY id DESC
         LIMIT $13",
        job = config.schema.table("job"),
    )));
    let rows: Vec<JobRow> = bind_filter!(query, filter)
        .bind(before.map(|id| id.0))
        .bind(i64::from(limit.min(MAX_PAGE)))
        .fetch_all(&config.pool.pool())
        .await?;
    rows.into_iter().map(JobRow::into_info).collect()
}

/// Returns one job with its history and control state, or `None` if no job has this id.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # #[derive(serde::Deserialize)]
/// # struct SendWelcome { user_id: i64 }
/// # async fn example(id: jalari::JobId) -> jalari::Result<()> {
/// if let Some(detail) = jalari::admin::job(id).await? {
///     let welcome: SendWelcome = detail.job.payload_json()?;
///     println!("user {} after {} changes", welcome.user_id, detail.history.len());
/// }
/// # Ok(())
/// # }
/// ```
pub async fn job(id: JobId) -> Result<Option<JobDetail>> {
    let config = config()?;
    let schema = &config.schema;
    let mut connection = config
        .pool
        .pool()
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    let Some(job) = load_job(&mut connection, schema, id).await? else {
        return Ok(None);
    };
    let history: Vec<HistoryRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempt, error, duration_ms, created_at FROM {job_history}
         WHERE job_id = $1 ORDER BY id",
        job_history = schema.table("job_history"),
    )))
    .bind(id.0)
    .fetch_all(&mut *connection)
    .await?;
    let control: Option<ControlRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT progress, lease, updated_at FROM {job_control} WHERE job_id = $1",
        job_control = schema.table("job_control"),
    )))
    .bind(id.0)
    .fetch_optional(&mut *connection)
    .await?;
    let steps: Vec<StepRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT key, state, starts, attempt, started_at, finished_at, error, output
         FROM {job_step} WHERE job_id = $1 ORDER BY started_at, key",
        job_step = schema.table("job_step"),
    )))
    .bind(id.0)
    .fetch_all(&mut *connection)
    .await?;
    let history = history
        .into_iter()
        .map(|row| {
            Ok(HistoryEntry {
                state: JobState::parse(&row.state)?,
                attempt: row.attempt,
                error: row.error,
                duration: row
                    .duration_ms
                    .map(|ms| Duration::from_millis(u64::try_from(ms).unwrap_or(0))),
                created_at: row.created_at,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(JobDetail {
        job,
        history,
        control: control.map(|row| ControlInfo {
            progress: row.progress,
            lease: row.lease,
            updated_at: row.updated_at,
        }),
        steps: steps
            .into_iter()
            .map(|row| StepInfo {
                key: row.key,
                state: row.state,
                starts: row.starts,
                attempt: row.attempt,
                started_at: row.started_at,
                finished_at: row.finished_at,
                error: row.error,
                output: row.output,
            })
            .collect(),
    }))
}

/// Deletes the [`step`](crate::step)s a job recorded, so its next run executes every step again.
///
/// It is meant for an operator who knows that the effects of the recorded steps did not happen,
/// or must happen again: the next run calls every step's closure, with new
/// [idempotency keys](crate::StepRun::idempotency_key), so a provider that deduplicates by key
/// does not recognise the repeat. A running job keeps its steps; stop it first, or wait until
/// it has failed or is waiting for a retry. Progress saved with
/// [`Checkpoint`](crate::Checkpoint) is kept.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::admin::ClearStepsOutcome;
///
/// # async fn example(id: jalari::JobId) -> jalari::Result<()> {
/// match jalari::admin::clear_steps(id).await? {
///     ClearStepsOutcome::Cleared(steps) => {
///         println!("{steps} steps cleared");
///         jalari::admin::retry(id).await?;
///     }
///     ClearStepsOutcome::Running => println!("job {id} is running, stop it first"),
///     _ => println!("job {id} does not exist"),
/// }
/// # Ok(())
/// # }
/// ```
pub async fn clear_steps(id: JobId) -> Result<ClearStepsOutcome> {
    let config = config()?;
    let (state, cleared): (Option<String>, i64) = sqlx::query_as(AssertSqlSafe(format!(
        "WITH target AS (
             SELECT id, state FROM {job} WHERE id = $1 FOR SHARE
         ),
         cleared AS (
             DELETE FROM {job_step}
             WHERE job_id IN (SELECT id FROM target WHERE state <> 'running')
             RETURNING key
         )
         SELECT (SELECT state FROM target), (SELECT count(*) FROM cleared)",
        job = config.schema.table("job"),
        job_step = config.schema.table("job_step"),
    )))
    .bind(id.0)
    .fetch_one(&config.pool.pool())
    .await?;
    Ok(match state.as_deref() {
        None => ClearStepsOutcome::NotFound,
        Some("running") => ClearStepsOutcome::Running,
        Some(_) => ClearStepsOutcome::Cleared(u64::try_from(cleared).unwrap_or(0)),
    })
}

/// Sums up what each queue's jobs did during the last `window`, from the job history.
///
/// Housekeeping deletes finished jobs with their history, so a window longer than the
/// retention undercounts. Queues with no activity in the window are absent.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or the database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// for queue in jalari::admin::throughput(std::time::Duration::from_secs(3600)).await? {
///     println!("{}: {} ok, {} failed", queue.queue, queue.succeeded, queue.failed);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn throughput(window: Duration) -> Result<Vec<Throughput>> {
    let config = config()?;
    let rows: Vec<ThroughputRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job.queue,
             count(*) FILTER (WHERE history.state = 'succeeded') AS succeeded,
             count(*) FILTER (WHERE history.state = 'failed') AS failed,
             count(*) FILTER (WHERE history.state = 'scheduled'
                 AND history.duration_ms IS NOT NULL
                 AND history.error IS DISTINCT FROM $2) AS retried,
             count(*) FILTER (WHERE history.state = 'scheduled'
                 AND history.duration_ms IS NOT NULL AND history.error = $2) AS snoozed,
             count(*) FILTER (WHERE history.state = 'discarded') AS discarded,
             count(*) FILTER (WHERE history.state = 'stopped'
                 AND history.duration_ms IS NOT NULL) AS stopped,
             count(*) FILTER (WHERE history.state = 'deleted') AS cancelled,
             avg(history.duration_ms)::float8 AS average_ms,
             max(history.duration_ms) AS max_ms
         FROM {job_history} AS history
         JOIN {job} AS job ON job.id = history.job_id
         WHERE history.created_at >= now() - $1
         GROUP BY job.queue
         ORDER BY job.queue",
        job = config.schema.table("job"),
        job_history = config.schema.table("job_history"),
    )))
    .bind(interval(window))
    .bind(SNOOZED)
    .fetch_all(&config.pool.pool())
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Throughput {
            queue: row.queue,
            succeeded: row.succeeded,
            failed: row.failed,
            retried: row.retried,
            snoozed: row.snoozed,
            discarded: row.discarded,
            stopped: row.stopped,
            cancelled: row.cancelled,
            average_duration: row
                .average_ms
                .filter(|ms| ms.is_finite() && *ms >= 0.0)
                .map(|ms| Duration::from_secs_f64(ms / 1000.0)),
            max_duration: row
                .max_ms
                .map(|ms| Duration::from_millis(u64::try_from(ms).unwrap_or(0))),
        })
        .collect())
}

pub(crate) async fn load_job(
    connection: &mut PgConnection,
    schema: &crate::Schema,
    id: JobId,
) -> Result<Option<JobInfo>> {
    let row: Option<JobRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT {JOB_COLUMNS} FROM {job} WHERE id = $1",
        job = schema.table("job"),
    )))
    .bind(id.0)
    .fetch_optional(connection)
    .await?;
    row.map(JobRow::into_info).transpose()
}
