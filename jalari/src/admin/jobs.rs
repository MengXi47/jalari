use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::storage::interval;
use crate::{Command, Error, ErrorKind, JobId, JobState, Result};

const JSON_CODEC: &str = "json";
const MAX_PAGE: u32 = 1000;

pub(crate) const JOB_COLUMNS: &str = "id, queue, task, state, codec, payload, job_key, queue_key, \
     priority, attempts, max_attempts, run_at, timeout_ms, last_error, context, fingerprint, \
     created_at, updated_at";

pub(crate) const FILTER_SQL: &str = "($1::text[] IS NULL OR state = ANY($1))
    AND ($2::text IS NULL OR queue = $2)
    AND ($3::text IS NULL OR task = $3)
    AND ($4::text IS NULL OR job_key = $4)
    AND ($5::text IS NULL OR starts_with(job_key, $5))
    AND ($6::timestamptz IS NULL OR created_at >= $6)
    AND ($7::timestamptz IS NULL OR created_at < $7)
    AND ($8::jsonb IS NULL OR context @> $8)";

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
    /// Higher runs first.
    pub priority: i16,
    /// Attempts started so far.
    pub attempts: i32,
    /// Attempts allowed.
    pub max_attempts: i32,
    /// When the job may run; while running, a short lease ahead of now.
    pub run_at: DateTime<Utc>,
    /// Per-attempt time limit, if any.
    pub timeout: Option<Duration>,
    /// Error of the last attempt that failed, kept until a later attempt succeeds.
    pub last_error: Option<String>,
    /// Context stored when the job was enqueued.
    pub context: Option<Value>,
    /// Fingerprint of the build that enqueued the job; only a worker of that build runs it.
    ///
    /// `None` for a job enqueued without one: before migration 7, by a task declared with
    /// `fingerprint = false`, by a process that does not link the task, or after
    /// [`clear_fingerprint`](super::clear_fingerprint). Any worker that runs the task takes
    /// such a job.
    pub fingerprint: Option<String>,
    /// When the job was enqueued.
    pub created_at: DateTime<Utc>,
    /// When the state last changed.
    pub updated_at: DateTime<Utc>,
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
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
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
            priority: self.priority,
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            timeout: self
                .timeout_ms
                .map(|ms| Duration::from_millis(u64::try_from(ms).unwrap_or(0))),
            last_error: self.last_error,
            context: self.context,
            fingerprint: self.fingerprint,
            created_at: self.created_at,
            updated_at: self.updated_at,
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

/// What [`stop`](crate::stop), [`cancel`](crate::cancel) and checkpoints recorded for a job.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlInfo {
    /// Request a worker has not acted on yet.
    pub pending: Option<Command>,
    /// Progress saved at the job's last checkpoint, kept while it waits or is stopped.
    pub progress: Option<Value>,
    /// Incremented each time the job's lease changes hands.
    pub lease: i64,
    /// When the control row last changed.
    pub updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ControlRow {
    command: Option<String>,
    progress: Option<Value>,
    lease: i64,
    updated_at: DateTime<Utc>,
}

/// One job with its history and control state, as returned by [`job`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDetail {
    /// The job itself.
    pub job: JobInfo,
    /// Every state change, oldest first.
    pub history: Vec<HistoryEntry>,
    /// Pending request and saved progress; `None` when nothing was ever recorded.
    pub control: Option<ControlInfo>,
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
         WHERE {FILTER_SQL} AND ($9::bigint IS NULL OR id < $9)
         ORDER BY id DESC
         LIMIT $10",
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
        "SELECT command, progress, lease, updated_at FROM {job_control} WHERE job_id = $1",
        job_control = schema.table("job_control"),
    )))
    .bind(id.0)
    .fetch_optional(&mut *connection)
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
            pending: row.command.as_deref().and_then(Command::parse),
            progress: row.progress,
            lease: row.lease,
            updated_at: row.updated_at,
        }),
    }))
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
                 AND history.duration_ms IS NOT NULL) AS retried,
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
    .fetch_all(&config.pool.pool())
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Throughput {
            queue: row.queue,
            succeeded: row.succeeded,
            failed: row.failed,
            retried: row.retried,
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
