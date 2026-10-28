use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool, Row};
use tracing::{debug, warn};

use super::checkpoint::RunControl;
use super::fence::begin_fenced;
use super::run::current_job;
use crate::init::config;
use crate::{Error, ErrorKind, JobError, Schema};

const MAX_KEY_LENGTH: usize = 255;
const MAX_OUTPUT_LENGTH: usize = 1024 * 1024;
const MAX_ERROR_LENGTH: usize = 1024;
const FINISH_WINDOW: Duration = Duration::from_secs(30);
const FINISH_BACKOFF: Duration = Duration::from_millis(100);
const FINISH_BACKOFF_LIMIT: Duration = Duration::from_millis(500);
const STARTED: &str = "started";
const DONE: &str = "done";

tokio::task_local! {
    static INSIDE: ();
}

pub(crate) fn in_step() -> bool {
    INSIDE.try_with(|()| ()).is_ok()
}

/// The step that [`step`] is running, handed to its closure.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn charge(order_id: i64, key: &str) -> Result<String, std::io::Error> { Ok(String::new()) }
/// # async fn example(order_id: i64) -> jalari::JobResult {
/// let charge_id = jalari::step("charge", |run| async move {
///     println!("step {} starts for the {} time", run.key(), run.starts());
///     Ok(charge(order_id, run.idempotency_key()).await?)
/// })
/// .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StepRun {
    key: String,
    starts: i32,
    idempotency_key: String,
}

impl StepRun {
    /// The key the step was started with.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// How often the step has started without being recorded as done, this start included.
    ///
    /// It is 1 on the first start. Above 1, an earlier execution started and was not recorded:
    /// it failed, its worker died, or the job was taken over, so its effect may or may not have
    /// happened.
    pub fn starts(&self) -> i32 {
        self.starts
    }

    /// A key for the step's effect that stays the same across retries and takeovers.
    ///
    /// It is a hyphenated UUID created with the step's record, so it is unique per step and
    /// job. Pass it to an API that deduplicates requests by key. An
    /// [`OnConflict::Replace`](crate::OnConflict::Replace) of the job or
    /// [`admin::clear_steps`](crate::admin::clear_steps) deletes the record, and the next start
    /// gets a new key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyState {
    InFlight,
    Ended,
    Done,
}

#[derive(Debug, Default)]
pub(crate) struct StepKeys(Mutex<HashMap<String, KeyState>>);

impl StepKeys {
    fn enter<'a>(&'a self, key: &'a str) -> Result<InFlight<'a>, JobError> {
        let mut keys = self.0.lock().unwrap();
        match keys.get(key) {
            Some(KeyState::InFlight) => Err(refused(
                key,
                "is already running in this run; give concurrent steps different keys",
            )),
            Some(KeyState::Done) => Err(refused(
                key,
                "already succeeded in this run; a key can succeed once per run",
            )),
            Some(KeyState::Ended) | None => {
                keys.insert(key.to_owned(), KeyState::InFlight);
                Ok(InFlight {
                    keys: self,
                    key,
                    done: false,
                })
            }
        }
    }

    fn visited(&self) -> Vec<String> {
        self.0.lock().unwrap().keys().cloned().collect()
    }

    fn set(&self, key: &str, state: KeyState) {
        if let Some(current) = self.0.lock().unwrap().get_mut(key) {
            *current = state;
        }
    }
}

struct InFlight<'a> {
    keys: &'a StepKeys,
    key: &'a str,
    done: bool,
}

impl InFlight<'_> {
    fn done(mut self) {
        self.done = true;
        self.keys.set(self.key, KeyState::Done);
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.keys.set(self.key, KeyState::Ended);
        }
    }
}

enum Started {
    Open(StepRun),
    Recorded(Value),
}

struct StepRecord<'a> {
    pool: PgPool,
    schema: &'a Schema,
    control: &'a RunControl,
    key: &'a str,
}

impl StepRecord<'_> {
    async fn start(&self, attempt: i32) -> Result<Started, JobError> {
        let row = self
            .try_start(attempt)
            .await
            .map_err(|err| self.failed("could not start", err))?;
        let fenced: bool = row.try_get("fenced").map_err(|err| self.decode(err))?;
        if !fenced {
            return Err(self.control.lose());
        }
        let state: Option<String> = row.try_get("state").map_err(|err| self.decode(err))?;
        match state.as_deref() {
            Some(STARTED) => Ok(Started::Open(StepRun {
                key: self.key.to_owned(),
                starts: row.try_get("starts").map_err(|err| self.decode(err))?,
                idempotency_key: row.try_get("nonce").map_err(|err| self.decode(err))?,
            })),
            Some(DONE) => {
                let output: Option<Value> =
                    row.try_get("output").map_err(|err| self.decode(err))?;
                Ok(Started::Recorded(output.unwrap_or(Value::Null)))
            }
            other => Err(JobError::new(format!(
                "step {:?}: the start found the record in state {other:?}",
                self.key
            ))),
        }
    }

    async fn try_start(&self, attempt: i32) -> sqlx::Result<sqlx::postgres::PgRow> {
        let mut transaction = begin_fenced(&self.pool).await?;
        let row = sqlx::query(AssertSqlSafe(format!(
            "WITH fence AS (
                 SELECT job_id FROM {job_control} WHERE job_id = $1 AND lease = $2 FOR SHARE
             ),
             upserted AS (
                 INSERT INTO {job_step} AS step (job_id, key, state, attempt, lease)
                 SELECT job_id, $3, '{STARTED}', $4, $2 FROM fence
                 ON CONFLICT (job_id, key) DO UPDATE
                 SET starts = CASE WHEN step.state = '{STARTED}' THEN step.starts + 1
                         ELSE step.starts END,
                     attempt = CASE WHEN step.state = '{STARTED}' THEN EXCLUDED.attempt
                         ELSE step.attempt END,
                     lease = CASE WHEN step.state = '{STARTED}' THEN EXCLUDED.lease
                         ELSE step.lease END
                 RETURNING step.state, step.nonce::text AS nonce, step.starts, step.output
             )
             SELECT EXISTS (SELECT 1 FROM fence) AS fenced, upserted.state, upserted.nonce,
                 upserted.starts, upserted.output
             FROM (VALUES (1)) AS answer LEFT JOIN upserted ON TRUE",
            job_control = self.schema.table("job_control"),
            job_step = self.schema.table("job_step"),
        )))
        .bind(self.control.job_id())
        .bind(self.control.lease())
        .bind(self.key)
        .bind(attempt)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(row)
    }

    async fn finish(&self, output: &Value) -> Result<(), JobError> {
        let deadline = Instant::now() + FINISH_WINDOW;
        let mut backoff = FINISH_BACKOFF;
        loop {
            match self.try_finish(output).await {
                Ok((true, 1)) => return Ok(()),
                Ok((false, _)) => return Err(self.control.lose()),
                Ok((true, _)) => {
                    return Err(JobError::new(format!(
                        "step {:?}: its record was cleared or finished elsewhere while it ran",
                        self.key
                    )));
                }
                Err(e) if unanswered(&e) && Instant::now() + backoff < deadline => {
                    if backoff == FINISH_BACKOFF {
                        warn!(error = %e, key = self.key, "the database did not answer the step's output, retrying");
                    } else {
                        debug!(error = %e, key = self.key, "the database did not answer the step's output, retrying");
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(FINISH_BACKOFF_LIMIT);
                }
                Err(e) => {
                    return Err(JobError::new(format!(
                        "step {:?}: could not record the output, it may have been recorded: {e}",
                        self.key
                    )));
                }
            }
        }
    }

    async fn try_finish(&self, output: &Value) -> crate::Result<(bool, i64)> {
        let mut transaction = begin_fenced(&self.pool).await?;
        let finished = sqlx::query_as(AssertSqlSafe(format!(
            "WITH fence AS (
                 SELECT job_id FROM {job_control} WHERE job_id = $1 AND lease = $2 FOR SHARE
             ),
             finished AS (
                 UPDATE {job_step} AS step
                 SET state = '{DONE}', output = $4, error = NULL,
                     finished_at = statement_timestamp()
                 FROM fence
                 WHERE step.job_id = fence.job_id AND step.key = $3 AND step.state = '{STARTED}'
                 RETURNING step.key
             )
             SELECT EXISTS (SELECT 1 FROM fence), (SELECT count(*) FROM finished)",
            job_control = self.schema.table("job_control"),
            job_step = self.schema.table("job_step"),
        )))
        .bind(self.control.job_id())
        .bind(self.control.lease())
        .bind(self.key)
        .bind(output)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(finished)
    }

    async fn record_error(&self, msg: &str) -> Result<(), JobError> {
        match self.try_record_error(capped(msg)).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(self.control.lose()),
            Err(e) => {
                warn!(error = %Error::from(e), key = self.key, "could not record the step's error");
                Ok(())
            }
        }
    }

    async fn try_record_error(&self, msg: &str) -> sqlx::Result<bool> {
        let mut transaction = begin_fenced(&self.pool).await?;
        let fenced = sqlx::query_scalar(AssertSqlSafe(format!(
            "WITH fence AS (
                 SELECT job_id FROM {job_control} WHERE job_id = $1 AND lease = $2 FOR SHARE
             ),
             recorded AS (
                 UPDATE {job_step} AS step
                 SET error = $4
                 FROM fence
                 WHERE step.job_id = fence.job_id AND step.key = $3 AND step.state = '{STARTED}'
                 RETURNING step.key
             )
             SELECT EXISTS (SELECT 1 FROM fence)",
            job_control = self.schema.table("job_control"),
            job_step = self.schema.table("job_step"),
        )))
        .bind(self.control.job_id())
        .bind(self.control.lease())
        .bind(self.key)
        .bind(msg)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(fenced)
    }

    fn failed(&self, action: &str, e: sqlx::Error) -> JobError {
        JobError::new(format!(
            "step {:?}: {action} the step: {}",
            self.key,
            Error::from(e)
        ))
    }

    fn decode(&self, e: sqlx::Error) -> JobError {
        JobError::new(format!("step {:?}: {}", self.key, Error::from(e)))
    }
}

pub(crate) async fn warn_unfinished(
    pool: &PgPool,
    schema: &Schema,
    control: &RunControl,
    attempt: i32,
) {
    let visited = control.steps().visited();
    if visited.is_empty() && attempt <= 1 {
        return;
    }
    let unfinished: sqlx::Result<Vec<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT key FROM {job_step}
         WHERE job_id = $1 AND state = '{STARTED}' AND NOT (key = ANY($2))
         ORDER BY key",
        job_step = schema.table("job_step"),
    )))
    .bind(control.job_id())
    .bind(&visited)
    .fetch_all(pool)
    .await;
    match unfinished {
        Ok(keys) if keys.is_empty() => {}
        Ok(keys) => warn!(
            steps = ?keys,
            "the job succeeded without finishing steps an earlier run started, so their effects \
             may or may not have happened"
        ),
        Err(e) => debug!(error = %Error::from(e), "could not look for unfinished steps"),
    }
}

fn encode<T: Serialize + DeserializeOwned>(output: &T) -> Result<(Value, T), String> {
    let value = serde_json::to_value(output)
        .map_err(|err| format!("the output cannot be serialized to JSON: {err}"))?;
    if holds_nul(&value) {
        return Err(
            "the output contains the character \\u0000, which PostgreSQL cannot store".to_owned(),
        );
    }
    let length = serde_json::to_vec(&value)
        .map_err(|err| format!("the output cannot be serialized to JSON: {err}"))?
        .len();
    if length > MAX_OUTPUT_LENGTH {
        return Err(format!(
            "the output is {length} bytes of JSON, above the limit of {MAX_OUTPUT_LENGTH}"
        ));
    }
    let decoded = serde_json::from_value(value.clone())
        .map_err(|err| format!("the output does not read back as the step's type: {err}"))?;
    Ok((value, decoded))
}

fn holds_nul(value: &Value) -> bool {
    match value {
        Value::String(text) => text.contains('\0'),
        Value::Array(items) => items.iter().any(holds_nul),
        Value::Object(fields) => fields
            .iter()
            .any(|(name, field)| name.contains('\0') || holds_nul(field)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn capped(text: &str) -> &str {
    let mut end = text.len().min(MAX_ERROR_LENGTH);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn unanswered(error: &Error) -> bool {
    matches!(
        error.kind,
        ErrorKind::ConnectionFailed
            | ErrorKind::ConnectionTerminated
            | ErrorKind::PoolTimedOut
            | ErrorKind::PoolClosed
            | ErrorKind::TlsFailed
            | ErrorKind::ProtocolViolation
    )
}

fn refused(key: &str, reason: &str) -> JobError {
    JobError::permanent(std::io::Error::other(format!("step {key:?} {reason}")))
}

fn permanent(msg: String) -> JobError {
    JobError::permanent(std::io::Error::other(msg))
}

/// Runs `f` once and records its output, so that later runs of the job return the recorded
/// output instead of running `f` again.
///
/// A step is identified by `key` within the job. The first call starts a record for it and runs
/// `f`; when `f` returns `Ok`, its output is stored and the record is done. Every later call with
/// the same key, in a retry, after a crash and takeover, or after a
/// [`restart`](crate::restart), returns the stored output without calling `f`. A record that was
/// started but never done, because `f` failed, the worker died or the job was taken over, makes
/// the next call run `f` again, so a step runs at least once and its effect may happen more than
/// once; [`StepRun::starts`] and [`StepRun::idempotency_key`] let `f` deal with that.
///
/// The output is stored as JSON, at most 1 MiB of it, and is checked before it is stored: it
/// must read back as `T`, so the run that executes `f` gets the output as it reads back, the
/// same value later runs get. When `f` returns an error, the error's text is kept with the
/// record and the error is returned unchanged, so [`JobError::discard`], [`snooze`](crate::snooze)
/// and interruptions work inside a step; the next call with the key runs `f` again.
///
/// Every write is fenced by the run's lease: a worker that lost the job cannot start or finish a
/// step, and its run is aborted at its next `.await` even when the error is ignored. Like
/// [`checkpoint`](crate::checkpoint), a step is a stop point: when the job was asked to stop, it
/// returns that error before starting.
///
/// # Errors
///
/// Returns an error if:
/// - It is called outside a running job's own task, for example in a task the job spawned (a
///   [`JobError::permanent`])
/// - `key` is empty or longer than 255 bytes (a [`JobError::permanent`])
/// - The same key is running in this run already, or already succeeded in it (a
///   [`JobError::permanent`])
/// - The job was asked to stop or to cancel, or its worker is shutting down
/// - Another worker has taken the job over (the job is aborted)
/// - The stored output does not deserialize into `T` (a [`JobError::permanent`])
/// - `f` returned an error, which is passed through unchanged
/// - The output of `f` cannot be serialized, is larger than 1 MiB of JSON, contains the
///   character `\u{0}`, which PostgreSQL cannot store, or does not deserialize back into `T`,
///   as a `NaN` float does not (a [`JobError::permanent`]; the record stays started)
/// - The database could not be reached or refused the write, for example after waiting 2
///   seconds for the lock that an open [`save_in`](crate::Checkpoint::save_in) transaction of
///   the same job holds; when the output could not be stored, the next call runs `f` again
///
/// # Examples
///
/// ```rust,no_run
/// use serde::{Deserialize, Serialize};
///
/// # async fn create_invoice(order_id: i64, key: &str) -> Result<String, std::io::Error> { Ok(String::new()) }
/// # async fn send_invoice(invoice_id: &str) -> Result<(), std::io::Error> { Ok(()) }
/// #[derive(Serialize, Deserialize)]
/// struct BillOrder {
///     order_id: i64,
/// }
///
/// #[jalari::job(name = "bill_order")]
/// impl jalari::Job for BillOrder {
///     async fn run(self) -> jalari::JobResult {
///         let invoice_id = jalari::step("create_invoice", |run| async move {
///             Ok(create_invoice(self.order_id, run.idempotency_key()).await?)
///         })
///         .await?;
///         jalari::step("send_invoice", |_| async move {
///             Ok(send_invoice(&invoice_id).await?)
///         })
///         .await
///     }
/// }
/// ```
pub async fn step<T, F, Fut>(key: &str, f: F) -> Result<T, JobError>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce(StepRun) -> Fut,
    Fut: Future<Output = Result<T, JobError>>,
{
    let Some(job) = current_job() else {
        return Err(permanent(format!(
            "step {key:?}: jalari::step works only in a running job's own task, not in tasks \
             it spawns"
        )));
    };
    if key.is_empty() || key.len() > MAX_KEY_LENGTH {
        return Err(permanent(format!(
            "step {key:?}: a key must be 1 to {MAX_KEY_LENGTH} bytes long"
        )));
    }
    let control = Arc::clone(job.control());
    if let Some(halt) = control.reach() {
        return Err(JobError::halted(halt));
    }
    let in_flight = control.steps().enter(key)?;
    let config = config().map_err(JobError::permanent)?;
    let record = StepRecord {
        pool: config.pool.pool(),
        schema: &config.schema,
        control: &control,
        key,
    };
    match record.start(job.attempt()).await? {
        Started::Recorded(output) => {
            let output = serde_json::from_value(output).map_err(|err| {
                permanent(format!(
                    "step {key:?}: the recorded output does not fit the step's type: {err}"
                ))
            })?;
            in_flight.done();
            Ok(output)
        }
        Started::Open(run) => match INSIDE.scope((), f(run)).await {
            Ok(output) => {
                let (value, output) = match encode(&output) {
                    Ok(encoded) => encoded,
                    Err(msg) => {
                        let msg = format!("step {key:?}: {msg}");
                        record.record_error(&msg).await?;
                        return Err(permanent(msg));
                    }
                };
                record.finish(&value).await?;
                in_flight.done();
                Ok(output)
            }
            Err(e) => {
                record.record_error(e.msg()).await?;
                Err(e)
            }
        },
    }
}
