use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool, Row};

use super::checkpoint::RunControl;
use super::fence::begin_fenced;
use super::run::current_job;
use crate::init::config;
use crate::{Error, JobError, Schema};

const MAX_KEY_LENGTH: usize = 255;
const STARTED: &str = "started";
const DONE: &str = "done";

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
/// - The database could not be reached or refused the write
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
        Started::Open(run) => {
            let output = f(run).await?;
            in_flight.done();
            Ok(output)
        }
    }
}
