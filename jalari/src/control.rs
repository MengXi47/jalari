use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::job::Command;
use crate::storage::JOB_CHANNEL;
use crate::worker::{Closing, INTERRUPTED, TAKEOVER_DELAY, close};
use crate::{JobId, Result, Schema};

/// What [`stop`] did with the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopOutcome {
    /// The job is stopped: it was waiting and no worker will take it until [`restart`], or it
    /// was already stopped.
    Stopped,
    /// A worker holds the job, or its last run crashed less than 6 seconds ago; the stop is
    /// recorded and applies when the worker sees it.
    ///
    /// A running job sees the request within about a second, stops at its next
    /// [`checkpoint`](crate::checkpoint) and is aborted after
    /// [`WorkerConfig::stop_timeout`](crate::WorkerConfig::stop_timeout). A job that returns `Ok`
    /// first is recorded as succeeded. Call `stop` again to learn the result.
    Requested,
    /// The job had already succeeded, failed or been cancelled.
    Finished,
    /// No job has this id, or housekeeping already removed it.
    NotFound,
}

/// What [`restart`] did with the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartOutcome {
    /// The stopped job is waiting again and continues from its saved progress.
    Restarted,
    /// The job is not stopped: it is waiting, running or finished, or its stop is still only
    /// [`Requested`](StopOutcome::Requested).
    NotStopped,
    /// No job has this id, or housekeeping already removed it.
    NotFound,
}

/// What [`cancel`] did with the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The waiting or stopped job is marked deleted, will never run and frees its `job_key`.
    Cancelled,
    /// A worker holds the job, or its last run crashed less than 6 seconds ago; the cancel is
    /// recorded and applies when the worker sees it.
    ///
    /// A running job stops at its next [`checkpoint`](crate::checkpoint) or is aborted after
    /// [`WorkerConfig::stop_timeout`](crate::WorkerConfig::stop_timeout), and is then marked
    /// deleted. A job that returns `Ok` first is recorded as succeeded. Call `cancel` again to
    /// learn the result.
    Requested,
    /// The job had already succeeded, failed or been cancelled.
    Finished,
    /// No job has this id, or housekeeping already removed it.
    NotFound,
}

enum Answer {
    Applied,
    Stopped,
    Requested,
    Finished,
    NotFound,
}

/// Stops a job so it releases everything it holds, keeping it for a later [`restart`].
///
/// A waiting job is stopped at once. A running job is asked to stop; see
/// [`StopOutcome::Requested`]. Either way the attempt it was making does not count toward
/// [`Job::MAX_ATTEMPTS`](crate::Job::MAX_ATTEMPTS). A stopped job keeps its `job_key`, so no
/// second job with that key can be enqueued and a recurring schedule skips its runs, but it
/// releases its `queue_key`, so other jobs with that key may run. Housekeeping never removes
/// stopped jobs.
///
/// Calling it again is safe and reports the current outcome. A pending [`cancel`] wins over a
/// stop.
///
/// # Errors
///
/// Returns an error if:
/// - [`init`](crate::init) has not completed
///   ([`NotInitialized`](crate::ErrorKind::NotInitialized))
/// - The database query fails
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example(id: jalari::JobId) -> jalari::Result<()> {
/// use jalari::StopOutcome;
///
/// loop {
///     match jalari::stop(id).await? {
///         StopOutcome::Requested => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
///         StopOutcome::Stopped => break println!("job {id} is stopped"),
///         StopOutcome::Finished | StopOutcome::NotFound => break,
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub async fn stop(id: JobId) -> Result<StopOutcome> {
    let answer = send(id, Command::Stop).await?;
    Ok(match answer {
        Answer::Applied | Answer::Stopped => StopOutcome::Stopped,
        Answer::Requested => StopOutcome::Requested,
        Answer::Finished => StopOutcome::Finished,
        Answer::NotFound => StopOutcome::NotFound,
    })
}

/// Removes a waiting, stopped or running job for good.
///
/// A waiting or stopped job is marked deleted at once, which frees its `job_key`; housekeeping
/// removes it after the cluster's deleted retention. A running job is asked to stop and is then
/// marked deleted; see [`CancelOutcome::Requested`].
///
/// # Errors
///
/// Returns an error under the same conditions as [`stop`].
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example(id: jalari::JobId) -> jalari::Result<()> {
/// if jalari::cancel(id).await? == jalari::CancelOutcome::Cancelled {
///     println!("job {id} will never run");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn cancel(id: JobId) -> Result<CancelOutcome> {
    let answer = send(id, Command::Cancel).await?;
    Ok(match answer {
        Answer::Applied => CancelOutcome::Cancelled,
        Answer::Stopped | Answer::Requested => CancelOutcome::Requested,
        Answer::Finished => CancelOutcome::Finished,
        Answer::NotFound => CancelOutcome::NotFound,
    })
}

/// Puts a stopped job back in its queue; it continues from its saved progress.
///
/// Its attempt count is what it was before the stop, and a job scheduled for later keeps its
/// run time.
///
/// # Errors
///
/// Returns an error under the same conditions as [`stop`].
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example(id: jalari::JobId) -> jalari::Result<()> {
/// if jalari::restart(id).await? == jalari::RestartOutcome::Restarted {
///     println!("job {id} will continue where it stopped");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn restart(id: JobId) -> Result<RestartOutcome> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let queue: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "WITH restarted AS (
             UPDATE {job}
             SET state = CASE WHEN run_at > statement_timestamp() THEN 'scheduled'
                     ELSE 'enqueued' END,
                 updated_at = statement_timestamp()
             WHERE id = $1 AND state = 'stopped'
             RETURNING id, state, attempts, queue
         ),
         cleared AS (
             UPDATE {job_control} SET command = NULL, updated_at = statement_timestamp()
             WHERE job_id IN (SELECT id FROM restarted) AND command = 'stop'
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, created_at)
             SELECT id, state, attempts, statement_timestamp() FROM restarted
         )
         SELECT queue FROM restarted",
        job = schema.table("job"),
        job_control = schema.table("job_control"),
        job_history = schema.table("job_history"),
    )))
    .bind(id.0)
    .fetch_optional(&mut *transaction)
    .await?;
    let outcome = match queue {
        Some(queue) => {
            notify(&mut transaction, schema, &queue).await?;
            RestartOutcome::Restarted
        }
        None if job_state(&mut transaction, schema, id).await?.is_some() => {
            RestartOutcome::NotStopped
        }
        None => RestartOutcome::NotFound,
    };
    transaction.commit().await?;
    Ok(outcome)
}

async fn send(id: JobId, command: Command) -> Result<Answer> {
    let config = config()?;
    let schema = &config.schema;
    let mut transaction = config.pool.pool().begin().await?;
    let answer = match job_state(&mut transaction, schema, id).await? {
        None => Answer::NotFound,
        Some((state, _)) if is_finished(&state) => Answer::Finished,
        Some((state, _)) if state == "stopped" && command == Command::Stop => Answer::Stopped,
        Some(_) => apply(&mut transaction, schema, id, command).await?,
    };
    transaction.commit().await?;
    Ok(answer)
}

async fn apply(
    connection: &mut PgConnection,
    schema: &Schema,
    id: JobId,
    command: Command,
) -> Result<Answer> {
    let pending: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO {job_control} AS control (job_id, command) VALUES ($1, $2)
         ON CONFLICT (job_id) DO UPDATE
         SET command = CASE WHEN control.command = 'cancel' THEN 'cancel'
                 ELSE EXCLUDED.command END,
             updated_at = statement_timestamp()
         RETURNING command",
        job_control = schema.table("job_control"),
    )))
    .bind(id.0)
    .bind(command.as_str())
    .fetch_one(&mut *connection)
    .await?;
    let pending = Command::parse(&pending).unwrap_or(command);

    let idle: Option<i32> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT attempts FROM {job} AS job
         WHERE id = $1 AND ({idle} OR (job.state = 'stopped' AND $2))
         FOR UPDATE SKIP LOCKED",
        job = schema.table("job"),
        idle = schema.idle_job("job", "$3", "$4"),
    )))
    .bind(id.0)
    .bind(pending == Command::Cancel)
    .bind(INTERRUPTED)
    .bind(TAKEOVER_DELAY)
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(attempts) = idle {
        close(connection, schema, &Closing::idle(id.0, attempts, pending)).await?;
        return Ok(if pending == command {
            Answer::Applied
        } else {
            Answer::Finished
        });
    }

    let answer = match job_state(connection, schema, id).await? {
        Some((state, queue)) if state == "scheduled" || state == "enqueued" => {
            notify(connection, schema, &queue).await?;
            return Ok(Answer::Requested);
        }
        Some((state, _)) if state == "stopped" => match command {
            Command::Stop => Answer::Stopped,
            Command::Cancel => return Ok(Answer::Requested),
        },
        Some(_) => Answer::Finished,
        None => Answer::NotFound,
    };
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {job_control} SET command = NULL, updated_at = statement_timestamp()
         WHERE job_id = $1 AND command = $2",
        job_control = schema.table("job_control"),
    )))
    .bind(id.0)
    .bind(command.as_str())
    .execute(connection)
    .await?;
    Ok(answer)
}

async fn job_state(
    connection: &mut PgConnection,
    schema: &Schema,
    id: JobId,
) -> Result<Option<(String, String)>> {
    let state = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, queue FROM {job} WHERE id = $1",
        job = schema.table("job"),
    )))
    .bind(id.0)
    .fetch_optional(connection)
    .await?;
    Ok(state)
}

fn is_finished(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "deleted")
}

async fn notify(connection: &mut PgConnection, schema: &Schema, queue: &str) -> Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(schema.notify_channel(JOB_CHANNEL))
        .bind(queue)
        .execute(connection)
        .await?;
    Ok(())
}
