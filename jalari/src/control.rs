use sqlx::{AssertSqlSafe, PgConnection};

use crate::init::config;
use crate::job::Command;
use crate::storage::JOB_CHANNEL;
use crate::{JobId, JobState, Result, Schema};

/// What [`stop`] did with the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopOutcome {
    /// The job is stopped: it was waiting and no worker will take it until [`restart`], or it
    /// was already stopped.
    Stopped,
    /// A worker is running the job, or ran it until it died less than about 8 seconds ago; the
    /// stop is recorded and applies when the worker sees it.
    ///
    /// A running job sees the request within about a second, stops at its next
    /// [`checkpoint`](crate::checkpoint) and is aborted after
    /// [`WorkerConfig::stop_timeout`](crate::WorkerConfig::stop_timeout). A job whose worker died
    /// is stopped by another worker without running again. A job that returns `Ok` first is
    /// recorded as succeeded. Call `stop` again to learn the result.
    Requested,
    /// The job had already succeeded, failed, been discarded or been cancelled.
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
    /// The job is stopped and its `expires_at` has passed, so it stays stopped; [`cancel`] it
    /// to free its `job_key`.
    Expired,
    /// No job has this id, or housekeeping already removed it.
    NotFound,
}

/// What [`cancel`] did with the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The waiting or stopped job is marked deleted, will never run and frees its `job_key`.
    Cancelled,
    /// A worker is running the job, or ran it until it died less than about 8 seconds ago; the
    /// cancel is recorded and applies when the worker sees it.
    ///
    /// A running job stops at its next [`checkpoint`](crate::checkpoint) or is aborted after
    /// [`WorkerConfig::stop_timeout`](crate::WorkerConfig::stop_timeout), and is then marked
    /// deleted. A job whose worker died is marked deleted by another worker without running
    /// again. A job that returns `Ok` first is recorded as succeeded. Call `cancel` again to
    /// learn the result.
    Requested,
    /// The job had already succeeded, failed, been discarded or been cancelled.
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
/// run time. A job stopped right after a worker aborted it at shutdown still waits the few
/// seconds the aborted run needs to stop. A job whose `expires_at` has passed stays stopped,
/// since it could never start again.
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
                 run_at = greatest(run_at, statement_timestamp()),
                 updated_at = statement_timestamp()
             WHERE id = $1 AND state = 'stopped'
                 AND (expires_at IS NULL OR expires_at > statement_timestamp())
             RETURNING id, state, attempts, queue
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, created_at)
             SELECT id, state, attempts, statement_timestamp() FROM restarted
         )
         SELECT queue FROM restarted",
        job = schema.table("job"),
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
        None => match job_state(&mut transaction, schema, id).await? {
            None => RestartOutcome::NotFound,
            Some((state, true)) if state == JobState::Stopped.as_str() => RestartOutcome::Expired,
            Some(_) => RestartOutcome::NotStopped,
        },
    };
    transaction.commit().await?;
    Ok(outcome)
}

async fn send(id: JobId, command: Command) -> Result<Answer> {
    let config = config()?;
    let schema = &config.schema;
    let pool = config.pool.pool();
    let sql = format!(
        "WITH changed AS (
             UPDATE {job} AS job
             SET command = CASE WHEN job.state = 'running'
                     THEN CASE WHEN job.command = 'cancel' THEN 'cancel' ELSE $2 END END,
                 state = CASE WHEN job.state = 'running' THEN 'running'
                     WHEN $2 = 'cancel' THEN 'deleted' ELSE 'stopped' END,
                 updated_at = CASE WHEN job.state = 'running' THEN job.updated_at
                     ELSE statement_timestamp() END
             WHERE job.id = $1
                 AND (job.state IN ('scheduled', 'enqueued', 'running')
                     OR (job.state = 'stopped' AND $2 = 'cancel'))
             RETURNING job.id, job.state, job.attempts
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, error, created_at)
             SELECT id, state, attempts, $3, statement_timestamp() FROM changed
             WHERE state <> 'running'
         ),
         cleared AS (
             UPDATE {job_control}
             SET lease = lease + 1,
                 progress = CASE WHEN $2 = 'cancel' THEN NULL ELSE progress END,
                 updated_at = statement_timestamp()
             WHERE job_id IN (SELECT id FROM changed WHERE state <> 'running')
         )
         SELECT (SELECT state FROM changed) AS changed,
             (SELECT state FROM {job} WHERE id = $1) AS current",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
        job_control = schema.table("job_control"),
    );
    loop {
        let (changed, current): (Option<String>, Option<String>) =
            sqlx::query_as(AssertSqlSafe(sql.as_str()))
                .bind(id.0)
                .bind(command.as_str())
                .bind(command.message())
                .fetch_one(&pool)
                .await?;
        let answer = match (changed.as_deref(), current.as_deref()) {
            (Some("running"), _) => Answer::Requested,
            (Some(_), _) => Answer::Applied,
            (None, None) => Answer::NotFound,
            (None, Some("stopped")) => Answer::Stopped,
            (None, Some(state)) if is_finished(state) => Answer::Finished,
            (None, Some(_)) => continue,
        };
        return Ok(answer);
    }
}

async fn job_state(
    connection: &mut PgConnection,
    schema: &Schema,
    id: JobId,
) -> Result<Option<(String, bool)>> {
    let state = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, coalesce(expires_at <= statement_timestamp(), FALSE)
         FROM {job} WHERE id = $1",
        job = schema.table("job"),
    )))
    .bind(id.0)
    .fetch_optional(connection)
    .await?;
    Ok(state)
}

fn is_finished(state: &str) -> bool {
    JobState::parse(state).is_ok_and(JobState::is_finished)
}

async fn notify(connection: &mut PgConnection, schema: &Schema, queue: &str) -> Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(schema.notify_channel(JOB_CHANNEL))
        .bind(queue)
        .execute(connection)
        .await?;
    Ok(())
}
