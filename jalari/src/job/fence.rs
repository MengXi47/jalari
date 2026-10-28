use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool, PgTransaction, Row};

use super::checkpoint::RunControl;
use crate::Schema;

const BEGIN_FENCED: &str = "BEGIN; SET LOCAL lock_timeout = '2s'";
const RAISED: &str = "22P02";
const RAISED_PREFIX: &str = "jalari: job ";
const LOST_RAISE: &str = " is no longer owned by this run";
const MISSING_RAISE: &str = " has no control row";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fenced {
    Written,
    Lost,
    Missing,
}

impl Fenced {
    fn from_row(written: bool, present: bool) -> Self {
        match (written, present) {
            (true, _) => Self::Written,
            (false, true) => Self::Lost,
            (false, false) => Self::Missing,
        }
    }
}

pub(crate) async fn begin_fenced(pool: &PgPool) -> sqlx::Result<PgTransaction<'static>> {
    pool.begin_with(BEGIN_FENCED).await
}

pub(crate) async fn write_progress(
    pool: &PgPool,
    schema: &Schema,
    control: &RunControl,
    value: &Value,
    seq: i64,
) -> sqlx::Result<Fenced> {
    let job_control = schema.table("job_control");
    let mut transaction = begin_fenced(pool).await?;
    let row = sqlx::query(AssertSqlSafe(format!(
        "WITH written AS (
             UPDATE {job_control} AS control
             SET progress = CASE WHEN $4 > control.progress_seq THEN $3 ELSE control.progress END,
                 progress_seq = greatest(control.progress_seq, $4),
                 updated_at = statement_timestamp()
             WHERE control.job_id = $1 AND control.lease = $2
             RETURNING control.job_id
         )
         SELECT EXISTS (SELECT 1 FROM written) AS written,
             EXISTS (SELECT 1 FROM {job_control} WHERE job_id = $1) AS present"
    )))
    .bind(control.job_id())
    .bind(control.lease())
    .bind(value)
    .bind(seq)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(Fenced::from_row(
        row.try_get("written")?,
        row.try_get("present")?,
    ))
}

pub(crate) async fn write_progress_in(
    transaction: &mut PgTransaction<'_>,
    schema: &Schema,
    control: &RunControl,
    value: &Value,
    seq: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "SELECT set_config('idle_in_transaction_session_timeout', '10s', true)
         FROM pg_settings
         WHERE name = 'idle_in_transaction_session_timeout'
             AND (setting::bigint = 0 OR setting::bigint > 10000)",
    )
    .execute(&mut **transaction)
    .await?;
    sqlx::query(AssertSqlSafe(format!(
        "WITH written AS (
             UPDATE {job_control} AS control
             SET lease = CASE WHEN control.lease = $2 THEN control.lease
                     ELSE ('{RAISED_PREFIX}' || control.job_id || '{LOST_RAISE}')::bigint END,
                 progress = CASE WHEN $4 > control.progress_seq THEN $3 ELSE control.progress END,
                 progress_seq = greatest(control.progress_seq, $4),
                 updated_at = statement_timestamp()
             WHERE control.job_id = $1
             RETURNING control.job_id
         )
         SELECT CASE WHEN count(*) = 1 THEN count(*)
             ELSE ('{RAISED_PREFIX}' || $1::text || '{MISSING_RAISE}, found ' || count(*))::bigint
             END
         FROM written",
        job_control = schema.table("job_control"),
    )))
    .bind(control.job_id())
    .bind(control.lease())
    .bind(value)
    .bind(seq)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) fn raised(e: &sqlx::Error) -> Option<Fenced> {
    let database_error = e.as_database_error()?;
    if database_error.code().as_deref() != Some(RAISED) {
        return None;
    }
    let message = database_error.message();
    let raised = &message[message.find(RAISED_PREFIX)?..];
    if raised.contains(LOST_RAISE) {
        Some(Fenced::Lost)
    } else if raised.contains(MISSING_RAISE) {
        Some(Fenced::Missing)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_only_a_missing_row_is_missing() {
        assert_eq!(Fenced::from_row(true, true), Fenced::Written);
        assert_eq!(Fenced::from_row(true, false), Fenced::Written);
        assert_eq!(Fenced::from_row(false, true), Fenced::Lost);
        assert_eq!(Fenced::from_row(false, false), Fenced::Missing);
    }
}
