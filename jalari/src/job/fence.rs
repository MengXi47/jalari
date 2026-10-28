use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool, PgTransaction, Row};

use super::checkpoint::RunControl;
use crate::Schema;

const BEGIN_FENCED: &str = "BEGIN; SET LOCAL lock_timeout = '2s'";

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
