use sqlx::{AssertSqlSafe, PgPool};

use crate::{Result, Schema};

const EXPIRED: &str = "the job expired while waiting to run";
const EXPIRY_BATCH: i64 = 100;

pub(super) async fn discard_expired(pool: &PgPool, schema: &Schema) -> Result<u64> {
    let sql = format!(
        "WITH expired AS (
             SELECT id FROM {job}
             WHERE expires_at <= statement_timestamp() AND state IN ('scheduled', 'enqueued')
             ORDER BY id LIMIT $1
             FOR NO KEY UPDATE SKIP LOCKED
         ),
         settled AS (
             UPDATE {job} AS job
             SET state = 'discarded', last_error = $2, updated_at = statement_timestamp()
             FROM expired WHERE job.id = expired.id
             RETURNING job.id, job.attempts
         ),
         logged AS (
             INSERT INTO {job_history} (job_id, state, attempt, error, created_at)
             SELECT id, 'discarded', attempts, $2, statement_timestamp() FROM settled
         ),
         cleared AS (
             UPDATE {job_control}
             SET lease = lease + 1, progress = NULL, updated_at = statement_timestamp()
             WHERE job_id IN (SELECT id FROM settled)
         )
         SELECT count(*) FROM settled",
        job = schema.table("job"),
        job_history = schema.table("job_history"),
        job_control = schema.table("job_control"),
    );
    let mut discarded = 0;
    loop {
        let settled: i64 = sqlx::query_scalar(AssertSqlSafe(sql.as_str()))
            .bind(EXPIRY_BATCH)
            .bind(EXPIRED)
            .fetch_one(pool)
            .await?;
        discarded += settled.unsigned_abs();
        if settled < EXPIRY_BATCH {
            return Ok(discarded);
        }
    }
}
