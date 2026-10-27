use sqlx::AssertSqlSafe;

use super::JobFilter;
use super::jobs::{FILTER_SQL, bind_filter};
use crate::init::config;
use crate::{Priority, Result};

const BATCH_SIZE: i64 = 100;

/// Sets the priority of the waiting and stopped jobs matching `filter` and returns how many
/// changed.
///
/// Only `scheduled`, `enqueued` and `stopped` jobs change; states in the filter narrow that set
/// further. Running and finished jobs keep their priority. The jobs are changed in batches of
/// 100 that each commit on their own, and `updated_at` is left alone. This is the only safe way
/// to move a backlog, because a raw `UPDATE` can write a value outside the four levels.
///
/// # Errors
///
/// Returns an error if [`init`](crate::init) has not completed or a database query fails.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> jalari::Result<()> {
/// let tenant = jalari::admin::JobFilter::new()
///     .context(serde_json::json!({ "tenant_id": "acme" }));
/// let changed = jalari::admin::set_priority(&tenant, jalari::Priority::High).await?;
/// println!("{changed} jobs of acme now run first");
/// # Ok(())
/// # }
/// ```
pub async fn set_priority(filter: &JobFilter, priority: Priority) -> Result<u64> {
    let config = config()?;
    let pool = config.pool.pool();
    let mut changed = 0_u64;
    let mut after = 0_i64;
    loop {
        let query = sqlx::query_as(AssertSqlSafe(format!(
            "WITH batch AS (
                 SELECT id FROM {job}
                 WHERE {FILTER_SQL} AND state IN ('scheduled', 'enqueued', 'stopped')
                     AND priority <> $12 AND id > $13
                 ORDER BY id
                 LIMIT $14
             ),
             target AS (
                 SELECT job.id FROM {job} AS job
                 WHERE job.id IN (SELECT id FROM batch)
                     AND job.state IN ('scheduled', 'enqueued', 'stopped')
                 ORDER BY job.id
                 FOR NO KEY UPDATE
             ),
             changed AS (
                 UPDATE {job} AS job SET priority = $12 FROM target
                 WHERE job.id = target.id
                 RETURNING job.id
             )
             SELECT (SELECT max(id) FROM batch), (SELECT count(*) FROM changed)",
            job = config.schema.table("job"),
        )));
        let (last, batch): (Option<i64>, i64) = bind_filter!(query, filter)
            .bind(priority.level())
            .bind(after)
            .bind(BATCH_SIZE)
            .fetch_one(&pool)
            .await?;
        changed += batch.cast_unsigned();
        match last {
            Some(last) => after = last,
            None => return Ok(changed),
        }
    }
}
