use super::JobFilter;
use crate::{Priority, Result};

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
#[expect(unused_variables, reason = "filled in by W4")]
pub async fn set_priority(filter: &JobFilter, priority: Priority) -> Result<u64> {
    todo!("W4")
}
