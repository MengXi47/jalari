use super::ClusterConfig;
use super::store::{load, lock, save};
use crate::Result;
use crate::init;

/// Reads the current cluster settings.
///
/// # Errors
///
/// Returns an error if:
/// - The `config` row was deleted ([`ConfigMissing`](crate::ErrorKind::ConfigMissing))
/// - [`init`](crate::init) has not completed or the database query fails
pub async fn get() -> Result<ClusterConfig> {
    let config = init::config()?;
    let mut connection = config.pool.pool().acquire().await?;
    load(&mut connection, &config.schema).await
}

/// Changes the cluster settings and returns the saved values.
///
/// `change` runs on the current settings while the row is locked, so concurrent updates never
/// overwrite each other's fields. Running workers are notified and apply the change right away.
/// Durations are kept to the millisecond, so the returned values are the ones read back.
///
/// # Errors
///
/// Returns an error if:
/// - A value is out of range ([`InvalidConfigValue`](crate::ErrorKind::InvalidConfigValue));
///   nothing is saved
/// - The `config` row was deleted ([`ConfigMissing`](crate::ErrorKind::ConfigMissing))
/// - [`init`](crate::init) has not completed or the database query fails
///
/// # Examples
///
/// ```rust,no_run
/// use std::time::Duration;
///
/// # async fn example() -> jalari::Result<()> {
/// jalari::cluster::update(|cluster| {
///     cluster.housekeeping_enabled = true;
///     cluster.failed_retention = Some(Duration::from_secs(30 * 24 * 60 * 60));
/// })
/// .await?;
/// # Ok(())
/// # }
/// ```
pub async fn update<F: FnOnce(&mut ClusterConfig)>(change: F) -> Result<ClusterConfig> {
    let config = init::config()?;
    let mut transaction = config.pool.pool().begin().await?;
    let mut cluster = lock(&mut transaction, &config.schema).await?;
    change(&mut cluster);
    cluster.validate()?;
    save(&mut transaction, &config.schema, &cluster).await?;
    let saved = load(&mut transaction, &config.schema).await?;
    transaction.commit().await?;
    Ok(saved)
}
