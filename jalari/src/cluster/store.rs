use std::time::Duration;

use sqlx::{AssertSqlSafe, PgConnection};

use super::ClusterConfig;
use crate::storage::{CONFIG_CHANNEL, interval};
use crate::{Error, ErrorKind, Result, Schema};

#[derive(sqlx::FromRow)]
struct ConfigRow {
    housekeeping_enabled: bool,
    housekeeping_interval_ms: i64,
    succeeded_retention_ms: i64,
    deleted_retention_ms: i64,
    failed_retention_ms: Option<i64>,
    worker_timeout_ms: i64,
}

impl From<ConfigRow> for ClusterConfig {
    fn from(row: ConfigRow) -> Self {
        Self {
            housekeeping_enabled: row.housekeeping_enabled,
            housekeeping_interval: millis(row.housekeeping_interval_ms),
            succeeded_retention: millis(row.succeeded_retention_ms),
            deleted_retention: millis(row.deleted_retention_ms),
            failed_retention: row.failed_retention_ms.map(millis),
            worker_timeout: millis(row.worker_timeout_ms),
        }
    }
}

pub(crate) async fn load(connection: &mut PgConnection, schema: &Schema) -> Result<ClusterConfig> {
    select(connection, schema, "").await
}

pub(crate) async fn lock(connection: &mut PgConnection, schema: &Schema) -> Result<ClusterConfig> {
    select(connection, schema, "FOR UPDATE").await
}

pub(crate) async fn save(
    connection: &mut PgConnection,
    schema: &Schema,
    cluster: &ClusterConfig,
) -> Result<()> {
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {config}
         SET housekeeping_enabled = $1, housekeeping_interval = justify_hours($2),
             succeeded_retention = justify_hours($3), deleted_retention = justify_hours($4),
             failed_retention = justify_hours($5), worker_timeout = justify_hours($6),
             updated_at = now()",
        config = schema.table("config"),
    )))
    .bind(cluster.housekeeping_enabled)
    .bind(interval(cluster.housekeeping_interval))
    .bind(interval(cluster.succeeded_retention))
    .bind(interval(cluster.deleted_retention))
    .bind(cluster.failed_retention.map(interval))
    .bind(interval(cluster.worker_timeout))
    .execute(&mut *connection)
    .await?;
    sqlx::query("SELECT pg_notify($1, '')")
        .bind(schema.notify_channel(CONFIG_CHANNEL))
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn select(
    connection: &mut PgConnection,
    schema: &Schema,
    locking: &str,
) -> Result<ClusterConfig> {
    let row: Option<ConfigRow> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT housekeeping_enabled,
             {housekeeping_interval} AS housekeeping_interval_ms,
             {succeeded_retention} AS succeeded_retention_ms,
             {deleted_retention} AS deleted_retention_ms,
             {failed_retention} AS failed_retention_ms,
             {worker_timeout} AS worker_timeout_ms
         FROM {config}
         {locking}",
        housekeeping_interval = interval_ms("housekeeping_interval"),
        succeeded_retention = interval_ms("succeeded_retention"),
        deleted_retention = interval_ms("deleted_retention"),
        failed_retention = interval_ms("failed_retention"),
        worker_timeout = interval_ms("worker_timeout"),
        config = schema.table("config"),
    )))
    .fetch_optional(connection)
    .await?;
    let row = row.ok_or_else(|| {
        Error::new(
            ErrorKind::ConfigMissing,
            format!(
                "{} has no row; restore it with INSERT INTO {} (id) VALUES (TRUE)",
                schema.table("config"),
                schema.table("config")
            ),
        )
    })?;
    Ok(row.into())
}

fn interval_ms(column: &str) -> String {
    format!("(EXTRACT(EPOCH FROM {column}) * 1000)::bigint")
}

fn millis(value: i64) -> Duration {
    Duration::from_millis(u64::try_from(value).unwrap_or(0))
}
