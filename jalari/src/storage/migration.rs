use sqlx::{AssertSqlSafe, PgConnection, PgPool};

use crate::{Error, ErrorKind, Result, Schema};

include!(concat!(env!("OUT_DIR"), "/migrations.rs"));

/// Table structure version this release of jalari needs; [`init`](crate::init) checks it.
pub const SCHEMA_VERSION: i32 = TEMPLATES[TEMPLATES.len() - 1].0;

const TABLES: [&str; 6] = [
    "job",
    "job_history",
    "recurring",
    "worker",
    "config",
    "schema_version",
];

/// One version of the table structure as plain SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Migration {
    /// Version number, starting at 1.
    pub version: i32,
    /// SQL with the schema and prefix filled in; it ends by recording the version.
    pub sql: String,
}

/// Returns every migration as SQL for `schema`, oldest first, for use with your own migration
/// tool.
///
/// Each script can safely run twice and records its version in `schema_version`, so
/// [`init`](crate::init) accepts the result. The scripts do not create the schema itself;
/// create it first unless it already exists. Released scripts never change.
///
/// # Examples
///
/// ```rust
/// let schema = jalari::Schema::prefixed("public", "jalari_")?;
/// for migration in jalari::migrations(&schema) {
///     println!("-- version {}\n{}", migration.version, migration.sql);
/// }
/// # Ok::<(), jalari::Error>(())
/// ```
pub fn migrations(schema: &Schema) -> Vec<Migration> {
    TEMPLATES
        .iter()
        .map(|&(version, template)| Migration {
            version,
            sql: render(template, version, schema),
        })
        .collect()
}

/// Creates or upgrades the tables to [`SCHEMA_VERSION`], creating the schema if needed.
///
/// Runs in one transaction under an advisory lock, so concurrent calls from several processes
/// are safe and a failure leaves nothing half applied. Already applied versions are skipped.
///
/// # Errors
///
/// Returns an error if:
/// - The role lacks the privileges to create the schema or tables
///   ([`PermissionDenied`](ErrorKind::PermissionDenied))
/// - The database cannot be reached or a statement fails
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = jalari::sqlx::PgPool::connect("postgres://localhost/app").await?;
/// jalari::migrate(&pool, &jalari::Schema::default()).await?;
/// # Ok(())
/// # }
/// ```
pub async fn migrate(pool: &PgPool, schema: &Schema) -> Result<()> {
    migrate_to(pool, schema, SCHEMA_VERSION).await
}

/// Like [`migrate`], but stops at `version`.
///
/// Useful when the database must match an older release of the library. Versions already past
/// `version` are left as they are; nothing is rolled back.
///
/// # Errors
///
/// Returns an error if:
/// - `version` is outside `1..=SCHEMA_VERSION`
///   ([`UnknownSchemaVersion`](ErrorKind::UnknownSchemaVersion))
/// - Any condition listed for [`migrate`] occurs
pub async fn migrate_to(pool: &PgPool, schema: &Schema, version: i32) -> Result<()> {
    if !(1..=SCHEMA_VERSION).contains(&version) {
        return Err(Error::new(
            ErrorKind::UnknownSchemaVersion,
            format!("version {version} is not between 1 and {SCHEMA_VERSION}"),
        ));
    }
    let mut transaction = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(schema.lock_key("migrate"))
        .execute(&mut *transaction)
        .await?;

    let schema_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(schema.name())
            .fetch_one(&mut *transaction)
            .await?;
    if !schema_exists {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            schema.quoted_name()
        )))
        .execute(&mut *transaction)
        .await?;
    }

    let current = current_version(&mut transaction, schema)
        .await?
        .unwrap_or(0);
    for migration in migrations(schema) {
        if migration.version > current && migration.version <= version {
            sqlx::raw_sql(AssertSqlSafe(migration.sql))
                .execute(&mut *transaction)
                .await?;
        }
    }

    transaction.commit().await?;
    Ok(())
}

/// Returns the installed table structure version, or `None` when the tables do not exist.
///
/// # Errors
///
/// Returns an error if the database cannot be reached or the query fails.
pub async fn schema_version(pool: &PgPool, schema: &Schema) -> Result<Option<i32>> {
    let mut connection = pool.acquire().await?;
    current_version(&mut connection, schema).await
}

async fn current_version(connection: &mut PgConnection, schema: &Schema) -> Result<Option<i32>> {
    let table = schema.table("schema_version");
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(&table)
        .fetch_one(&mut *connection)
        .await?;
    if !exists {
        return Ok(None);
    }
    let version = sqlx::query_scalar(AssertSqlSafe(format!("SELECT version FROM {table}")))
        .fetch_optional(&mut *connection)
        .await?;
    Ok(version)
}

fn render(template: &str, version: i32, schema: &Schema) -> String {
    let mut sql = template
        .replace("{prefix}", schema.prefix())
        .replace("{version}", &version.to_string());
    for table in TABLES {
        sql = sql.replace(&format!("{{{table}}}"), &schema.table(table));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schemas() -> [Schema; 3] {
        [
            Schema::default(),
            Schema::prefixed("public", "jalari_").unwrap(),
            Schema::prefixed(&"s".repeat(63), &"p".repeat(37)).unwrap(),
        ]
    }

    #[test]
    fn test_versions_are_sequential_and_end_at_schema_version() {
        let versions: Vec<i32> = TEMPLATES.iter().map(|&(version, _)| version).collect();
        let expected: Vec<i32> = (1..=SCHEMA_VERSION).collect();
        assert_eq!(versions, expected);
    }

    #[test]
    fn test_rendered_sql_has_no_placeholders() {
        for schema in schemas() {
            for migration in migrations(&schema) {
                assert!(!migration.sql.contains(['{', '}']), "{}", migration.sql);
            }
        }
    }

    #[test]
    fn test_rendered_identifiers_fit_postgres_limit() {
        for schema in schemas() {
            for migration in migrations(&schema) {
                for identifier in migration.sql.split('"').skip(1).step_by(2) {
                    assert!(identifier.len() <= 63, "{identifier}");
                }
            }
        }
    }

    #[test]
    fn test_each_migration_ends_by_recording_its_version() {
        let schema = Schema::default();
        for migration in migrations(&schema) {
            let last_statement = migration.sql.trim_end().rsplit(';').nth(1).unwrap();
            assert!(last_statement.contains(&schema.table("schema_version")));
            assert!(last_statement.contains(&format!("VALUES (TRUE, {})", migration.version)));
        }
    }

    #[test]
    fn test_migrations_never_create_the_schema() {
        for migration in migrations(&Schema::default()) {
            assert!(!migration.sql.to_uppercase().contains("CREATE SCHEMA"));
        }
    }
}
