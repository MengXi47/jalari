use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{ErrorKind, SCHEMA_VERSION, Schema};

async fn test_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return None;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await
        .unwrap();
    Some(pool)
}

fn unique_schema_name() -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("jalari_test_{}_{nanos}_{count}", std::process::id())
}

fn schema_for(name: &str, prefix: Option<&str>) -> Schema {
    match prefix {
        Some(prefix) => Schema::prefixed(name, prefix).unwrap(),
        None => Schema::named(name).unwrap(),
    }
}

async fn create_schema(pool: &PgPool, name: &str) {
    sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
        .execute(pool)
        .await
        .unwrap();
}

async fn drop_schema(pool: &PgPool, name: &str) {
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS \"{name}\" CASCADE"
    )))
    .execute(pool)
    .await
    .unwrap();
}

async fn snapshot(pool: &PgPool, name: &str) -> Vec<String> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname || ':' || c.relkind::text
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1
         UNION ALL
         SELECT table_name || '.' || column_name || ':' || data_type || ':' || is_nullable
             || ':' || coalesce(column_default, '')
         FROM information_schema.columns
         WHERE table_schema = $1
         UNION ALL
         SELECT indexname || ':' || indexdef
         FROM pg_indexes
         WHERE schemaname = $1
         UNION ALL
         SELECT r.relname || '.' || con.conname || ':' || pg_get_constraintdef(con.oid)
         FROM pg_constraint con
         JOIN pg_class r ON r.oid = con.conrelid
         JOIN pg_namespace n ON n.oid = r.relnamespace
         WHERE n.nspname = $1
         ORDER BY 1",
    )
    .bind(name)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|row| row.replace(name, "<schema>"))
        .collect()
}

#[tokio::test]
async fn test_migrate_to_rejects_unknown_versions() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();

    for version in [0, SCHEMA_VERSION + 1] {
        let err = jalari::migrate_to(&pool, &schema, version)
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::UnknownSchemaVersion);
    }
    jalari::migrate_to(&pool, &schema, SCHEMA_VERSION)
        .await
        .unwrap();
    assert_eq!(
        jalari::schema_version(&pool, &schema).await.unwrap(),
        Some(SCHEMA_VERSION)
    );

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_migrate_creates_schema_and_records_version() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();

    assert_eq!(jalari::schema_version(&pool, &schema).await.unwrap(), None);
    jalari::migrate(&pool, &schema).await.unwrap();
    assert_eq!(
        jalari::schema_version(&pool, &schema).await.unwrap(),
        Some(SCHEMA_VERSION)
    );

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_migrate_is_idempotent() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();

    jalari::migrate(&pool, &schema).await.unwrap();
    let first = snapshot(&pool, &name).await;
    jalari::migrate(&pool, &schema).await.unwrap();
    let second = snapshot(&pool, &name).await;
    assert_eq!(first, second);

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_concurrent_migrate_succeeds() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();

    let (first, second) = tokio::join!(
        jalari::migrate(&pool, &schema),
        jalari::migrate(&pool, &schema)
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(
        jalari::schema_version(&pool, &schema).await.unwrap(),
        Some(SCHEMA_VERSION)
    );

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_exported_sql_matches_migrate() {
    let Some(pool) = test_pool().await else {
        return;
    };
    for prefix in [None, Some("jalari_")] {
        let migrated_name = unique_schema_name();
        let exported_name = unique_schema_name();
        let migrated = schema_for(&migrated_name, prefix);
        let exported = schema_for(&exported_name, prefix);

        jalari::migrate(&pool, &migrated).await.unwrap();
        create_schema(&pool, &exported_name).await;
        for migration in jalari::migrations(&exported) {
            sqlx::raw_sql(AssertSqlSafe(migration.sql))
                .execute(&pool)
                .await
                .unwrap();
        }

        assert_eq!(
            snapshot(&pool, &migrated_name).await,
            snapshot(&pool, &exported_name).await
        );
        assert_eq!(
            jalari::schema_version(&pool, &exported).await.unwrap(),
            Some(SCHEMA_VERSION)
        );

        drop_schema(&pool, &migrated_name).await;
        drop_schema(&pool, &exported_name).await;
    }
}

#[tokio::test]
async fn test_rerunning_exported_sql_keeps_version() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();

    jalari::migrate(&pool, &schema).await.unwrap();
    for migration in jalari::migrations(&schema) {
        sqlx::raw_sql(AssertSqlSafe(migration.sql))
            .execute(&pool)
            .await
            .unwrap();
    }
    assert_eq!(
        jalari::schema_version(&pool, &schema).await.unwrap(),
        Some(SCHEMA_VERSION)
    );

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_prefixed_schema_places_tables_in_existing_schema() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    create_schema(&pool, &name).await;
    let schema = Schema::prefixed(&name, "jalari_").unwrap();

    jalari::migrate(&pool, &schema).await.unwrap();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename::text FROM pg_tables WHERE schemaname = $1 ORDER BY 1",
    )
    .bind(&name)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        tables,
        [
            "jalari_config",
            "jalari_job",
            "jalari_job_history",
            "jalari_recurring",
            "jalari_schema_version",
            "jalari_worker",
        ]
    );

    drop_schema(&pool, &name).await;
}
