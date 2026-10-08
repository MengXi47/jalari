#![cfg(feature = "cli")]

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use jalari::sqlx::{self, AssertSqlSafe, PgPool};
use jalari::{SCHEMA_VERSION, Schema};

fn unique_name() -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("jalari_test_cli_{}_{nanos}_{count}", std::process::id())
}

fn jalari(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_jalari"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn database_url() -> Option<String> {
    let url = std::env::var("DATABASE_URL").ok();
    if url.is_none() {
        eprintln!("DATABASE_URL is not set, skipping");
    }
    url
}

async fn drop_schema(pool: &PgPool, name: &str) {
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS \"{name}\" CASCADE"
    )))
    .execute(pool)
    .await
    .unwrap();
}

async fn schema_exists(pool: &PgPool, name: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[test]
fn test_sql_prints_the_default_migration() {
    let expected = jalari::migrations(&Schema::default()).pop().unwrap().sql;
    let printed = stdout(&jalari(&["sql", "--from", &SCHEMA_VERSION.to_string()]));
    assert_eq!(printed, expected);
}

#[test]
fn test_sql_writes_one_file_per_version() {
    let out_dir = std::env::temp_dir().join(unique_name());
    let schema = Schema::prefixed("app", "jalari_").unwrap();

    stdout(&jalari(&[
        "sql",
        "--schema",
        "app",
        "--prefix",
        "jalari_",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]));
    for migration in jalari::migrations(&schema) {
        let path: PathBuf = out_dir.join(format!("{:04}.sql", migration.version));
        assert_eq!(fs::read_to_string(path).unwrap(), migration.sql);
    }

    fs::remove_dir_all(out_dir).unwrap();
}

#[test]
fn test_sql_rejects_invalid_arguments() {
    let cases: [&[&str]; 4] = [
        &["sql", "--from", "0"],
        &["sql", "--to", &(SCHEMA_VERSION + 1).to_string()],
        &["sql", "--schema", "Bad-Name"],
        &["sql", "--schema", "pg_jalari"],
    ];
    for args in cases {
        let output = jalari(args);
        assert!(!output.status.success(), "{args:?}");
        assert!(!output.stderr.is_empty(), "{args:?}");
    }
}

#[tokio::test]
async fn test_migrate_and_status() {
    let Some(url) = database_url() else {
        return;
    };
    let pool = PgPool::connect(&url).await.unwrap();
    let name = unique_name();
    let schema = Schema::named(&name).unwrap();
    let common = ["--database-url", url.as_str(), "--schema", name.as_str()];
    let with = |command: &str, extra: &[&str]| {
        let mut args = vec![command];
        args.extend(common);
        args.extend(extra);
        jalari(&args)
    };

    let status = stdout(&with("status", &[]));
    assert!(status.contains("not installed"), "{status}");

    let dry_run = stdout(&with("migrate", &["--dry-run"]));
    assert!(dry_run.starts_with(&format!("CREATE SCHEMA \"{name}\";")));
    assert!(dry_run.contains(&jalari::migrations(&schema)[0].sql));
    assert!(!schema_exists(&pool, &name).await);

    let migrated = stdout(&with("migrate", &[]));
    assert!(migrated.contains("migrated"), "{migrated}");
    assert_eq!(
        jalari::schema_version(&pool, &schema).await.unwrap(),
        Some(SCHEMA_VERSION)
    );

    let again = stdout(&with("migrate", &[]));
    assert!(again.contains("nothing to apply"), "{again}");
    let status = stdout(&with("status", &[]));
    assert!(status.contains("up to date"), "{status}");

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_migrate_reads_database_url_from_env() {
    let Some(url) = database_url() else {
        return;
    };
    let pool = PgPool::connect(&url).await.unwrap();
    let name = unique_name();

    let output = Command::new(env!("CARGO_BIN_EXE_jalari"))
        .args(["migrate", "--schema", &name])
        .env("DATABASE_URL", &url)
        .output()
        .unwrap();
    stdout(&output);
    assert!(schema_exists(&pool, &name).await);

    drop_schema(&pool, &name).await;
}

#[test]
fn test_unreachable_database_fails_with_message() {
    let output = jalari(&[
        "status",
        "--database-url",
        "postgres://jalari@127.0.0.1:1/none",
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ConnectionFailed"), "{stderr}");
}
