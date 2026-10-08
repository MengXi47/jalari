use std::time::Duration;

use jalari::sqlx::postgres::PgPoolOptions;
use jalari::sqlx::{self, PgPool};
use jalari::{Error, ErrorKind};

async fn test_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return None;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await
        .unwrap();
    Some(pool)
}

#[tokio::test]
async fn test_syntax_error_maps_to_query_failed_with_sqlstate() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let error = Error::from(sqlx::query("SELEC 1").execute(&pool).await.unwrap_err());
    assert_eq!(error.kind, ErrorKind::QueryFailed);
    assert!(error.msg.starts_with("42601: "), "{}", error.msg);
}

#[tokio::test]
async fn test_statement_timeout_maps_to_query_canceled() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout = '10ms'")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let source = sqlx::query("SELECT pg_sleep(1)")
        .execute(&mut *transaction)
        .await
        .unwrap_err();
    assert_eq!(Error::from(source).kind, ErrorKind::QueryCanceled);
}

#[tokio::test]
async fn test_terminated_backend_maps_to_connection_terminated() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let mut connection = pool.acquire().await.unwrap();
    let source = sqlx::query("SELECT pg_terminate_backend(pg_backend_pid())")
        .execute(&mut *connection)
        .await
        .unwrap_err();
    assert_eq!(Error::from(source).kind, ErrorKind::ConnectionTerminated);
}
