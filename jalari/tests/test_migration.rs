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
async fn test_version_7_adds_the_fingerprint_columns() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    let columns = [
        "job.fingerprint:text:YES:".to_owned(),
        "worker.tasks:jsonb:YES:".to_owned(),
    ];

    jalari::migrate_to(&pool, &schema, 6).await.unwrap();
    let before = snapshot(&pool, &name).await;
    assert!(columns.iter().all(|column| !before.contains(column)));
    jalari::migrate_to(&pool, &schema, 7).await.unwrap();
    let after = snapshot(&pool, &name).await;
    assert!(columns.iter().all(|column| after.contains(column)));
    assert_eq!(
        jalari::schema_version(&pool, &schema).await.unwrap(),
        Some(7)
    );

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_version_8_adds_the_lock_columns_and_constraints() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    let columns = [
        "job.locked_by:uuid:YES:",
        "job.locked_at:timestamp with time zone:YES:",
        "job.lock_token:bigint:YES:",
        "job.queue_key_slot:smallint:YES:",
        "job.command:text:YES:",
    ];

    jalari::migrate_to(&pool, &schema, 7).await.unwrap();
    let before = snapshot(&pool, &name).await;
    assert!(
        columns
            .iter()
            .all(|column| !before.iter().any(|row| row == column))
    );
    jalari::migrate_to(&pool, &schema, 8).await.unwrap();
    let after = snapshot(&pool, &name).await;
    for column in columns {
        assert!(after.iter().any(|row| row == column), "{column}");
    }
    for index in [
        "job_job_key_active_idx",
        "job_queue_key_running_idx",
        "job_running_idx",
    ] {
        assert!(
            after
                .iter()
                .any(|row| row.starts_with(&format!("{index}:CREATE"))),
            "{index}"
        );
    }
    assert!(
        !after
            .iter()
            .any(|row| row.starts_with("job_job_key_held_idx:"))
    );
    for constraint in ["job_state_check", "job_command_check", "job_running_check"] {
        let definition = after
            .iter()
            .find(|row| row.starts_with(&format!("job.{constraint}:")))
            .unwrap_or_else(|| panic!("{constraint} is missing"));
        assert!(definition.ends_with("NOT VALID"), "{definition}");
    }

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO \"{name}\".job (task, payload, state, max_attempts)
         VALUES ('shape', '\\x00', 'enqueued', 1)"
    )))
    .execute(&pool)
    .await
    .unwrap();
    let ownerless = sqlx::query(AssertSqlSafe(format!(
        "UPDATE \"{name}\".job SET state = 'running'"
    )))
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        ownerless.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );

    drop_schema(&pool, &name).await;
}

async fn insert_legacy_job(pool: &PgPool, name: &str, attempts: i32, max_attempts: i32) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO \"{name}\".job (task, payload, state, attempts, max_attempts, updated_at)
         VALUES ('legacy', '\\x00', 'enqueued', $1, $2, now() - interval '1 minute')
         RETURNING id"
    )))
    .bind(attempts)
    .bind(max_attempts)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn legacy_shape(pool: &PgPool, name: &str, id: i64) -> (String, i32, Vec<(String, i32)>) {
    let (state, attempts): (String, i32) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts FROM \"{name}\".job WHERE id = $1"
    )))
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap();
    let history: Vec<(String, i32)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempt FROM \"{name}\".job_history
         WHERE job_id = $1 AND error = 'the worker stopped during the last attempt'
         ORDER BY id"
    )))
    .bind(id)
    .fetch_all(pool)
    .await
    .unwrap();
    (state, attempts, history)
}

#[tokio::test]
async fn test_version_8_records_attempts_a_0_6_worker_left_unfinished() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    jalari::migrate_to(&pool, &schema, 7).await.unwrap();
    let retryable = insert_legacy_job(&pool, &name, 1, 3).await;
    let exhausted = insert_legacy_job(&pool, &name, 3, 3).await;
    let finished = insert_legacy_job(&pool, &name, 1, 3).await;
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO \"{name}\".job_history (job_id, state, attempt, error)
         VALUES ($1, 'enqueued', 1, 'retried after a failure')"
    )))
    .bind(finished)
    .execute(&pool)
    .await
    .unwrap();

    jalari::migrate(&pool, &schema).await.unwrap();
    assert_eq!(
        legacy_shape(&pool, &name, retryable).await,
        ("scheduled".to_owned(), 1, vec![("scheduled".to_owned(), 1)])
    );
    assert_eq!(
        legacy_shape(&pool, &name, exhausted).await,
        ("failed".to_owned(), 3, vec![("failed".to_owned(), 3)])
    );
    assert_eq!(
        legacy_shape(&pool, &name, finished).await,
        ("enqueued".to_owned(), 1, vec![])
    );

    drop_schema(&pool, &name).await;
}

async fn insert_legacy_command(pool: &PgPool, name: &str, id: i64, command: &str) {
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO \"{name}\".job_control (job_id, command, progress)
         VALUES ($1, $2, '{{\"step\": 3}}')"
    )))
    .bind(id)
    .bind(command)
    .execute(pool)
    .await
    .unwrap();
}

async fn settled_shape(
    pool: &PgPool,
    name: &str,
    id: i64,
) -> (String, i32, Vec<(String, i32, String)>) {
    let (state, attempts): (String, i32) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempts FROM \"{name}\".job WHERE id = $1"
    )))
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap();
    let history: Vec<(String, i32, String)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT state, attempt, error FROM \"{name}\".job_history WHERE job_id = $1 ORDER BY id"
    )))
    .bind(id)
    .fetch_all(pool)
    .await
    .unwrap();
    (state, attempts, history)
}

#[tokio::test]
async fn test_version_8_settles_requests_a_0_6_worker_left_pending() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    jalari::migrate_to(&pool, &schema, 7).await.unwrap();
    let stopped_mid_attempt = insert_legacy_job(&pool, &name, 2, 3).await;
    insert_legacy_command(&pool, &name, stopped_mid_attempt, "stop").await;
    let exhausted_and_cancelled = insert_legacy_job(&pool, &name, 3, 3).await;
    insert_legacy_command(&pool, &name, exhausted_and_cancelled, "cancel").await;
    let cancelled_while_stopped = insert_legacy_job(&pool, &name, 0, 3).await;
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE \"{name}\".job SET state = 'stopped' WHERE id = $1"
    )))
    .bind(cancelled_while_stopped)
    .execute(&pool)
    .await
    .unwrap();
    insert_legacy_command(&pool, &name, cancelled_while_stopped, "cancel").await;

    jalari::migrate(&pool, &schema).await.unwrap();
    let stop = "the job was asked to stop".to_owned();
    let cancel = "the job was asked to cancel".to_owned();
    assert_eq!(
        settled_shape(&pool, &name, stopped_mid_attempt).await,
        (
            "stopped".to_owned(),
            1,
            vec![("stopped".to_owned(), 1, stop)]
        )
    );
    assert_eq!(
        settled_shape(&pool, &name, exhausted_and_cancelled).await,
        (
            "deleted".to_owned(),
            2,
            vec![("deleted".to_owned(), 2, cancel.clone())]
        )
    );
    assert_eq!(
        settled_shape(&pool, &name, cancelled_while_stopped).await,
        (
            "deleted".to_owned(),
            0,
            vec![("deleted".to_owned(), 0, cancel)]
        )
    );
    let progress: Vec<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT progress::text FROM \"{name}\".job_control ORDER BY job_id"
    )))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(progress, [Some("{\"step\": 3}".to_owned()), None, None]);
    let command_column: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = 'job_control' AND column_name = 'command')",
    )
    .bind(&name)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!command_column);

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_version_8_refuses_to_run_while_a_0_6_job_runs() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    jalari::migrate_to(&pool, &schema, 7).await.unwrap();
    let running = insert_legacy_job(&pool, &name, 1, 3).await;
    let mut old_worker = pool.begin().await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM \"{name}\".job WHERE id = $1 FOR UPDATE"
    )))
    .bind(running)
    .execute(&mut *old_worker)
    .await
    .unwrap();

    let err = jalari::migrate(&pool, &schema).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::LockTimeout);
    assert!(err.msg.contains("0.6"), "{}", err.msg);
    assert_eq!(
        jalari::schema_version(&pool, &schema).await.unwrap(),
        Some(7)
    );

    old_worker.rollback().await.unwrap();
    jalari::migrate(&pool, &schema).await.unwrap();
    assert_eq!(
        legacy_shape(&pool, &name, running).await,
        ("scheduled".to_owned(), 1, vec![("scheduled".to_owned(), 1)])
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
            "jalari_job_control",
            "jalari_job_history",
            "jalari_job_step",
            "jalari_queue_pause",
            "jalari_recurring",
            "jalari_schema_version",
            "jalari_worker",
        ]
    );

    drop_schema(&pool, &name).await;
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .map(|code| code.into_owned())
}

async fn execute(pool: &PgPool, sql: String) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(drop)
}

async fn insert_job(pool: &PgPool, name: &str, state: &str, priority: i16) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "INSERT INTO \"{name}\".job (task, payload, state, priority, max_attempts)
         VALUES ('shape', '\\x00', $1, $2, 3)
         RETURNING id"
    )))
    .bind(state)
    .bind(priority)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn test_version_9_adds_the_option_columns_and_the_pause_table() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    let columns = [
        "job.parent_id:bigint:YES:",
        "job.root_id:bigint:YES:",
        "job.queue_key_limit:smallint:YES:",
        "job.expires_at:timestamp with time zone:YES:",
        "recurring.priority:smallint:NO:0",
        "queue_pause.queue:text:NO:",
        "queue_pause.paused_at:timestamp with time zone:NO:now()",
    ];

    jalari::migrate_to(&pool, &schema, 8).await.unwrap();
    let before = snapshot(&pool, &name).await;
    assert!(
        columns
            .iter()
            .all(|column| !before.iter().any(|row| row == column))
    );
    jalari::migrate_to(&pool, &schema, 9).await.unwrap();
    let after = snapshot(&pool, &name).await;
    for column in columns {
        assert!(after.iter().any(|row| row == column), "{column}");
    }
    for index in ["job_parent_id_idx", "job_root_id_idx", "job_expires_at_idx"] {
        assert!(
            after
                .iter()
                .any(|row| row.starts_with(&format!("{index}:CREATE"))),
            "{index}"
        );
    }
    for constraint in [
        "job_priority_check",
        "job_lineage_check",
        "job_queue_key_limit_check",
        "job_queue_key_slot_check",
    ] {
        let definition = after
            .iter()
            .find(|row| row.starts_with(&format!("job.{constraint}:")))
            .unwrap_or_else(|| panic!("{constraint} is missing"));
        assert!(definition.ends_with("NOT VALID"), "{definition}");
    }
    assert!(
        after
            .iter()
            .any(|row| row.starts_with("recurring.recurring_priority_check:"))
    );

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_version_9_clamps_priorities_into_the_four_levels() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    jalari::migrate_to(&pool, &schema, 8).await.unwrap();
    let waiting = insert_job(&pool, &name, "enqueued", 7).await;
    let failed = insert_job(&pool, &name, "failed", -3).await;
    let kept = insert_job(&pool, &name, "scheduled", 1).await;

    jalari::migrate(&pool, &schema).await.unwrap();
    let priorities: Vec<(i64, i16)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id, priority FROM \"{name}\".job ORDER BY id"
    )))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(priorities, [(waiting, 2), (failed, -1), (kept, 1)]);

    for statement in [
        format!("UPDATE \"{name}\".job SET priority = 3 WHERE id = {waiting}"),
        format!("UPDATE \"{name}\".job SET priority = -2 WHERE id = {waiting}"),
        format!(
            "INSERT INTO \"{name}\".recurring (name, cron, task, payload, max_attempts,
                 next_run_at, priority)
             VALUES ('nightly', '0 0 3 * * *', 'shape', '\\x00', 1, now(), 3)"
        ),
    ] {
        let error = execute(&pool, statement.clone()).await.unwrap_err();
        assert_eq!(sqlstate(&error).as_deref(), Some("23514"), "{statement}");
    }
    execute(
        &pool,
        format!("UPDATE \"{name}\".job SET priority = -1 WHERE id = {waiting}"),
    )
    .await
    .unwrap();

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_upgrade_of_a_populated_version_8_matches_a_fresh_schema() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let upgraded_name = unique_schema_name();
    let fresh_name = unique_schema_name();
    let upgraded = Schema::named(&upgraded_name).unwrap();
    let fresh = Schema::named(&fresh_name).unwrap();

    jalari::migrate_to(&pool, &upgraded, 8).await.unwrap();
    for (state, priority) in [
        ("enqueued", 5),
        ("scheduled", 0),
        ("failed", -4),
        ("stopped", 1),
        ("succeeded", 0),
    ] {
        insert_job(&pool, &upgraded_name, state, priority).await;
    }
    execute(
        &pool,
        format!(
            "INSERT INTO \"{upgraded_name}\".job (task, payload, state, max_attempts, queue_key,
                 locked_by, locked_at, lock_token, queue_key_slot)
             VALUES ('shape', '\\x00', 'running', 3, 'k', gen_random_uuid(), now(), 1, 0)"
        ),
    )
    .await
    .unwrap();
    execute(
        &pool,
        format!(
            "INSERT INTO \"{upgraded_name}\".recurring (name, cron, task, payload, max_attempts,
                 next_run_at)
             VALUES ('nightly', '0 0 3 * * *', 'shape', '\\x00', 1, now())"
        ),
    )
    .await
    .unwrap();
    jalari::migrate(&pool, &upgraded).await.unwrap();
    jalari::migrate(&pool, &fresh).await.unwrap();

    assert_eq!(
        snapshot(&pool, &upgraded_name).await,
        snapshot(&pool, &fresh_name).await
    );
    let out_of_range: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM \"{upgraded_name}\".job WHERE priority NOT BETWEEN -1 AND 2"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(out_of_range, 0);
    let recurring_priority: i16 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT priority FROM \"{upgraded_name}\".recurring"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(recurring_priority, 0);

    drop_schema(&pool, &upgraded_name).await;
    drop_schema(&pool, &fresh_name).await;
}

#[tokio::test]
async fn test_lineage_shape_is_enforced() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    jalari::migrate(&pool, &schema).await.unwrap();
    let id = insert_job(&pool, &name, "enqueued", 0).await;

    for assignment in ["parent_id = 1", "root_id = 1"] {
        let statement = format!("UPDATE \"{name}\".job SET {assignment} WHERE id = {id}");
        let error = execute(&pool, statement).await.unwrap_err();
        assert_eq!(sqlstate(&error).as_deref(), Some("23514"), "{assignment}");
    }
    execute(
        &pool,
        format!("UPDATE \"{name}\".job SET parent_id = 1, root_id = 1 WHERE id = {id}"),
    )
    .await
    .unwrap();

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_slot_shape_is_enforced() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    jalari::migrate(&pool, &schema).await.unwrap();
    let unkeyed = insert_job(&pool, &name, "enqueued", 0).await;
    let keyed = insert_job(&pool, &name, "enqueued", 0).await;
    execute(
        &pool,
        format!("UPDATE \"{name}\".job SET queue_key = 'k' WHERE id = {keyed}"),
    )
    .await
    .unwrap();
    let running = |slot: i16, limit: Option<i16>| {
        format!(
            "UPDATE \"{name}\".job SET state = 'running', locked_by = gen_random_uuid(),
                 locked_at = now(), lock_token = 1, queue_key_slot = {slot},
                 queue_key_limit = {}
             WHERE id = {keyed}",
            limit.map_or("NULL".to_owned(), |limit| limit.to_string())
        )
    };

    for statement in [
        format!("UPDATE \"{name}\".job SET queue_key_limit = 2 WHERE id = {unkeyed}"),
        format!("UPDATE \"{name}\".job SET queue_key_limit = 1 WHERE id = {keyed}"),
        running(1, None),
        running(2, Some(2)),
        running(-1, Some(2)),
    ] {
        let error = execute(&pool, statement.clone()).await.unwrap_err();
        assert_eq!(sqlstate(&error).as_deref(), Some("23514"), "{statement}");
    }
    for statement in [running(0, None), running(1, Some(2))] {
        execute(&pool, statement.clone())
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }

    drop_schema(&pool, &name).await;
}

#[tokio::test]
async fn test_version_10_adds_the_step_table_the_copy_number_and_the_missed_policy() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let name = unique_schema_name();
    let schema = Schema::named(&name).unwrap();
    let columns = [
        "job_step.job_id:bigint:NO:",
        "job_step.key:text:NO:",
        "job_step.state:text:NO:",
        "job_step.nonce:uuid:NO:gen_random_uuid()",
        "job_step.starts:integer:NO:1",
        "job_step.attempt:integer:NO:",
        "job_step.lease:bigint:NO:",
        "job_step.output:jsonb:YES:",
        "job_step.error:text:YES:",
        "job_step.started_at:timestamp with time zone:NO:now()",
        "job_step.finished_at:timestamp with time zone:YES:",
        "job_control.progress_seq:bigint:NO:0",
        "recurring.missed:text:NO:'once'::text",
    ];

    assert_eq!(SCHEMA_VERSION, 10);
    jalari::migrate_to(&pool, &schema, 9).await.unwrap();
    let before = snapshot(&pool, &name).await;
    assert!(
        columns
            .iter()
            .all(|column| !before.iter().any(|row| row == column))
    );
    jalari::migrate(&pool, &schema).await.unwrap();
    let after = snapshot(&pool, &name).await;
    for column in columns {
        assert!(after.iter().any(|row| row == column), "{column}");
    }
    for constraint in [
        "job_step.job_step_state_check",
        "job_step.job_step_finished_check",
        "job_step.job_step_key_check",
        "recurring.recurring_missed_check",
    ] {
        assert!(
            after
                .iter()
                .any(|row| row.starts_with(&format!("{constraint}:"))),
            "{constraint}"
        );
    }
    jalari::migrate(&pool, &schema).await.unwrap();
    assert_eq!(snapshot(&pool, &name).await, after);
    drop_schema(&pool, &name).await;
}
