mod common;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use common::{Database, RunningWorker, wait_until, worker_config};
use jalari::sqlx::postgres::PgListener;
use jalari::sqlx::{self, AssertSqlSafe};
use jalari::{
    EnqueueOptions, EnqueueOutcome, ErrorKind, Job, JobId, JobResult, OnConflict, Priority, Worker,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(15);

#[derive(Serialize, Deserialize)]
struct Digest {
    user_id: u32,
}

#[jalari::job(queue = "many_idle")]
impl Job for Digest {
    const NAME: &'static str = "many_digest";
    const MAX_ATTEMPTS: i32 = 4;
    const TIMEOUT: Option<Duration> = Some(Duration::from_secs(20));

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Fanout {
    children: u32,
}

#[jalari::job(queue = "many_run")]
impl Job for Fanout {
    const NAME: &'static str = "many_fanout";

    async fn run(self) -> JobResult {
        let children: Vec<_> = (0..self.children)
            .map(|user_id| (Digest { user_id }, EnqueueOptions::new()))
            .collect();
        jalari::enqueue_many(&children).await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct StoredJob {
    queue: String,
    task: String,
    state: String,
    payload: Vec<u8>,
    job_key: Option<String>,
    queue_key: Option<String>,
    max_attempts: i32,
    timeout_ms: Option<i32>,
    context: Option<Value>,
    fingerprint: Option<String>,
    priority: i16,
    parent_id: Option<i64>,
    root_id: Option<i64>,
    queue_key_limit: Option<i16>,
    run_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    run_after_created_us: i64,
    expires_after_created_us: Option<i64>,
}

fn digest(user_id: u32) -> Digest {
    Digest { user_id }
}

fn digests(count: u32) -> Vec<(Digest, EnqueueOptions)> {
    (0..count)
        .map(|user_id| (digest(user_id), EnqueueOptions::new()))
        .collect()
}

async fn stored(database: &Database, id: JobId) -> StoredJob {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT queue, task, state, payload, job_key, queue_key, max_attempts, timeout_ms,
             context, fingerprint, priority, parent_id, root_id, queue_key_limit, run_at,
             expires_at, (extract(epoch FROM run_at - created_at) * 1000000)::bigint
                 AS run_after_created_us,
             (extract(epoch FROM expires_at - created_at) * 1000000)::bigint
                 AS expires_after_created_us
         FROM {} WHERE id = $1",
        database.table("job")
    )))
    .bind(id.0)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn count(database: &Database) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {}",
        database.table("job")
    )))
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

async fn payloads(database: &Database, ids: &[i64]) -> HashMap<i64, Vec<u8>> {
    let rows: Vec<(i64, Vec<u8>)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id, payload FROM {} WHERE id = ANY($1)",
        database.table("job")
    )))
    .bind(ids)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    rows.into_iter().collect()
}

async fn delete_digests(database: &Database) {
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {} WHERE task = 'many_digest'",
        database.table("job")
    )))
    .execute(&database.pool)
    .await
    .unwrap();
}

fn inserted(outcome: &EnqueueOutcome) -> JobId {
    let EnqueueOutcome::Inserted(id) = *outcome else {
        panic!("expected Inserted, got {outcome:?}");
    };
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_enqueue_many() {
    let Some(database) = Database::start("many", 6).await else {
        return;
    };

    check_many_empty_is_noop(&database).await;
    check_many_inserts_in_input_order(&database).await;
    check_many_keeps_existing_key(&database).await;
    check_many_dedupes_batch_keys(&database).await;
    check_many_rejects_replace(&database).await;
    check_many_inherits_context_per_item(&database).await;
    check_many_notifies_each_queue_once(&database).await;
    check_many_in_follows_transaction(&database).await;
    check_many_splits_large_batches(&database).await;
    check_columns_order_keyed_first_unkeyed_last(&database).await;
    check_many_records_lineage(&database).await;

    database.finish().await;
}

async fn check_many_empty_is_noop(database: &Database) {
    let before = count(database).await;
    let outcomes = jalari::enqueue_many::<Digest>(&[]).await.unwrap();
    assert!(outcomes.is_empty());
    let mut connection = database.pool.acquire().await.unwrap();
    let outcomes = jalari::enqueue_many_in::<Digest>(&mut connection, &[])
        .await
        .unwrap();
    assert!(outcomes.is_empty());
    assert_eq!(count(database).await, before);
}

async fn check_many_inserts_in_input_order(database: &Database) {
    let run_at =
        DateTime::from_timestamp_micros((Utc::now() + TimeDelta::minutes(30)).timestamp_micros())
            .unwrap();
    let expires_at = run_at + TimeDelta::hours(2);
    let options = [
        EnqueueOptions::new(),
        EnqueueOptions::new()
            .delay(Duration::from_secs(3600))
            .priority(Priority::High)
            .max_attempts(7)
            .queue_key("tenant:1")
            .queue_key_limit(3),
        EnqueueOptions::new()
            .queue("many_other")
            .run_at(run_at)
            .timeout(Duration::from_secs(5))
            .expires_at(expires_at)
            .context(&json!({"tenant": "acme"})),
        EnqueueOptions::new()
            .job_key("ordered:1")
            .expire_after(Duration::from_secs(600)),
    ];
    let items: Vec<_> = options
        .iter()
        .enumerate()
        .map(|(index, options)| (digest(u32::try_from(index).unwrap()), options.clone()))
        .collect();
    let outcomes = jalari::enqueue_many(&items).await.unwrap();
    assert_eq!(outcomes.len(), items.len());
    let ids: Vec<JobId> = outcomes.iter().map(inserted).collect();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");

    let mut batched = Vec::new();
    for id in &ids {
        batched.push(stored(database, *id).await);
    }
    let states: Vec<&str> = batched.iter().map(|job| job.state.as_str()).collect();
    assert_eq!(states, ["enqueued", "scheduled", "scheduled", "enqueued"]);
    delete_digests(database).await;
    for (index, ((job, options), mut batched)) in items.iter().zip(batched).enumerate() {
        let single = jalari::enqueue_with(job, options.clone()).await.unwrap();
        let mut single = stored(database, inserted(&single)).await;
        if index == 2 {
            assert_eq!(
                (batched.run_at, batched.expires_at),
                (run_at, Some(expires_at))
            );
            single.run_after_created_us = batched.run_after_created_us;
            single.expires_after_created_us = batched.expires_after_created_us;
        } else {
            single.run_at = batched.run_at;
            single.expires_at = batched.expires_at;
        }
        if let (Some(single), Some(batched)) = (
            single.expires_after_created_us.take(),
            batched.expires_after_created_us.take(),
        ) {
            assert!(
                (single - batched).abs() < 1_000_000,
                "{single} vs {batched}"
            );
        }
        assert_eq!(batched, single, "item {index}");
    }
    delete_digests(database).await;
}

async fn check_many_keeps_existing_key(database: &Database) {
    let waiting = jalari::enqueue_with(&digest(1), EnqueueOptions::new().job_key("held:wait"))
        .await
        .unwrap()
        .id();
    let running = jalari::enqueue_with(&digest(2), EnqueueOptions::new().job_key("held:run"))
        .await
        .unwrap()
        .id();
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {} SET state = 'running', attempts = 1, locked_by = gen_random_uuid(),
             locked_at = now(), lock_token = 1
         WHERE id = $1",
        database.table("job")
    )))
    .bind(running.0)
    .execute(&database.pool)
    .await
    .unwrap();

    let outcomes = jalari::enqueue_many(&[
        (digest(3), EnqueueOptions::new().job_key("held:wait")),
        (digest(4), EnqueueOptions::new().job_key("held:run")),
        (digest(5), EnqueueOptions::new().job_key("held:new")),
    ])
    .await
    .unwrap();
    assert_eq!(outcomes[0], EnqueueOutcome::Kept(waiting));
    assert_eq!(outcomes[1], EnqueueOutcome::Kept(running));
    let fresh = inserted(&outcomes[2]);
    assert_eq!(stored(database, waiting).await.payload, br#"{"user_id":1}"#);
    assert_eq!(stored(database, running).await.payload, br#"{"user_id":2}"#);
    assert_eq!(stored(database, fresh).await.payload, br#"{"user_id":5}"#);
    delete_digests(database).await;
}

async fn check_many_dedupes_batch_keys(database: &Database) {
    let before = count(database).await;
    let outcomes = jalari::enqueue_many(&[
        (digest(1), EnqueueOptions::new().job_key("dup")),
        (digest(2), EnqueueOptions::new()),
        (digest(3), EnqueueOptions::new().job_key("dup")),
    ])
    .await
    .unwrap();
    let first = inserted(&outcomes[0]);
    inserted(&outcomes[1]);
    assert_eq!(outcomes[2], EnqueueOutcome::Kept(first));
    assert_eq!(count(database).await, before + 2);
    assert_eq!(stored(database, first).await.payload, br#"{"user_id":1}"#);
    delete_digests(database).await;
}

async fn check_many_rejects_replace(database: &Database) {
    let before = count(database).await;
    let items = [
        (digest(1), EnqueueOptions::new()),
        (
            digest(2),
            EnqueueOptions::new()
                .job_key("replace")
                .on_conflict(OnConflict::Replace),
        ),
    ];
    let error = jalari::enqueue_many(&items).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidOptions);
    assert!(error.msg.contains("Replace"), "{}", error.msg);
    let mut connection = database.pool.acquire().await.unwrap();
    let error = jalari::enqueue_many_in(&mut connection, &items)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidOptions);
    assert_eq!(count(database).await, before);
}

async fn check_many_inherits_context_per_item(database: &Database) {
    let scoped = json!({"tenant": "acme"});
    let explicit = json!({"tenant": "globex"});
    let items = [
        (digest(1), EnqueueOptions::new()),
        (digest(2), EnqueueOptions::new().context(&explicit)),
        (digest(3), EnqueueOptions::new().no_context()),
    ];
    let outcomes = jalari::scope(&scoped, jalari::enqueue_many(&items))
        .await
        .unwrap();
    let mut contexts = Vec::new();
    for outcome in &outcomes {
        contexts.push(stored(database, inserted(outcome)).await.context);
    }
    assert_eq!(
        contexts,
        [Some(scoped.clone()), Some(explicit.clone()), None]
    );

    let built_outside = jalari::enqueue_many(&items[..1]);
    let outcomes = jalari::scope(&scoped, built_outside).await.unwrap();
    assert_eq!(
        stored(database, inserted(&outcomes[0])).await.context,
        Some(scoped)
    );

    let outcomes = jalari::enqueue_many(&items[..1]).await.unwrap();
    assert_eq!(stored(database, inserted(&outcomes[0])).await.context, None);
    delete_digests(database).await;
}

async fn check_many_notifies_each_queue_once(database: &Database) {
    let mut listener = PgListener::connect_with(&database.pool).await.unwrap();
    listener
        .listen(&format!("{}.job", database.schema_name))
        .await
        .unwrap();
    let on = |queue: &str| EnqueueOptions::new().queue(queue);
    jalari::enqueue_many(&[
        (digest(1), on("many_notify_a")),
        (digest(2), on("many_notify_a")),
        (digest(3), on("many_notify_b")),
        (digest(4), on("many_notify_a")),
    ])
    .await
    .unwrap();

    let mut seen = Vec::new();
    while let Ok(notification) =
        tokio::time::timeout(Duration::from_millis(500), listener.recv()).await
    {
        let payload = notification.unwrap().payload().to_owned();
        if payload.starts_with("many_notify_") {
            seen.push(payload);
        }
    }
    seen.sort();
    assert_eq!(seen, ["many_notify_a", "many_notify_b"]);
    delete_digests(database).await;
}

async fn check_many_in_follows_transaction(database: &Database) {
    let before = count(database).await;
    let items = digests(5);

    let mut transaction = database.pool.begin().await.unwrap();
    let outcomes = jalari::enqueue_many_in(&mut transaction, &items)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 5);
    transaction.rollback().await.unwrap();
    assert_eq!(count(database).await, before);

    let mut transaction = database.pool.begin().await.unwrap();
    let outcomes = jalari::enqueue_many_in(&mut transaction, &items)
        .await
        .unwrap();
    assert_eq!(count(database).await, before);
    transaction.commit().await.unwrap();
    assert_eq!(count(database).await, before + 5);
    for (index, outcome) in outcomes.iter().enumerate() {
        assert_eq!(
            stored(database, inserted(outcome)).await.payload,
            format!(r#"{{"user_id":{index}}}"#).into_bytes()
        );
    }
    delete_digests(database).await;
}

async fn check_many_splits_large_batches(database: &Database) {
    let before = count(database).await;
    let items: Vec<_> = (0..12_000_u32)
        .map(|user_id| {
            let options = match user_id {
                11_000 => EnqueueOptions::new().job_key("large:10"),
                user_id if user_id % 3 == 0 => {
                    EnqueueOptions::new().job_key(&format!("large:{}", 20_000 - user_id))
                }
                user_id if user_id % 3 == 1 => {
                    EnqueueOptions::new().job_key(&format!("large:{user_id}"))
                }
                _ => EnqueueOptions::new(),
            };
            (digest(user_id), options)
        })
        .collect();
    let outcomes = jalari::enqueue_many(&items).await.unwrap();
    assert_eq!(outcomes.len(), items.len());
    let first_of_ten = inserted(&outcomes[10]);
    assert_eq!(outcomes[11_000], EnqueueOutcome::Kept(first_of_ten));
    assert_eq!(count(database).await, before + 11_999);

    let ids: Vec<i64> = outcomes.iter().map(|outcome| outcome.id().0).collect();
    let stored = payloads(database, &ids).await;
    for (user_id, outcome) in outcomes.iter().enumerate() {
        if user_id == 11_000 {
            continue;
        }
        inserted(outcome);
        assert_eq!(
            stored[&outcome.id().0],
            format!(r#"{{"user_id":{user_id}}}"#).into_bytes(),
            "outcome {user_id}"
        );
    }
    let distinct: HashSet<i64> = ids.iter().copied().collect();
    assert_eq!(distinct.len(), 11_999);
    delete_digests(database).await;
}

const KEY_ORDER: [&str; 6] = [
    "order:C",
    "order:a",
    "order:b",
    "order:c",
    r#"{"user_id":1}"#,
    r#"{"user_id":3}"#,
];

async fn insert_order(database: &Database, log: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(format!("TRUNCATE {log}")))
        .execute(&database.pool)
        .await
        .unwrap();
    let keyed = |user_id: u32, key: &str| (digest(user_id), EnqueueOptions::new().job_key(key));
    jalari::enqueue_many(&[
        (digest(1), EnqueueOptions::new()),
        keyed(2, "order:b"),
        (digest(3), EnqueueOptions::new()),
        keyed(4, "order:a"),
        keyed(5, "order:C"),
        keyed(6, "order:c"),
        keyed(7, "order:b"),
    ])
    .await
    .unwrap();
    delete_digests(database).await;
    let order: Vec<(Option<String>, Vec<u8>)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT job_key, payload FROM {log} ORDER BY position"
    )))
    .fetch_all(&database.pool)
    .await
    .unwrap();
    order
        .into_iter()
        .map(|(job_key, payload)| job_key.unwrap_or_else(|| String::from_utf8(payload).unwrap()))
        .collect()
}

async fn check_columns_order_keyed_first_unkeyed_last(database: &Database) {
    let log = database.table("many_insert_log");
    let schema = &database.schema_name;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {log} (position bigserial PRIMARY KEY, job_key text, payload bytea);
         CREATE FUNCTION \"{schema}\".many_insert_log() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO {log} (job_key, payload) VALUES (NEW.job_key, NEW.payload);
             RETURN NEW;
         END $$;
         CREATE TRIGGER many_insert_log BEFORE INSERT ON {job} FOR EACH ROW
             EXECUTE FUNCTION \"{schema}\".many_insert_log();",
        job = database.table("job"),
    )))
    .execute(&database.pool)
    .await
    .unwrap();

    assert_eq!(insert_order(database, &log).await, KEY_ORDER);

    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP TRIGGER many_insert_log ON {job}; DROP TABLE {log};",
        job = database.table("job"),
    )))
    .execute(&database.pool)
    .await
    .unwrap();
}

async fn check_many_records_lineage(database: &Database) {
    let worker = RunningWorker::spawn(
        Worker::builder()
            .queue("many_run", 2)
            .config(worker_config(Duration::from_millis(100)))
            .scheduler(false)
            .build()
            .await
            .unwrap(),
    );
    let parent = jalari::enqueue(&Fanout { children: 3 }).await.unwrap().id();
    let children = || async {
        sqlx::query_as::<_, (Option<i64>, Option<i64>)>(AssertSqlSafe(format!(
            "SELECT parent_id, root_id FROM {} WHERE task = 'many_digest'",
            database.table("job")
        )))
        .fetch_all(&database.pool)
        .await
        .unwrap()
    };
    wait_until("the fanout's children", WAIT, || async {
        children().await.len() == 3
    })
    .await;
    worker.stop().await;
    assert_eq!(children().await, vec![(Some(parent.0), Some(parent.0)); 3]);

    let top_level = jalari::enqueue_many(&digests(1)).await.unwrap();
    let row = stored(database, inserted(&top_level[0])).await;
    assert_eq!((row.parent_id, row.root_id), (None, None));
    delete_digests(database).await;
}
