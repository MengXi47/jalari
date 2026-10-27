use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::{AssertSqlSafe, FromRow, PgConnection, Postgres};

use super::{EnqueueOptions, EnqueueOutcome, EnqueueRequest, OnConflict};
use crate::init::config;
use crate::job::JOB_KEY_STATES;
use crate::storage::JOB_CHANNEL;
use crate::{Error, ErrorKind, Job, JobId, Result, Schema};

const CHUNK_ROWS: usize = 5_000;
const CHUNK_BYTES: usize = 32 * 1024 * 1024;
const MAX_CONFLICT_RETRIES: usize = 3;
const COLUMNS: [(&str, &str); 17] = [
    ("queue", "text"),
    ("task", "text"),
    ("payload", "bytea"),
    ("job_key", "text"),
    ("queue_key", "text"),
    ("max_attempts", "integer"),
    ("timeout_ms", "integer"),
    ("context", "jsonb"),
    ("fingerprint", "text"),
    ("run_at", "timestamptz"),
    ("delay", "interval"),
    ("priority", "smallint"),
    ("parent_id", "bigint"),
    ("root_id", "bigint"),
    ("queue_key_limit", "smallint"),
    ("expires_at", "timestamptz"),
    ("expire_after", "interval"),
];

/// Enqueues many jobs of one type with a single statement per few thousand jobs.
///
/// The outcomes come back in input order. A `job_key` that appears twice in the batch answers
/// [`Kept`](EnqueueOutcome::Kept) with the first job's id. The whole batch commits in one
/// transaction or not at all, and the workers of each queue get one notification.
///
/// # Errors
///
/// Returns an error if:
/// - Any condition listed for [`enqueue_with`](crate::enqueue_with) occurs for any job
/// - Any options ask for [`OnConflict::Replace`](crate::OnConflict::Replace)
///   ([`InvalidOptions`](crate::ErrorKind::InvalidOptions))
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendDigest { user_id: i64 }
/// # #[jalari::job(name = "send_digest")]
/// # impl jalari::Job for SendDigest {
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn example(user_ids: Vec<i64>) -> jalari::Result<()> {
/// let digests: Vec<_> = user_ids
///     .into_iter()
///     .map(|user_id| {
///         let options = jalari::EnqueueOptions::new().job_key(&format!("digest:{user_id}"));
///         (SendDigest { user_id }, options)
///     })
///     .collect();
/// let outcomes = jalari::enqueue_many(&digests).await?;
/// println!("{} digests enqueued or kept", outcomes.len());
/// # Ok(())
/// # }
/// ```
pub fn enqueue_many<T: Job>(
    jobs: &[(T, EnqueueOptions)],
) -> impl Future<Output = Result<Vec<EnqueueOutcome>>> + Send + use<T> {
    let batch = Batch::build(jobs);
    async move { batch?.submit().await }
}

/// Enqueues many jobs on a connection you control, such as an open transaction.
///
/// Nothing is visible to workers until your transaction commits, and nothing is enqueued if it
/// rolls back. Otherwise it behaves like [`enqueue_many`].
///
/// # Errors
///
/// Returns an error if any condition listed for [`enqueue_many`] occurs.
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct ImportChunk { index: u32 }
/// # #[jalari::job(name = "import_chunk")]
/// # impl jalari::Job for ImportChunk {
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// # async fn example(pool: jalari::sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let chunks: Vec<_> = (0..10)
///     .map(|index| (ImportChunk { index }, jalari::EnqueueOptions::new()))
///     .collect();
/// let mut transaction = pool.begin().await?;
/// jalari::enqueue_many_in(&mut transaction, &chunks).await?;
/// transaction.commit().await?;
/// # Ok(())
/// # }
/// ```
pub fn enqueue_many_in<'c, T: Job>(
    connection: &'c mut PgConnection,
    jobs: &[(T, EnqueueOptions)],
) -> impl Future<Output = Result<Vec<EnqueueOutcome>>> + Send + use<'c, T> {
    let batch = Batch::build(jobs);
    async move { batch?.submit_in(connection).await }
}

struct Batch(Vec<EnqueueRequest>);

enum Slot {
    Sent(usize),
    Duplicate(usize),
}

#[derive(FromRow)]
struct ChunkRow {
    ordinality: i64,
    id: Option<i64>,
    inserted: bool,
    queue: Option<String>,
}

impl Batch {
    fn build<T: Job>(jobs: &[(T, EnqueueOptions)]) -> Result<Self> {
        jobs.iter()
            .map(|(job, options)| {
                let request = EnqueueRequest::build(job, options.clone())?;
                if request.on_conflict == OnConflict::Replace {
                    return Err(Error::new(
                        ErrorKind::InvalidOptions,
                        format!(
                            "{}: OnConflict::Replace cannot be used in enqueue_many",
                            request.task
                        ),
                    ));
                }
                Ok(request)
            })
            .collect::<Result<_>>()
            .map(Self)
    }

    async fn submit(self) -> Result<Vec<EnqueueOutcome>> {
        if self.0.is_empty() {
            return Ok(Vec::new());
        }
        let mut transaction = config()?.pool.pool().begin().await?;
        let outcomes = self.submit_in(&mut transaction).await?;
        transaction.commit().await?;
        Ok(outcomes)
    }

    async fn submit_in(self, connection: &mut PgConnection) -> Result<Vec<EnqueueOutcome>> {
        if self.0.is_empty() {
            return Ok(Vec::new());
        }
        let schema = &config()?.schema;
        let mut slots = Vec::with_capacity(self.0.len());
        let mut sent = Vec::new();
        let mut first_of_key = HashMap::new();
        for request in self.0 {
            let request = request.inherit_context()?;
            if let Some(job_key) = &request.job_key {
                if let Some(&first) = first_of_key.get(job_key) {
                    slots.push(Slot::Duplicate(first));
                    continue;
                }
                first_of_key.insert(job_key.clone(), sent.len());
            }
            slots.push(Slot::Sent(sent.len()));
            sent.push(request);
        }

        let chunks = chunks(&sent);
        let mut sent: Vec<Option<EnqueueRequest>> = sent.into_iter().map(Some).collect();
        let mut outcomes = vec![None; sent.len()];
        let mut woken = Vec::new();
        for chunk in chunks {
            let columns = Columns::from(
                chunk
                    .iter()
                    .filter_map(|&index| sent[index].take())
                    .collect::<Vec<_>>(),
            );
            for (row, outcome) in insert_chunk(connection, schema, columns, &mut woken).await? {
                outcomes[chunk[row]] = Some(outcome);
            }
        }
        notify(connection, schema, woken).await?;

        let outcomes = outcomes
            .into_iter()
            .collect::<Option<Vec<EnqueueOutcome>>>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::QueryFailed,
                    "insert returned no row for a job".to_owned(),
                )
            })?;
        Ok(slots
            .into_iter()
            .map(|slot| match slot {
                Slot::Sent(index) => outcomes[index],
                Slot::Duplicate(index) => EnqueueOutcome::Kept(outcomes[index].id()),
            })
            .collect())
    }
}

fn chunks(requests: &[EnqueueRequest]) -> Vec<Vec<usize>> {
    let mut order: Vec<usize> = (0..requests.len()).collect();
    order.sort_by_key(|&index| {
        let job_key = requests[index].job_key.as_deref();
        (job_key.is_none(), job_key)
    });
    let mut chunks = Vec::new();
    let mut chunk: Vec<usize> = Vec::new();
    let mut bytes = 0;
    for index in order {
        let request = &requests[index];
        let size = request.payload.len()
            + request
                .context
                .as_ref()
                .map_or(0, |context| context.to_string().len());
        if !chunk.is_empty() && (chunk.len() == CHUNK_ROWS || bytes + size > CHUNK_BYTES) {
            chunks.push(std::mem::take(&mut chunk));
            bytes = 0;
        }
        chunk.push(index);
        bytes += size;
    }
    chunks.push(chunk);
    for chunk in &mut chunks {
        chunk.sort_unstable();
    }
    chunks
}

async fn insert_chunk(
    connection: &mut PgConnection,
    schema: &Schema,
    mut columns: Columns,
    woken: &mut Vec<String>,
) -> Result<Vec<(usize, EnqueueOutcome)>> {
    let sql = format!(
        "WITH {insert}
         SELECT input.ordinality, coalesce(inserted.id, holder.id) AS id,
             inserted.id IS NOT NULL AS inserted, inserted.queue
         FROM enqueue_input AS input
         LEFT JOIN enqueue_rows AS inserted ON inserted.id = input.id
         LEFT JOIN {job} AS holder ON inserted.id IS NULL AND holder.job_key = input.job_key
             AND holder.state IN {JOB_KEY_STATES}
         ORDER BY input.ordinality",
        insert = insert_sql(schema, 1, None),
        job = schema.table("job"),
    );
    let mut rows: Vec<usize> = (0..columns.len()).collect();
    let mut outcomes = Vec::with_capacity(rows.len());
    for _ in 0..MAX_CONFLICT_RETRIES {
        let returned = bind_columns(sqlx::query(AssertSqlSafe(sql.as_str())), &columns)
            .try_map(|row: PgRow| ChunkRow::from_row(&row))
            .fetch_all(&mut *connection)
            .await?;
        let mut lost = Vec::new();
        for returned in returned {
            let position = usize::try_from(returned.ordinality - 1)
                .map_err(|_| Error::new(ErrorKind::QueryFailed, "bad ordinality".to_owned()))?;
            match (returned.inserted, returned.id, returned.queue) {
                (true, Some(id), Some(queue)) => {
                    woken.push(queue);
                    outcomes.push((rows[position], EnqueueOutcome::Inserted(JobId(id))));
                }
                (false, Some(id), _) => {
                    outcomes.push((rows[position], EnqueueOutcome::Kept(JobId(id))));
                }
                _ => lost.push(position),
            }
        }
        if lost.is_empty() {
            return Ok(outcomes);
        }
        let holders = holders(connection, schema, &columns, &lost).await?;
        let mut retried = Vec::new();
        for position in lost {
            match holders.get(&position) {
                Some(&id) => outcomes.push((rows[position], EnqueueOutcome::Kept(id))),
                None => retried.push(position),
            }
        }
        if retried.is_empty() {
            return Ok(outcomes);
        }
        columns = columns.rows(&retried);
        rows = retried.into_iter().map(|position| rows[position]).collect();
    }
    Err(Error::new(
        ErrorKind::JobKeyContention,
        format!(
            "job_keys {:?} kept changing state while enqueuing",
            columns.job_key
        ),
    ))
}

async fn holders(
    connection: &mut PgConnection,
    schema: &Schema,
    columns: &Columns,
    positions: &[usize],
) -> Result<HashMap<usize, JobId>> {
    let mut by_key = HashMap::new();
    for &position in positions {
        let job_key = columns.job_key[position].as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::QueryFailed,
                "insert returned no row for a job without a job_key".to_owned(),
            )
        })?;
        by_key.insert(job_key.as_str(), position);
    }
    let sql = format!(
        "SELECT job_key, id FROM {job} WHERE job_key = ANY($1) AND state IN {JOB_KEY_STATES}",
        job = schema.table("job"),
    );
    let found: Vec<(String, i64)> = sqlx::query_as(AssertSqlSafe(sql))
        .bind(by_key.keys().collect::<Vec<_>>())
        .fetch_all(connection)
        .await?;
    Ok(found
        .into_iter()
        .filter_map(|(job_key, id)| {
            by_key
                .get(job_key.as_str())
                .map(|&position| (position, JobId(id)))
        })
        .collect())
}

async fn notify(
    connection: &mut PgConnection,
    schema: &Schema,
    mut queues: Vec<String>,
) -> Result<()> {
    if queues.is_empty() {
        return Ok(());
    }
    queues.sort_unstable();
    queues.dedup();
    sqlx::query("SELECT pg_notify($1, queue) FROM unnest($2::text[]) AS queue")
        .bind(schema.notify_channel(JOB_CHANNEL))
        .bind(queues)
        .execute(connection)
        .await?;
    Ok(())
}

pub(crate) struct Columns {
    queue: Vec<String>,
    task: Vec<String>,
    payload: Vec<Vec<u8>>,
    job_key: Vec<Option<String>>,
    queue_key: Vec<Option<String>>,
    max_attempts: Vec<i32>,
    timeout_ms: Vec<Option<i32>>,
    context: Vec<Option<Value>>,
    fingerprint: Vec<Option<&'static str>>,
    run_at: Vec<Option<DateTime<Utc>>>,
    delay: Vec<Option<Duration>>,
    priority: Vec<i16>,
    parent_id: Vec<Option<i64>>,
    root_id: Vec<Option<i64>>,
    queue_key_limit: Vec<Option<i16>>,
    expires_at: Vec<Option<DateTime<Utc>>>,
    expire_after: Vec<Option<Duration>>,
}

impl From<Vec<EnqueueRequest>> for Columns {
    fn from(requests: Vec<EnqueueRequest>) -> Self {
        let mut columns = Self::with_capacity(requests.len());
        for request in requests {
            columns.queue.push(request.queue);
            columns.task.push(request.task);
            columns.payload.push(request.payload);
            columns.job_key.push(request.job_key);
            columns.queue_key.push(request.queue_key);
            columns.max_attempts.push(request.max_attempts);
            columns.timeout_ms.push(request.timeout_ms);
            columns.context.push(request.context);
            columns.fingerprint.push(request.fingerprint);
            columns.run_at.push(request.run_at);
            columns.delay.push(request.delay);
            columns.priority.push(request.priority);
            columns.parent_id.push(request.parent_id);
            columns.root_id.push(request.root_id);
            columns.queue_key_limit.push(request.queue_key_limit);
            columns.expires_at.push(request.expires_at);
            columns.expire_after.push(request.expire_after);
        }
        columns
    }
}

impl Columns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Vec::with_capacity(capacity),
            task: Vec::with_capacity(capacity),
            payload: Vec::with_capacity(capacity),
            job_key: Vec::with_capacity(capacity),
            queue_key: Vec::with_capacity(capacity),
            max_attempts: Vec::with_capacity(capacity),
            timeout_ms: Vec::with_capacity(capacity),
            context: Vec::with_capacity(capacity),
            fingerprint: Vec::with_capacity(capacity),
            run_at: Vec::with_capacity(capacity),
            delay: Vec::with_capacity(capacity),
            priority: Vec::with_capacity(capacity),
            parent_id: Vec::with_capacity(capacity),
            root_id: Vec::with_capacity(capacity),
            queue_key_limit: Vec::with_capacity(capacity),
            expires_at: Vec::with_capacity(capacity),
            expire_after: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    fn rows(&self, positions: &[usize]) -> Self {
        let mut columns = Self::with_capacity(positions.len());
        for &position in positions {
            columns.queue.push(self.queue[position].clone());
            columns.task.push(self.task[position].clone());
            columns.payload.push(self.payload[position].clone());
            columns.job_key.push(self.job_key[position].clone());
            columns.queue_key.push(self.queue_key[position].clone());
            columns.max_attempts.push(self.max_attempts[position]);
            columns.timeout_ms.push(self.timeout_ms[position]);
            columns.context.push(self.context[position].clone());
            columns.fingerprint.push(self.fingerprint[position]);
            columns.run_at.push(self.run_at[position]);
            columns.delay.push(self.delay[position]);
            columns.priority.push(self.priority[position]);
            columns.parent_id.push(self.parent_id[position]);
            columns.root_id.push(self.root_id[position]);
            columns.queue_key_limit.push(self.queue_key_limit[position]);
            columns.expires_at.push(self.expires_at[position]);
            columns.expire_after.push(self.expire_after[position]);
        }
        columns
    }
}

pub(crate) fn insert_sql(schema: &Schema, first_parameter: usize, source: Option<&str>) -> String {
    let arrays = COLUMNS
        .iter()
        .enumerate()
        .map(|(offset, (_, sql_type))| format!("${}::{sql_type}[]", first_parameter + offset))
        .collect::<Vec<_>>()
        .join(", ");
    let names = COLUMNS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    let source = source.map_or_else(String::new, |source| format!("{source} CROSS JOIN "));
    format!(
        "enqueue_input AS MATERIALIZED (
             SELECT item.ordinality, nextval(pg_get_serial_sequence('{job}', 'id')) AS id,
                 item.queue, item.task, item.payload, item.job_key, item.queue_key,
                 item.max_attempts, item.timeout_ms, item.context, item.fingerprint,
                 COALESCE(item.run_at, now() + COALESCE(item.delay, interval '0')) AS run_at,
                 item.priority, item.parent_id, item.root_id, item.queue_key_limit,
                 COALESCE(item.expires_at, statement_timestamp() + item.expire_after)
                     AS expires_at
             FROM {source}unnest({arrays}) WITH ORDINALITY AS item ({names}, ordinality)
         ),
         enqueue_rows AS (
             INSERT INTO {job} (id, queue, task, payload, state, job_key, queue_key,
                 max_attempts, run_at, timeout_ms, context, fingerprint, priority, parent_id,
                 root_id, queue_key_limit, expires_at)
             OVERRIDING SYSTEM VALUE
             SELECT id, queue, task, payload,
                 CASE WHEN run_at > now() THEN 'scheduled' ELSE 'enqueued' END,
                 job_key, queue_key, max_attempts, run_at, timeout_ms, context, fingerprint,
                 priority, parent_id, root_id, queue_key_limit, expires_at
             FROM enqueue_input
             ORDER BY job_key COLLATE \"C\" NULLS LAST, ordinality
             ON CONFLICT (job_key) WHERE job_key IS NOT NULL AND state IN {JOB_KEY_STATES}
             DO NOTHING
             RETURNING id, queue
         )",
        job = schema.table("job"),
    )
}

pub(crate) fn bind_columns<'q>(
    query: Query<'q, Postgres, PgArguments>,
    columns: &Columns,
) -> Query<'q, Postgres, PgArguments> {
    query
        .bind(&columns.queue)
        .bind(&columns.task)
        .bind(&columns.payload)
        .bind(&columns.job_key)
        .bind(&columns.queue_key)
        .bind(&columns.max_attempts)
        .bind(&columns.timeout_ms)
        .bind(&columns.context)
        .bind(&columns.fingerprint)
        .bind(&columns.run_at)
        .bind(&columns.delay)
        .bind(&columns.priority)
        .bind(&columns.parent_id)
        .bind(&columns.root_id)
        .bind(&columns.queue_key_limit)
        .bind(&columns.expires_at)
        .bind(&columns.expire_after)
}
