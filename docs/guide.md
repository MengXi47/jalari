# jalari guide

How to set up jalari, define and enqueue jobs, schedule them and run workers. The API
reference with every option is on docs.rs; this guide shows how the pieces fit together.

- [Installation](#installation)
- [Setup](#setup)
- [Defining jobs](#defining-jobs)
- [Enqueueing](#enqueueing)
- [Recurring jobs](#recurring-jobs)
- [Running workers](#running-workers)
- [Stopping, restarting and cancelling](#stopping-restarting-and-cancelling)
- [Deployment](#deployment)
- [Cluster settings and cleanup](#cluster-settings-and-cleanup)
- [Monitoring](#monitoring)
- [Errors](#errors)
- [Examples](#examples)

## Installation

```toml
[dependencies]
jalari = "0.4"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

jalari needs PostgreSQL 14 or newer and Rust 1.94 or newer. It does not enable TLS; turn on
a TLS feature of `sqlx` in your own crate if your database needs it.

## Setup

Call `jalari::init` once at startup, before anything else. It stores the pool and the
schema for the whole process and checks that the tables are at the version this release
expects.

```rust
let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
jalari::init(pool)
    .schema(jalari::Schema::default())
    .migrate()
    .await?;
```

Every call in this guide is written with its full `jalari::` path. Only the `Job` and
`JobResult` names are imported, because implementing a job needs them.

### Where the tables live

| Schema | Tables |
|---|---|
| `jalari::Schema::default()` | `jalari.job`, `jalari.recurring`... |
| `jalari::Schema::named("jobs")?` | `jobs.job`, `jobs.recurring`... |
| `jalari::Schema::prefixed("public", "jalari_")?` | `public.jalari_job`, `public.jalari_recurring`... |

Every process that talks to the same jobs must use the same schema. Separate schemas or
prefixes give fully independent installations in one database.

### Creating the tables

| Way | When |
|---|---|
| `jalari::init(..).migrate()` | The app's role may create tables |
| `jalari migrate` (CLI) | A deploy step creates tables with a separate role |
| `jalari::migrations(&schema)` or `jalari sql` | Your own migration tool applies the SQL |

Without `.migrate()`, `jalari::init` only checks the version and returns
`jalari::ErrorKind::SchemaVersionMismatch` when the tables are missing or outdated.

The CLI is behind the `cli` feature:

```bash
cargo install jalari --features cli
```

| Command | Effect |
|---|---|
| `jalari status` | Show the installed and the latest version |
| `jalari migrate` | Create or upgrade the tables |
| `jalari migrate --dry-run` | Print the SQL that would run |
| `jalari sql --schema public --prefix jalari_` | Print the SQL without a database |

`--database-url` falls back to the `DATABASE_URL` environment variable. Migrations run in one
transaction under an advisory lock, so concurrent runs are safe.

Version 3 replaces the `queue_key` index on the job table with two smaller partial indexes. It
builds them without `CONCURRENTLY`, so writes to the job table wait for the build while the
upgrade runs, and dropping the old index waits for every running job to finish while blocking
other access to the table. Stop the workers before upgrading. Workers of an older release that
start after the upgrade fail `init`, because it requires the exact table version.

Version 4 adds the `job_control` table and the `stopped` state, and replaces the unique
`job_key` index with one that also covers stopped jobs. Changing the state checks scans the job
table and blocks all access to it until the upgrade commits, and it waits for every running
job to finish first. Stop the workers before upgrading, as for version 3.

Version 5 adds the interval and end-condition columns to the `recurring` table. It only adds
nullable columns and checks to that small table, so it runs quickly, but workers of an older
release fail `init` after it, as for every version.

Version 6 adds a GIN index on the job table's context column for
[listing jobs](#listing-jobs) by context. Building it reads the whole job table and blocks
writes to it until the upgrade commits.

## Defining jobs

A job is a serializable struct with an `impl Job` inside `#[jalari::job]`. Its fields are the
job's arguments.

```rust
use std::io;
use std::time::Duration;

use jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

#[jalari::job(queue = "emails")]
impl Job for SendEmail {
    const NAME: &'static str = "send_email";
    const MAX_ATTEMPTS: i32 = 5;
    const TIMEOUT: Option<Duration> = Some(Duration::from_secs(30));

    async fn run(self) -> JobResult {
        if !self.to.contains('@') {
            return Err(jalari::JobError::permanent(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid address {:?}", self.to),
            )));
        }
        mailer::send(&self.to, &self.subject).await?;
        Ok(())
    }
}
```

| Item | Meaning | Default |
|---|---|---|
| `NAME` | Stable name stored with every job; must be unique in the binary | required |
| `MAX_ATTEMPTS` | Attempts before the job is marked failed, including the first and any interrupted by a crash | 10 |
| `TIMEOUT` | Longest time one attempt may run | none |
| `type Context` | Value `run` receives from the worker's middleware; see [Context and middleware](#context-and-middleware) | `()` |
| `queue = "..."` | Queue the job goes to unless the enqueue says otherwise | `default` |

`run` returns `Ok(())` on success. Any error converts into a retryable `jalari::JobError`
through `?`. Return `jalari::JobError::permanent(e)` when retrying cannot help, for example
invalid input.
A panic or a timeout counts as a failed attempt.

A job can run more than once if its worker dies mid-run, so keep `run` idempotent.

## Enqueueing

```rust
use std::time::Duration;

jalari::enqueue(&job).await?;

let options = jalari::EnqueueOptions::new().delay(Duration::from_secs(600));
jalari::enqueue_with(&job, options).await?;
```

| `jalari::EnqueueOptions` | Effect |
|---|---|
| `delay(d)` / `run_at(t)` | Run no earlier than the given time |
| `queue(name)` | Use another queue than the job's default |
| `job_key(key)` | At most one waiting or running job per key |
| `on_conflict(..)` | `KeepExisting` (default) or `Replace` the waiting job with the same key |
| `queue_key(key)` | Jobs sharing the key never run at the same time |
| `timeout(d)` | Override `Job::TIMEOUT` for this job |

Every enqueue returns a `jalari::EnqueueOutcome`:

| Outcome | Meaning |
|---|---|
| `Inserted(id)` | A new job was created |
| `Kept(id)` | A job with the same `job_key` already existed and was kept |
| `Replaced(id)` | The waiting job with the same `job_key` was overwritten |
| `Running(id)` | The job with the same `job_key` is running and could not be replaced |

`Replace` overwrites a job that is waiting, waiting for a retry, or claimed by a worker that
has not started it yet; that claim is dropped. For a running job it waits about 100 ms before
returning `Running`, so a worker that is only inspecting the job is not mistaken for one that
runs it. If the job's last attempt was interrupted (see
[Crashes and lost connections](#crashes-and-lost-connections)), the replace records the
interruption, and the new payload waits the same 6 seconds as any interrupted job, together
with the jobs sharing its `queue_key`. A dropped claim counts as an interrupted attempt. A job
that is stopped, or has a pending stop or cancel, is never replaced and returns `Kept` (see
[What a stop keeps and releases](#what-a-stop-keeps-and-releases)).

### Inside your transaction

`jalari::enqueue_in` writes the job on your connection. Workers see it only after you commit,
and it disappears on rollback.

```rust
let mut transaction = pool.begin().await?;
let user_id: i64 = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
    .bind(&email)
    .fetch_one(&mut *transaction)
    .await?;
jalari::enqueue_in(
    &mut transaction,
    &SendWelcome { user_id },
    jalari::EnqueueOptions::new(),
)
.await?;
transaction.commit().await?;
```

## Recurring jobs

There are two kinds of schedules, and they never affect each other.

| | Declared in code | Added at runtime |
|---|---|---|
| Create | `#[jalari::job(cron = "...")]` | `jalari::recurring::add_or_update(name, schedule, &job)` |
| Change | Edit the code and deploy | `update_schedule`, `pause`, `resume`, `remove` |
| Schedule | Cron only | Cron or interval, with optional end conditions |
| Arguments | `Default::default()` | Any value you pass |
| Name | The job's `NAME` | Any name you choose |

### Declared in code

```rust
#[derive(Serialize, Deserialize, Default)]
struct NightlyReport;

#[jalari::job(cron = "0 0 3 * * *", timezone = "Asia/Taipei", queue = "reports")]
impl Job for NightlyReport {
    const NAME: &'static str = "nightly_report";

    async fn run(self) -> JobResult {
        Ok(())
    }
}
```

Workers sync these schedules at startup: a changed expression takes effect on the next deploy,
and a removed declaration deletes the schedule. They cannot be changed at runtime.

### Added at runtime

```rust
let cron = jalari::Cron::new("0 0 9 * * MON-FRI")?.timezone("Europe/Berlin")?;
let digest = Digest { team: "sales".into() };
jalari::recurring::add_or_update("digest:sales", cron, &digest).await?;

jalari::recurring::update_schedule("digest:sales", jalari::Cron::new("0 30 8 * * *")?).await?;
jalari::recurring::pause("digest:sales").await?;
jalari::recurring::resume("digest:sales").await?;
jalari::recurring::remove("digest:sales").await?;
```

`add_or_update` and `update_schedule` take a `jalari::Cron`, a `jalari::Interval` or a
`jalari::Schedule`. Calling `add_or_update` with the same schedule again only updates the
payload: the next run, the run count and a finished state are kept, so calling it at every
start of your application is safe. A different schedule starts over.

`jalari::recurring::trigger(name)` enqueues a schedule's job immediately, for either kind,
without moving its timetable. `jalari::recurring::list()` returns every schedule with its next
run, its run count and when it finished.

### Intervals

An interval runs every N units from an anchor, which cron cannot express: every 5 days from
October 3rd stays aligned with October 3rd instead of restarting on the 1st of each month.

```rust
let anchor = NaiveDate::from_ymd_opt(2026, 10, 3).unwrap().and_hms_opt(9, 0, 0).unwrap();
let every_five_days = jalari::Interval::new(5, jalari::IntervalUnit::Days, anchor)?
    .timezone("Asia/Taipei")?;
jalari::recurring::add_or_update("digest:sales", every_five_days, &digest).await?;
```

- The anchor is a local time in the interval's time zone, `UTC` by default, and is the first
  run. An anchor in the past is not run; the next run is the first aligned time after now.
- Seconds, minutes and hours count absolute time from the anchor.
- Days, weeks, months and years count on the local calendar, so each run keeps the anchor's
  wall-clock time across daylight saving changes. A repeated local hour runs once, at its first
  occurrence; a skipped one runs at the first valid time after the gap, as a cron does.
- Monthly and yearly runs whose day does not exist in the target month move to that month's
  last day, and later months use the anchor's day again: every month from January 31st runs
  on February 28th or 29th, then on March 31st.
- The next run is computed directly from the anchor, never by stepping through the missed
  rounds, so a long outage costs nothing.

### End conditions

Both cron and interval schedules can end:

```rust
let schedule = jalari::Schedule::from(every_five_days)
    .until(Utc.with_ymd_and_hms(2026, 12, 31, 0, 0, 0).unwrap())
    .max_runs(10)?;
jalari::recurring::add_or_update("digest:sales:trial", schedule, &digest).await?;
```

- `until` allows the last run at or before that time; `max_runs` finishes the schedule once it
  has enqueued that many runs. Only runs the scheduler enqueued count: `trigger` does not, and
  neither does a round skipped because the previous job was still waiting or running.
- A finished schedule stays in the list with `finished_at` set and no `next_run_at`. It never
  fires again, `pause` and `resume` leave it finished, and `trigger` still works.
- `update_schedule` or `add_or_update` with a different schedule resets the run count to zero
  and un-finishes it; the same schedule leaves it as it is.

### Cron format

- Five fields (`minute hour day month weekday`) or six with seconds first.
- The day fields also take `L` for the last day of the month or the last given weekday
  (`5L`), `#` for the nth given weekday (`1#2`) and `W` for the weekday nearest a day
  (`15W`).
- Evaluated in the given IANA time zone, `UTC` by default. A repeated local hour during a
  daylight saving change fires once; a skipped one fires at the first valid time after the gap.
- After an outage, each schedule enqueues one catch-up run, not every missed one.
- Each scheduled time is enqueued at most once, however many workers run the scheduler.
- While a schedule's previous job is still waiting or running, no second one is added.

## Running workers

```rust
use std::time::Duration;

let mut config = jalari::WorkerConfig::default();
config.poll_interval = Duration::from_secs(2);

let worker = jalari::Worker::builder()
    .queue("default", 4)
    .queue("emails", 8)
    .config(config)
    .retry_policy(jalari::ExponentialBackoff {
        base: Duration::from_secs(10),
        max: Duration::from_secs(600),
        jitter: 0.2,
    })
    .build()
    .await?;

let shutdown = jalari::CancellationToken::new();
let signal = shutdown.clone();
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.ok();
    signal.cancel();
});
worker.run(shutdown).await;
```

A worker runs every `#[jalari::job]` linked into its binary and only takes jobs from the
queues it lists. Each running job holds one pooled connection, so the pool needs at least
the sum of all concurrencies plus two; `build()` checks this.

| `jalari::WorkerConfig` field | Meaning | Default |
|---|---|---|
| `poll_interval` | Fallback check when no notification arrives | 5 s |
| `shutdown_timeout` | How long shutdown waits for running jobs before aborting them | 30 s |
| `max_claim_tries` | Jobs skipped per pass while their `queue_key` is busy | 10 |
| `heartbeat_interval` | How often the worker reports that it is alive | 30 s |
| `stop_timeout` | How long a stopped or cancelled job may take to reach a checkpoint before it is aborted | 30 s |

### Retries

The default `jalari::ExponentialBackoff` waits 5 s, 10 s, 20 s... up to 1 hour, with 20%
jitter. Implement `jalari::RetryPolicy` for a different strategy. A job is marked failed when it reaches
`MAX_ATTEMPTS` or returns a permanent error.

An attempt is counted when it starts, so an attempt cut short by a crash counts too, unless it
ends in a [stop or cancel](#stopping-restarting-and-cancelling). When a
crashed worker's job is picked up again with no attempts left, it is marked failed with
`the worker stopped during the last attempt` instead of running once more. This keeps a job
that brings down its process from crashing workers forever.

### Shutdown

Cancelling the token stops the worker from claiming new jobs and waits for running ones.
Jobs still running after `shutdown_timeout` are aborted and count as an attempt; another
worker runs them again. A job that reaches a [checkpoint](#checkpoints) during the shutdown
gets an error from it; when it returns that error, the job goes back to its queue without
counting the attempt and keeps its saved progress.

A long job can notice the shutdown and stop early, for example after saving its progress:

```rust
for id in ids {
    tokio::select! {
        () = jalari::shutdown_requested() => {
            return Err(jalari::JobError::new("stopped for shutdown".to_owned()));
        }
        result = export(id) => result?,
    }
}
```

`jalari::current_job()` returns the running attempt's id, name, queue and attempt number. The
id stays the same across retries, so it works as an idempotency key. Both functions only work
in the job's own task, not in tasks it spawns.

### Crashes and lost connections

When a worker process dies, PostgreSQL releases its jobs. The next worker that finds one
records the interrupted attempt in the job history and runs the job again 6 seconds later. A
`Replace` that finds the job does the same before it writes the new payload, so the new
payload also waits 6 seconds.

While a job runs, its worker checks the connection holding the job's lock every second. If
that connection is lost (the database restarts, the network fails, or the session is ended by
an administrator or by `idle_in_transaction_session_timeout`), the worker aborts the job
within 3 seconds. Other workers wait 6 seconds before running an interrupted job again, so the
old run has stopped before the new one starts. The wait also covers other jobs with the same
`queue_key`, since the interrupted run may still hold on to that key. The guarantee holds as long as a worker process
is not frozen for several seconds, for example by a virtual machine pause.

The same check keeps a long job's transaction busy, so `idle_in_transaction_session_timeout`
never ends it while the job is still running. jalari sets that timeout to 10 seconds on the
job's own transaction, overriding the pool's setting: if a worker freezes or the network fails
without closing the connection, PostgreSQL ends the session after 10 seconds without a check,
and another worker takes the job over about 16 seconds after the failure instead of waiting
for TCP keepalives.

## Stopping, restarting and cancelling

A stop frees everything a job holds: its task, its pooled connection, the row lock and the
`queue_key` its worker holds, and the worker's slot, plus whatever the job's own code holds as it
returns. The job stays in the database as `stopped` until you restart or cancel it, also
across worker restarts.

```rust
match jalari::stop(id).await? {
    jalari::StopOutcome::Stopped => println!("stopped"),
    jalari::StopOutcome::Requested => println!("a worker holds it; call stop again to see"),
    jalari::StopOutcome::Finished | jalari::StopOutcome::NotFound => {}
}

jalari::restart(id).await?;
jalari::cancel(id).await?;
```

### Checkpoints

A job learns about a stop at its checkpoints. A checkpoint can also save progress, so a
restart, a retry or a takeover after a crash continues where the job left off:

```rust
#[derive(Serialize, Deserialize, Default)]
struct Cursor {
    last_id: i64,
}

async fn run(self) -> JobResult {
    let (saved, checkpoint) = jalari::resume::<Cursor>()?;
    let mut cursor = saved.unwrap_or_default();
    while let Some(batch) = fetch(cursor.last_id).await? {
        upload(&batch).await?;
        cursor.last_id = batch.last_id;
        checkpoint.save(&cursor).await?;
    }
    Ok(())
}
```

`jalari::resume::<P>()` returns the progress saved by an earlier run, `None` on the first
run, and the `Checkpoint<P>` that saves the same type. `checkpoint.save(&progress)` never
touches the database: it keeps a JSON copy at most once a second, and the worker writes the
latest copies every 5 seconds. It returns an error once the job should stop, and `?` unwinds
the job. Jobs without progress call `jalari::checkpoint().await?` instead. Both do nothing
outside a job. A job that finishes discards its progress, and so does a `Replace`.

### What each call does

| Call | Waiting job | Running job | Stopped job | Finished job |
|---|---|---|---|---|
| `stop` | `Stopped` at once | `Requested` | `Stopped` | `Finished` |
| `cancel` | `Cancelled` at once | `Requested` | `Cancelled` | `Finished` |
| `restart` | `NotStopped` | `NotStopped` | `Restarted` | `NotStopped` |

An id with no job returns `NotFound`. A job whose worker crashed less than 6 seconds ago
counts as running.

A running job sees a request within about a second. How it ends:

| The job | It ends as |
|---|---|
| Returns the checkpoint's error | `stopped` (or `deleted` for a cancel) with the progress that checkpoint saved |
| Reaches no checkpoint within `stop_timeout` | aborted at its current `.await`, then `stopped` with the last saved progress |
| Returns any other error, panics or times out | `stopped` (or `deleted`), not retried |
| Returns `Ok` first | `succeeded`; a later `stop` returns `Finished` |
| Its worker crashes | `stopped` (or `deleted`) by the next worker, after the usual 6 second delay; it never runs again |

`stop` and `cancel` are safe to repeat and report the current state, so call them again until
they stop returning `Requested`. A cancel wins over a stop that is still pending.

### What a stop keeps and releases

- A stop or cancel never counts as an attempt. A restarted job has the attempts it had before
  the stop and continues from its saved progress.
- A stopped job keeps its `job_key`: enqueueing the same key returns `Kept`, even with
  `Replace`, and a recurring schedule skips its runs until the job is restarted or cancelled.
- A pending stop or cancel beats `Replace` too: while `stop` or `cancel` returns `Requested`,
  enqueueing the same key with `Replace` returns `Kept`, and the request still applies.
- A stopped job releases its `queue_key`, so other jobs with the key can run.
- Housekeeping never deletes stopped jobs. A cancelled job is `deleted` and frees its
  `job_key`; housekeeping removes it after `deleted_retention`.

### Limits

- The abort after `stop_timeout` drops the job at an `.await`. Code that never reaches one,
  work in `spawn_blocking` and tasks the job spawned keep running, and their side effects may
  outlive the stop, as with `TIMEOUT` and shutdown aborts.
- `stop` on a crashed job in a queue no worker serves returns `Requested` until a worker
  takes the queue, or until a later `stop` after the 6 second delay settles it.
- A crash loses at most about 6 seconds of progress: one second before a copy is kept, and
  five before it is written.
- During a rolling upgrade, workers of 0.3 fail `init` against the new tables, as with every
  migration.

## Context and middleware

A worker is a separate process, so the request that enqueued a job cannot hand it its
connections or caller directly. jalari carries a small context instead: it is captured when the
job is enqueued, stored with the job, and given to middleware that rebuilds whatever the job
needs on the worker. jalari attaches no meaning to it; tenants, users, databases and tracing are
all up to your middleware.

```
 enqueue   jalari::scope(&caller, ..) or EnqueueOptions::context(&caller)
           -> stored once in the job's context column, kept across retries
 worker    middleware reads job.context::<Caller>(), rebuilds, next.provide(value)
           -> the job receives &value as its Context
```

### Capturing a context

Wrap the code that enqueues in `jalari::scope`, typically once in your request middleware:

```rust
#[derive(Serialize, Deserialize)]
struct Caller {
    tenant_id: String,
    user_id: i64,
}

jalari::scope(&caller, async {
    jalari::enqueue_in(&mut transaction, &BuildReport { request_id }, jalari::EnqueueOptions::new())
        .await
})
.await?;
```

| Source | Used when |
|---|---|
| `EnqueueOptions::context(&value)` | Set on the enqueue |
| `EnqueueOptions::no_context()` | Set on the enqueue; stores nothing |
| The surrounding `jalari::scope` | Neither option is set |
| The running job's context | Enqueued from inside a job, which therefore passes its context on |

The context is serialized as JSON next to the job, so store identifiers only, never tokens or
passwords. Whoever can insert into the job table can write any context, so middleware should
look identifiers up again rather than trust them. Runtime schedules from
`jalari::recurring::add_or_update` keep the context of the scope they were created in.

### Writing middleware

```rust
use jalari::{JobError, JobMeta, JobMiddleware, JobResult, Next};

struct TenantMiddleware {
    db_router: DbRouter,
}

impl JobMiddleware for TenantMiddleware {
    type Provides = RequestContext;

    async fn check(&self) -> Result<(), JobError> {
        self.db_router.load_catalog().await?;
        Ok(())
    }

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        let Some(caller) = job.context::<Caller>()? else {
            return Err(JobError::permanent(MissingCaller));
        };
        let context = self.db_router.request_context(caller).await?;
        next.provide(context).run().await
    }
}

#[jalari::job(queue = "reports")]
impl Job for BuildReport {
    const NAME: &'static str = "build_report";
    type Context = RequestContext;

    async fn run(self, context: &RequestContext) -> JobResult {
        let mut transaction = context.db.begin().await?;
        // ...
        Ok(())
    }
}

let worker = jalari::Worker::builder()
    .middleware(TracingMiddleware)
    .middleware(TenantMiddleware { db_router })
    .queue("reports", 4)
    .build()
    .await?;
```

- The first middleware added is the outermost.
- A middleware with `type Provides = ()` wraps every job. Any other middleware only wraps jobs
  whose `Context` is its `Provides`, so jobs that need no tenant are never rejected by it.
- Returning an error instead of calling `next.run()` rejects the attempt. A permanent error
  fails the job; any other error is retried like a failed run. `job.context::<T>()` returns a
  permanent error when the stored context does not decode as `T`.
- `build()` fails with `MissingContext` when a job whose default queue the worker serves needs a
  context no middleware provides, with `DuplicateContextProvider` when two middleware provide the
  same type, and with `MiddlewareCheckFailed` when a `check` fails. Jobs on other queues whose
  context is missing are simply not taken, so workers with different middleware can share the
  database.

### Choosing a database per job

The type decides which kind of resource a job gets; the stored context can decide which one.
Wrap each resource in its own type so a job cannot reach the wrong one:

```rust
struct ReportDb(PgPool);

struct TargetDb {
    name: String,
    pool: PgPool,
}

impl JobMiddleware for DatabaseMiddleware {
    type Provides = TargetDb;

    async fn call(&self, job: &JobMeta, next: Next<'_>) -> JobResult {
        let Some(target) = job.context::<Target>()? else {
            return Err(JobError::permanent(MissingTarget));
        };
        let pool = self.databases.get(&target.database).await?;
        next.provide(TargetDb { name: target.database, pool }).run().await
    }
}
```

Because the context is stored once, every retry of a job reaches the same database.

### Pools and connections

Create the pools your middleware uses when the worker starts and keep them in the middleware;
jalari only uses the pool given to `jalari::init`. Connect eagerly so a wrong address fails at
startup, and use `check` to verify each database. Give job pools a connection per concurrent job
and an `idle_in_transaction_session_timeout`, so a frozen or disconnected worker cannot hold
locks on your tables:

```rust
let pool = PgPoolOptions::new()
    .max_connections(8)
    .acquire_timeout(Duration::from_secs(5))
    .after_connect(|connection, _| Box::pin(async move {
        sqlx::query("SET idle_in_transaction_session_timeout = '30s'")
            .execute(&mut *connection)
            .await?;
        Ok(())
    }))
    .connect(&url)
    .await?;
```

The pool given to `jalari::init` may set session options such as `statement_timeout`, for
example as `-c` startup options. jalari never changes a connection's session settings. It only
uses `SET LOCAL`, which ends with its own transaction, so every connection returns to the pool
with the values it had.

- A running job keeps one transaction open for its whole run. jalari sets
  `idle_in_transaction_session_timeout` to 10 seconds in that transaction and turns off
  `transaction_timeout` on PostgreSQL 17 and later, so neither setting on the pool limits how
  long a job may run.
- Every other jalari statement runs under the pool's settings, `statement_timeout` included.
  None of them waits for long on its own. Jobs that share a `queue_key` are skipped while the
  key is taken, not waited for.
- An enqueue with a `job_key` waits while another open transaction is inserting or replacing
  a job with the same key, for example your own transaction that called `jalari::enqueue_in`.
  It fails with `jalari::ErrorKind::QueryCanceled` if the wait outlasts `statement_timeout`.
- `jalari::migrate` waits for a concurrent migration, and an upgrade waits for running jobs as
  described in [Creating the tables](#creating-the-tables). Give it a pool without a short
  `statement_timeout`, or apply the SQL from `jalari sql` with your own migration tool.

A job's writes to your own tables commit separately from jalari marking it done. If the worker
dies in between, the job runs again, so make those writes idempotent, for example with a table
keyed by `jalari::current_job()` id in the same transaction. When the job tables and your data
live in different databases, enqueueing inside your transaction is no longer possible either.

## Deployment

The same jobs can run in several shapes; pick per process.

```
 one process                      split processes
 ┌──────────────────────┐        ┌───────────┐     ┌──────────────────┐
 │ web server           │        │ web       │     │ worker binary    │
 │  enqueue             │        │  enqueue  │     │  worker.run      │
 │  worker.run (spawn)  │        └─────┬─────┘     └────────┬─────────┘
 └──────────┬───────────┘              └─────────┬──────────┘
            ▼                                    ▼
       PostgreSQL                           PostgreSQL
```

- A process that only enqueues calls `jalari::init` and never builds a `jalari::Worker`.
- Start more worker processes to add capacity; they need no configuration to find each
  other.
- Different workers can serve different queues, for example one machine for `reports`.
- The scheduler runs on every worker by default. Running it on several is safe and keeps
  schedules firing if one stops. Turn it off with `.scheduler(false)` on workers that should
  only process jobs, but keep at least one with it on.

## Cluster settings and cleanup

Settings shared by every worker live in the `config` table and change at runtime:

```rust
use std::time::Duration;

jalari::cluster::update(|cluster| {
    cluster.housekeeping_enabled = true;
    cluster.succeeded_retention = Duration::from_secs(3 * 24 * 60 * 60);
    cluster.failed_retention = Some(Duration::from_secs(30 * 24 * 60 * 60));
})
.await?;
```

`jalari::cluster::get()` reads the current values.

| `jalari::ClusterConfig` field | Meaning | Default |
|---|---|---|
| `housekeeping_enabled` | Delete finished jobs after their retention | off |
| `housekeeping_interval` | How often the cleanup runs across the cluster | 10 min |
| `succeeded_retention` | How long succeeded jobs are kept | 1 day |
| `deleted_retention` | How long deleted jobs are kept | 7 days |
| `failed_retention` | How long failed jobs are kept; `None` keeps them. A job with a `queue_key` whose last attempt was interrupted stays at least 6 s | `None` |
| `worker_timeout` | Missed-heartbeat time before a worker leaves the list | 5 min |

Workers apply changes immediately. One worker per interval performs the cleanup, deleting in
small batches. Waiting jobs are never deleted, however old.

## Monitoring

```rust
for worker in jalari::admin::workers().await? {
    println!("{}@{} scheduler={} last seen {}",
        worker.pid, worker.hostname, worker.scheduler, worker.heartbeat_at);
}

for schedule in jalari::recurring::list().await? {
    println!("{} next at {:?} after {} runs", schedule.name, schedule.next_run_at,
        schedule.run_count);
}

for count in jalari::admin::stats().await? {
    println!("{} {}: {}", count.queue, count.state, count.count);
}

for queue in jalari::admin::throughput(Duration::from_secs(3600)).await? {
    println!("{}: {} succeeded, {} failed, {} retried, max {:?}",
        queue.queue, queue.succeeded, queue.failed, queue.retried, queue.max_duration);
}
```

If no listed worker has `scheduler` set, recurring schedules are not firing. `stats` counts
the jobs in every queue by state; `throughput` sums up the job history of the last window,
so it only sees as far back as the retention housekeeping keeps.

### Listing jobs

```rust
let filter = jalari::admin::JobFilter::new()
    .state(jalari::JobState::Failed)
    .context(serde_json::json!({ "tenant_id": "acme" }));
let total = jalari::admin::count(&filter).await?;
let page = jalari::admin::jobs(&filter, 50, None).await?;
let next_page = jalari::admin::jobs(&filter, 50, page.last().map(|job| job.id)).await?;

if let Some(detail) = jalari::admin::job(page[0].id).await? {
    let job: SendWelcome = detail.job.payload_json()?;
    for entry in &detail.history {
        println!("{} attempt {} {:?}", entry.state, entry.attempt, entry.error);
    }
    if let Some(control) = &detail.control {
        println!("pending {:?} progress {:?}", control.pending, control.progress);
    }
}
```

`JobFilter` combines any of: states, queue, task, exact `job_key` or a prefix, a creation
time range, and a context the stored context must contain (PostgreSQL's `@>`, so
`{"tenant_id": "acme"}` matches whatever else the context holds). Pages are cut by id, newest
first: pass the last id of a page as `before` for the next one, and they stay consistent while
jobs come and go. `job` returns the job, every state change from its history and its control
row with any pending stop or cancel and the progress saved at its last checkpoint. Payloads
come back as the stored bytes with their codec; `payload_json` decodes the `json` ones.

Migration 6 adds a GIN index on the context column for the context filter. Every insert or
update of a job that has a context also updates that index; jobs without a context are not in
it. The index is built without `CONCURRENTLY`, so writes to the job table wait while the
upgrade runs.

### Retrying and rescheduling

```rust
match jalari::admin::retry(id).await? {
    jalari::admin::RetryOutcome::Retried => {}
    jalari::admin::RetryOutcome::KeyTaken(other) => println!("job {other} holds the key"),
    outcome => println!("{outcome:?}"),
}
let summary = jalari::admin::retry_failed(&filter).await?;
jalari::admin::run_now(id).await?;
```

`retry` makes a failed job due now with its attempts reset, so it gets its full
`max_attempts` again; `last_error` stays until an attempt succeeds, the change is recorded in
the history, and the queue's workers are notified. A failed job whose `job_key` is held by a
waiting, running or stopped job stays failed and the outcome names that job. `retry_failed`
does the same for every failed job matching the filter, in batches of 100 that each commit on
their own. `run_now` makes a scheduled job, or a retry waiting for its backoff, due now; a
running job, or one interrupted less than 6 seconds ago, is reported as `Running` and left
alone. To delete a job, [`cancel`](#stopping-restarting-and-cancelling) it: a waiting or stopped
job is marked deleted at once, a running one once it stops.

## Errors

Every fallible call returns `jalari::Error` with a `kind` to match on and a `msg` for people.

```rust
match jalari::recurring::pause("nightly_report").await {
    Ok(()) => {}
    Err(e) if e.kind == jalari::ErrorKind::RecurringManagedByCode => {
        println!("declared in code, change it with a deploy");
    }
    Err(e) => return Err(e.into()),
}
```

Database errors are classified by SQLSTATE. `ConnectionFailed`, `PoolTimedOut`,
`SerializationFailure`, `Deadlock` and `LockTimeout` are usually worth retrying.

## Examples

```bash
export DATABASE_URL=postgres://localhost/app
cargo run -p jalari --example migrate
cargo run -p jalari --example basic
cargo run -p jalari --example stop_restart
cargo run -p jalari --example middleware
cargo run -p jalari --example split_worker
cargo run -p jalari --example split_web
cargo run -p jalari --example split_control -- stop 42
```

| Example | Shows |
|---|---|
| [`migrate`](../jalari/examples/migrate.rs) | Creating and checking the tables; `--dry-run` prints the SQL instead |
| [`basic`](../jalari/examples/basic.rs) | Defining, enqueueing and running jobs in one process: errors and retries, `job_key` with `KeepExisting` and `Replace`, `queue_key`, delays, transactions, recurring schedules, `current_job`, `shutdown_requested`, cluster settings and monitoring. It runs until Ctrl-C |
| [`stop_restart`](../jalari/examples/stop_restart.rs) | `stop`, `restart` and `cancel` on waiting and running jobs, checkpoints with and without progress, `stop_timeout` aborting a job without checkpoints, and a shutdown that the next worker resumes from. It exits when done |
| [`middleware`](../jalari/examples/middleware.rs) | Capturing a context with `jalari::scope` and `EnqueueOptions::context`, middleware that wraps every job or provides a per-tenant database, `check`, rejected jobs, and a job that passes its context to the jobs it enqueues. It exits when done |
| [`split_web`](../jalari/examples/split_web.rs), [`split_worker`](../jalari/examples/split_worker.rs) and [`split_control`](../jalari/examples/split_control.rs) | The same jobs in separate binaries: one enqueues, one runs them (`--no-scheduler` turns its scheduler off), and one stops, restarts, cancels or shows a job from yet another process |

The examples use `jalari::Schema::default()`, so they share the `jalari` schema of the
database they run against.
