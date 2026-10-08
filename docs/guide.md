# jalari guide

How to set up jalari, define and enqueue jobs, schedule them and run workers. The API
reference with every option is on docs.rs; this guide shows how the pieces fit together.

- [Installation](#installation)
- [Setup](#setup)
- [Defining jobs](#defining-jobs)
- [Enqueueing](#enqueueing)
- [Recurring jobs](#recurring-jobs)
- [Running workers](#running-workers)
- [Deployment](#deployment)
- [Cluster settings and cleanup](#cluster-settings-and-cleanup)
- [Monitoring](#monitoring)
- [Errors](#errors)
- [Examples](#examples)

## Installation

```toml
[dependencies]
jalari = "0.1"
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
| Create | `#[jalari::job(cron = "...")]` | `jalari::recurring::add_or_update(name, cron, &job)` |
| Change | Edit the code and deploy | `update_schedule`, `pause`, `resume`, `remove` |
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

`jalari::recurring::trigger(name)` enqueues a schedule's job immediately, for either kind,
without moving its timetable. `jalari::recurring::list()` returns every schedule with its next
run.

### Cron format

- Five fields (`minute hour day month weekday`) or six with seconds first.
- Evaluated in the given IANA time zone, `UTC` by default. A repeated local hour during a
  daylight saving change fires once; a skipped one still fires.
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

### Retries

The default `jalari::ExponentialBackoff` waits 5 s, 10 s, 20 s... up to 1 hour, with 20%
jitter. Implement `jalari::RetryPolicy` for a different strategy. A job is marked failed when it reaches
`MAX_ATTEMPTS` or returns a permanent error.

An attempt is counted when it starts, so an attempt cut short by a crash counts too. When a
crashed worker's job is picked up again with no attempts left, it is marked failed with
`the worker stopped during the last attempt` instead of running once more. This keeps a job
that brings down its process from crashing workers forever.

### Shutdown

Cancelling the token stops the worker from claiming new jobs and waits for running ones.
Jobs still running after `shutdown_timeout` are aborted and count as an attempt; another
worker runs them again.

### Crashes and lost connections

When a worker process dies, PostgreSQL releases its jobs. The next worker that finds one
records the interrupted attempt in the job history and runs the job again 6 seconds later.

While a job runs, its worker checks the connection holding the job's lock every second. If
that connection is lost (the database restarts, the network fails, or the session is ended by
an administrator or by `idle_in_transaction_session_timeout`), the worker aborts the job
within 3 seconds. Other workers wait 6 seconds before running an interrupted job again, so the
old run has stopped before the new one starts. The wait also covers other jobs with the same
`queue_key`, since the interrupted run may still hold on to that key. The guarantee holds as long as a worker process
is not frozen for several seconds, for example by a virtual machine pause.

The same check keeps a long job's transaction busy, so `idle_in_transaction_session_timeout`
never ends it while the job is still running. jalari sets that timeout to 10 seconds on the
job's own transaction, overriding the server default: if a worker freezes or the network fails
without closing the connection, PostgreSQL ends the session after 10 seconds without a check,
and another worker takes the job over about 16 seconds after the failure instead of waiting
for TCP keepalives.

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
| `failed_retention` | How long failed jobs are kept; `None` keeps them | `None` |
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
    println!("{} next at {}", schedule.name, schedule.next_run_at);
}
```

If no listed worker has `scheduler` set, recurring schedules are not firing. Jobs and their
history are plain tables, so `SELECT state, count(*) FROM jalari.job GROUP BY state` works
too.

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
cargo run -p jalari --example basic
cargo run -p jalari --example split_worker
cargo run -p jalari --example split_web
```

- [`basic`](../jalari/examples/basic.rs) uses every feature in one process.
- [`split_web`](../jalari/examples/split_web.rs) and
  [`split_worker`](../jalari/examples/split_worker.rs) run the same jobs as separate web and
  worker binaries; pass `--no-scheduler` to a worker to turn its scheduler off.
