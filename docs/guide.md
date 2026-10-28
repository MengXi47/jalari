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
- [Progress and steps](#progress-and-steps)
- [Pausing queues](#pausing-queues)
- [Context and middleware](#context-and-middleware)
- [Deployment](#deployment)
- [Cluster settings and cleanup](#cluster-settings-and-cleanup)
- [Monitoring](#monitoring)
- [Known limits](#known-limits)
- [Errors](#errors)
- [Examples](#examples)

## Installation

```toml
[dependencies]
jalari = "0.8"
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

`jalari::init` takes a `PgPool`, or any type that implements `jalari::PoolSource` when the pool
lives in your own application state, and returns a `jalari::InitBuilder` that runs the setup
when awaited.

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
`jalari::SCHEMA_VERSION` is the version this release needs, and
`jalari::schema_version(&pool, &schema)` reads the installed one, `None` before the first
migration. `jalari::migrate(&pool, &schema)` creates or upgrades the tables without `init`, and
`jalari::migrate_to` stops at an older version for a database that must match an older release.

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

Version 7 adds the [code fingerprint](#code-fingerprints) columns: `fingerprint` on the job
table and `tasks` on the worker table. Both are nullable with no default, so the upgrade is
instant. Jobs enqueued before it have no fingerprint, and any worker that knows their task
runs them. A worker of an older release that is still running claims jobs without checking
fingerprints, so the check holds once every worker runs this release. The fingerprint includes
the jalari version, so every later jalari upgrade changes the fingerprint of every job. Jobs
enqueued by the old release then wait for workers of the old release. Let the queues drain
before you stop the last old worker, or release the jobs with
[`clear_fingerprint`](#jobs-that-wait-for-another-build) after the upgrade.

Version 8 switches to the lock model of 0.7: a running job is a `running` row that names its
worker, not a row locked by an open transaction. It adds five nullable columns to the job
table (`locked_by`, `locked_at`, `lock_token`, `queue_key_slot` and `command`), the `running`
and `discarded` states, an index of running jobs per worker and a unique index that keeps a
`queue_key` to one running job, and it moves pending stop and cancel requests from
`job_control` onto the job row. Every 0.6 process must stop before the upgrade, enqueuers
included: a 0.6 worker does not know the running state, and a 0.6 enqueue with a `job_key`
fails with `JobKeyContention` against a running job. The upgrade checks this where it can: it
fails with `LockTimeout` and a message naming 0.6 while a 0.6 worker still runs a job, and
changes nothing. It records the attempts that 0.6 workers left unfinished as interrupted and
settles the requests they left pending, as a 0.6 worker would have. The new checks are added
`NOT VALID`, since existing rows satisfy them by construction, so the upgrade does not scan
the tables to validate them; run `ALTER TABLE ... VALIDATE CONSTRAINT` later if you want them
marked valid. Building the indexes reads the job table and blocks writes to it until the
upgrade commits.

Version 9 ships with 0.7.0 as well. It adds a `priority` column to the `recurring` table, the
`parent_id`, `root_id`, `queue_key_limit` and `expires_at` columns to the job table, and the
`queue_pause` table. The new columns are nullable or have a constant default, so adding them
is instant. It moves any job priority outside the four [levels](#priority) to the nearest one,
which scans the job table once, and adds the job table's checks `NOT VALID`. It builds three partial
indexes, on `parent_id`, `root_id` and `expires_at`; each build reads the job table, and writes
to the job table wait until the upgrade commits. [Upgrading from 0.6](#upgrading-from-06)
lists the steps for both versions.

Version 10 ships with 0.8.0. It creates the `job_step` table that records [steps](#steps), adds
the `progress_seq` column to `job_control`, which numbers the copies of a job's progress, and
the `missed` column to the `recurring` table for the [missed-run policy](#missed-runs). Both
columns have a constant default, so adding them is instant; the foreign key from `job_step` to
the job table blocks writes to the job table until the upgrade commits, normally for a fraction
of a second. [Upgrading from 0.7](#upgrading-from-07) lists what to upgrade first.

## Defining jobs

A job is a serializable struct with an `impl Job` inside `#[jalari::job]`. Its fields are the
job's arguments.

```rust
use std::io;

use jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

#[jalari::job(name = "send_email", queue = "emails", max_attempts = 5, timeout = 30)]
impl Job for SendEmail {
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

| `#[jalari::job(..)]` argument | Meaning | Default |
|---|---|---|
| `name = "..."` | Stable name stored with every job; must be unique in the binary, among names and aliases | the type's name, as written |
| `queue = "..."` | Queue the job goes to unless the enqueue says otherwise; must not be empty | `default` |
| `max_attempts = 5` | Attempts before the job is marked failed, including the first and any interrupted by a crash | 10 |
| `timeout = 30` | Longest time one attempt may run, in seconds | none |
| `priority = "high"` | Which due jobs of the queue run first: `"low"`, `"normal"`, `"high"` or `"urgent"`; see [Priority](#priority) | `"normal"` |
| `aliases = ["old_name"]` | Earlier names whose waiting jobs and schedules this type still runs; see [Renaming a job](#renaming-a-job) | none |
| `checkpoints = true` | Declares that the job reaches [checkpoints](#checkpoints) regularly, so an abort at `stop_timeout` or `shutdown_timeout` is logged as an error | `false` |
| `cron = "..."`, `timezone = "..."` | Makes the job [recurring](#declared-in-code); both are parsed at compile time by the parsers the workers use, and the time zone is only valid with `cron` | none, `UTC` |
| `missed = "skip"` | What the scheduler does with a run it finds more than a minute overdue: `"once"` enqueues one catch-up run, `"skip"` none; only valid with `cron`; see [Missed runs](#missed-runs) | `"once"` |
| `fingerprint = true` | Enqueue with a [code fingerprint](#code-fingerprints) and run only jobs of the same build | `false` |
| `crate = ::path` | Path at which the generated code reaches jalari, written as a path or as a string literal, for crates that use it through another crate; see [Using jalari through another crate](#using-jalari-through-another-crate) | `::jalari` |

`name`, `max_attempts`, `timeout`, `priority` and `aliases` set the `Job` constants `NAME`,
`MAX_ATTEMPTS`, `TIMEOUT`, `PRIORITY` and `ALIASES`. Declaring a constant in the impl instead,
as 0.6 did, still works; setting one both ways is a compile error, and a timeout below one
second needs `const TIMEOUT` in the impl. `#[jalari::job]` rejects an empty name, a
`max_attempts` below 1, a zero timeout or one above `i32::MAX` milliseconds, and an alias that
is empty, equals the name or repeats, at compile time. It also rejects an empty `queue`, a
`cron` expression or `timezone` that a worker would refuse at startup, and a `cron` job whose
struct does not implement `Default`, each with the error on that argument. A struct that is not
a plain path, such as a generic one, needs `name`.

Two items stay in the impl. `type Context` is the value `run` receives from the worker's
middleware, `()` by default; see [Context and middleware](#context-and-middleware).
`RETRY_POLICY` replaces the worker's retry policy for this job type; see [Retries](#retries).

`run` returns `Ok(())` on success. Any error converts into a retryable `jalari::JobError`
through `?`. Return `jalari::JobError::permanent(e)` when retrying cannot help, for example
invalid input. A panic or a timeout counts as a failed attempt. A job that times out is
aborted at its current `.await`, and the worker waits up to a second for the aborted code to be
dropped before it records the outcome. [How an attempt ends](#retries) lists every outcome,
including snoozing and discarding.

Inside `run`, `jalari::current_job()` returns a `jalari::JobRun` for the attempt: its id, name,
queue, attempt number and `max_attempts`, when the job was created and when this attempt became
due, its priority, the previous attempt's error, and the job that enqueued it.

A job can run more than once if its worker dies mid-run, so keep `run` idempotent.
[Progress and steps](#progress-and-steps) shows how a job records how far it got, so the next
run does not repeat its effects.

### Renaming a job

The name is stored with every job and schedule. Keep the old name in `aliases` when you rename
a job, so the jobs and schedules stored under it still run:

```rust
#[jalari::job(name = "send_welcome", queue = "emails", aliases = ["welcome_email"])]
impl Job for SendWelcome {
    async fn run(self) -> JobResult {
        Ok(())
    }
}
```

New enqueues use the new name. A job stored under an alias runs through this type, and
`current_job().name()` reports the new name. A runtime schedule stored under an alias is
moved to the new name the next time it fires; `recurring::trigger` before that enqueues under
the alias, which still runs. A schedule declared in code under the old name is deleted and
created again under the new one, which resets its run count and last run. Code fingerprints
leave out names and aliases, so a fingerprinted job enqueued under the old name stays runnable
by the renamed build.

Once `jalari::admin::count(&JobFilter::new().task("welcome_email"))` is 0 and
`jalari::recurring::list()` shows no schedule with that task, remove the alias.

A job without `name` takes the struct's name, so renaming the struct renames the job. Add the
struct's old name to `aliases` in the same change. With `fingerprint = true`, renaming the
struct also changes the hashed code, so the jobs waiting under the old name wait for the old
build until you release them with `clear_fingerprint`; a rename through `name` does not.

### Code fingerprints

Fingerprints are off by default. Turn them on for a job with `fingerprint = true`:

```rust
#[derive(Serialize, Deserialize)]
struct ExportReport {
    report_id: i64,
}

#[jalari::job(name = "export_report", queue = "exports", fingerprint = true)]
impl Job for ExportReport {
    async fn run(self) -> JobResult {
        Ok(())
    }
}
```

`#[jalari::job]` then hashes the job's code when the crate compiles, giving a fingerprint of 32
hex digits. Every enqueue stores the fingerprint of the build that enqueued the job. A worker takes
a job only when the job has no fingerprint or when the job's fingerprint equals the worker's
own fingerprint for that task. A job enqueued by another build of its code waits for a worker
of that build, the same way a job under an unknown name waits for a worker that knows the name.

The fingerprint covers:

- the `impl Job` block and the definition of the job's struct
- every function, method, constant, static, type, trait implementation, `use` declaration and
  `macro_rules!` that the job's code reaches, directly or through other items, in the job's
  crate and in its `path` dependencies
- the `Cargo.lock` version of each registry crate that the reached code names
- the jalari version

Formatting, comments and code that the job cannot reach leave it unchanged. The same sources
and the same `Cargo.lock` give the same fingerprint on every machine and in every directory.

The name, the aliases, the priority, `RETRY_POLICY`, the `checkpoints` flag and the `missed`
policy are left out, in the attribute and in the impl, so renaming a job with an alias, changing
its priority, retry policy or missed-run policy, or declaring checkpoints leaves waiting jobs
runnable by the new build.
`max_attempts` and `timeout` are covered in either form, and the two forms hash differently:
moving `const MAX_ATTEMPTS: i32 = 4` into `max_attempts = 4` changes the fingerprint.

With [`crate`](#using-jalari-through-another-crate), the `crate` path is left out too, and so are
the job attribute's own path and every path that reaches jalari through the `crate` path: such a
path is hashed as the same path through `jalari`, and the crate it goes through is neither
analysed nor pinned for it. When the job names that crate by another path, such as
`platform::billing::rates`, the crate is covered like any other dependency.

The analysis matches names rather than resolving paths, so it errs toward a change. Editing a
function with the same name as one the job calls, in any module, changes the fingerprint too.
A method call reaches the methods of that name on the types the reached code names, and naming
a trait reaches all of its implementations. Code that a build script generates into `OUT_DIR`
and names that macros such as `paste!` build are not analysed. When the compiler does not
report the source file of the job, as in some IDE and non-Cargo builds, the fingerprint covers
every item of the crate and of its `path` dependencies.

This holds on your own machine as much as in production. After you edit a job and restart its
worker, the jobs that the earlier build enqueued, before the edit or from a process that still
runs the old code, wait, and the worker logs `no live worker runs these jobs, enqueued by
another build of the task` when it starts and every five minutes after. Restart every process
that enqueues the task too, or release the waiting jobs with
[`jalari::admin::clear_fingerprint`](#jobs-that-wait-for-another-build).

A job without `fingerprint = true` is enqueued without a fingerprint, and the workers of any
build run it whatever fingerprint it carries, including the jobs that jalari 0.6.0 and 0.6.1
stamped by default. Leave it off for jobs whose arguments and behavior stay compatible across
deploys. [Deployment](#deployment) describes rolling deploys, and
[Jobs that wait for another build](#jobs-that-wait-for-another-build) shows how to find and
release them.

### Using jalari through another crate

The code that `#[jalari::job]` generates names `::jalari`, so a crate that defines jobs must
otherwise depend on jalari directly. When your crates reach jalari through one crate of your
own, here called `platform`, that crate re-exports it:

```rust
#[doc(hidden)]
pub use jalari as __jalari;
```

A crate that depends only on `platform` then defines jobs with the re-exported attribute and
names the re-export in `crate`:

```rust
use platform::__jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendInvoice {
    invoice_id: i64,
}

#[platform::__jalari::job(name = "send_invoice", crate = ::platform::__jalari)]
impl Job for SendInvoice {
    async fn run(self) -> JobResult {
        Ok(())
    }
}
```

`crate` takes a path, written as the defining crate would write it, and every path the
attribute generates starts with it. A string literal holding the path, such as
`"::platform::__jalari"`, works the same. A value that is not a plain path, or a second
`crate`, is a compile error.

So that the crates that define jobs never write the path, `platform` can export a wrapper that
passes `$crate`:

```rust
#[macro_export]
macro_rules! job {
    (($($arguments:tt)*) $($item:tt)*) => {
        #[$crate::__jalari::job(crate = $crate::__jalari, $($arguments)*)]
        $($item)*
    };
}
```

```rust
platform::job! {
    (name = "send_invoice")
    impl Job for SendInvoice {
        async fn run(self) -> JobResult {
            Ok(())
        }
    }
}
```

The [code fingerprint](#code-fingerprints) reads every path through `platform::__jalari` as the
same path through `jalari`, so moving a fingerprinted job behind `platform`, renaming `platform`
or bumping its version leaves waiting jobs runnable, as long as the job reaches `platform` only
through `__jalari`.

A job declared through the wrapper is fingerprinted like a job written as a plain item.

`platform` can export an attribute macro instead, from a proc-macro crate of its own. The macro
emits the job attribute through the re-export in front of the item it receives, adding `crate`
to the arguments it was given:

```rust
quote! {
    #[::platform::__jalari::job(crate = ::platform::__jalari, #arguments)]
    #item
}
```

A crate that depends only on `platform` then writes `#[platform::job(name = "send_invoice")]` on
a plain `impl Job` block. Such a job is fingerprinted like the same job written against jalari,
whatever the attribute macro is called: the fingerprint reads its attribute as `#[jalari::job]`
with the arguments written there. Arguments that the macro adds itself are not among them. A
`queue`, `cron` or `timezone` that it adds is not covered, and a `max_attempts` or `timeout` that
it adds is covered through the constant it sets, but hashes differently from the same argument
written at the impl. Name the macro by its path, as above: in a crate that imports it with
`use platform::job;`, every fingerprinted job reaches `platform` through that import, and its
fingerprint covers `platform` and the macro's own code like any other dependency.

The repository's [`tests/facade`](../tests/facade) holds such a pair: `facade` re-exports jalari,
and `facade-consumer` defines a plain and a recurring job through it without depending on
jalari. `cargo test -p facade-consumer` builds them, checks every argument of the recurring
job's attribute, checks that its fingerprint equals that of the same job written against jalari,
declares a third job through `facade::job!` and checks its fingerprint the same way, and with
`DATABASE_URL` set runs the plain job and the third one on a worker. `facade-macros` provides
`#[facade::job_attr]`, an attribute macro that also sets `queue = "facade"` when no queue is
given, and the consumer declares a plain and a recurring job through it, checked the same way.

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
| `priority(p)` | Override the job type's [priority](#priority) for this job |
| `job_key(key)` | At most one waiting or running job per key |
| `on_conflict(..)` | `KeepExisting` (default) or `Replace` the waiting job with the same key |
| `queue_key(key)` | Jobs sharing the key never run at the same time |
| `queue_key_limit(n)` | Let up to `n` jobs sharing the `queue_key` run at once; see [Key limits](#key-limits) |
| `expire_after(d)` / `expires_at(t)` | Discard the job if it is still waiting at that time; see [Expiry](#expiry) |
| `max_attempts(n)` | Override `max_attempts` for this job; a value below 1 is `InvalidOptions` |
| `timeout(d)` | Override the job type's timeout for this job |
| `context(&value)` / `no_context()` | Set or drop the stored [context](#capturing-a-context) |

Every enqueue returns a `jalari::EnqueueOutcome`. Its `id()` is the job's `jalari::JobId`, which
`stop`, `cancel`, `restart` and the admin calls take:

| Outcome | Meaning |
|---|---|
| `Inserted(id)` | A new job was created |
| `Kept(id)` | A job with the same `job_key` already existed and was kept |
| `Replaced(id)` | The waiting job with the same `job_key` was overwritten |
| `Running(id)` | The job with the same `job_key` is running and could not be replaced |

`Replace` overwrites a job that is waiting, waiting for a retry or snoozed, including its
priority, `max_attempts`, key limit, expiry and the enqueuing job. A kept job keeps its own.
A running job returns
`Running` at once, including a job whose worker died less than about 8 seconds ago (see
[Crashes and lost connections](#crashes-and-lost-connections)); once another worker records
the interrupted attempt, the job is waiting again and the next `Replace` overwrites it. A job
that is stopped, or has a pending stop or cancel, is never replaced and returns `Kept` (see
[What a stop keeps and releases](#what-a-stop-keeps-and-releases)).

### Priority

```rust
#[jalari::job(name = "password_reset", queue = "emails", priority = "urgent")]
impl Job for PasswordReset {
    async fn run(self) -> JobResult {
        Ok(())
    }
}

let options = jalari::EnqueueOptions::new().priority(jalari::Priority::Low);
jalari::enqueue_with(&digest, options).await?;
```

Workers take the due jobs of a queue from the highest priority down, `Urgent`, `High`,
`Normal` and `Low`, and within a level the earliest `run_at` first. Priority never makes a job
run before its `run_at`, never interrupts a running job and is kept across retries. A lower
level runs only while no higher job of the queue is due, so keep `Urgent` for few jobs; a
steady stream of them holds every other level back. A future job of a high priority does not
delay due jobs of a lower one. `jalari::admin::set_priority` moves jobs that are already
waiting; see [Retrying and rescheduling](#retrying-and-rescheduling).

On PostgreSQL 14 to 17 the claim reads the queue's waiting jobs in priority order from an
index that also holds jobs that are not due yet. Every claim reads past the future jobs of
the higher levels before it reaches a due job of a lower one, so many far-future
high-priority jobs, such as 100,000 urgent jobs scheduled for tomorrow, slow every claim of
their queue on those versions. PostgreSQL 18 skips them with a skip scan. On the older
versions, keep such jobs in a queue of their own.

The levels are stored as -1, 0, 1 and 2 in the job table's `priority` column, which a check
keeps in that range.

### Key limits

```rust
let options = jalari::EnqueueOptions::new()
    .queue_key(&format!("tenant:{tenant_id}"))
    .queue_key_limit(4);
jalari::enqueue_with(&SyncInvoices { tenant_id }, options).await?;
```

A `queue_key` without a limit lets one job of the key run at a time. With `queue_key_limit(n)`
a job runs only while one of the first `n` slots of its key is free, `n` being its own limit,
so jobs of one key with different limits share the same slots: a job with no limit needs slot
0 and waits while a wider job holds it. A claim takes the highest free slot below the job's
limit, which leaves slot 0 to jobs without a limit as long as possible. A limit of 0 or one
without a `queue_key` is `InvalidOptions`. A slot is freed when the job leaves `running`,
including a takeover after its worker crashed, so a slot never has two runs at once.

Jobs without a limit cost what a `queue_key` always cost: the claim finds the keys whose slot
0 is taken once per statement. A job with a limit counts the running jobs of its key for
every waiting candidate the claim passes, so a queue with thousands of waiting limited jobs on
saturated keys pays that count on every claim. Two claims that race for the last slot of a
key are settled by the database; the loser tries again, up to `max_claim_tries`.

### Expiry

```rust
let options = jalari::EnqueueOptions::new()
    .job_key(&format!("otp:{user_id}"))
    .expire_after(Duration::from_secs(120));
jalari::enqueue_with(&SendOneTimeCode { user_id }, options).await?;
```

A job with an expiry is discarded if it is still waiting at `expires_at`; a running attempt is
never cut short. That covers every wait: the first run, a retry, a snooze, a job given back at
shutdown and a restarted job. `expire_after` counts from the enqueue on the database clock,
`delay` included. No worker starts a job past its expiry, and every worker settles expired
jobs within about a second: the job becomes `discarded` with `the job expired while waiting to
run`, its `job_key` is free again and housekeeping removes it after `deleted_retention`.
Paused queues expire too. `jalari::admin::retry` of a failed job past its expiry answers
`Expired` and leaves it failed, and `jalari::restart` of a stopped one answers `Expired` and
leaves it stopped.

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

### Many jobs at once

```rust
let digests: Vec<_> = users
    .iter()
    .map(|user| {
        let options = jalari::EnqueueOptions::new().job_key(&format!("digest:{}", user.id));
        (SendDigest { user_id: user.id }, options)
    })
    .collect();
let outcomes = jalari::enqueue_many(&digests).await?;
```

`jalari::enqueue_many` inserts every job with one statement per 5,000 jobs or 32 MiB of
payload, all in one transaction, and wakes each queue's workers once. The outcomes come back
in input order. A `job_key` that appears twice in the batch answers `Kept` with the first
job's id. `jalari::enqueue_many_in` does the same on your connection, inside your transaction.
Both reject `OnConflict::Replace` with `InvalidOptions` before anything is sent. Two batches
whose keys overlap take the keys in the same order, so they wait on each other instead of
deadlocking.

### After the job succeeds

A job that enqueues follow-up work with `jalari::enqueue` creates it at once, so a retry of
the job creates it again. `jalari::enqueue_on_success` creates it only when the attempt
succeeds, in the same statement that records the success:

```rust
async fn run(self) -> JobResult {
    let report = build_report(self.month).await?;
    jalari::enqueue_on_success(&EmailReport { id: report.id })?;
    Ok(())
}
```

```rust
let options = jalari::EnqueueOptions::new()
    .job_key(&format!("poll:{}", self.feed))
    .delay(Duration::from_secs(60));
jalari::enqueue_on_success_with(&PollFeed { feed: self.feed.clone() }, options)?;
```

- The job is serialized at the call, with the running job's context, and kept with the
  attempt. It is inserted only if the attempt ends `succeeded`. A retry, a failure, a snooze, a
  discard, a stop, a cancel, a shutdown or a crash drops it, and the next attempt enqueues
  again. A job that returns `Ok` after a stop was requested still succeeds and inserts it.
- Its delay counts from the success. It may reuse the running job's own `job_key`, since the
  same statement frees that key, so a job can reschedule itself as in the second example.
- Only `KeepExisting` is accepted: a job whose key is taken at the success is dropped.
  `Replace` is `InvalidOptions`. The call returns no id; use `jalari::enqueue` when you need
  one.
- Called outside a running job's own task, including tasks the job spawns, it returns
  `NotInJob` instead of enqueueing.
- The keyed jobs are inserted in key order, so two jobs that succeed together with crossing
  keys never deadlock. A key held by a transaction that has not committed, such as your own
  `enqueue_in`, can hold the success back for at most 2 seconds. After that, or on any other
  database error from the insert, the attempt is recorded as a failed attempt with `could not
  enqueue the jobs buffered for success`, the retry policy decides what follows, and the next
  attempt enqueues again.

### Jobs enqueued by jobs

A job enqueued from inside a running job records that job as its parent, and the first job of
the chain as its root. `jalari::current_job()` reports both:

```rust
for page in 0..self.pages {
    jalari::enqueue(&ExportPage { export_id: self.export_id, page }).await?;
}

let run = jalari::current_job().expect("inside a job");
tracing::info!(root = %run.root_id(), parent = ?run.parent_id(), "page export");
```

`parent_id()` is `None` for a job enqueued from outside a job, and `root_id()` is then the
job's own id. Jobs from `enqueue_many` and `enqueue_on_success` record the same. Only the
job's own task records a parent: a task the job spawns has no running job, and an enqueue
whose future was created outside the job records none either. A `Replace` from inside a job
gives the replaced job the replacer's parent and root, and a `Replace` from outside clears
them. [Listing jobs](#listing-jobs) finds a job's children and its whole tree.

## Recurring jobs

There are two kinds of schedules, and they never affect each other.

| | Declared in code | Added at runtime |
|---|---|---|
| Create | `#[jalari::job(cron = "...")]` | `jalari::recurring::add_or_update(name, schedule, &job)` |
| Change | Edit the code and deploy | `update_schedule`, `pause`, `resume`, `remove` |
| Schedule | Cron only | Cron or interval, with optional end conditions |
| Arguments | `Default::default()` | Any value you pass |
| Name | The job's `NAME` | Any name you choose |

Each run of a task with `fingerprint = true` carries the [code fingerprint](#code-fingerprints)
of the task as registered in the process that enqueues it, the scheduler or a
`recurring::trigger` call, so a schedule follows the deployed code. A `trigger` from a process
that does not link the task enqueues the run without a fingerprint.

A schedule stores the job type's [priority](#priority) when it is written, and every run is
enqueued with it, also by a `trigger` from a process that does not link the task. A schedule
declared in code takes the current priority whenever a worker with the scheduler starts; a
runtime schedule takes it at its next `add_or_update`. Schedules created before 0.7.0 run at
`Normal` until then. `RecurringInfo::priority` shows it.

### Declared in code

```rust
#[derive(Serialize, Deserialize, Default)]
struct NightlyReport;

#[jalari::job(
    name = "nightly_report",
    cron = "0 0 3 * * *",
    timezone = "Asia/Taipei",
    queue = "reports"
)]
impl Job for NightlyReport {
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
start of your application is safe. A different cron or interval starts over.

`jalari::recurring::trigger(name)` enqueues a schedule's job immediately, for either kind,
without moving its timetable. `jalari::recurring::list()` returns every schedule with its next
run, its run count and when it finished. `Cron`, `Interval` and `Schedule` each have
`next_after(t)`, which computes the next run after `t` without the database, to check a
schedule before you store it; the one on `Schedule` ignores the end conditions.

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
  rounds, so a long outage costs nothing. A run found overdue fires once, or not at all under
  the `Skip` policy for [missed runs](#missed-runs).

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
- `update_schedule` or `add_or_update` with a different cron or interval resets the run count
  to zero and un-finishes it. Changing only `until` or `max_runs` keeps the count: raising
  `max_runs` from 10 to 20 after 10 runs gives 10 more, and lowering it below the count
  finishes the schedule. The same schedule leaves it as it is. Times are kept to whole
  microseconds, as the database stores them.

### Cron format

- Five fields (`minute hour day month weekday`) or six with seconds first.
- The day fields also take `L` for the last day of the month or the last given weekday
  (`5L`), `#` for the nth given weekday (`1#2`) and `W` for the weekday nearest a day
  (`15W`).
- Evaluated in the given IANA time zone, `UTC` by default. A repeated local hour during a
  daylight saving change fires once; a skipped one fires at the first valid time after the gap.
- After an outage, each schedule enqueues one catch-up run, not every missed one, unless its
  policy is to skip [missed runs](#missed-runs).
- Each scheduled time is enqueued at most once, however many workers run the scheduler.
- While a schedule's previous job is still waiting or running, no second one is added.

### Missed runs

A run is missed when the scheduler finds it more than a minute overdue, after an outage or
while no worker ran the scheduler. The schedule's policy decides what happens to it:

| Policy | The missed run | The next run |
|---|---|---|
| `Missed::Once`, the default | One catch-up run is enqueued for all the missed ones | The first one after now |
| `Missed::Skip` | Nothing is enqueued; a warning names the schedule and how late the run was | The first one after now |

```rust
#[derive(Serialize, Deserialize, Default)]
struct SampleMetrics;

#[jalari::job(name = "sample_metrics", cron = "0 */5 * * * *", missed = "skip")]
impl Job for SampleMetrics {
    async fn run(self) -> JobResult {
        Ok(())
    }
}

let every_five_minutes = jalari::Schedule::from(jalari::Cron::new("0 */5 * * * *")?)
    .missed(jalari::Missed::Skip);
jalari::recurring::add_or_update("metrics:eu", every_five_minutes, &sample).await?;
```

- A run less than 60 seconds late always fires under both policies, even on a 10-second
  interval, so ordinary scheduler lateness never skips a run. The 60 seconds are fixed.
- A skipped run does not count toward `max_runs` and leaves `last_run_at` as it was. A
  schedule whose `until` has passed finishes, as it would after a fired run.
- In the attribute, `missed` requires `cron`, and a value other than `"once"` or `"skip"` is a
  compile error. It is left out of the [code fingerprint](#code-fingerprints).
- A declared schedule takes its policy at every worker start, as it takes its priority. A
  runtime schedule takes it from the `Schedule` given to `add_or_update` or
  `update_schedule`, so a plain `Cron` or `Interval` sets it back to `Once`. Changing only the
  policy keeps the run count and the next run. `RecurringInfo::missed` shows it.

The [`missed_runs`](../jalari/examples/missed_runs.rs) example makes two hourly schedules, one
per policy, 3 hours overdue and prints what each enqueued, then shows a run 10 seconds late
firing under `Skip`.

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
queues it lists. A running job holds no connection: workers use the pool only for the short
statements that claim jobs, store their progress and finish them, so the concurrency does not
depend on the pool size.
The pool needs at least two connections, one for the notification listener and one for those
statements; `build()` checks this. Each worker also opens one session of its own, its
[lifeline](#crashes-and-lost-connections).

| `jalari::WorkerConfig` field | Meaning | Default |
|---|---|---|
| `poll_interval` | Fallback check when no notification arrives | 5 s |
| `shutdown_timeout` | How long shutdown waits for running jobs before aborting them | 30 s |
| `max_claim_tries` | Retries of a claim that lost a race for the same `queue_key` slot | 10 |
| `stop_timeout` | How long a stopped or cancelled job may take to reach a checkpoint before it is aborted | 30 s |

Each free slot of a queue claims one job with a single statement: the due job with the highest
[priority](#priority), then the earliest `run_at`, then the lowest id. The claim skips the
jobs of a [paused queue](#pausing-queues), jobs past their [expiry](#expiry), jobs whose
`queue_key` has no free [slot](#key-limits), and jobs enqueued by another build of a
fingerprinted task. A claim that loses a race for a key's last slot tries again at once, up
to `max_claim_tries` times; out of tries, it waits for the next notification or
`poll_interval`.

### Retries

| The attempt | The job becomes | Attempt counted | `last_error` | Saved progress |
|---|---|---|---|---|
| Returns `Ok(())` | `succeeded` | yes | cleared | dropped |
| Returns an error with attempts left | `scheduled`, after the retry policy's delay | yes | the error | kept |
| Returns an error on its last attempt, or a `JobError::permanent` | `failed` | yes | the error | dropped |
| Returns `JobError::discard(e)` | `discarded` | yes | the error | dropped |
| Returns `jalari::snooze(delay)` | `scheduled`, after `delay` | no | unchanged, except that an interrupted attempt's note is cleared | kept |
| Returns the error from a checkpoint or `jalari::interrupted()` during [shutdown](#shutdown) | `enqueued` | no | unchanged | kept |
| Panics or times out | as for an error | yes | the panic or timeout | as for an error |

A pending [stop or cancel](#stopping-restarting-and-cancelling) wins over every row but the
first. The default `jalari::ExponentialBackoff` waits 5 s, 10 s, 20 s... up to 1 hour, with 20%
jitter. Implement `jalari::RetryPolicy` for a different strategy, and set it for the worker
with `WorkerBuilder::retry_policy` or for one job type with `RETRY_POLICY` in the impl:

```rust
#[jalari::job(name = "deliver_webhook", queue = "webhooks", max_attempts = 20)]
impl Job for DeliverWebhook {
    const RETRY_POLICY: Option<&'static dyn jalari::RetryPolicy> =
        Some(&jalari::ExponentialBackoff {
            base: Duration::from_secs(30),
            max: Duration::from_secs(6 * 60 * 60),
            jitter: 0.3,
        });

    async fn run(self) -> JobResult {
        Ok(())
    }
}
```

The policy comes from the build that runs the attempt, so jobs already waiting follow a new
policy after a deploy. `EnqueueOptions::max_attempts` overrides the job type's `max_attempts`
for one job, and `jalari::admin::retry` gives that job as many attempts again.

`JobError::retry_after(delay)` keeps the failure but replaces the policy's delay, for a rate
limit that says when to come back. The attempt still counts, and on the last attempt or for a
permanent error it changes nothing. `jalari::snooze(delay)` is not a failure: the job runs
again after `delay` with the same attempt number, its progress and `job_key`, and its
`queue_key` is free while it waits. Middleware can return a snooze too, for example from a
rate limiter. There is no limit on how often a job snoozes, so a job that always snoozes runs
forever. Every delay is capped at 100 years.

`JobError::discard(e)` ends the job for good without making it a failure: a discarded job is
not `failed`, so `retry` and `retry_failed` leave it alone, `throughput` counts it apart, and
housekeeping removes it after `deleted_retention`. Use it when the work no longer makes
sense, such as a user that was deleted.

An attempt is counted when it starts, so an attempt cut short by a crash counts too, unless it
ends in a [stop or cancel](#stopping-restarting-and-cancelling). When another worker takes
over a crashed worker's job with no attempts left, it marks the job failed with
`the worker stopped during the last attempt` instead of running it once more. This keeps a
job that brings down its process from crashing workers forever.

### Shutdown

Cancelling the token stops the worker from claiming new jobs and waits for running ones.
Jobs still running after `shutdown_timeout` are aborted and count as an attempt; the worker
saves their progress and hands them back, and another worker runs them again 7 seconds later,
so whatever the aborted run spawned has time to stop. A job that reaches a
[checkpoint](#checkpoints) during the shutdown gets an error from it; when it returns that
error, the job goes back to its queue without counting the attempt and keeps its saved progress.

A job blocked in a long `.await` can notice the shutdown, a stop or a cancel at once:

```rust
for id in ids {
    tokio::select! {
        error = jalari::interrupted() => return Err(error),
        result = export(id) => result?,
    }
}
```

`jalari::interrupted()` completes with the error to return: the same one a checkpoint would
return. Return it unchanged. During a shutdown any other error counts as a failed attempt; a
stop or a cancel settles the job whatever it returns. When both branches
are ready at once, either may win, so the unit of work must be safe to run again.
`jalari::shutdown_requested()` covers shutdown only and gives no error; return
`jalari::checkpoint().await?` from its branch.

`jalari::current_job()` returns the running attempt. Its id stays the same across retries, so
it works as an idempotency key. `current_job()` and `interrupted()` only work in the job's own
task. A task the job spawns gets the same signal from `run.interrupted()`, where `run` is the
`current_job()` value taken before the spawn; it also completes once the attempt timed out,
with the shutdown error although only that attempt ended.
`run.cancellation()` is the shutdown signal as a `CancellationToken`, for code that takes one;
it is also cancelled once the attempt timed out.

### Crashes and lost connections

Each worker holds a lifeline: one session outside the pool, named `jalari lifeline <worker id>`
in `pg_stat_activity`, that keeps an advisory lock on the worker's id and records a heartbeat
every second. The lifeline runs on a thread of its own, so job code that blocks the worker's
runtime threads does not delay it. PostgreSQL itself ends a lifeline that stays silent for 11
seconds, through `idle_session_timeout` set on that session only, and the lock goes with the
session. The worker also sets `tcp_user_timeout` to 11 seconds there, which only servers that
support it (Linux) apply; others, macOS among them, ignore it and report 0, and
`idle_session_timeout` alone ends the session. Every worker checks every second, and once when
it starts before it claims anything, for running jobs whose worker's lock is gone and whose last
heartbeat is at least 7 seconds old, and takes them over.

| What happened | Old run stops | Job runs again |
|---|---|---|
| The worker process dies | at once | 6 to 8 s later |
| The worker's host vanishes without closing its connections (power cut, kernel panic, destroyed VM) | at once | 10 to 12 s later |
| The lifeline is ended while the process lives (`pg_terminate_backend`) | within about 1 s | 6 to 8 s later |
| The database restarts | within about 1 s | 6 to 8 s later if the database is back by then, otherwise within about 1 s of it accepting connections again |
| The worker freezes for less than 10 s | never; it continues | never |
| The worker freezes for 10 s or more | as soon as it wakes up | 10 to 12 s after the freeze began |
| Job code blocks every runtime thread of the worker | never; it continues | never |
| The database answers heartbeats with errors, such as statement timeouts while the worker row is locked | never; it continues | never |
| The network goes silent | within 3 s | 10 to 13 s after the failure |
| One of the jobs holds a [`save_in`](#durable-progress) transaction open when its worker freezes or loses its lifeline | as above | that job about 11 s after the transaction went idle; the worker's other jobs as above |

A job taken over continues from the progress its [checkpoints](#checkpoints) last saved, and
the interrupted attempt is recorded in the job history with
`the worker stopped during the last attempt`. When that attempt was the job's last, the job
fails instead. Jobs sharing the interrupted job's `queue_key` wait until it is taken over. A
pending stop or cancel is settled at the takeover and the job does not run again.

A transaction that `checkpoint.save_in` wrote to keeps the job's control row locked until it
ends, and a job cannot be taken over while its row is locked. `save_in` sets
`idle_in_transaction_session_timeout` to 10 seconds for that transaction, so PostgreSQL ends it
once a frozen or stopped worker leaves it idle, and the job is taken over at the next check. The
worker's other jobs are not held back: their takeover and the worker's batched progress writes
pass over the locked row. In jalari's tests, with one job holding such a transaction when its
worker's lifeline was ended, the three other jobs ran again 7.0 seconds after the lifeline ended
and the holding job 11.1 seconds after its transaction went idle, and on a live worker another
job's progress was written 5.0 seconds after the row was locked, at the next regular write.

A worker whose heartbeat gets no answer within 2 seconds, or whose lifeline connection
breaks, aborts every job it runs, closes its lifeline and registers again under a new id. The
7 second wait covers the time such a worker needs to notice, so the old run has stopped before
the new one starts.

When PostgreSQL answers a heartbeat with an error instead, for example a statement timeout
because a lock holds the worker's row, the session is alive and keeps its lock, so no other
worker can take the jobs over. The worker logs a warning, keeps its jobs running and sends the
next heartbeat. Its claims are refused once its last successful heartbeat is 3 seconds old, so
it starts nothing new until a heartbeat succeeds again. In jalari's tests a job ran once, with
attempt 1, through a 12 second stall, and a job enqueued during the stall started about 0.1
seconds after it ended. An error that ends the session, such as `pg_terminate_backend`, still
aborts the jobs at once. After 30 seconds without a successful heartbeat the worker gives up as
well: it aborts its jobs and registers again, 30.2 seconds into such a stall in jalari's tests.
The heartbeat grows old during a stall, so when the lifeline breaks in one, another worker can
take the jobs over within about a second instead of 7; a refused heartbeat is followed by the
next one right away, so the worker learns of the break at once, and in jalari's tests the old
run stopped within milliseconds, before the job ran again.

The worker also keeps its own count on its monotonic clock: it trusts its lifeline for 11
seconds after it sent the last heartbeat the database answered, the same 11 seconds after which
PostgreSQL ends a silent lifeline. A takeover needs that session gone, and the server only
starts counting once the heartbeat has arrived, so a worker that may have been taken over always
finds its 11 seconds gone; one that wakes just past them may abort although its lock was still
held. A worker frozen for 10 seconds or more, for example by a virtual machine pause, is taken
over while it is frozen; when it wakes up those 11 seconds have passed, so it aborts its jobs at
once, within milliseconds in jalari's tests, without waiting for the database to answer. In that
moment the job's writes to the job tables are rejected, but its own side effects are not.

Aborting a job drops its task at its next `.await`. Code that does not yield keeps running: a
blocking loop, `spawn_blocking`, threads, child processes and tasks the job spawned itself. In
every case above such code can still run while the job runs again elsewhere, for as long as it
takes. Blocked runtime threads do not cost the worker its lifeline: in jalari's tests two jobs
that blocked both runtime threads for 12 seconds each ran once and the worker kept its id. A
runtime that has not run for 10 seconds cannot stop or time out its jobs, so the lifeline logs
an error, `the job runtime has not run for 10 s`, and an info line when the runtime runs
again; it only logs, and the worker keeps its lifeline and its jobs.

A worker also hands back jobs it owns but no longer runs, within a second, and they run again 7
seconds later. When the database committed a claim but the reply never reached the worker, the
job never started, so the attempt does not count. The exception is a claim that commits just
before the worker loses its lifeline: that job is taken over with the others, and the attempt
counts although it never ran.

A worker keeps trying to record a finished attempt's outcome while the database does not
answer, for 30 seconds, also after it lost its lifeline; a try that waits for a pool
connection can add up to the pool's `acquire_timeout` to that. The owner check on the job row
decides: the outcome is recorded only if no other worker has taken the job over, and a run
that lost the job writes nothing. A job that finishes while the database restarts therefore
keeps its outcome when the database is back within about 7 seconds; after a longer outage the
retry and the takeover race once the database is back. When the outcome still is not
recorded after 30 seconds, or the database refuses it five times a second apart, the attempt
counts and is recorded as interrupted, so a job whose outcome can never be saved still stops at
its `max_attempts`.

If no worker runs at all, a dead worker's jobs stay `running` until a worker starts, and
`jalari::admin::stranded()` lists them. Do not end lifeline sessions from scripts that clean up
idle connections: every job of that worker is aborted and taken over.

## Stopping, restarting and cancelling

A stop frees everything a job holds: its task, the `queue_key`, and the worker's slot, plus
whatever the job's own code holds as it returns. The job stays in the database as `stopped`
until you restart or cancel it, also across worker restarts.

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
outside a job. A job that finishes discards its progress, and so does a `Replace`. When a
crash must not lose the copy, [store it before going on](#durable-progress).

A job that waits in one long `.await` between checkpoints races it against
`jalari::interrupted()`, as in [Shutdown](#shutdown); it then stops within about a second of
`stop` instead of waiting for `stop_timeout`.

Declare `checkpoints = true` on a job that reaches checkpoints regularly. When such a job is
aborted at `stop_timeout` or `shutdown_timeout`, the worker logs an error that says how long it
went without a checkpoint, or that it kept running after a checkpoint asked it to stop. The
flag changes nothing else.

### Blocking code

Blocking code belongs in `spawn_blocking`, and it can stop itself there. `jalari::interrupted()`
and `jalari::current_job()` read a task-local that `spawn_blocking` does not carry, so they do
not work inside it. Take the run handle in the job's own task, move it in, and poll
`is_interrupted()` between pieces of work:

```rust
let run = jalari::current_job().expect("inside a job");
let polled = run.clone();
let finished = tokio::task::spawn_blocking(move || {
    for chunk in chunks {
        if polled.is_interrupted() {
            return false;
        }
        process(chunk);
    }
    true
})
.await?;
if !finished {
    return Err(run.interrupted().await);
}
```

`is_interrupted()` turns `true` when `interrupted()` would complete: on a stop, a cancel, a
shutdown or a lost job, and once the attempt has timed out and been aborted. It never turns
back. Once the loop has returned, return the error of `run.interrupted()`, which is ready at
once, so a stop, a cancel or a shutdown settles the job as it does for async code. In jalari's
tests such a loop, polling every 10 milliseconds, saw a stop about 1.0 second after the request
and a 1 second timeout 1.0 seconds after the attempt started. The
[`blocking_work`](../jalari/examples/blocking_work.rs) example stops such a loop and times one
out.

### What each call does

| Call | Waiting job | Running job | Stopped job | Finished job |
|---|---|---|---|---|
| `stop` | `Stopped` at once | `Requested` | `Stopped` | `Finished` |
| `cancel` | `Cancelled` at once | `Requested` | `Cancelled` | `Finished` |
| `restart` | `NotStopped` | `NotStopped` | `Restarted` | `NotStopped` |

The calls return a `jalari::StopOutcome`, a `jalari::CancelOutcome` and a
`jalari::RestartOutcome`. An id with no job returns `NotFound`. A job whose worker died less
than about 8 seconds ago counts as running.

A running job sees a request within about a second. How it ends:

| The job | It ends as |
|---|---|
| Returns the checkpoint's error, or the error from `interrupted()` | `stopped` (or `deleted` for a cancel) with the progress it saved last |
| Reaches no checkpoint within `stop_timeout` | aborted at its current `.await`, then `stopped` with the last saved progress; the worker waits up to a second for the aborted code to be dropped first |
| Returns any other error, panics or times out | `stopped` (or `deleted`), not retried |
| Returns `Ok` first | `succeeded`; a later `stop` returns `Finished` |
| Its worker crashes | `stopped` (or `deleted`) by another worker 6 to 8 seconds later; it never runs again |

`stop` and `cancel` are safe to repeat and report the current state, so call them again until
they stop returning `Requested`. A cancel wins over a stop that is still pending.

A snoozed job is waiting, so `stop` settles it at once. `restart` keeps its snooze time, and
`jalari::admin::run_now` after the restart makes it due at once. A stopped job past its
[expiry](#expiry) cannot be restarted: `restart` answers `Expired`.

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
  outlive the stop, as with `timeout` and shutdown aborts. After a `stop_timeout` or `timeout`
  abort, the worker waits up to a second for the job's task to be dropped, so drop guards have
  run before the outcome is recorded. A job that never yields cannot be dropped: its outcome is
  recorded a second after the abort, and the stop takes up to `stop_timeout` plus a second.
  [Blocking code](#blocking-code) shows how code in `spawn_blocking` stops itself.
- A job a worker aborted at shutdown keeps waiting the 7 seconds after the abort when it is
  stopped and restarted or retried in between.

## Progress and steps

A job that a crash, a retry, a takeover or a restart interrupts runs again from the start,
unless it recorded how far it got. [Checkpoints](#checkpoints) keep a cursor in memory and
write it every few seconds; the calls below record a cursor or a step's result before the job
goes on, so the next run neither loses it nor repeats the work behind it.

| Call | What a crash keeps | Cost | Use it for |
|---|---|---|---|
| `checkpoint.save(&progress)` | the progress, up to about 6 seconds behind | none | a loop whose units may run again without harm |
| `checkpoint.save_now(&progress).await?` | the progress, stored before the call returns | one short transaction | a loop whose units have effects outside the database, such as a page sent to another system |
| `checkpoint.save_in(&mut transaction, &progress).await?` | the progress, committed with your rows | one statement in your transaction | a loop that writes to tables in the database jalari uses |
| `jalari::step(key, f).await?` | each step's output, stored once `f` returns | two short transactions | a few named effects whose results later steps need |

### Durable progress

`checkpoint.save` keeps its copy in memory, so a crash loses the last few seconds of progress
and the job repeats that work. `save_now` and `save_in` store the copy before the job goes on:

```rust
let (saved, checkpoint) = jalari::resume::<Cursor>()?;
let mut cursor = saved.unwrap_or_default();
while let Some(batch) = fetch(cursor.last_id).await? {
    let mut transaction = pool.begin().await?;
    insert_rows(&mut transaction, &batch).await?;
    cursor.last_id = batch.last_id;
    checkpoint.save_in(&mut transaction, &cursor).await?;
    transaction.commit().await?;
}
```

With `save_in`, a unit's rows and its progress commit together, so a retry, a takeover or a
restart never applies a unit twice. With `save_now`, an effect outside the database can still
happen twice when the worker dies after the effect and before the call returns; give such
effects an idempotency key.

- **Fenced.** Both writes check the run's lease. Once another worker has taken the job over,
  they store nothing and return the lost error, and the job is aborted at its next `.await`
  even when the error is ignored. A leftover call from an earlier run, for example from a task
  the job spawned, is refused the same way.
- **The newest copy wins.** Every copy carries a number, and the database keeps the highest.
  `save`, `save_now`, `save_in` and the copy the worker writes when the attempt ends can land
  in any order; a rolled-back `save_in` leaves no trace, and an older copy never replaces a
  committed newer one.
- **Stop points.** Both return the stop error when the job was asked to stop, like `save`.
  `save_now` then keeps the copy for the stopped state without writing; `save_in` writes first
  and then returns the error, so the transaction rolls back with the unit it carried when you
  propagate the error.
- **What a crash loses.** At most about 6 seconds of progress saved with `save`: one second
  before a copy is kept, and five before it is written. A copy for which `save_now` returned,
  or whose `save_in` transaction committed, is never lost.
- **`save_now`** waits at most 2 seconds for a lock. A database error means the copy may have
  been stored; the worker writes it again later. A missing control row is a permanent error: the
  pool given to `jalari::init` reaches another database or schema than the worker.
- **`save_in`** does not commit; call it as the last statement before `commit`. The
  transaction must run on the database and schema jalari uses, at `READ COMMITTED`, the
  default; under `REPEATABLE READ` or `SERIALIZABLE` it fails with a serialization error when
  the worker wrote the job's progress after the transaction's snapshot, and the unit can be
  retried in a new transaction. Commit within 10 seconds: the call sets
  `idle_in_transaction_session_timeout` to 10 seconds for the rest of the transaction, unless a
  shorter limit is set already. Make no other jalari call for the same job on another
  connection before the commit: `save_now` and `jalari::step` would wait for the lock the
  transaction holds and fail after 2 seconds.
- **An ignored `save_in` error rolls everything back.** When the lease moved, the statement
  fails and PostgreSQL aborts the transaction: `transaction.commit()` then returns `Ok` and
  stores nothing, neither your rows nor the progress. Rolling back to a savepoint taken before
  the call is the exception: it keeps the transaction alive without the progress, so do not
  wrap `save_in` in a savepoint.

The [`durable_progress`](../jalari/examples/durable_progress.rs) example runs one job per call,
kills its worker process mid-run and counts the pages each job delivered twice after the
takeover.

### Steps

A cursor suits a loop over many items. A job that does a few named things with effects outside
the database, such as charging a card and then emailing a receipt, wraps each one in a step:

```rust
async fn run(self) -> JobResult {
    let charge_id = jalari::step("charge", |run| async move {
        Ok(charge(self.order_id, self.amount, run.idempotency_key()).await?)
    })
    .await?;
    jalari::step("receipt", |_| async move {
        Ok(send_receipt(self.order_id, &charge_id).await?)
    })
    .await
}
```

`jalari::step(key, f)` runs `f` and records its output under `key` for the job. Every later run
of the job, after a retry, a crash and takeover, a restart or `admin::retry`, gets the recorded
output back without running `f`. So when the receipt fails, the retry does not charge again.

**A step runs at least once.** The output is stored after `f` returns. When the worker dies
while `f` runs, or after the effect and before the output is stored, the next run of the job
runs `f` again. A takeover can also start the step on another worker while the old worker is
still inside `f`: the old worker cannot store anything once it lost the job, but an effect it
already started goes on. In the race test the old run was dropped at its next `.await` as soon
as its worker lost the lifeline, and the takeover came at least 6.0 seconds later, so the two
never overlapped there. They overlap when the old worker cannot notice the loss in time, such
as a frozen process, and an effect handed to another thread or already sent outlives the run in
any case. `run.starts()` says how often the step started without being recorded, so a value
above 1 means an earlier execution may have had its effect.

**Keys.**

- Build a key from the payload or from earlier steps' outputs, never from the time or a random
  value: the next run must call the step with the same key to find its record.
- A key succeeds once per run. Calling it again after it succeeded in the same run, or while it
  is running in a `join!`, is a permanent error. After `f` returned an error, or after the step
  future was dropped by a `timeout`, the same run may call it again.
- Keys are 1 to 255 bytes. There is no limit on the number of steps, but each one costs two
  short transactions, so do not use a step per item of a long loop; keep a cursor with
  `checkpoint.save_now` instead.

**Idempotency keys.** `run.idempotency_key()` is a UUID stored with the step's record. It stays
the same across retries and takeovers, so an API that deduplicates requests by key turns a
repeated `f` into one effect. Payment providers keep such keys only for a limited time, often
about a day, so a job retried with `admin::retry` days later is not deduplicated by them. A
`Replace` of the job or `admin::clear_steps` deletes the record, and the next start gets a new
key.

**At most once.** For an effect that must never happen twice and has no idempotency key, give up
on a repeated start instead of running `f` again:

```rust
let sent = jalari::step("sms", |run| async move {
    if run.starts() > 1 {
        return Ok(false);
    }
    send_sms(&self.phone).await?;
    Ok(true)
})
.await?;
```

Every earlier start counts, including one whose `f` returned an error, so such a step gives up
after any failure, and a message may be lost instead of sent twice.

**Outputs.** The output is stored as JSON, at most 1 MiB per step, and must read back as the
step's type: a string with `\u0000`, a `NaN` float or a type whose `Deserialize` does not
accept its own `Serialize` output fails the job permanently and leaves the step started, with
the reason in its error. The run that executes `f` gets the output as it reads back, so every
run sees the same value. When `f` returns an error, its text is kept with the record and the
error is returned unchanged, so `JobError::discard`, `jalari::snooze` and interruptions work
inside a step. A step is a stop point like `checkpoint`.

**Effects must outlive the process.** `jalari::enqueue_on_success` is refused inside a step with
`NotInJob`: the child would exist only in memory while the step is recorded as done, and a
failed attempt would lose it for good. Call it after the step returns, or enqueue the child with
`jalari::enqueue_in` in a transaction that `checkpoint.save_in` writes to, so both commit
together.

**Life cycle.**

| When | The recorded steps |
|---|---|
| The job is retried, taken over after a crash, or restarted after a stop | kept: done steps return their output, started ones run again |
| The job fails for good | kept, so `admin::retry` does not repeat the done ones |
| The job succeeds | kept until housekeeping removes the job |
| The job is replaced with `OnConflict::Replace` | deleted with its progress; do not replace a job whose recorded effects must not happen again |
| `admin::clear_steps(id)` | deleted, unless the job is running |
| Housekeeping removes the job | deleted with it |

`jalari::admin::job(id)` lists the steps in `JobDetail::steps`, with their state, starts,
attempt, times, error and output. When a job succeeds without calling a step that an earlier
run started and never finished, for example because a deploy renamed its key, the worker logs a
warning that names those steps, since their effects may or may not have happened.

`admin::clear_steps` is for an operator who knows the recorded effects did not happen, or wants
them to happen again: the next run runs every step, with new idempotency keys. It answers
`Cleared(n)`, `Running` for a running job, whose steps it keeps, or `NotFound`.

The [`steps`](../jalari/examples/steps.rs) example charges an order with an idempotency key,
sends a receipt with the at-most-once recipe, simulates a crash between each effect and its
record, and prints and clears the steps.

**Limits.**

- A step works only in the job's own task. Called from a task the job spawned, it returns a
  permanent error.
- Start no step while a [`save_in`](#durable-progress) transaction of the same job is open: the
  step waits 2 seconds for the lock that transaction holds and fails with a retryable error.
- During a rolling upgrade from 0.7, a `Replace` from a 0.7 process keeps the job's steps, so the
  replaced job gets the old payload's outputs. Upgrade those processes first, as
  [Upgrading from 0.7](#upgrading-from-07) says.

## Pausing queues

```rust
jalari::admin::pause_queue("exports").await?;
run_database_maintenance().await?;
jalari::admin::resume_queue("exports").await?;

for paused in jalari::admin::paused_queues().await? {
    println!("{} paused since {}", paused.queue, paused.paused_at);
}
```

A paused queue starts no new jobs on any worker. Once `pause_queue` returns, no claim that
starts afterwards takes a job of that queue; a claim already running at that instant can still
start one. Running jobs finish. Enqueueing, retries, `stop`, `cancel`, `Replace` and expiry
work as usual, and the jobs they make due wait. Recurring schedules keep enqueueing, and their
`job_key` keeps one run waiting. `run_now` answers `Enqueued` and the job waits.

The pause is a row in the `queue_pause` table, so it holds across worker restarts and covers
workers that start later. `pause_queue` and `resume_queue` return whether they changed
anything, and accept any queue name, including one no worker serves yet. `paused_queues`
returns a `jalari::admin::PausedQueue` for each paused queue, with the time it was paused.
Workers learn of a resume through a notification, or within `poll_interval` if it is lost.
`stranded()` marks the jobs of a paused queue with `paused`, and the five-minute warning skips
them.

The CLI has the same commands, and `jalari status` lists the paused queues:

```bash
jalari pause exports
jalari resume exports
```

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

#[jalari::job(name = "build_report", queue = "reports")]
impl Job for BuildReport {
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
- A middleware may return `jalari::snooze(..)` instead of calling `next.run()`, for example
  from a rate limiter, and may call `jalari::enqueue_on_success`, also after `next.run()`
  returned.
- The error from a checkpoint or `jalari::interrupted()` must pass through unchanged, or a
  shutdown counts the attempt as failed. `JobError::is_interruption()` tells such errors
  apart, and `is_permanent()`, `is_discarded()`, `snoozed_for()` and `retry_delay()` tell the
  others; `msg()` is the text that `last_error` stores.

`JobMeta` describes the attempt: `id`, `name`, `queue`, `attempt`, `max_attempts` and
`is_last_attempt`; `created_at`, when the job was first enqueued; `scheduled_at`, when this
attempt became due, so `now - scheduled_at` is the time it waited in the queue; `priority`;
`last_error`, the previous attempt's error; `parent_id` and `root_id`; and the stored context,
decoded with `context::<T>()` or as JSON with `raw_context()`.

### After the attempt

`finished` runs after the outcome of an attempt is saved, with the outcome as the worker
recorded it:

```rust
impl JobMiddleware for Alerts {
    type Provides = ();

    async fn call(&self, _job: &JobMeta, next: Next<'_>) -> JobResult {
        next.run().await
    }

    async fn finished(&self, job: &JobMeta, outcome: &jalari::Finished) {
        match outcome {
            jalari::Finished::Failed { error } => self.pager.alert(job.id(), error).await,
            jalari::Finished::Retrying { .. } if job.attempt() + 1 == job.max_attempts() => {
                self.pager.warn_next_is_last(job.id()).await;
            }
            _ => {}
        }
    }
}
```

| `jalari::Finished` | The attempt |
|---|---|
| `Succeeded` | succeeded |
| `Retrying { delay, error }` | failed and runs again after `delay` |
| `Failed { error }` | failed and the job will not run again |
| `Discarded { error }` | discarded the job |
| `Snoozed { delay }` | snoozed |
| `Released` | was given back at shutdown without counting |
| `Stopped`, `Cancelled` | ended by a stop or a cancel |

- It runs once per attempt, after the result is committed, so it reads the job's final state
  and its history row. With [`enqueue_on_success`](#after-the-job-succeeds) it runs after the
  follow-up jobs are inserted. When the insert was refused, it sees the failed attempt.
- Only middleware that wrapped the attempt are called, innermost first. An attempt that never
  ran through the middleware is not reported, and neither is anything that happens outside a
  run: a crashed worker's job taken over by another worker, an attempt whose result could not
  be saved, a stop or cancel of a waiting job, expiry, and admin calls.
- It only observes. Each hook runs in its own task with the job's context, so
  `jalari::enqueue` in it inherits the context; `current_job()` is `None` there. A panic is
  logged and the next hook still runs. The worker waits for the hooks before it claims its
  next job, so keep them short.

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
example as `-c` startup options. jalari never changes a pooled connection's session settings,
so every connection returns to the pool with the values it had.

- A running job holds no connection and no transaction, so no pool setting limits how long a job
  may run. `save_now` takes a pool connection for one short transaction with a 2 second
  `lock_timeout`. `save_in` runs on your transaction and sets its
  `idle_in_transaction_session_timeout` for that transaction only. Every jalari statement runs
  under the pool's settings, `statement_timeout` included, and none of them waits for long on
  its own. Jobs that share a `queue_key` are skipped while the key is taken, not waited for.
- The lifeline is a connection the worker opens next to the pool with the pool's connect
  options; the pool's `after_connect` does not run on it, so a `SET ROLE`, `search_path` or
  other setting made there does not apply. The lifeline only reads and writes jalari's tables,
  always with the schema name, so the role in the connect options needs the privileges on them
  itself; when it lacks them, heartbeats are refused and the worker claims nothing until it gives
  up after 30 seconds. On that session alone the worker sets
  `idle_session_timeout` and `tcp_user_timeout` to 11 seconds (the latter only where the server
  supports it), `statement_timeout` to 1.5 seconds and, on PostgreSQL 17 and later,
  `transaction_timeout` to 0.
- An enqueue with a `job_key` waits while another open transaction is inserting or replacing
  a job with the same key, for example your own transaction that called `jalari::enqueue_in`.
  It fails with `jalari::ErrorKind::QueryCanceled` if the wait outlasts `statement_timeout`.
- `jalari::migrate` waits for a concurrent migration, and an upgrade waits for running jobs as
  described in [Creating the tables](#creating-the-tables). Give it a pool without a short
  `statement_timeout`, or apply the SQL from `jalari sql` with your own migration tool.

Because no jalari session keeps a transaction open while a job runs, a long job does not hold
back the xmin horizon: VACUUM keeps removing dead rows in every table of the database, the
job tables included. Before 0.7 every running job held a transaction for its whole run, which
pinned the horizon for as long as the longest job.

Workers need real PostgreSQL sessions: the lifeline's advisory lock and the notification
listener live as long as their session. Point the pool at PostgreSQL directly or at a pooler
in session mode. Behind a transaction pooler a dead worker's lock could survive on a server
connection the pooler reuses, and its jobs would never be taken over; `build()` checks that a
session keeps its advisory lock across statements and fails with `NoSessionConnection`
otherwise, but it cannot catch every pooler configuration.

A job's writes to your own tables commit separately from jalari marking it done. If the worker
dies in between, the job runs again, so make those writes idempotent, for example with a table
keyed by `jalari::current_job()` id in the same transaction, or commit each unit's writes
together with its progress through [`checkpoint.save_in`](#durable-progress). When the job
tables and your data live in different databases, enqueueing inside your transaction and
`save_in` are no longer possible either.

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

### Before going live

- Give the process time to stop. After its token is cancelled, `Worker::run` waits up to
  `shutdown_timeout` (30 seconds by default) for running jobs. A supervisor or container
  runtime that kills the process sooner turns an orderly stop into a crash: the jobs are taken
  over 7 to 8 seconds later and the attempt counts. Set the stop grace period above
  `shutdown_timeout`, or `shutdown_timeout` below it.
- Run at least two workers. A dead worker's jobs are taken over by another worker. With one
  worker they wait until that process is back, and it then takes over its own previous
  incarnation's jobs 7 seconds after their last heartbeat.
- Keep the worker process alive. A host that stops idle processes stops the worker and its
  scheduler with it; nothing runs until it is started again, and each schedule then fires its
  one catch-up run.
- Rotate database credentials with the pool in mind. The pool and the lifeline open new
  connections with the options the pool was built with, so once the old password is revoked
  every reconnect fails until the process restarts with the new one. Start processes with the
  new credentials before revoking the old ones.
- Keep blocking work off the runtime. A blocking client, `std::thread::sleep` or a CPU-bound
  loop belongs in `spawn_blocking`. The lifeline keeps beating while the runtime is blocked,
  but a stop, a cancel, a timeout and shutdown reach a job only at an `.await`, and a runtime
  that has not run for 10 seconds makes the lifeline log `the job runtime has not run`. Inside
  `spawn_blocking`, poll `JobRun::is_interrupted()` between pieces of work, as
  [Blocking code](#blocking-code) shows.

For a task with `fingerprint = true`, a worker runs only the jobs enqueued by its own build of
the task's code, as [code fingerprints](#code-fingerprints) describes. A task whose code a
deploy did not change keeps its fingerprint, so both builds run its jobs. For a task whose code
changed, jobs that the old build enqueued wait for an old worker, and jobs that the new build
enqueued wait for a new worker. During a rolling deploy, keep at least one old worker running
until the old build's jobs have drained. Jobs left behind after the last old worker stops wait
until you release them with `jalari::admin::clear_fingerprint`. Every worker logs a warning
every five minutes while due jobs in its queues have no live worker that can run them, and
`jalari::admin::stranded()` lists those jobs.

### Upgrading from 0.7

0.8.0 brings migration 10. It adds two columns with defaults and creates the empty `job_step`
table, whose foreign key on the job table blocks writes to the job table until the migration
commits, normally for a fraction of a second. 0.7 processes that are already running keep
working after the upgrade. A 0.7 process started after it fails `init` with
`SchemaVersionMismatch`, because each release checks for its own version, so start only 0.8
processes from then on. Upgrade every process that enqueues with `OnConflict::Replace` before
deploying code that calls `jalari::step`: a 0.7 `Replace` bumps the job's lease without deleting
its [steps](#steps), so the replaced job would get the old payload's step outputs.

From 0.8 on, a worker creates a job's control row when it claims the job, so
`jalari::admin::job` shows a `control` entry for every job a worker has claimed, and
housekeeping removes the row together with the job.

### Upgrading from 0.6

0.7.0 brings migrations 8 and 9 and changes how fingerprints are computed.

1. Stop every 0.6 process, enqueuers included. Migration 8 refuses to run, with `LockTimeout`
   and a message naming 0.6, while a 0.6 worker still runs a job.
2. Run the migration: `.migrate()`, `jalari migrate`, or the SQL from `jalari sql`. Migration
   9 scans the job table once and builds three indexes, and writes to the job table wait for
   it; [Creating the tables](#creating-the-tables) has the details.
3. Start the 0.7.0 workers. Their pool needs at least two connections, which `build()` checks,
   and each worker opens one more session, its [lifeline](#crashes-and-lost-connections).

Every fingerprint changes in 0.7.0: the name, the aliases, the priority and the retry policy
are no longer hashed, and every release changes all fingerprints through the jalari version
anyway. Jobs that a 0.6 build enqueued with `fingerprint = true` wait for a 0.6 worker, which
can no longer run. Let those queues drain before you stop the last 0.6 worker, or find the
jobs with `jalari::admin::stranded()` afterwards and release them with
`jalari::admin::clear_fingerprint()`. A failed job that `jalari::admin::retry` or
`retry_failed` retries gets the fingerprint of the caller's build when the caller links the
task. Moving a job's `max_attempts` or `timeout` from the impl into the attribute changes its
fingerprint once more, so make that move in the same deploy.

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
| `deleted_retention` | How long deleted and discarded jobs are kept, expired jobs included | 7 days |
| `failed_retention` | How long failed jobs are kept; `None` keeps them | `None` |
| `worker_timeout` | Missed-heartbeat time before a worker leaves the list; departed workers are deleted after it or 12 s, whichever is longer. Takeover does not depend on it | 5 min |

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
    println!("{}: {} succeeded, {} failed, {} retried, {} snoozed, {} discarded, max {:?}",
        queue.queue, queue.succeeded, queue.failed, queue.retried, queue.snoozed,
        queue.discarded, queue.max_duration);
}
```

`workers` returns a `jalari::admin::WorkerInfo` per live worker, with its queues as `QueueInfo`
values and the tasks it runs. If no listed worker has `scheduler` set, recurring schedules are
not firing. `stats` returns a `JobCount` for every queue and state, `running` included;
`throughput` returns a `Throughput` per queue that sums up the job history of the last window,
so it only sees as far back as the retention housekeeping keeps. `retried` counts attempts that
returned an error and run again and `snoozed` counts snoozes; `discarded` counts jobs that
discarded themselves or expired, and they are not in `failed`. A running job's `JobInfo` names
the worker in `worker_id`, when its attempt started in `running_since`, and a stop or cancel it
has not acted on yet in `pending`.

### Jobs that wait for another build

```rust
for group in jalari::admin::stranded().await? {
    println!("{} {} jobs in {} wait for fingerprint {:?} since {}",
        group.count, group.task, group.queue, group.fingerprint, group.since);
}

let filter = jalari::admin::JobFilter::new().task("send_email");
let released = jalari::admin::clear_fingerprint(&filter).await?;
```

`stranded` lists the due jobs that no live worker can run as `jalari::admin::StrandedJobs`
groups, one per queue, task and [code fingerprint](#code-fingerprints). A job is listed when no
worker with a recent heartbeat both processes its queue and runs its task at a matching
fingerprint. A job whose task no running binary knows is listed too. Jobs that only wait for a
busy worker are not listed. A worker of a release before migration 7 counts as able to run
every task of its queues. Running jobs are listed when their worker's heartbeat is older than
the cluster's `worker_timeout`, or than 12 seconds when it is shorter, which only happens when
no live worker is left to take them over. Each worker logs a warning for the groups in its own
queues when it starts and every five minutes after: `no live worker runs these jobs, enqueued
by another build of the task` when the worker knows the task, and `no live worker knows this
task` when no running binary links it. Both carry the queue, the task, the fingerprint, the
count and since when the oldest job is due. A group in a [paused queue](#pausing-queues) has
`paused` set and is not warned about, and jobs past their [expiry](#expiry) are not listed.
Jobs stored under an [alias](#renaming-a-job) of a linked job count as known.

`clear_fingerprint` removes the fingerprint from every scheduled, enqueued or stopped job that
matches the filter, so any worker that knows the task runs it, and notifies the workers of
their queues. Use it when the build those jobs wait for will not run again. `JobInfo` carries
each job's `fingerprint`, and `WorkerInfo::tasks` maps every task a worker runs to its
fingerprint, `None` for a task declared without `fingerprint = true`, aliases included.

### Listing jobs

```rust
let filter = jalari::admin::JobFilter::new()
    .state(jalari::JobState::Failed)
    .context(serde_json::json!({ "tenant_id": "acme" }));
let total = jalari::admin::count(&filter).await?;
let page = jalari::admin::jobs(&filter, 50, None).await?;
let urgent = jalari::admin::count(&jalari::admin::JobFilter::new()
    .priority(jalari::Priority::Urgent)
    .state(jalari::JobState::Enqueued)).await?;
let tree = jalari::admin::jobs(&jalari::admin::JobFilter::new().root(export_id), 100, None).await?;
let next_page = jalari::admin::jobs(&filter, 50, page.last().map(|job| job.id)).await?;

if let Some(detail) = jalari::admin::job(page[0].id).await? {
    let job: SendWelcome = detail.job.payload_json()?;
    for entry in &detail.history {
        println!("{} attempt {} {:?}", entry.state, entry.attempt, entry.error);
    }
    if let Some(control) = &detail.control {
        println!("pending {:?} progress {:?}", detail.job.pending, control.progress);
    }
}
```

`JobFilter` combines any of: states, priorities, queue, task, exact `job_key` or a prefix, a
creation time range, a context the stored context must contain (PostgreSQL's `@>`, so
`{"tenant_id": "acme"}` matches whatever else the context holds), the parent, and the root.
`parent(id)` matches the jobs that job enqueued; `root(id)` matches that job and every job
below it. Repeated `state` or `priority` calls match any of them. Pages are cut by id, newest
first: pass the last id of a page as `before` for the next one, and they stay consistent while
jobs come and go. `job` returns a `jalari::admin::JobDetail`: the `JobInfo`, whose `pending`
names a stop or cancel the running job has not acted on yet; every state change as a
`HistoryEntry`; and a `ControlInfo` with the progress saved at the last checkpoint. Payloads
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

let backlog = jalari::admin::JobFilter::new().queue("reports").priority(jalari::Priority::Normal);
let moved = jalari::admin::set_priority(&backlog, jalari::Priority::High).await?;
```

`retry` makes a failed job due now with its attempts reset, so it gets its full `max_attempts`
again; `last_error` stays until an attempt succeeds, the change is recorded in the history, and
the queue's workers are notified. A failed job whose `job_key` is held by a waiting, running or
stopped job stays failed and the outcome names that job. `retry_failed` does the same for every
failed job matching the filter, in batches of 100 that each commit on their own. Both give each
job the [code fingerprint](#code-fingerprints) of the caller's build when the caller links the
job's task, and keep the stored one otherwise. A discarded job is not failed, so both leave it
alone. A failed job past its [expiry](#expiry) answers `Expired` and stays failed, and
`RetrySummary::expired` counts those. `run_now` makes a scheduled job, a retry waiting for its
backoff or a snoozed job due now and answers a `RunNowOutcome`: a running job, or one a worker
aborted at shutdown less than 7 seconds ago, is reported as `Running` and left alone, and a
stopped or finished one as `NotWaiting`. To delete a job,
[`cancel`](#stopping-restarting-and-cancelling) it: a waiting or stopped job is marked deleted
at once, a running one once it stops.

`set_priority` moves the scheduled, enqueued and stopped jobs that match the filter to another
[priority](#priority), in batches of 100 that each commit on their own, and returns how many
changed. Running and finished jobs keep theirs. It leaves `updated_at` alone and wakes no
worker, since no job became due.

## Known limits

- One job's progress that PostgreSQL cannot store, such as a string with a `\u0000`, fails the
  worker's batched progress write for every job it runs, until that job's run ends. Its own
  outcome cannot be saved either, so its attempt is recorded as interrupted and counts.
- Every enqueue's notification wakes the idle slots of every worker serving the queue, and all
  but one find nothing. At about 4,000 enqueues a second over four workers this measured about
  7 empty claims per job, which costs client CPU and pool time, not job latency.
- While one scheduler holds a due schedule in its firing transaction, the other schedulers
  check again every 100 ms, capped by `poll_interval`, instead of waiting for the next due
  time. A fire commits in milliseconds, so this lasts as long as a fire does.
- On PostgreSQL 14 to 17, many far-future jobs of a high [priority](#priority) slow every
  claim of their queue.
- While a [`save_in`](#durable-progress) transaction is open, its job cannot be taken over, and
  a release of that job by its own worker, after a lost claim reply, an abandoned close or a
  shutdown abort, waits for the transaction too. PostgreSQL ends an idle one after 10 seconds;
  a transaction that keeps running statements holds the job for as long as it runs.
- `save_in` fails with a serialization error under `REPEATABLE READ` or `SERIALIZABLE` when the
  worker wrote the job's progress after the transaction's snapshot.
- A [step](#steps) whose `f` returns an error with a `\u0000` in its text cannot store that
  text: the worker logs a warning, the record keeps its previous error, and the error is still
  returned to the job.
- While the database answers heartbeats with errors, the worker keeps its jobs but its
  heartbeat ages past the 7 second takeover grace. If its lifeline session then ends, another
  worker can take the jobs over within milliseconds, while this worker notices at its next
  beat, up to a second later. The outcome stays fenced; side effects in that second can overlap.
- `admin::workers()` and `admin::stranded()` judge a worker by the age of its heartbeat against
  the cluster's `worker_timeout`. While the database refuses heartbeats, a live worker's
  heartbeat ages by up to 30 seconds, so with a `worker_timeout` below that the worker drops out
  of `workers()` and its running jobs are listed by `stranded()` until a heartbeat succeeds.

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

`InvalidOptions` means an `EnqueueOptions` value cannot be used where it was passed: a value
out of range, such as `max_attempts(0)` or a `queue_key_limit` without a `queue_key`, or
`OnConflict::Replace` in `enqueue_many` or `enqueue_on_success_with`. Its message names the
option. `NotInJob` means a function that only works in a running job's own task, such as
`enqueue_on_success`, was called elsewhere, or `enqueue_on_success` was called inside a
[step](#steps). `DuplicateJobName` from `build()` names every
name or alias that two linked jobs share.

## Examples

```bash
export DATABASE_URL=postgres://localhost/app
cargo run -p jalari --example migrate
cargo run -p jalari --example basic
cargo run -p jalari --example outcomes
cargo run -p jalari --example queues
cargo run -p jalari --example missed_runs
cargo run -p jalari --example stop_restart
cargo run -p jalari --example durable_progress
cargo run -p jalari --example steps
cargo run -p jalari --example blocking_work
cargo run -p jalari --example middleware
cargo run -p jalari --example split_worker
cargo run -p jalari --example split_web
cargo run -p jalari --example split_control -- stop 42
cargo run -p jalari --example split_control -- stranded
```

| Example | Shows |
|---|---|
| [`migrate`](../jalari/examples/migrate.rs) | Creating and checking the tables; `--dry-run` prints the SQL instead |
| [`basic`](../jalari/examples/basic.rs) | Defining, enqueueing and running jobs in one process: errors and retries, `job_key` with `KeepExisting` and `Replace`, `queue_key`, delays, transactions, recurring schedules, `current_job`, `interrupted`, a job that reschedules itself with `enqueue_on_success_with` under its own `job_key`, cluster settings and monitoring. It runs until Ctrl-C |
| [`outcomes`](../jalari/examples/outcomes.rs) | Every way an attempt ends, printed by a middleware's `finished` hook: `retry_after` for a rate limit, a per-job `RETRY_POLICY` that runs out of attempts, `snooze` on the same attempt, `JobError::discard`, and a job that expires with `expire_after` before it runs next to one with `expires_at` that runs in time. The jobs read `created_at`, `scheduled_at`, `priority` and `last_error` from `current_job`, the middleware marks the last attempt with `is_last_attempt`, and the run ends with the job list and the queue's `throughput`. It exits when done |
| [`queues`](../jalari/examples/queues.rs) | `pause_queue`, `paused_queues` and `resume_queue`; `enqueue_many` with a key repeated in the batch, and `enqueue_many_in` inside a transaction; priorities from the `priority` attribute and from `EnqueueOptions::priority`, moved with `set_priority` while the queue is paused, then run in priority order; `queue_key_limit` letting two jobs of one key run at once; and a job that enqueues children and a grandchild, listed with `JobFilter::parent` and `JobFilter::root`. It exits when done |
| [`missed_runs`](../jalari/examples/missed_runs.rs) | Two hourly runtime schedules built with `Schedule::missed`, one `Missed::Once` and one `Missed::Skip`, pushed 3 hours overdue in SQL: `Once` enqueues one catch-up run and `Skip` none, then a run 10 seconds late fires under `Skip`. Each step prints the schedules' `run_count`, `last_run_at`, `next_run_at` and the jobs they enqueued. It exits when done |
| [`stop_restart`](../jalari/examples/stop_restart.rs) | `stop`, `restart` and `cancel` on waiting and running jobs, checkpoints with and without progress and `checkpoints = true`, a job blocked in one long sleep that `interrupted()` stops within a second, `stop_timeout` aborting a job without checkpoints, and a shutdown that the next worker resumes from. It exits when done |
| [`durable_progress`](../jalari/examples/durable_progress.rs) | Three jobs deliver pages in a cursor loop, one storing the cursor with `save`, one with `save_now` and one with `save_in` in the same transaction as its page row. The program kills its first worker process with SIGKILL mid-run, prints each job's stored cursor, lets a second worker take the jobs over, and counts the pages each job delivered twice: only the `save` job repeats the pages after its last written copy. It exits when done |
| [`steps`](../jalari/examples/steps.rs) | An order job with a `charge` step that passes `idempotency_key()` to a fake payment gateway and a `receipt` step written with the `starts() > 1` at-most-once recipe. A simulated crash after each effect, before its step is recorded, makes the next attempt charge again under the same key, which the gateway deduplicates, and give up on the receipt; the last attempt reads the charge from its record without calling the gateway. The run ends with `JobDetail::steps` and `admin::clear_steps`. It exits when done |
| [`blocking_work`](../jalari/examples/blocking_work.rs) | A loop in `spawn_blocking` that polls `JobRun::is_interrupted` between chunks: run to the end, stopped with `jalari::stop` and noticing it within a second, and ended by a 2 second `timeout`. It exits when done |
| [`middleware`](../jalari/examples/middleware.rs) | Capturing a context with `jalari::scope` and `EnqueueOptions::context`, middleware that wraps every job or provides a per-tenant database, `check`, rejected jobs, a job that passes its context to the jobs it enqueues, the `finished` hook with `is_last_attempt`, and a job stored under an old name that runs through `aliases`. It exits when done |
| [`split_web`](../jalari/examples/split_web.rs), [`split_worker`](../jalari/examples/split_worker.rs) and [`split_control`](../jalari/examples/split_control.rs) | The same jobs in separate binaries: one enqueues, one runs them (`--no-scheduler` turns its scheduler off), and one stops, restarts, cancels or shows a job, lists the jobs no live worker can run (`stranded`) or releases a task's jobs to any build (`release <task>`) from yet another process. `vacuum_tables` is the only job without `fingerprint = true`; edit `common/jobs.rs` and rebuild only `split_worker` to see the other jobs wait for the build that enqueued them |

The examples use `jalari::Schema::default()`, so they share the `jalari` schema of the
database they run against. [Using jalari through another crate](#using-jalari-through-another-crate)
points at the facade crates, which run as a test rather than an example.
