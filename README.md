<p align="center">
  <img src="docs/banner.png" alt="jalari: background jobs for Rust that need nothing but PostgreSQL" width="640">
</p>

<p align="center">
  <a href="https://crates.io/crates/jalari"><img src="https://img.shields.io/crates/v/jalari.svg" alt="crates.io"></a>
  <a href="https://docs.rs/jalari"><img src="https://img.shields.io/docsrs/jalari" alt="docs.rs"></a>
  <a href="https://github.com/MengXi47/jalari/actions/workflows/rust.yml"><img src="https://github.com/MengXi47/jalari/actions/workflows/rust.yml/badge.svg" alt="Rust"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="License"></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/MSRV-1.94-orange.svg" alt="MSRV"></a>
  <a href="https://www.postgresql.org"><img src="https://img.shields.io/badge/PostgreSQL-14%2B-336791.svg" alt="PostgreSQL"></a>
</p>

jalari keeps jobs, schedules and workers in PostgreSQL tables. Declare a job with
`#[jalari::job]`, enqueue it from anywhere, and run as many workers as you need.

```toml
[dependencies]
jalari = "0.1"
```

## Features

- **Jobs** — immediate or delayed, enqueued alone or inside your own transaction.
- **Cron** — schedules declared in code, plus schedules added, changed and removed at runtime,
  with time zones and daylight saving handled.
- **Deduplication and exclusion** — `job_key` keeps one job per key; `queue_key` keeps jobs
  with the same key from running at once.
- **Retries** — exponential backoff with jitter, permanent errors, timeouts, and panics
  isolated to the job that caused them.
- **Named queues** — each worker chooses its queues and how many jobs run at once.
- **Graceful shutdown** — running jobs finish before a worker exits.
- **Monitoring and cleanup** — a live list of workers, and optional deletion of finished jobs.
- **Table setup your way** — from code, with the `jalari` CLI, or as SQL for your own
  migrations.

## How it works

```
   Your app                Schedules
  ┌───────────────┐      ┌───────────────┐
  │ enqueue a job │      │  cron is due  │
  └───────┬───────┘      └───────┬───────┘
          └──────────┬───────────┘
                     ▼
        ┌─────────────────────────┐
        │       PostgreSQL        │
        │   jobs wait in a table  │
        └────────────┬────────────┘
                     │  wake a worker, lock one job
                     ▼
        ┌─────────────────────────┐
        │         Workers         │
        │ run it, save the result │
        └─────────────────────────┘
```

Every job moves through the same states:

```
  waiting ──▶ running ──▶ succeeded
     ▲           │
     └── retry ◀─┤  attempt failed, attempts left
                 ▼
               failed   out of attempts, or an error that retrying cannot fix
```

- A worker locks the job it takes and keeps the lock until the job is done, so no one else
  can take it in the meantime.
- Workers wake up the moment a job arrives, and check on their own from time to time in case
  a wake-up is missed.
- When a schedule is due, its job is created and the schedule moves to its next time in one
  step, so a run is never lost or doubled.

## Multiple workers

```
   ┌ web × N ┐      ┌ worker A ┐  ┌ worker B ┐  ┌ worker C ┐
   │ enqueue │      │ emails   │  │ emails   │  │ reports  │
   └────┬────┘      │ scheduler│  │ scheduler│  │          │
        │           └────┬─────┘  └────┬─────┘  └────┬─────┘
        └────────────────┴──────┬──────┴─────────────┘
                                ▼
                           PostgreSQL
```

- **Add capacity by starting processes.** Workers coordinate only through the database and
  need no configuration to find each other.
- **No job runs twice at once.** Other workers skip rows that are locked. A worker that loses
  its database connection aborts its jobs within 3 seconds, and other workers wait 6 seconds
  before running an interrupted job again.
- **Crashes recover on their own.** When a worker dies, its jobs are released and another
  worker runs them again within seconds.
- **Each cron run is enqueued once**, however many workers run the scheduler. If one stops,
  the others keep schedules firing.
- **Run workers where you like** — inside your web server, or as separate binaries that serve
  different queues.

Jobs run at least once: a job interrupted by a crash runs again, so make it idempotent. An
interrupted run still counts as an attempt, so a job that keeps crashing its worker is marked
failed once it runs out of attempts.

## Example

```rust
use jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

#[jalari::job(queue = "emails")]
impl Job for SendEmail {
    const NAME: &'static str = "send_email";

    async fn run(self) -> JobResult {
        println!("sending to {}", self.to);
        Ok(())
    }
}

jalari::init(pool).migrate().await?;
jalari::enqueue(&SendEmail { to: "ada@example.com".into() }).await?;

let worker = jalari::Worker::builder().queue("emails", 4).build().await?;
worker.run(shutdown).await;
```

## Documentation

- [Guide](docs/guide.md) — setup, jobs, enqueueing, cron, workers, deployment and settings
- [Examples](jalari/examples) — complete programs, including split web and worker binaries
- [Contributing](CONTRIBUTING.md)

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
