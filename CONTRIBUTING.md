# Contributing to jalari

Install stable Rust and PostgreSQL 14 or newer. The integration tests need a
database where the connecting role may create schemas; each test creates its own
uniquely named schema and drops it afterwards. Without `DATABASE_URL` the
integration tests skip instead of failing.

```bash
createdb jalari_test
export DATABASE_URL=postgres://$USER@localhost/jalari_test
```

sqlx does not fall back to the operating system user, so the URL must name one.

## Workflow

Create a branch from `main`, make the change, and run:

```bash
cargo build --workspace --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Run `cargo fmt --all` to apply formatting. Add tests for changed behavior and
target pull requests at `main`. All CI checks must pass before merging.
[CI](.github/workflows/rust.yml) runs the checks above against PostgreSQL 14 and
18, builds the documentation, and checks that the workspace still compiles with
the minimum supported Rust version, 1.94.

Check the documentation when public APIs or rustdoc change:

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

## Code style

- Document every public item with rustdoc: a one-line summary, details only
  where the signature is not enough, `# Examples` on main types and methods, and
  `# Errors` on fallible functions. Examples that need a database are `no_run`.
- Private code carries no comments; explain decisions in the pull request.
- Background errors are logged with `tracing`; errors returned to the caller
  are not logged.

## Migrations

Table changes go in a new file `jalari/migrations/<version>_<description>.sql`
with the next version number; `build.rs` picks it up without code changes.

- Never edit a migration that has been released. Users' migration tools store a
  checksum of every applied file.
- Write plain DDL without comments, using the `{job}`, `{prefix}` and similar
  placeholders, and make every statement safe to run twice.
- End the file by recording its version in `{schema_version}`, as `0001_init.sql`
  does.

Inspect the rendered SQL with `cargo run -p jalari --features cli -- sql`.

## Concurrency

Changes to claiming, scheduling or shutdown need tests that cover concurrent
workers and failures, not just the single-worker path. A job must never be
claimed by two workers at once, and a scheduled cron run must never be enqueued
twice.

## License

Contributions are dual-licensed under MIT and Apache-2.0.
