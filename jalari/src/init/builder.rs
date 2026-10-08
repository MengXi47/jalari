use std::future::{Future, IntoFuture};
use std::pin::Pin;

use super::config::{Config, already_initialized, install, is_initialized};
use crate::{Error, ErrorKind, PoolSource, Result, SCHEMA_VERSION, Schema};

/// Starts the one-time global setup that every other jalari call relies on.
///
/// Await the returned builder to finish. It checks that the tables exist at
/// [`SCHEMA_VERSION`] and stores the pool and schema for the rest of the process. Call it once,
/// at startup, before enqueuing jobs or building a [`Worker`](crate::Worker).
///
/// # Errors
///
/// Awaiting the builder returns an error if:
/// - `init` already completed ([`AlreadyInitialized`](ErrorKind::AlreadyInitialized))
/// - The tables are missing or at another version
///   ([`SchemaVersionMismatch`](ErrorKind::SchemaVersionMismatch)); see [`InitBuilder::migrate`]
/// - The database cannot be reached or a migration fails
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = jalari::sqlx::PgPool::connect("postgres://localhost/app").await?;
/// jalari::init(pool)
///     .schema(jalari::Schema::named("jobs")?)
///     .migrate()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn init<P: PoolSource>(pool: P) -> InitBuilder {
    InitBuilder {
        pool: Box::new(pool),
        schema: Schema::default(),
        migrate: false,
    }
}

/// Options for [`init`]; await it to run the setup.
#[must_use]
pub struct InitBuilder {
    pool: Box<dyn PoolSource>,
    schema: Schema,
    migrate: bool,
}

impl InitBuilder {
    /// Uses this schema or table prefix instead of the default `jalari` schema.
    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = schema;
        self
    }

    /// Creates or upgrades the tables before the version check, like [`migrate`](crate::migrate).
    ///
    /// Needs a role that may create tables, and the schema itself when it does not exist yet.
    /// Leave it off when tables are managed by the `jalari` CLI or your own migrations.
    pub fn migrate(mut self) -> Self {
        self.migrate = true;
        self
    }

    async fn initialize(self) -> Result<()> {
        if is_initialized() {
            return Err(already_initialized());
        }

        let pool = self.pool.pool();
        if self.migrate {
            crate::migrate(&pool, &self.schema).await?;
        }

        let found = crate::schema_version(&pool, &self.schema).await?;
        if found != Some(SCHEMA_VERSION) {
            return Err(Error::new(
                ErrorKind::SchemaVersionMismatch,
                format!(
                    "schema {:?} needs version {SCHEMA_VERSION} but has {found:?}; \
                     call init(..).migrate(), run `jalari migrate` or apply the exported SQL",
                    self.schema.name()
                ),
            ));
        }

        install(Config {
            pool: self.pool,
            schema: self.schema,
        })
    }
}

impl IntoFuture for InitBuilder {
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.initialize())
    }
}
