use sqlx::PgPool;

/// Source of the connection pool that jalari uses for every query.
///
/// Implemented for [`PgPool`]; implement it to hand jalari a pool owned by another
/// structure.
///
/// # Examples
///
/// ```rust,no_run
/// use jalari::sqlx::PgPool;
///
/// struct Database {
///     pool: PgPool,
/// }
///
/// impl jalari::PoolSource for Database {
///     fn pool(&self) -> PgPool {
///         self.pool.clone()
///     }
/// }
///
/// # async fn example(pool: PgPool) -> jalari::Result<()> {
/// jalari::init(Database { pool }).await?;
/// # Ok(())
/// # }
/// ```
pub trait PoolSource: Send + Sync + 'static {
    /// Returns a handle to the pool; cloning a [`PgPool`] is cheap.
    fn pool(&self) -> PgPool;
}

impl PoolSource for PgPool {
    fn pool(&self) -> PgPool {
        self.clone()
    }
}
