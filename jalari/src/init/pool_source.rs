use sqlx::PgPool;

/// Source of the connection pool that jalari uses for every query.
///
/// Implemented for [`PgPool`]; implement it to hand jalari a pool owned by another
/// structure.
pub trait PoolSource: Send + Sync + 'static {
    /// Returns a handle to the pool; cloning a [`PgPool`] is cheap.
    fn pool(&self) -> PgPool;
}

impl PoolSource for PgPool {
    fn pool(&self) -> PgPool {
        self.clone()
    }
}
