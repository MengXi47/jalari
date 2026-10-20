#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = std::env::args().any(|arg| arg == "--dry-run");
    let schema = jalari::Schema::default();

    if dry_run {
        for migration in jalari::migrations(&schema) {
            println!("-- version {}\n{}", migration.version, migration.sql);
        }
        return Ok(());
    }

    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let before = jalari::schema_version(&pool, &schema).await?;
    println!(
        "schema version before: {before:?}, this release needs {}",
        jalari::SCHEMA_VERSION
    );

    jalari::migrate(&pool, &schema).await?;

    let after = jalari::schema_version(&pool, &schema).await?;
    println!("schema version after: {after:?}");

    jalari::init(pool).schema(schema).await?;
    println!("init accepts the tables");
    Ok(())
}
