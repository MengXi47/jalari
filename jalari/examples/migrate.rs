#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let schema = jalari::Schema::default();

    jalari::migrate(&pool, &schema).await?;

    let version = jalari::schema_version(&pool, &schema).await?;
    println!("schema version: {version:?}");
    Ok(())
}
