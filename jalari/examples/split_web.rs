#[path = "common/jobs.rs"]
mod jobs;

use jobs::{DailyReport, SendEmail};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool).schema(jalari::Schema::default()).await?;

    for n in 0..10 {
        jalari::enqueue(&SendEmail {
            to: format!("user{n}@example.com"),
            subject: "hello from web".to_owned(),
        })
        .await?;
    }

    jalari::enqueue(&DailyReport {
        include_failed: true,
    })
    .await?;

    jalari::recurring::add_or_update(
        "promo",
        jalari::Cron::new("*/10 * * * * *")?,
        &SendEmail {
            to: "promo@example.com".to_owned(),
            subject: "promo".to_owned(),
        },
    )
    .await?;

    println!("enqueued 11 jobs and scheduled \"promo\"; no worker runs in this process");
    Ok(())
}
