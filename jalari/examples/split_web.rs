#[path = "common/jobs.rs"]
mod jobs;

use jobs::{DailyReport, Export, SendEmail, VacuumTables};

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

    let export = jalari::enqueue(&Export { rows: 120 }).await?.id();
    jalari::enqueue(&VacuumTables).await?;

    println!("enqueued 13 jobs and scheduled \"promo\"; no worker runs in this process");
    println!("export job {export} runs for a minute; control it from another process with");
    println!("  cargo run -p jalari --example split_control -- stop {export}");
    println!("  cargo run -p jalari --example split_control -- restart {export}");
    println!("  cargo run -p jalari --example split_control -- cancel {export}");
    println!("every job but vacuum_tables carries the fingerprint of this build, so a worker");
    println!("built from a changed common/jobs.rs leaves them waiting; list and release them with");
    println!("  cargo run -p jalari --example split_control -- stranded");
    println!("  cargo run -p jalari --example split_control -- release send_email");
    Ok(())
}
