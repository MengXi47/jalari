use std::time::Duration;

use jalari::{CancelOutcome, JobId, StopOutcome};

const USAGE: &str = "usage: split_control <stop|restart|cancel|status> <job id>";

async fn status(pool: &jalari::sqlx::PgPool, id: JobId) -> jalari::sqlx::Result<()> {
    let row: Option<(String, String, i32, Option<String>)> = jalari::sqlx::query_as(
        "SELECT job.task, job.state, job.attempts, control.progress::text
         FROM jalari.job AS job
         LEFT JOIN jalari.job_control AS control ON control.job_id = job.id
         WHERE job.id = $1",
    )
    .bind(id.0)
    .fetch_optional(pool)
    .await?;
    match row {
        Some((task, state, attempts, progress)) => println!(
            "job {id} ({task}): {state}, {attempts} attempts, saved progress {}",
            progress.as_deref().unwrap_or("none")
        ),
        None => println!("job {id} does not exist"),
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [command, id] = args.as_slice() else {
        return Err(USAGE.into());
    };
    let id = JobId(id.parse().map_err(|_| USAGE)?);

    let pool = jalari::sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool.clone())
        .schema(jalari::Schema::default())
        .await?;

    match command.as_str() {
        "stop" => loop {
            match jalari::stop(id).await? {
                StopOutcome::Requested => {
                    println!("stop requested; the worker stops the job at its next checkpoint");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                outcome => break println!("stop: {outcome:?}"),
            }
        },
        "cancel" => loop {
            match jalari::cancel(id).await? {
                CancelOutcome::Requested => {
                    println!("cancel requested; the worker ends the job at its next checkpoint");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                outcome => break println!("cancel: {outcome:?}"),
            }
        },
        "restart" => println!("restart: {:?}", jalari::restart(id).await?),
        "status" => {}
        _ => return Err(USAGE.into()),
    }
    status(&pool, id).await?;
    Ok(())
}
