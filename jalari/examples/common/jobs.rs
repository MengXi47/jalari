use std::time::Duration;

use jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SendEmail {
    pub to: String,
    pub subject: String,
}

#[jalari::job(queue = "emails")]
impl Job for SendEmail {
    const NAME: &'static str = "send_email";

    async fn run(self) -> JobResult {
        println!(
            "[pid {}] sending {:?} to {}",
            std::process::id(),
            self.subject,
            self.to
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct DailyReport {
    pub include_failed: bool,
}

#[jalari::job(cron = "0 0 3 * * *", timezone = "Asia/Taipei", queue = "reports")]
impl Job for DailyReport {
    const NAME: &'static str = "daily_report";

    async fn run(self) -> JobResult {
        println!(
            "[pid {}] daily report, include_failed = {}",
            std::process::id(),
            self.include_failed
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Export {
    pub rows: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct Cursor {
    next_row: u64,
}

#[jalari::job(queue = "exports")]
impl Job for Export {
    const NAME: &'static str = "export";

    async fn run(self) -> JobResult {
        let (saved, checkpoint) = jalari::resume::<Cursor>()?;
        let mut cursor = saved.unwrap_or_default();
        println!(
            "[pid {}] export starting at row {} of {}",
            std::process::id(),
            cursor.next_row,
            self.rows
        );
        while cursor.next_row < self.rows {
            tokio::time::sleep(Duration::from_millis(500)).await;
            cursor.next_row += 1;
            if cursor.next_row % 10 == 0 {
                println!(
                    "[pid {}] export at row {}",
                    std::process::id(),
                    cursor.next_row
                );
            }
            checkpoint.save(&cursor).await?;
        }
        println!("[pid {}] export finished", std::process::id());
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct VacuumTables;

#[jalari::job(queue = "maintenance", fingerprint = false)]
impl Job for VacuumTables {
    const NAME: &'static str = "vacuum_tables";

    async fn run(self) -> JobResult {
        println!(
            "[pid {}] vacuuming tables; this task runs on any build",
            std::process::id()
        );
        Ok(())
    }
}
