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
