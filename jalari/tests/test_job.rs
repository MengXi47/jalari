use std::io;
use std::time::Duration;

use jalari::__private::Provided;
use jalari::{ErrorKind, Job, JobError, JobResult, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Greet {
    name: String,
}

#[jalari::job]
impl Job for Greet {
    const NAME: &'static str = "test_greet";

    async fn run(self) -> JobResult {
        if self.name.is_empty() {
            return Err(JobError::new("name is empty".to_owned()));
        }
        Ok(())
    }
}

struct UserName(String);

#[derive(Serialize, Deserialize)]
struct GreetUser;

#[jalari::job]
impl Job for GreetUser {
    const NAME: &'static str = "test_greet_user";
    type Context = UserName;

    async fn run(self, user: &UserName) -> JobResult {
        if user.0.is_empty() {
            return Err(JobError::new("no user".to_owned()));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Nightly;

#[jalari::job(cron = "0 0 3 * * *", timezone = "Asia/Taipei", queue = "reports")]
impl Job for Nightly {
    const NAME: &'static str = "test_nightly";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[test]
fn test_job_attribute_registers_with_default_queue() {
    let registration = jalari::__private::find("test_greet").unwrap();
    assert_eq!(registration.queue, "default");
    assert!(registration.cron.is_none());
}

#[test]
fn test_cron_attribute_records_schedule_and_default_payload() {
    let registration = jalari::__private::find("test_nightly").unwrap();
    assert_eq!(registration.queue, "reports");
    let cron = registration.cron.as_ref().unwrap();
    assert_eq!(cron.expression, "0 0 3 * * *");
    assert_eq!(cron.timezone, "Asia/Taipei");
    assert_eq!((cron.payload)().unwrap(), b"null");
}

#[tokio::test]
async fn test_registered_run_decodes_payload_and_reports_result() {
    let registration = jalari::__private::find("test_greet").unwrap();
    let run = |payload: &[u8]| (registration.run)(Provided::new(), payload);

    assert_eq!(run(br#"{"name":"a"}"#).await, Ok(()));

    let failure = run(br#"{"name":""}"#).await.unwrap_err();
    assert_eq!(failure.msg(), "name is empty");
    assert!(!failure.is_permanent());

    let undecodable = run(b"not json").await.unwrap_err();
    assert!(undecodable.is_permanent());
}

#[test]
fn test_job_attribute_defaults_context_to_unit() {
    let greet = jalari::__private::find("test_greet").unwrap();
    assert!((greet.context)().is_unit());

    let greet_user = jalari::__private::find("test_greet_user").unwrap();
    let key = (greet_user.context)();
    assert!(!key.is_unit());
    assert!(key.name.ends_with("UserName"));
}

#[tokio::test]
async fn test_run_without_provided_context_is_retryable_error() {
    let registration = jalari::__private::find("test_greet_user").unwrap();
    let error = (registration.run)(Provided::new(), b"null")
        .await
        .unwrap_err();
    assert!(!error.is_permanent());
    assert!(error.msg().contains("UserName"), "{}", error.msg());
}

#[test]
fn test_job_error_from_any_error_is_retryable() {
    let error = JobError::from(io::Error::other("boom"));
    assert_eq!(error.msg(), "boom");
    assert!(!error.is_permanent());
    assert!(JobError::permanent(io::Error::other("bad input")).is_permanent());
}

#[tokio::test]
async fn test_enqueue_before_init_fails() {
    let error = jalari::enqueue(&Greet {
        name: "a".to_owned(),
    })
    .await
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::NotInitialized);
}

#[tokio::test]
async fn test_worker_build_validates_queues_before_init() {
    let error = Worker::builder().build().await.err().unwrap();
    assert_eq!(error.kind, ErrorKind::NoQueueConfigured);

    let error = Worker::builder().queue("a", 0).build().await.err().unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidConcurrency);

    let error = Worker::builder()
        .queue("a", 1)
        .queue("a", 2)
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::DuplicateQueue);

    let mut config = WorkerConfig::default();
    config.heartbeat_interval = Duration::ZERO;
    let error = Worker::builder()
        .queue("a", 1)
        .config(config)
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidHeartbeatInterval);

    let error = Worker::builder().queue("a", 1).build().await.err().unwrap();
    assert_eq!(error.kind, ErrorKind::NotInitialized);
}
