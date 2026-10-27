use jalari::{ErrorKind, Job, JobResult, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Renamed;

#[jalari::job]
impl Job for Renamed {
    const NAME: &'static str = "renamed";
    const ALIASES: &'static [&'static str] = &["taken"];

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Taken;

#[jalari::job]
impl Job for Taken {
    const NAME: &'static str = "taken";

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct First;

#[jalari::job]
impl Job for First {
    const NAME: &'static str = "first";
    const ALIASES: &'static [&'static str] = &["shared"];

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Second;

#[jalari::job]
impl Job for Second {
    const NAME: &'static str = "second";
    const ALIASES: &'static [&'static str] = &["shared"];

    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Alone;

#[jalari::job]
impl Job for Alone {
    const NAME: &'static str = "alone";
    const ALIASES: &'static [&'static str] = &["alone_before"];

    async fn run(self) -> JobResult {
        Ok(())
    }
}

async fn build_error() -> String {
    let error = Worker::builder()
        .queue("default", 1)
        .build()
        .await
        .err()
        .expect("the build fails");
    assert_eq!(error.kind, ErrorKind::DuplicateJobName);
    error.msg
}

#[tokio::test]
async fn test_alias_equal_to_another_name_fails_build() {
    let message = build_error().await;
    assert!(
        message.contains("\"taken\" is the NAME or an alias of both \"renamed\" and \"taken\""),
        "{message}"
    );
}

#[tokio::test]
async fn test_alias_shared_by_two_jobs_fails_build() {
    let message = build_error().await;
    assert!(
        message.contains("\"shared\" is the NAME or an alias of both \"first\" and \"second\""),
        "{message}"
    );
}

#[tokio::test]
async fn test_distinct_aliases_are_not_reported() {
    let message = build_error().await;
    assert!(!message.contains("alone"), "{message}");
    assert_eq!(
        message.matches("is the NAME or an alias of both").count(),
        2
    );
}
