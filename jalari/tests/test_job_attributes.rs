mod common;

use std::time::Duration;

use common::Database;
use jalari::admin::{self, JobFilter};
use jalari::{EnqueueOutcome, Job, JobResult, Priority};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AttributeDefaultNamedJob;

#[jalari::job(queue = "attributes")]
impl Job for AttributeDefaultNamedJob {
    async fn run(self) -> JobResult {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Declared;

#[jalari::job(
    name = "attribute_declared",
    queue = "attributes",
    max_attempts = 5,
    timeout = 30,
    priority = "high",
    aliases = ["attribute_declared_before"],
    checkpoints = true
)]
impl Job for Declared {
    async fn run(self) -> JobResult {
        Ok(())
    }
}

fn inserted(outcome: EnqueueOutcome) -> jalari::JobId {
    match outcome {
        EnqueueOutcome::Inserted(id) => id,
        other => panic!("expected a new job, got {other:?}"),
    }
}

#[tokio::test]
async fn test_job_attributes() {
    let Some(database) = Database::start("attributes", 4).await else {
        return;
    };
    check_default_name_enqueues_and_lists_by_task().await;
    check_attribute_metadata_reaches_the_row().await;
    database.finish().await;
}

async fn check_default_name_enqueues_and_lists_by_task() {
    assert_eq!(
        <AttributeDefaultNamedJob as Job>::NAME,
        "AttributeDefaultNamedJob"
    );
    let id = inserted(jalari::enqueue(&AttributeDefaultNamedJob).await.unwrap());
    let filter = JobFilter::new().task("AttributeDefaultNamedJob");
    assert_eq!(admin::count(&filter).await.unwrap(), 1);
    let listed = admin::jobs(&filter, 10, None).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].queue, "attributes");
}

async fn check_attribute_metadata_reaches_the_row() {
    assert_eq!(<Declared as Job>::NAME, "attribute_declared");
    assert_eq!(<Declared as Job>::ALIASES, ["attribute_declared_before"]);
    let id = inserted(jalari::enqueue(&Declared).await.unwrap());
    let job = admin::job(id).await.unwrap().expect("the job exists").job;
    assert_eq!(job.task, "attribute_declared");
    assert_eq!(job.queue, "attributes");
    assert_eq!(job.priority, Priority::High);
    assert_eq!(job.max_attempts, 5);
    assert_eq!(job.timeout, Some(Duration::from_secs(30)));
    let registration = jalari::__private::find("attribute_declared_before")
        .expect("the alias finds the registration");
    assert_eq!(registration.name, "attribute_declared");
    assert!(registration.checkpoints);
}
