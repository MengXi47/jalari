use std::time::{Duration, SystemTime, UNIX_EPOCH};

use facade::__jalari::__private::registrations;
use facade::__jalari::recurring::Missed;
use facade::__jalari::sqlx::postgres::PgPoolOptions;
use facade::__jalari::sqlx::{self, AssertSqlSafe};
use facade::__jalari::{
    CancellationToken, JobState, Priority, Schema, Worker, WorkerConfig, admin, enqueue, init,
};
use facade_consumer::{AttrEcho, ECHOED, Echo, WrappedEcho};

#[test]
fn test_jobs_register_through_the_facade() {
    let echo = registrations()
        .find(|registration| registration.name == "facade_echo")
        .expect("facade_echo is registered");
    assert_eq!(echo.queue, "default");
    assert!(echo.cron.is_none());
    assert_eq!(echo.fingerprint, None);

    let wrapped = registrations()
        .find(|registration| registration.name == "facade_wrapped_echo")
        .expect("facade_wrapped_echo is registered through the job! wrapper");
    assert_eq!(wrapped.queue, "default");
    assert!(wrapped.cron.is_none());

    let nightly = registrations()
        .find(|registration| registration.name == "facade_nightly")
        .expect("facade_nightly is registered");
    assert_eq!(nightly.queue, "facade_nightly");
    assert_eq!(nightly.max_attempts, 3);
    assert_eq!(nightly.timeout, Some(Duration::from_secs(30)));
    assert_eq!(nightly.priority, Priority::High);
    assert_eq!(nightly.aliases, ["facade_nightly_old"]);
    assert!(nightly.checkpoints);
    assert_eq!(nightly.fingerprint.map(str::len), Some(32));
    let cron = nightly.cron.as_ref().expect("facade_nightly is recurring");
    assert_eq!(cron.expression, "0 0 3 * * *");
    assert_eq!(cron.timezone, "Asia/Taipei");
    assert_eq!(cron.missed, Missed::Skip);

    let attr_echo = registrations()
        .find(|registration| registration.name == "facade_attr_echo")
        .expect("facade_attr_echo is registered through the job_attr attribute");
    assert_eq!(attr_echo.queue, "facade");
    assert!(attr_echo.cron.is_none());
    assert_eq!(attr_echo.fingerprint.map(str::len), Some(32));

    let attr_nightly = registrations()
        .find(|registration| registration.name == "facade_attr_nightly")
        .expect("facade_attr_nightly is registered through the job_attr attribute");
    assert_eq!(attr_nightly.queue, "facade_attr_nightly");
    assert_eq!(attr_nightly.max_attempts, 3);
    assert_eq!(attr_nightly.timeout, Some(Duration::from_secs(30)));
    assert_eq!(attr_nightly.priority, Priority::High);
    assert_eq!(attr_nightly.aliases, ["facade_attr_nightly_old"]);
    assert!(attr_nightly.checkpoints);
    assert_eq!(attr_nightly.fingerprint.map(str::len), Some(32));
    let cron = attr_nightly
        .cron
        .as_ref()
        .expect("facade_attr_nightly is recurring");
    assert_eq!(cron.expression, "0 0 3 * * *");
    assert_eq!(cron.timezone, "Asia/Taipei");
    assert_eq!(cron.missed, Missed::Skip);
}

#[tokio::test]
async fn test_worker_runs_a_job_defined_through_the_facade() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is not set, skipping");
        return;
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let schema_name = format!("jalari_facade_{}_{nanos}", std::process::id());
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    init(pool.clone())
        .schema(Schema::named(&schema_name).unwrap())
        .migrate()
        .await
        .unwrap();

    let mut config = WorkerConfig::default();
    config.poll_interval = Duration::from_millis(100);
    config.shutdown_timeout = Duration::from_secs(10);
    let worker = Worker::builder()
        .queue("default", 1)
        .queue("facade", 1)
        .config(config)
        .build()
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(worker.run(shutdown.clone()));

    let job_ids = [
        enqueue(&Echo {
            word: "through the facade".to_owned(),
        })
        .await
        .unwrap()
        .id(),
        enqueue(&WrappedEcho {
            word: "through the wrapper".to_owned(),
        })
        .await
        .unwrap()
        .id(),
        enqueue(&AttrEcho {
            word: "through the attribute".to_owned(),
        })
        .await
        .unwrap()
        .id(),
    ];
    tokio::time::timeout(Duration::from_secs(30), async {
        for job_id in job_ids {
            loop {
                let detail = admin::job(job_id).await.unwrap().expect("the job exists");
                if detail.job.state == JobState::Succeeded {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await
    .expect("every job succeeds within 30 s");
    let mut echoed = ECHOED.lock().unwrap().clone();
    echoed.sort();
    assert_eq!(
        echoed,
        [
            "through the attribute",
            "through the facade",
            "through the wrapper"
        ]
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(60), running)
        .await
        .expect("the worker stops within 60 s")
        .unwrap();
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
}
