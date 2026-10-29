use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

const PROMPT: Duration = Duration::from_millis(200);
const STILL_HELD: Duration = Duration::from_millis(300);

fn finishing() -> AttemptFuture {
    Box::pin(async { Ok(()) })
}

fn panicking() -> AttemptFuture {
    Box::pin(async { panic!("job exploded") })
}

struct Spinner {
    started: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}

impl Spinner {
    fn new() -> Self {
        Self {
            started: Arc::default(),
            release: Arc::default(),
        }
    }

    fn body(&self) -> AttemptFuture {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            started.store(true, Ordering::SeqCst);
            while !release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(())
        })
    }

    fn release(&self) {
        self.release.store(true, Ordering::SeqCst);
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.release();
    }
}

struct Flag(Arc<AtomicBool>);

impl Drop for Flag {
    fn drop(&mut self) {
        std::thread::sleep(STILL_HELD);
        self.0.store(true, Ordering::SeqCst);
    }
}

fn sleeping(dropped: &Arc<AtomicBool>) -> AttemptFuture {
    let flag = Flag(Arc::clone(dropped));
    Box::pin(async move {
        let _flag = flag;
        tokio::time::sleep(Duration::from_secs(600)).await;
        Ok(())
    })
}

async fn wait_for(flag: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !flag.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

fn slot() -> Slot {
    Slot::new(Arc::from("emails"), 0)
}

#[tokio::test]
async fn test_returned_reports_the_result_and_frees_the_slot() {
    let shutdown = CancellationToken::new();
    let mut slot = slot();
    let mut attempt = slot.vacate(&shutdown).await.unwrap().launch(1, finishing());
    assert!(matches!(
        attempt.returned().await,
        Returned::Finished(Ok(()))
    ));
    drop(attempt);
    assert!(matches!(slot.body, Body::Idle));
    let vacated = tokio::time::timeout(PROMPT, slot.vacate(&shutdown)).await;
    assert!(matches!(vacated, Ok(Some(_))));
}

#[tokio::test]
async fn test_panic_is_reported_and_the_slot_serves_the_next_attempt() {
    let shutdown = CancellationToken::new();
    let mut slot = slot();
    let mut attempt = slot.vacate(&shutdown).await.unwrap().launch(1, panicking());
    let returned = attempt.returned().await;
    drop(attempt);
    assert_eq!(
        returned.into_result().unwrap_err().msg(),
        "panicked: job exploded"
    );
    let mut attempt = slot.vacate(&shutdown).await.unwrap().launch(2, finishing());
    assert!(matches!(
        attempt.returned().await,
        Returned::Finished(Ok(()))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_held_slot_claims_nothing_until_a_body_that_does_not_yield_returns() {
    let shutdown = CancellationToken::new();
    let spinner = Spinner::new();
    let mut slot = slot();
    let mut attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, spinner.body());
    wait_for(&spinner.started).await;
    let aborted = Instant::now();
    attempt.abort_and_wait(Cause::TimedOut).await;
    assert!(aborted.elapsed() >= ABORT_WAIT, "{:?}", aborted.elapsed());
    drop(attempt);
    assert!(matches!(
        slot.body,
        Body::Held {
            cause: Cause::TimedOut,
            warned: true,
            ..
        }
    ));
    let vacated = tokio::time::timeout(STILL_HELD, slot.vacate(&shutdown)).await;
    assert!(vacated.is_err(), "a held slot was vacated");
    spinner.release();
    let vacated = tokio::time::timeout(PROMPT, slot.vacate(&shutdown)).await;
    assert!(matches!(vacated, Ok(Some(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_returned_on_a_held_attempt_waits_for_the_body() {
    let shutdown = CancellationToken::new();
    let spinner = Spinner::new();
    let mut slot = slot();
    let mut attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, spinner.body());
    wait_for(&spinner.started).await;
    attempt.abort_and_wait(Cause::StopTimeout).await;
    let early = tokio::time::timeout(STILL_HELD, attempt.returned()).await;
    assert!(early.is_err(), "a held attempt returned while its body ran");
    spinner.release();
    let returned = tokio::time::timeout(PROMPT, attempt.returned()).await;
    assert!(matches!(returned, Ok(Returned::Finished(Ok(())))));
    drop(attempt);
    assert!(matches!(slot.body, Body::Idle));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_aborted_body_is_dropped_before_its_result_arrives() {
    let shutdown = CancellationToken::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let mut slot = slot();
    let mut attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, sleeping(&dropped));
    tokio::time::sleep(Duration::from_millis(20)).await;
    attempt.abort_and_wait(Cause::StopTimeout).await;
    assert!(dropped.load(Ordering::SeqCst));
    drop(attempt);
    assert!(matches!(slot.body, Body::Idle));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dropped_attempt_holds_the_slot_until_the_body_is_gone() {
    let shutdown = CancellationToken::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let mut slot = slot();
    let attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, sleeping(&dropped));
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(attempt);
    assert!(matches!(
        slot.body,
        Body::Held {
            cause: Cause::Lost,
            ..
        }
    ));
    let vacated = tokio::time::timeout(STILL_HELD + PROMPT, slot.vacate(&shutdown)).await;
    assert!(matches!(vacated, Ok(Some(_))));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_vacate_refuses_during_shutdown() {
    let shutdown = CancellationToken::new();
    let spinner = Spinner::new();
    let mut slot = slot();
    let attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, spinner.body());
    wait_for(&spinner.started).await;
    drop(attempt);
    let refused = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let vacated = slot.vacate(&shutdown).await.is_none();
            (vacated, slot)
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();
    let (refused, slot) = tokio::time::timeout(PROMPT, refused)
        .await
        .unwrap()
        .unwrap();
    assert!(refused);
    spinner.release();
    let mut idle = Slot::new(Arc::clone(slot.queue()), 1);
    assert!(idle.vacate(&shutdown).await.is_none());
}

#[test]
fn test_returned_texts() {
    let text = |returned: Returned| returned.into_result().unwrap_err().msg().to_owned();
    assert_eq!(
        text(Returned::Panicked("job exploded".to_owned())),
        "panicked: job exploded"
    );
    assert_eq!(
        text(Returned::Vanished(
            "the attempt ended without a result".to_owned()
        )),
        "job task ended unexpectedly: the attempt ended without a result"
    );
    assert_eq!(
        text(Returned::Aborted),
        "job task ended unexpectedly: the attempt was aborted"
    );
}
