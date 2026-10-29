use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

const PROMPT: Duration = Duration::from_millis(200);
const STILL_HELD: Duration = Duration::from_millis(300);
const PLACEMENTS: [Placement; 2] = [Placement::Thread, Placement::Task];

fn finishing() -> AttemptFuture {
    Box::pin(async { Ok(()) })
}

fn named(names: &Arc<Mutex<Vec<String>>>, panics: bool) -> AttemptFuture {
    let names = Arc::clone(names);
    Box::pin(async move {
        let name = std::thread::current().name().map(str::to_owned);
        names.lock().unwrap().push(name.unwrap_or_default());
        assert!(!panics, "job exploded");
        Ok(())
    })
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

fn slot(threads: &SlotThreads, placement: Placement) -> Slot {
    threads.slot(&Arc::from("emails"), 0, placement)
}

async fn returns_and_frees_the_slot(placement: Placement) {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let mut slot = slot(&threads, placement);
    let mut attempt = slot.vacate(&shutdown).await.unwrap().launch(1, finishing());
    assert!(matches!(
        attempt.returned().await,
        Returned::Finished(Ok(()))
    ));
    drop(attempt);
    assert!(matches!(slot.body, Body::Idle));
    let vacated = tokio::time::timeout(PROMPT, slot.vacate(&shutdown)).await;
    assert!(matches!(vacated, Ok(Some(_))), "{placement:?}");
}

async fn panic_is_reported_and_the_slot_serves_the_next_attempt(placement: Placement) {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let names = Arc::default();
    let mut slot = slot(&threads, placement);
    let mut attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, named(&names, true));
    let returned = attempt.returned().await;
    drop(attempt);
    assert_eq!(
        returned.into_result().unwrap_err().msg(),
        "panicked: job exploded"
    );
    let mut attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(2, named(&names, false));
    assert!(matches!(
        attempt.returned().await,
        Returned::Finished(Ok(()))
    ));
    if placement == Placement::Thread {
        assert_eq!(*names.lock().unwrap(), ["emails:0", "emails:0"]);
    }
}

async fn held_slot_claims_nothing_until_its_body_returns(placement: Placement) {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let spinner = Spinner::new();
    let mut slot = slot(&threads, placement);
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
    assert!(matches!(vacated, Ok(Some(_))), "{placement:?}");
}

async fn returned_on_a_held_attempt_waits_for_the_body(placement: Placement) {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let spinner = Spinner::new();
    let mut slot = slot(&threads, placement);
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
    assert!(matches!(slot.body, Body::Idle), "{placement:?}");
}

async fn aborted_body_is_dropped_before_its_result_arrives(placement: Placement) {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let mut slot = slot(&threads, placement);
    let mut attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, sleeping(&dropped));
    tokio::time::sleep(Duration::from_millis(20)).await;
    attempt.abort_and_wait(Cause::StopTimeout).await;
    assert!(dropped.load(Ordering::SeqCst), "{placement:?}");
    drop(attempt);
    assert!(matches!(slot.body, Body::Idle));
}

async fn dropped_attempt_holds_the_slot_until_the_body_is_gone(placement: Placement) {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let mut slot = slot(&threads, placement);
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
    assert!(matches!(vacated, Ok(Some(_))), "{placement:?}");
    assert!(dropped.load(Ordering::SeqCst));
}

async fn vacate_refuses_during_shutdown(placement: Placement) {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let spinner = Spinner::new();
    let mut slot = slot(&threads, placement);
    let attempt = slot
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, spinner.body());
    wait_for(&spinner.started).await;
    drop(attempt);
    let refused = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { slot.vacate(&shutdown).await.is_none() }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();
    let refused = tokio::time::timeout(PROMPT, refused)
        .await
        .unwrap()
        .unwrap();
    assert!(refused, "{placement:?}");
    let mut idle = threads.slot(&Arc::from("emails"), 1, placement);
    assert!(idle.vacate(&shutdown).await.is_none());
}

macro_rules! on_both_placements {
    ($($scenario:ident => $test:ident,)+) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $test() {
                for placement in PLACEMENTS {
                    $scenario(placement).await;
                }
            }
        )+
    };
}

on_both_placements! {
    returns_and_frees_the_slot => test_returned_reports_the_result_and_frees_the_slot,
    panic_is_reported_and_the_slot_serves_the_next_attempt =>
        test_panic_is_reported_and_the_slot_serves_the_next_attempt,
    held_slot_claims_nothing_until_its_body_returns =>
        test_held_slot_claims_nothing_until_a_body_that_does_not_yield_returns,
    returned_on_a_held_attempt_waits_for_the_body =>
        test_returned_on_a_held_attempt_waits_for_the_body,
    aborted_body_is_dropped_before_its_result_arrives =>
        test_aborted_body_is_dropped_before_its_result_arrives,
    dropped_attempt_holds_the_slot_until_the_body_is_gone =>
        test_dropped_attempt_holds_the_slot_until_the_body_is_gone,
    vacate_refuses_during_shutdown => test_vacate_refuses_during_shutdown,
}

#[tokio::test]
async fn test_slot_thread_body_does_not_block_a_current_thread_caller() {
    const SPIN: Duration = Duration::from_millis(500);
    const TICK: Duration = Duration::from_millis(20);
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let mut slot = slot(&threads, Placement::Thread);
    let body: AttemptFuture = Box::pin(async {
        let started = std::time::Instant::now();
        while started.elapsed() < SPIN {
            std::hint::spin_loop();
        }
        Ok(())
    });
    let mut attempt = slot.vacate(&shutdown).await.unwrap().launch(1, body);
    let spinning = std::time::Instant::now();
    let mut widest = Duration::ZERO;
    while spinning.elapsed() < SPIN {
        let ticked = std::time::Instant::now();
        tokio::time::sleep(TICK).await;
        widest = widest.max(ticked.elapsed());
    }
    assert!(widest < TICK * 5, "the caller stalled for {widest:?}");
    assert!(matches!(
        attempt.returned().await,
        Returned::Finished(Ok(()))
    ));
}

#[tokio::test]
async fn test_retire_joins_idle_threads_and_leaves_stuck_ones() {
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    let spinner = Spinner::new();
    let mut stuck = threads.slot(&Arc::from("emails"), 0, Placement::Thread);
    let mut idle = threads.slot(&Arc::from("emails"), 1, Placement::Thread);
    let attempt = stuck
        .vacate(&shutdown)
        .await
        .unwrap()
        .launch(1, spinner.body());
    wait_for(&spinner.started).await;
    drop(attempt);
    assert!(idle.vacate(&shutdown).await.is_some());
    drop(stuck);
    drop(idle);
    let retiring = Instant::now();
    let retirement = threads.retire().await;
    assert!(
        retiring.elapsed() < ABORT_WAIT + PROMPT,
        "{:?}",
        retiring.elapsed()
    );
    assert_eq!(retirement, Retirement { joined: 1, left: 1 });
}

#[tokio::test]
async fn test_retire_returns_at_once_without_threads() {
    let threads = SlotThreads::new();
    drop(threads.slot(&Arc::from("emails"), 0, Placement::Task));
    let retiring = Instant::now();
    assert_eq!(threads.retire().await, Retirement { joined: 0, left: 0 });
    assert!(retiring.elapsed() < PROMPT);
}

#[test]
fn test_thread_name_keeps_the_index_within_fifteen_bytes() {
    assert_eq!(thread_name("emails", 0), "emails:0");
    assert_eq!(thread_name("thumbnails", 12), "thumbnails:12");
    assert_eq!(thread_name("very_long_queue_name", 3), "very_long_que:3");
    assert_eq!(thread_name("überlangewarteschlange", 120), "überlangew:120");
    for (queue, index) in [("q", 0), ("very_long_queue_name", 4095), ("ééééééééé", 7)] {
        assert!(thread_name(queue, index).len() <= THREAD_NAME_LIMIT);
    }
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

#[tokio::test(start_paused = true)]
async fn test_paused_clock_cannot_see_code_on_a_slot_thread() {
    const COMPUTE: Duration = Duration::from_millis(300);
    const DEADLINE: Duration = Duration::from_secs(10);
    let shutdown = CancellationToken::new();
    let threads = SlotThreads::new();
    for placement in PLACEMENTS {
        let mut slot = slot(&threads, placement);
        let body: AttemptFuture = Box::pin(async {
            std::thread::sleep(COMPUTE);
            Ok(())
        });
        let mut attempt = slot.vacate(&shutdown).await.unwrap().launch(1, body);
        let returned = tokio::time::timeout(DEADLINE, attempt.returned()).await;
        match placement {
            Placement::Thread => assert!(returned.is_err(), "the paused clock waited"),
            Placement::Task => assert!(matches!(returned, Ok(Returned::Finished(Ok(()))))),
        }
    }
}
