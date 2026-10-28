use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use jalari::admin::ClearStepsOutcome;
use jalari::sqlx::PgPool;
use jalari::{Job, JobError, JobId, JobResult, JobState};
use serde::{Deserialize, Serialize};

static CHARGES: Mutex<BTreeMap<String, String>> = Mutex::new(BTreeMap::new());
static GATEWAY_CALLS: AtomicU32 = AtomicU32::new(0);
static RECEIPTS_SENT: AtomicU32 = AtomicU32::new(0);
static CRASH_AFTER_CHARGE: AtomicBool = AtomicBool::new(true);
static CRASH_AFTER_RECEIPT: AtomicBool = AtomicBool::new(true);

async fn gateway_charge(order_id: i64, cents: i64, idempotency_key: &str) -> String {
    GATEWAY_CALLS.fetch_add(1, Ordering::SeqCst);
    let mut charges = CHARGES.lock().unwrap();
    if let Some(charge_id) = charges.get(idempotency_key) {
        println!("    [gateway] key seen before, returning {charge_id} without charging again");
        return charge_id.clone();
    }
    let charge_id = format!("ch_{}", charges.len() + 1);
    println!("    [gateway] charged {cents} cents for order {order_id} as {charge_id}");
    charges.insert(idempotency_key.to_owned(), charge_id.clone());
    charge_id
}

async fn send_receipt(email: &str, charge_id: &str) {
    RECEIPTS_SENT.fetch_add(1, Ordering::SeqCst);
    println!("    [mailer] receipt for {charge_id} sent to {email}");
}

fn simulated_crash(after: &str) -> JobError {
    JobError::new(format!(
        "simulated crash after {after}, before the step was recorded"
    ))
    .retry_after(Duration::from_millis(300))
}

#[derive(Serialize, Deserialize)]
struct PlaceOrder {
    order_id: i64,
    cents: i64,
    email: String,
}

#[jalari::job(name = "place_order", queue = "orders", max_attempts = 5)]
impl Job for PlaceOrder {
    async fn run(self) -> JobResult {
        let attempt = jalari::current_job().map_or(0, |run| run.attempt());
        println!("  [place_order {}] attempt {attempt}", self.order_id);
        let (order_id, cents) = (self.order_id, self.cents);

        let charge_id: String = jalari::step("charge", |run| async move {
            println!(
                "  [charge] start {} with idempotency key {}",
                run.starts(),
                run.idempotency_key()
            );
            let charge_id = gateway_charge(order_id, cents, run.idempotency_key()).await;
            if CRASH_AFTER_CHARGE.swap(false, Ordering::SeqCst) {
                println!("  [charge] the worker \"crashes\" after the gateway call");
                return Err(simulated_crash("the charge"));
            }
            Ok(charge_id)
        })
        .await?;
        println!("  [place_order {order_id}] charge step returned {charge_id}");

        let email = self.email;
        let sent: bool = jalari::step("receipt", |run| async move {
            if run.starts() > 1 {
                println!(
                    "  [receipt] start {}: an earlier start may have sent it, so it is not sent again",
                    run.starts()
                );
                return Ok(false);
            }
            send_receipt(&email, &charge_id).await;
            if CRASH_AFTER_RECEIPT.swap(false, Ordering::SeqCst) {
                println!("  [receipt] the worker \"crashes\" after sending");
                return Err(simulated_crash("the receipt"));
            }
            Ok(true)
        })
        .await?;
        println!("  [place_order {order_id}] receipt step returned {sent}");
        Ok(())
    }
}

async fn wait_until_finished(id: JobId) -> Result<JobState, Box<dyn std::error::Error>> {
    for _ in 0..300 {
        let detail = jalari::admin::job(id).await?.ok_or("job not found")?;
        if matches!(detail.job.state, JobState::Succeeded | JobState::Failed) {
            return Ok(detail.job.state);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("job {id} did not finish").into())
}

async fn print_steps(id: JobId) -> Result<(), Box<dyn std::error::Error>> {
    let detail = jalari::admin::job(id).await?.ok_or("job not found")?;
    if detail.steps.is_empty() {
        println!("  (no recorded steps)");
    }
    for step in &detail.steps {
        println!(
            "  {:<8} {:<7} starts {} attempt {} output {} error {:?}",
            step.key,
            step.state,
            step.starts,
            step.attempt,
            step.output
                .as_ref()
                .map_or_else(|| "-".to_owned(), ToString::to_string),
            step.error,
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    jalari::init(pool)
        .schema(jalari::Schema::default())
        .migrate()
        .await?;

    let worker = jalari::Worker::builder()
        .queue("orders", 2)
        .scheduler(false)
        .build()
        .await?;
    let shutdown = jalari::CancellationToken::new();
    let worker_task = tokio::spawn(worker.run(shutdown.child_token()));

    println!("== An order whose worker \"crashes\" after each effect, before the step is recorded");
    let order = PlaceOrder {
        order_id: 1001,
        cents: 4_990,
        email: "ada@example.com".to_owned(),
    };
    let id = jalari::enqueue(&order).await?.id();
    let state = wait_until_finished(id).await?;
    println!("job {id} ended {state}");
    println!(
        "the gateway was called {} times and charged {} time: attempt 2 reused the idempotency \
         key, and attempt 3 read the charge from the step's record without calling it; {} \
         receipt was sent",
        GATEWAY_CALLS.load(Ordering::SeqCst),
        CHARGES.lock().unwrap().len(),
        RECEIPTS_SENT.load(Ordering::SeqCst),
    );

    println!("\n== The recorded steps, from admin::job");
    print_steps(id).await?;

    println!("\n== An operator clears the steps");
    match jalari::admin::clear_steps(id).await? {
        ClearStepsOutcome::Cleared(steps) => println!("clear_steps({id}) -> Cleared({steps})"),
        outcome => println!("clear_steps({id}) -> {outcome:?}"),
    }
    print_steps(id).await?;
    println!("a later run of this job would charge again, under a new idempotency key");

    shutdown.cancel();
    worker_task.await?;
    Ok(())
}
