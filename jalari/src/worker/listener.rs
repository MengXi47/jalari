use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgListener;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::runner::{Shared, Wakers};
use crate::storage::{CONFIG_CHANNEL, JOB_CHANNEL, RECURRING_CHANNEL};

const MIN_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

pub(super) async fn listen(shared: Arc<Shared>, wakers: Arc<Wakers>, shutdown: CancellationToken) {
    let job_channel = shared.config.schema.notify_channel(JOB_CHANNEL);
    let recurring_channel = shared.config.schema.notify_channel(RECURRING_CHANNEL);
    let config_channel = shared.config.schema.notify_channel(CONFIG_CHANNEL);
    let channels = [
        job_channel.as_str(),
        recurring_channel.as_str(),
        config_channel.as_str(),
    ];
    let mut delay = MIN_RECONNECT_DELAY;
    while !shutdown.is_cancelled() {
        match shared.connect_listener(&channels).await {
            Ok(mut listener) => {
                delay = MIN_RECONNECT_DELAY;
                wake_all(&wakers);
                loop {
                    let message = tokio::select! {
                        () = shutdown.cancelled() => return,
                        message = listener.try_recv() => message,
                    };
                    match message {
                        Ok(Some(notification)) if notification.channel() == recurring_channel => {
                            wake(&wakers.scheduler);
                        }
                        Ok(Some(notification)) if notification.channel() == config_channel => {
                            wake(&wakers.housekeeping);
                        }
                        Ok(Some(notification)) => {
                            if let Some(waker) = wakers.queues.get(notification.payload()) {
                                waker.notify_one();
                            }
                        }
                        Ok(None) => wake_all(&wakers),
                        Err(e) => {
                            warn!(error = %e, "notification listener failed, reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, ?delay, "failed to start the notification listener"),
        }

        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(delay) => {}
        }
        delay = delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
    }
}

impl Shared {
    async fn connect_listener(&self, channels: &[&str]) -> sqlx::Result<PgListener> {
        let mut listener = PgListener::connect_with(&self.config.pool.pool()).await?;
        listener.listen_all(channels.iter().copied()).await?;
        Ok(listener)
    }
}

fn wake_all(wakers: &Wakers) {
    for waker in wakers.queues.values() {
        wake(waker);
    }
    wake(&wakers.scheduler);
    wake(&wakers.housekeeping);
}

fn wake(waker: &Notify) {
    waker.notify_waiters();
    waker.notify_one();
}
