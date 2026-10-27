use std::time::Duration;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_CLAIM_TRIES: u32 = 10;
const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Timings of a single worker, set with [`WorkerBuilder::config`](crate::WorkerBuilder::config).
///
/// Cluster-wide settings such as retention live in [`ClusterConfig`](crate::ClusterConfig). How
/// fast a dead or frozen worker's jobs are taken over is fixed by jalari, not configured here; see
/// the guide's "Crashes and lost connections".
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// let mut config = jalari::WorkerConfig::default();
/// config.poll_interval = Duration::from_secs(1);
/// config.shutdown_timeout = Duration::from_secs(120);
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    /// How often an idle queue checks for jobs without a notification; defaults to 5 seconds.
    ///
    /// Notifications usually wake workers at once; polling catches delayed jobs that become due
    /// and jobs inserted without a notification.
    pub poll_interval: Duration,
    /// How long [`Worker::run`](crate::Worker::run) waits for running jobs after shutdown is
    /// requested before aborting them; defaults to 30 seconds.
    pub shutdown_timeout: Duration,
    /// How many times a claim tries again after another worker took the same `queue_key` at the
    /// same instant; defaults to 10.
    pub max_claim_tries: u32,
    /// How long a running job may take to reach a [`checkpoint`](crate::checkpoint) after
    /// [`stop`](crate::stop) or [`cancel`](crate::cancel) before it is aborted; defaults to 30
    /// seconds.
    pub stop_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            max_claim_tries: DEFAULT_MAX_CLAIM_TRIES,
            stop_timeout: DEFAULT_STOP_TIMEOUT,
        }
    }
}
