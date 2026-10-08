use std::time::Duration;

use crate::{Error, ErrorKind, Result};

const MIN_HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);
const MIN_WORKER_TIMEOUT: Duration = Duration::from_secs(1);

/// Cluster-wide settings read from the `config` table.
///
/// Obtain it with [`jalari::cluster::get`](crate::cluster::get) and change it with
/// [`jalari::cluster::update`](crate::cluster::update). Per-worker settings live in
/// [`WorkerConfig`](crate::WorkerConfig) instead.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    /// Whether workers delete finished jobs once their retention passes; defaults to `false`.
    pub housekeeping_enabled: bool,
    /// How often the cleanup runs, across the whole cluster; defaults to 10 minutes, at least
    /// 1 second.
    pub housekeeping_interval: Duration,
    /// How long succeeded jobs are kept after finishing; defaults to 1 day.
    pub succeeded_retention: Duration,
    /// How long deleted jobs are kept after finishing; defaults to 7 days.
    pub deleted_retention: Duration,
    /// How long failed jobs are kept after finishing; `None`, the default, keeps them forever.
    pub failed_retention: Option<Duration>,
    /// How long a worker may miss heartbeats before it leaves
    /// [`jalari::admin::workers`](crate::admin::workers); defaults to 5 minutes, at least
    /// 1 second.
    pub worker_timeout: Duration,
}

impl ClusterConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.housekeeping_interval < MIN_HOUSEKEEPING_INTERVAL {
            return Err(invalid(format!(
                "housekeeping_interval {:?} must be at least {MIN_HOUSEKEEPING_INTERVAL:?}",
                self.housekeeping_interval
            )));
        }
        if self.worker_timeout < MIN_WORKER_TIMEOUT {
            return Err(invalid(format!(
                "worker_timeout {:?} must be at least {MIN_WORKER_TIMEOUT:?}",
                self.worker_timeout
            )));
        }
        Ok(())
    }
}

fn invalid(msg: String) -> Error {
    Error::new(ErrorKind::InvalidConfigValue, msg)
}
