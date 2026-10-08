//! Settings shared by every worker in the cluster, stored in the `config` table.

mod api;
mod config;
pub(crate) mod store;

pub use api::{get, update};
pub use config::ClusterConfig;
