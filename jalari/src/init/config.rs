use std::sync::OnceLock;

use crate::{Error, ErrorKind, PoolSource, Result, Schema};

static CONFIG: OnceLock<Config> = OnceLock::new();

pub(crate) struct Config {
    pub(crate) pool: Box<dyn PoolSource>,
    pub(crate) schema: Schema,
}

pub(crate) fn config() -> Result<&'static Config> {
    CONFIG.get().ok_or_else(|| {
        Error::new(
            ErrorKind::NotInitialized,
            "call jalari::init before using jalari".to_owned(),
        )
    })
}

pub(super) fn is_initialized() -> bool {
    CONFIG.get().is_some()
}

pub(super) fn install(config: Config) -> Result<()> {
    CONFIG.set(config).map_err(|_| already_initialized())
}

pub(super) fn already_initialized() -> Error {
    Error::new(
        ErrorKind::AlreadyInitialized,
        "jalari::init can only be called once per process".to_owned(),
    )
}
