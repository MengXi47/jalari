use std::collections::BTreeMap;

use super::lock::Lock;

#[derive(Debug, Clone, Default)]
pub(crate) struct Workspace {
    pub(crate) crates: Vec<CrateSource>,
    pub(crate) lock: Option<Lock>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CrateSource {
    pub(crate) package: String,
    pub(crate) roots: Vec<String>,
    pub(crate) files: BTreeMap<String, String>,
    pub(crate) deps: BTreeMap<String, Dependency>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Dependency {
    Path(usize),
    Registry(String),
}

pub(crate) struct JobSite<'a> {
    pub(crate) item: &'a syn::ItemImpl,
    pub(crate) file: Option<&'a str>,
}
