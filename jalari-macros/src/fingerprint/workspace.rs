use std::collections::BTreeMap;
use std::path::PathBuf;

use super::lock::Lock;

#[derive(Debug, Clone, Default)]
pub(crate) struct Workspace {
    pub(crate) crates: Vec<CrateSource>,
    pub(crate) lock: Option<Lock>,
}

#[derive(Debug, Clone, Default, Hash)]
pub(crate) struct CrateSource {
    pub(crate) package: String,
    pub(crate) dir: PathBuf,
    pub(crate) roots: Vec<String>,
    pub(crate) files: BTreeMap<String, String>,
    pub(crate) deps: BTreeMap<String, Dependency>,
}

impl CrateSource {
    pub(crate) fn path(&self, file: &str) -> PathBuf {
        let mut path = self.dir.clone();
        path.extend(file.split('/'));
        path
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Dependency {
    Path(usize),
    Registry(String),
}

pub(crate) struct JobSite<'a> {
    pub(crate) item: &'a syn::ItemImpl,
    pub(crate) file: Option<&'a str>,
}
