use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::{env, fs};

use toml::de::{DeTable, DeValue};

use super::analyse::{Stamp, stamp};
use super::index::Index;
use super::lock::Lock;
use super::reach::{declaring_crate, locates};
use super::reexport::Reexport;
use super::workspace::{CrateSource, Dependency, JobSite, Workspace};

const MAX_CACHED_INDEXES: usize = 16;
const MAX_DIRECTORY_DEPTH: usize = 32;

static INDEXES: Mutex<BTreeMap<u64, Arc<Index>>> = Mutex::new(BTreeMap::new());

static TRACKED: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

#[derive(Default)]
pub(crate) struct CallSite {
    pub(crate) files: Vec<PathBuf>,
    pub(crate) attribute: Option<String>,
}

pub(crate) fn of_call_site(
    item: &syn::ItemImpl,
    call_site: &CallSite,
    jalari: &syn::Path,
) -> Stamp {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from);
    let local_files: Vec<&Path> = call_site.files.iter().map(PathBuf::as_path).collect();
    let reexport = Reexport::from_path(jalari).map(|reexport| match &call_site.attribute {
        Some(attribute) => reexport.with_wrapper(attribute),
        None => reexport,
    });
    let stamp = stamp_at(
        manifest_dir.as_deref(),
        &local_files,
        item,
        reexport.as_ref(),
    );
    Stamp {
        value: stamp.value,
        inputs: untracked(stamp.inputs),
    }
}

fn untracked(inputs: BTreeSet<PathBuf>) -> BTreeSet<PathBuf> {
    let mut tracked = TRACKED.lock().unwrap_or_else(PoisonError::into_inner);
    let mut fresh = BTreeSet::new();
    for input in inputs {
        if tracked.insert(input.clone()) {
            fresh.insert(input);
        }
    }
    fresh
}

pub(crate) fn stamp_at(
    manifest_dir: Option<&Path>,
    local_files: &[&Path],
    item: &syn::ItemImpl,
    reexport: Option<&Reexport>,
) -> Stamp {
    let mut workspace = manifest_dir.map(load).unwrap_or_default();
    let (all, roots): (Vec<String>, Vec<PathBuf>) = workspace
        .crates
        .first()
        .map(|primary| {
            let roots = primary.roots.iter().map(|root| primary.path(root));
            (primary.roots.clone(), roots.collect())
        })
        .unwrap_or_default();
    let found = manifest_dir
        .into_iter()
        .flat_map(|dir| {
            local_files
                .iter()
                .filter_map(move |file| relative(dir, file))
        })
        .find_map(|file| located(&mut workspace, &all, &file, item, reexport));
    let mut stamp = found.unwrap_or_else(|| {
        set_roots(&mut workspace, all);
        let index = cached(&workspace, reexport);
        let site = JobSite { item, file: None };
        stamp(&index, &workspace, &site, env!("CARGO_PKG_VERSION"))
    });
    stamp.inputs.extend(roots);
    stamp
}

fn located(
    workspace: &mut Workspace,
    all: &[String],
    file: &str,
    item: &syn::ItemImpl,
    reexport: Option<&Reexport>,
) -> Option<Stamp> {
    let primary = workspace.crates.first()?;
    let likely: Vec<String> = all
        .iter()
        .filter(|root| could_contain(&primary.files, root, file))
        .cloned()
        .collect();
    let site = JobSite {
        item,
        file: Some(file),
    };
    for candidates in [likely, all.to_vec()] {
        set_roots(workspace, candidates);
        let index = cached(workspace, reexport);
        if locates(&index, &site) {
            let reexport = match (reexport, declaring_crate(&index, &site)) {
                (Some(reexport), Some(krate)) => reexport.with_macro_crate(&krate),
                (reexport, _) => reexport.cloned(),
            };
            set_roots(workspace, roots_containing(&index, file));
            let index = cached(workspace, reexport.as_ref());
            return Some(stamp(&index, workspace, &site, env!("CARGO_PKG_VERSION")));
        }
    }
    None
}

fn set_roots(workspace: &mut Workspace, roots: Vec<String>) {
    if let Some(primary) = workspace.crates.first_mut() {
        primary.roots = roots;
    }
}

fn roots_containing(index: &Index, file: &str) -> Vec<String> {
    index
        .units
        .iter()
        .filter(|unit| unit.krate == 0 && unit.files.contains(file))
        .map(|unit| unit.root.clone())
        .collect()
}

fn could_contain(files: &BTreeMap<String, String>, root: &str, file: &str) -> bool {
    if root == file {
        return true;
    }
    let Some(text) = files.get(root) else {
        return false;
    };
    if text.contains("#[path") {
        return true;
    }
    let dir = root.rsplit_once('/').map_or("", |(dir, _)| dir);
    let rest = if dir.is_empty() {
        Some(file)
    } else {
        file.strip_prefix(dir)
            .and_then(|rest| rest.strip_prefix('/'))
    };
    let Some(first) = rest.and_then(|rest| rest.split('/').next()) else {
        return false;
    };
    text.contains(first.strip_suffix(".rs").unwrap_or(first))
}

pub(crate) fn load(manifest_dir: &Path) -> Workspace {
    let mut loader = Loader {
        crates: Vec::new(),
        packages: BTreeMap::new(),
        shared: BTreeMap::new(),
    };
    loader.primary(manifest_dir);
    Workspace {
        crates: loader.crates,
        lock: find_lock(manifest_dir),
    }
}

fn cached(workspace: &Workspace, reexport: Option<&Reexport>) -> Arc<Index> {
    let key = content_key(workspace, reexport);
    let mut cache = INDEXES.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(index) = cache.get(&key) {
        return Arc::clone(index);
    }
    let index = Arc::new(Index::build(workspace, reexport.cloned()));
    if cache.len() >= MAX_CACHED_INDEXES {
        cache.clear();
    }
    cache.insert(key, Arc::clone(&index));
    index
}

fn content_key(workspace: &Workspace, reexport: Option<&Reexport>) -> u64 {
    let mut hasher = DefaultHasher::new();
    workspace.crates.hash(&mut hasher);
    reexport.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone, Default)]
struct Spec {
    package: Option<String>,
    path: Option<PathBuf>,
    workspace: bool,
}

struct Manifest {
    package: String,
    lib_name: String,
    lib: Option<String>,
    targets: Vec<String>,
    dependencies: BTreeMap<String, Spec>,
    dev_dependencies: BTreeMap<String, Spec>,
}

struct Loader {
    crates: Vec<CrateSource>,
    packages: BTreeMap<PathBuf, (usize, String)>,
    shared: BTreeMap<PathBuf, BTreeMap<String, Spec>>,
}

impl Loader {
    fn primary(&mut self, dir: &Path) {
        let Some(manifest) = read_manifest(dir) else {
            return;
        };
        let files = read_files(dir);
        let roots = candidate_roots(&manifest, &files);
        self.crates.push(CrateSource {
            package: manifest.package.clone(),
            dir: canonical(dir),
            roots,
            files: files.clone(),
            deps: BTreeMap::new(),
        });
        let lib = manifest.lib.clone().map(|lib| {
            let index = self.crates.len();
            self.crates.push(CrateSource {
                package: manifest.package.clone(),
                dir: canonical(dir),
                roots: vec![lib],
                files,
                deps: BTreeMap::new(),
            });
            self.packages
                .insert(canonical(dir), (index, manifest.lib_name.clone()));
            index
        });
        let normal = self.dependencies(dir, &manifest.dependencies);
        let mut all = normal.clone();
        all.extend(self.dependencies(dir, &manifest.dev_dependencies));
        if let Some(index) = lib {
            self.crates[index].deps = normal;
            all.insert(manifest.lib_name.clone(), Dependency::Path(index));
        }
        self.crates[0].deps = all;
    }

    fn package(&mut self, dir: &Path) -> Option<(usize, String)> {
        let key = canonical(dir);
        if let Some(found) = self.packages.get(&key) {
            return Some(found.clone());
        }
        let manifest = read_manifest(dir)?;
        let lib = manifest.lib.clone()?;
        let index = self.crates.len();
        self.crates.push(CrateSource {
            package: manifest.package.clone(),
            dir: canonical(dir),
            roots: vec![lib],
            files: read_files(dir),
            deps: BTreeMap::new(),
        });
        self.packages
            .insert(key, (index, manifest.lib_name.clone()));
        let deps = self.dependencies(dir, &manifest.dependencies);
        self.crates[index].deps = deps;
        Some((index, manifest.lib_name))
    }

    fn dependencies(
        &mut self,
        dir: &Path,
        specs: &BTreeMap<String, Spec>,
    ) -> BTreeMap<String, Dependency> {
        let mut deps = BTreeMap::new();
        for (key, spec) in specs {
            let spec = self.resolve(dir, key, spec);
            let ident = key.replace('-', "_");
            match &spec.path {
                Some(path) => {
                    if let Some((index, lib_name)) = self.package(path) {
                        let name = if spec.package.is_some() {
                            ident
                        } else {
                            lib_name
                        };
                        deps.insert(name, Dependency::Path(index));
                    }
                }
                None => {
                    let package = spec.package.clone().unwrap_or_else(|| key.clone());
                    deps.insert(ident, Dependency::Registry(package));
                }
            }
        }
        deps
    }

    fn resolve(&mut self, dir: &Path, key: &str, spec: &Spec) -> Spec {
        if !spec.workspace {
            return Spec {
                path: spec.path.as_ref().map(|path| dir.join(path)),
                ..spec.clone()
            };
        }
        let shared = self
            .shared
            .entry(dir.to_path_buf())
            .or_insert_with(|| shared_dependencies(dir))
            .get(key)
            .cloned()
            .unwrap_or_default();
        Spec {
            package: spec.package.clone().or(shared.package),
            path: shared.path,
            workspace: false,
        }
    }
}

fn shared_dependencies(manifest_dir: &Path) -> BTreeMap<String, Spec> {
    for dir in manifest_dir.ancestors() {
        let Ok(text) = fs::read_to_string(dir.join("Cargo.toml")) else {
            continue;
        };
        let Ok(document) = DeTable::parse(&text) else {
            continue;
        };
        let Some(workspace) = table(document.get_ref(), "workspace") else {
            continue;
        };
        return table(workspace, "dependencies")
            .map(|dependencies| {
                specs(dependencies)
                    .into_iter()
                    .map(|(key, spec)| {
                        let path = spec.path.map(|path| dir.join(path));
                        (key, Spec { path, ..spec })
                    })
                    .collect()
            })
            .unwrap_or_default();
    }
    BTreeMap::new()
}

fn read_manifest(dir: &Path) -> Option<Manifest> {
    let text = fs::read_to_string(dir.join("Cargo.toml")).ok()?;
    let document = DeTable::parse(&text).ok()?;
    let root = document.get_ref();
    let package = string(table(root, "package")?, "name")?;
    let lib_table = table(root, "lib");
    let lib_name = lib_table
        .and_then(|lib| string(lib, "name"))
        .unwrap_or_else(|| package.replace('-', "_"));
    let lib = match lib_table.and_then(|lib| string(lib, "path")) {
        Some(path) => Some(clean(&path)),
        None => dir
            .join("src/lib.rs")
            .is_file()
            .then(|| "src/lib.rs".to_owned()),
    };
    let mut targets = Vec::new();
    for kind in ["bin", "test", "example", "bench"] {
        let Some(DeValue::Array(entries)) = root.get(kind).map(|value| value.get_ref()) else {
            continue;
        };
        for entry in entries {
            if let Some(path) = entry
                .get_ref()
                .as_table()
                .and_then(|target| string(target, "path"))
            {
                targets.push(clean(&path));
            }
        }
    }
    let mut dependencies = BTreeMap::new();
    let mut dev_dependencies = BTreeMap::new();
    let mut tables = vec![root];
    if let Some(targets) = table(root, "target") {
        tables.extend(
            targets
                .values()
                .filter_map(|value| value.get_ref().as_table()),
        );
    }
    for source in tables {
        if let Some(found) = table(source, "dependencies") {
            dependencies.extend(specs(found));
        }
        if let Some(found) = table(source, "dev-dependencies") {
            dev_dependencies.extend(specs(found));
        }
    }
    Some(Manifest {
        package,
        lib_name,
        lib,
        targets,
        dependencies,
        dev_dependencies,
    })
}

fn specs(dependencies: &DeTable<'_>) -> BTreeMap<String, Spec> {
    dependencies
        .iter()
        .map(|(key, value)| {
            let spec = match value.get_ref().as_table() {
                Some(fields) => Spec {
                    package: string(fields, "package"),
                    path: string(fields, "path").map(PathBuf::from),
                    workspace: fields
                        .get("workspace")
                        .and_then(|value| value.get_ref().as_bool())
                        == Some(true),
                },
                None => Spec::default(),
            };
            (key.get_ref().to_string(), spec)
        })
        .collect()
}

fn table<'a, 'i>(parent: &'a DeTable<'i>, key: &str) -> Option<&'a DeTable<'i>> {
    parent.get(key)?.get_ref().as_table()
}

fn string(parent: &DeTable<'_>, key: &str) -> Option<String> {
    parent.get(key)?.get_ref().as_str().map(str::to_owned)
}

fn candidate_roots(manifest: &Manifest, files: &BTreeMap<String, String>) -> Vec<String> {
    let mut roots: BTreeSet<String> = files
        .keys()
        .filter(|path| is_target_root(path))
        .cloned()
        .collect();
    roots.extend(manifest.lib.iter().cloned());
    roots.extend(manifest.targets.iter().cloned());
    roots
        .into_iter()
        .filter(|root| files.contains_key(root))
        .collect()
}

fn is_target_root(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    match parts.as_slice() {
        ["src", "lib.rs" | "main.rs"] => true,
        ["src", "bin", file] | ["tests" | "examples" | "benches", file] => file.ends_with(".rs"),
        ["src", "bin", _, "main.rs"] | ["tests" | "examples" | "benches", _, "main.rs"] => true,
        _ => false,
    }
}

fn read_files(dir: &Path) -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();
    walk(dir, dir, 0, &mut files);
    files
}

fn walk(base: &Path, dir: &Path, depth: usize, files: &mut BTreeMap<String, String>) {
    if depth > MAX_DIRECTORY_DEPTH {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if metadata.is_dir() {
            if name != "target" && !path.join("Cargo.toml").is_file() {
                walk(base, &path, depth + 1, files);
            }
            continue;
        }
        if !name.ends_with(".rs") {
            continue;
        }
        let Some(relative) = path.strip_prefix(base).ok().and_then(path_string) else {
            continue;
        };
        if let Ok(text) = fs::read_to_string(&path) {
            files.insert(relative, normalize(&text));
        }
    }
}

fn normalize(text: &str) -> String {
    text.strip_prefix('\u{feff}')
        .unwrap_or(text)
        .replace("\r\n", "\n")
}

fn find_lock(manifest_dir: &Path) -> Option<Lock> {
    manifest_dir.ancestors().find_map(|dir| {
        fs::read_to_string(dir.join("Cargo.lock"))
            .ok()
            .map(|text| Lock::parse(&text))
    })
}

fn relative(manifest_dir: &Path, file: &Path) -> Option<String> {
    let rest = canonical(file)
        .strip_prefix(canonical(manifest_dir))
        .ok()?
        .to_path_buf();
    path_string(&rest)
}

fn canonical(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn clean(path: &str) -> String {
    path.split(['/', '\\'])
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/")
}

fn path_string(path: &Path) -> Option<String> {
    let parts: Option<Vec<&str>> = path
        .components()
        .map(|component| match component {
            Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect();
    Some(parts?.join("/"))
}
