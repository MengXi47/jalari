use std::collections::BTreeSet;

use quote::ToTokens;

use super::hash::{Fnv, canonical};
use super::index::{Index, ItemId, hashed_job};
use super::lock::Lock;
use super::reach::{MemberRule, reach, written_attributes};
use super::reexport::{JALARI, Reexport};
use super::workspace::{CrateSource, Dependency, JobSite, Workspace};

const TAG: &str = "jalari-fingerprint/1";
const WHOLE_TAG: &str = "jalari-fingerprint/1/whole";

pub(crate) fn fingerprint(
    index: &Index,
    workspace: &Workspace,
    site: &JobSite<'_>,
    jalari_version: &str,
) -> u128 {
    let mut item = site.item.clone();
    if let Some(reexport) = &index.reexport {
        reexport.rewrite_item(&mut item);
    }
    if let Some(written) = written_attributes(index, site) {
        keep_written(&mut item.attrs, written);
    }
    let item = hashed_job(&item, index.reexport.as_ref());
    let site = &JobSite {
        item: &item,
        file: site.file,
    };
    let (tag, items, pins) = match reach(index, site, MemberRule::Gated) {
        Some(reached) => {
            let mut roots = reached.roots.clone();
            for &item in &reached.items {
                let record = &index.items[item];
                let unit = &index.units[record.unit];
                for glob in unit.globs.get(&record.file).into_iter().flatten() {
                    roots.insert((unit.krate, glob.clone()));
                }
            }
            let pins = pins(
                workspace,
                index.reexport.as_ref(),
                roots.iter().map(|(krate, name)| (*krate, name.as_str())),
            );
            (TAG, reached.items.into_iter().collect::<Vec<_>>(), pins)
        }
        None => {
            let roots = workspace
                .crates
                .iter()
                .enumerate()
                .flat_map(|(krate, source)| {
                    source.deps.keys().map(move |name| (krate, name.as_str()))
                });
            (
                WHOLE_TAG,
                (0..index.items.len()).collect(),
                pins(workspace, index.reexport.as_ref(), roots),
            )
        }
    };
    let texts = texts(index, workspace, &items);
    let mut fnv = Fnv::new();
    fnv.field(tag.as_bytes());
    fnv.field(jalari_version.as_bytes());
    fnv.field(canonical(site.item.to_token_stream()).as_bytes());
    fnv.count(texts.len());
    for text in &texts {
        fnv.field(text.as_bytes());
    }
    fnv.count(pins.len());
    for pin in &pins {
        fnv.field(pin.as_bytes());
    }
    fnv.finish()
}

fn keep_written(attributes: &mut Vec<syn::Attribute>, written: &[String]) {
    let mut unmatched: Vec<&str> = written.iter().map(String::as_str).collect();
    attributes.retain(|attribute| {
        let text = canonical(attribute.to_token_stream());
        match unmatched.iter().position(|written| *written == text) {
            Some(position) => {
                unmatched.swap_remove(position);
                true
            }
            None => false,
        }
    });
}

pub(crate) fn hex(fingerprint: u128) -> String {
    format!("{fingerprint:032x}")
}

pub(crate) fn texts<'i>(index: &'i Index, workspace: &Workspace, items: &[ItemId]) -> Vec<&'i str> {
    let unique: BTreeSet<(&str, &str, &str)> = items
        .iter()
        .map(|&item| {
            let record = &index.items[item];
            let krate = index.units[record.unit].krate;
            let package = workspace
                .crates
                .get(krate)
                .map_or("", |source| source.package.as_str());
            (package, record.file.as_str(), record.text.as_str())
        })
        .collect();
    let mut texts: Vec<&str> = unique.into_iter().map(|(_, _, text)| text).collect();
    texts.sort_unstable();
    texts
}

fn pins<'a>(
    workspace: &Workspace,
    reexport: Option<&Reexport>,
    roots: impl Iterator<Item = (usize, &'a str)>,
) -> BTreeSet<String> {
    let Some(lock) = &workspace.lock else {
        return BTreeSet::new();
    };
    roots
        .filter_map(|(krate, name)| {
            let source = workspace.crates.get(krate)?;
            match source.deps.get(name) {
                Some(Dependency::Registry(package)) => lock.pin(&source.package, package),
                Some(Dependency::Path(_)) => None,
                None if name == JALARI => reexported_jalari(workspace, lock, source, reexport?),
                None => None,
            }
        })
        .collect()
}

fn reexported_jalari(
    workspace: &Workspace,
    lock: &Lock,
    source: &CrateSource,
    reexport: &Reexport,
) -> Option<String> {
    match source.deps.get(reexport.root())? {
        Dependency::Registry(_) => lock.pin(&source.package, JALARI),
        Dependency::Path(facade) => {
            let facade = workspace.crates.get(*facade)?;
            facade
                .deps
                .values()
                .find_map(|dependency| match dependency {
                    Dependency::Registry(package) if package == JALARI => {
                        lock.pin(&facade.package, package)
                    }
                    _ => None,
                })
        }
    }
}
