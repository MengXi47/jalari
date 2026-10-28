use std::collections::BTreeSet;

use quote::ToTokens;

use super::hash::canonical;
use super::index::{ImplId, Index, ItemId, ItemKind, Refs, impl_refs, is_keyword};
use super::workspace::JobSite;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberRule {
    Gated,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "only the precision measurement compares the two rules"
        )
    )]
    NameOnly,
}

#[derive(Debug, Default)]
pub(crate) struct Reached {
    pub(crate) items: BTreeSet<ItemId>,
    pub(crate) units: BTreeSet<usize>,
    pub(crate) roots: BTreeSet<(usize, String)>,
    open: BTreeSet<ImplId>,
    named: BTreeSet<ItemId>,
    names: BTreeSet<String>,
    members: BTreeSet<String>,
    modules: BTreeSet<String>,
    implemented: BTreeSet<String>,
}

enum Event {
    Name(String),
    Implemented(String),
    Member(String),
    Module(String),
    Unit(usize),
    Item(ItemId, bool),
    Open(ImplId),
}

pub(crate) fn locates(index: &Index, site: &JobSite<'_>) -> bool {
    site_units(index, site).is_some()
}

pub(crate) fn declaring_crate(index: &Index, site: &JobSite<'_>) -> Option<String> {
    let job = site_job(site)?;
    let call = index
        .units
        .iter()
        .filter(|unit| unit.krate == 0)
        .find_map(|unit| unit.declared.get(&job))?;
    let first = match call.as_slice() {
        [name] => index
            .nominal
            .get(name)?
            .iter()
            .map(|&item| &index.items[item])
            .find(|item| {
                item.kind == ItemKind::Use
                    && item.file == job.0
                    && index.units[item.unit].krate == 0
            })?
            .refs
            .roots
            .first()?
            .clone(),
        [first, ..] => first.clone(),
        [] => return None,
    };
    Some(match first.as_str() {
        "self" | "super" => "crate".to_owned(),
        _ => first,
    })
}

fn site_job(site: &JobSite<'_>) -> Option<(String, String)> {
    Some((
        site.file?.to_owned(),
        canonical(site.item.self_ty.to_token_stream()),
    ))
}

fn site_units(index: &Index, site: &JobSite<'_>) -> Option<Vec<usize>> {
    let job = site_job(site)?;
    let file = job.0.as_str();
    let units: Vec<usize> = index
        .units
        .iter()
        .enumerate()
        .filter(|(_, unit)| unit.krate == 0 && unit.files.contains(file))
        .map(|(id, _)| id)
        .collect();
    units
        .iter()
        .any(|&unit| index.units[unit].jobs.contains(&job))
        .then_some(units)
}

pub(crate) fn reach(index: &Index, site: &JobSite<'_>, rule: MemberRule) -> Option<Reached> {
    let units = site_units(index, site)?;
    let mut walk = Walk {
        index,
        rule,
        reached: Reached::default(),
        queue: units.into_iter().map(Event::Unit).collect(),
    };
    walk.refs(&impl_refs(site.item), 0);
    walk.run();
    Some(walk.reached)
}

struct Walk<'i> {
    index: &'i Index,
    rule: MemberRule,
    reached: Reached,
    queue: Vec<Event>,
}

impl Walk<'_> {
    fn run(&mut self) {
        while let Some(event) = self.queue.pop() {
            match event {
                Event::Name(name) => {
                    if !is_keyword(&name) && self.reached.names.insert(name.clone()) {
                        self.name(&name, None);
                    }
                }
                Event::Implemented(name) => {
                    if self.reached.implemented.insert(name.clone()) {
                        self.implemented(&name, None);
                    }
                }
                Event::Member(name) => {
                    if self.reached.members.insert(name.clone()) {
                        self.member(&name, None);
                    }
                }
                Event::Module(name) => {
                    if self.reached.modules.insert(name.clone()) {
                        self.module(&name, None);
                    }
                }
                Event::Unit(unit) => self.unit(unit),
                Event::Item(item, by_name) => self.item(item, by_name),
                Event::Open(id) => self.open(id),
            }
        }
    }

    fn live(&self, unit: usize, only: Option<usize>) -> bool {
        match only {
            Some(only) => unit == only,
            None => self.reached.units.contains(&unit),
        }
    }

    fn crate_units(&mut self, name: &str) {
        let index = self.index;
        for &krate in index.crates.get(name).into_iter().flatten() {
            for &unit in index.crate_units.get(krate).into_iter().flatten() {
                self.queue.push(Event::Unit(unit));
            }
        }
    }

    fn name(&mut self, name: &str, only: Option<usize>) {
        let index = self.index;
        for &item in index.nominal.get(name).into_iter().flatten() {
            if self.live(index.items[item].unit, only) {
                self.queue.push(Event::Item(item, true));
            }
        }
        for &id in index.gated.get(name).into_iter().flatten() {
            if self.live(index.impls[id].unit, only) {
                self.queue.push(Event::Open(id));
            }
        }
        for &item in index.opaque.get(name).into_iter().flatten() {
            if self.live(index.items[item].unit, only) {
                self.queue.push(Event::Item(item, true));
            }
        }
        self.crate_units(name);
    }

    fn implemented(&mut self, name: &str, only: Option<usize>) {
        let index = self.index;
        for &item in index.nominal.get(name).into_iter().flatten() {
            if self.live(index.items[item].unit, only) {
                self.queue.push(Event::Item(item, false));
            }
        }
        self.crate_units(name);
    }

    fn member(&mut self, name: &str, only: Option<usize>) {
        let index = self.index;
        for declaring in index.declaring.get(name).into_iter().flatten() {
            self.queue.push(Event::Name(declaring.clone()));
        }
        for &item in index.members.get(name).into_iter().flatten() {
            if !self.live(index.items[item].unit, only) {
                continue;
            }
            let ItemKind::Member(id) = index.items[item].kind else {
                continue;
            };
            match self.rule {
                MemberRule::Gated => {
                    if self.reached.open.contains(&id) {
                        self.queue.push(Event::Item(item, true));
                    }
                }
                MemberRule::NameOnly => {
                    self.queue.push(Event::Item(item, true));
                    self.queue.push(Event::Item(index.impls[id].header, true));
                }
            }
        }
    }

    fn module(&mut self, name: &str, only: Option<usize>) {
        let index = self.index;
        for &item in index.modules.get(name).into_iter().flatten() {
            if self.live(index.items[item].unit, only) {
                self.queue.push(Event::Item(item, true));
                if let Some(name) = &index.items[item].name {
                    self.queue.push(Event::Name(name.clone()));
                }
            }
        }
    }

    fn unit(&mut self, unit: usize) {
        if !self.reached.units.insert(unit) {
            return;
        }
        let index = self.index;
        let only = Some(unit);
        for name in self.reached.names.clone() {
            self.name(&name, only);
        }
        for name in self.reached.implemented.clone() {
            self.implemented(&name, only);
        }
        for name in self.reached.members.clone() {
            self.member(&name, only);
        }
        for name in self.reached.modules.clone() {
            self.module(&name, only);
        }
        let record = &index.units[unit];
        for &item in &record.always {
            self.queue.push(Event::Item(item, true));
        }
        for &id in &record.open {
            self.queue.push(Event::Open(id));
        }
        for name in &record.uses {
            self.crate_units(name);
        }
    }

    fn item(&mut self, item: ItemId, by_name: bool) {
        let index = self.index;
        let first = self.reached.items.insert(item);
        let newly_named = by_name && self.reached.named.insert(item);
        let record = &index.items[item];
        let krate = index.units[record.unit].krate;
        if record.kind == ItemKind::Use {
            if first {
                for root in &record.refs.roots {
                    self.reached.roots.insert((krate, root.clone()));
                }
            }
            if newly_named {
                for name in &record.refs.names {
                    self.queue.push(Event::Name(name.clone()));
                }
            } else if first {
                for name in &record.refs.names {
                    self.queue.push(Event::Implemented(name.clone()));
                }
            }
            return;
        }
        if first {
            self.refs(&record.refs, krate);
        }
    }

    fn refs(&mut self, refs: &Refs, krate: usize) {
        for name in &refs.names {
            self.queue.push(Event::Name(name.clone()));
        }
        for name in &refs.members {
            self.queue.push(Event::Member(name.clone()));
        }
        for name in &refs.modules {
            self.queue.push(Event::Module(name.clone()));
        }
        for name in &refs.implemented {
            self.queue.push(Event::Implemented(name.clone()));
        }
        for root in &refs.roots {
            self.reached.roots.insert((krate, root.clone()));
        }
    }

    fn open(&mut self, id: ImplId) {
        if !self.reached.open.insert(id) {
            return;
        }
        let index = self.index;
        let record = &index.impls[id];
        self.queue.push(Event::Item(record.header, true));
        for &member in &record.members {
            let visible = record.is_trait
                || index.items[member]
                    .name
                    .as_ref()
                    .is_none_or(|name| self.reached.members.contains(name));
            if visible {
                self.queue.push(Event::Item(member, true));
            }
        }
    }
}
