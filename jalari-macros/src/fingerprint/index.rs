use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Delimiter, Group, Literal, Spacing, TokenStream, TokenTree};
use quote::ToTokens;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};
use syn::{
    Attribute, ExprMethodCall, ImplItem, ItemExternCrate, ItemImpl, ItemMacro, ItemUse, Macro,
    Meta, MetaNameValue, Path, Token, TraitItem, UseTree, parse_quote,
};

use super::hash::canonical;
use super::reexport::Reexport;
use super::workspace::{Dependency, Workspace};

pub(crate) type ItemId = usize;
pub(crate) type ImplId = usize;

const UNHASHED_CONSTANTS: [&str; 4] = ["NAME", "ALIASES", "PRIORITY", "RETRY_POLICY"];

const UNHASHED_ARGUMENTS: [&str; 6] = [
    "name",
    "aliases",
    "priority",
    "checkpoints",
    "missed",
    "crate",
];

const KEYWORDS: &[&str] = &[
    "_",
    "abstract",
    "as",
    "async",
    "await",
    "become",
    "box",
    "break",
    "const",
    "continue",
    "crate",
    "do",
    "dyn",
    "else",
    "enum",
    "extern",
    "false",
    "final",
    "fn",
    "for",
    "gen",
    "if",
    "impl",
    "in",
    "let",
    "loop",
    "macro",
    "match",
    "mod",
    "move",
    "mut",
    "override",
    "priv",
    "pub",
    "macro_rules",
    "ref",
    "return",
    "self",
    "Self",
    "static",
    "struct",
    "super",
    "trait",
    "true",
    "try",
    "type",
    "typeof",
    "unsafe",
    "unsized",
    "use",
    "virtual",
    "where",
    "while",
];

pub(crate) struct Index {
    pub(crate) units: Vec<Unit>,
    pub(crate) items: Vec<Item>,
    pub(crate) impls: Vec<Impl>,
    pub(crate) nominal: BTreeMap<String, Vec<ItemId>>,
    pub(crate) members: BTreeMap<String, Vec<ItemId>>,
    pub(crate) declaring: BTreeMap<String, BTreeSet<String>>,
    pub(crate) gated: BTreeMap<String, Vec<ImplId>>,
    pub(crate) modules: BTreeMap<String, Vec<ItemId>>,
    pub(crate) opaque: BTreeMap<String, Vec<ItemId>>,
    pub(crate) crates: BTreeMap<String, BTreeSet<usize>>,
    pub(crate) crate_units: Vec<Vec<usize>>,
    pub(crate) reexport: Option<Reexport>,
}

#[derive(Default)]
pub(crate) struct Unit {
    pub(crate) krate: usize,
    pub(crate) root: String,
    pub(crate) files: BTreeSet<String>,
    pub(crate) uses: BTreeSet<String>,
    pub(crate) globs: BTreeMap<String, BTreeSet<String>>,
    pub(crate) jobs: BTreeSet<(String, String)>,
    pub(crate) declared: BTreeMap<(String, String), Vec<String>>,
    pub(crate) always: Vec<ItemId>,
    pub(crate) open: Vec<ImplId>,
}

pub(crate) struct Item {
    pub(crate) unit: usize,
    pub(crate) file: String,
    pub(crate) name: Option<String>,
    pub(crate) kind: ItemKind,
    pub(crate) text: String,
    pub(crate) refs: Refs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ItemKind {
    Nominal,
    Use,
    Member(ImplId),
    Header(ImplId),
    Opaque,
}

#[derive(Debug, Default)]
pub(crate) struct Refs {
    pub(crate) names: BTreeSet<String>,
    pub(crate) members: BTreeSet<String>,
    pub(crate) modules: BTreeSet<String>,
    pub(crate) implemented: BTreeSet<String>,
    pub(crate) roots: BTreeSet<String>,
}

pub(crate) struct Impl {
    pub(crate) unit: usize,
    pub(crate) is_trait: bool,
    pub(crate) header: ItemId,
    pub(crate) members: Vec<ItemId>,
}

impl Index {
    pub(crate) fn build(workspace: &Workspace, reexport: Option<Reexport>) -> Self {
        let mut builder = Builder {
            workspace,
            index: Index {
                units: Vec::new(),
                items: Vec::new(),
                impls: Vec::new(),
                nominal: BTreeMap::new(),
                members: BTreeMap::new(),
                declaring: BTreeMap::new(),
                gated: BTreeMap::new(),
                modules: BTreeMap::new(),
                opaque: BTreeMap::new(),
                crates: BTreeMap::new(),
                crate_units: vec![Vec::new(); workspace.crates.len()],
                reexport,
            },
            local: BTreeSet::new(),
            gates: Vec::new(),
            macros: BTreeMap::new(),
            triggers: Vec::new(),
        };
        for (krate, source) in workspace.crates.iter().enumerate() {
            for root in &source.roots {
                if source.files.contains_key(root) {
                    let unit = builder.index.units.len();
                    builder.index.units.push(Unit {
                        krate,
                        root: root.clone(),
                        ..Unit::default()
                    });
                    builder.index.crate_units[krate].push(unit);
                    builder.file(unit, root, Vec::new(), true);
                }
            }
            for dependency in source.deps.iter() {
                if let (ident, Dependency::Path(target)) = dependency {
                    builder
                        .index
                        .crates
                        .entry(ident.clone())
                        .or_default()
                        .insert(*target);
                }
            }
        }
        builder.finish()
    }
}

struct Builder<'w> {
    workspace: &'w Workspace,
    index: Index,
    local: BTreeSet<String>,
    gates: Vec<(ImplId, BTreeSet<String>)>,
    macros: BTreeMap<String, BTreeSet<String>>,
    triggers: Vec<(ItemId, Option<String>, BTreeSet<String>)>,
}

struct Place<'a> {
    unit: usize,
    file: &'a str,
    module: Vec<String>,
    dir: String,
    file_dir: String,
    inline: bool,
}

impl Builder<'_> {
    fn file(&mut self, unit: usize, path: &str, module: Vec<String>, mod_rs: bool) {
        let workspace = self.workspace;
        let krate = self.index.units[unit].krate;
        let Some(content) = workspace.crates[krate].files.get(path) else {
            return;
        };
        if !self.index.units[unit].files.insert(path.to_owned()) {
            return;
        }
        let file_dir = parent(path).to_owned();
        let dir = if mod_rs {
            file_dir.clone()
        } else {
            join(&file_dir, stem(path)).unwrap_or_default()
        };
        let mut parsed = match syn::parse_file(content) {
            Ok(parsed) => parsed,
            Err(_) => {
                self.unparseable(unit, path, content);
                return;
            }
        };
        let mut declared = Vec::new();
        unfold(&mut parsed.items, &mut declared);
        if let Some(reexport) = &self.index.reexport {
            reexport.rewrite_file(&mut parsed);
        }
        let mut scan = Scan::default();
        scan.visit_file(&parsed);
        let record = &mut self.index.units[unit];
        record.uses.extend(scan.uses);
        if !scan.globs.is_empty() {
            record
                .globs
                .entry(path.to_owned())
                .or_default()
                .extend(scan.globs);
        }
        record
            .jobs
            .extend(scan.jobs.into_iter().map(|job| (path.to_owned(), job)));
        record.declared.extend(
            declared
                .into_iter()
                .map(|(job, call)| ((path.to_owned(), job), call)),
        );
        let place = Place {
            unit,
            file: path,
            module,
            dir,
            file_dir,
            inline: false,
        };
        self.items(&place, &parsed.items);
    }

    fn unparseable(&mut self, unit: usize, path: &str, content: &str) {
        let mut refs = Refs::default();
        let text = match content.parse::<TokenStream>() {
            Ok(tokens) => {
                let tokens = match &self.index.reexport {
                    Some(reexport) => reexport.rewrite_tokens(tokens),
                    None => tokens,
                };
                token_refs(tokens.clone(), &mut refs, true, false);
                canonical(tokens)
            }
            Err(_) => {
                for word in content.split(|c: char| !(c.is_alphanumeric() || c == '_')) {
                    if !word.is_empty() && !is_keyword(word) {
                        refs.names.insert(word.to_owned());
                        refs.members.insert(word.to_owned());
                    }
                }
                content.split_whitespace().collect::<Vec<_>>().join(" ")
            }
        };
        let id = self.push(unit, path, None, ItemKind::Opaque, text, refs);
        self.index.units[unit].always.push(id);
    }

    fn push(
        &mut self,
        unit: usize,
        file: &str,
        name: Option<String>,
        kind: ItemKind,
        text: String,
        refs: Refs,
    ) -> ItemId {
        let id = self.index.items.len();
        self.index.items.push(Item {
            unit,
            file: file.to_owned(),
            name,
            kind,
            text,
            refs,
        });
        id
    }

    fn items(&mut self, place: &Place<'_>, items: &[syn::Item]) {
        for item in items {
            match item {
                syn::Item::Mod(module) => self.module(place, module),
                syn::Item::Impl(item_impl) => self.implementation(place, item_impl),
                syn::Item::Use(item_use) => self.uses(place, item_use),
                syn::Item::Macro(item_macro) => self.macro_item(place, item, item_macro),
                syn::Item::Fn(function) => self.nominal(place, &function.sig.ident, item, false),
                syn::Item::Struct(definition) => self.nominal(place, &definition.ident, item, true),
                syn::Item::Enum(definition) => self.nominal(place, &definition.ident, item, true),
                syn::Item::Union(definition) => self.nominal(place, &definition.ident, item, true),
                syn::Item::Type(definition) => self.nominal(place, &definition.ident, item, true),
                syn::Item::TraitAlias(definition) => {
                    self.nominal(place, &definition.ident, item, true);
                }
                syn::Item::Trait(definition) => {
                    let name = definition.ident.to_string();
                    for trait_item in &definition.items {
                        let member = match trait_item {
                            TraitItem::Fn(function) => &function.sig.ident,
                            TraitItem::Const(constant) => &constant.ident,
                            TraitItem::Type(associated) => &associated.ident,
                            _ => continue,
                        };
                        self.index
                            .declaring
                            .entry(member.to_string())
                            .or_default()
                            .insert(name.clone());
                    }
                    self.nominal(place, &definition.ident, item, true);
                }
                syn::Item::Const(constant) => self.nominal(place, &constant.ident, item, false),
                syn::Item::Static(definition) => {
                    self.nominal(place, &definition.ident, item, false);
                }
                syn::Item::ExternCrate(extern_crate) => {
                    let ident = match &extern_crate.rename {
                        Some((_, rename)) => rename,
                        None => &extern_crate.ident,
                    };
                    self.nominal(place, ident, item, false);
                }
                other => self.opaque(place, other, None),
            }
        }
    }

    fn module(&mut self, place: &Place<'_>, module: &syn::ItemMod) {
        let name = module.ident.to_string();
        let mut path = place.module.clone();
        path.push(name.clone());
        let attribute = path_attribute(&module.attrs);
        if let Some((_, content)) = &module.content {
            let dir = match &attribute {
                Some(relative) => join(&place.dir, relative),
                None => join(&place.dir, &name),
            };
            let inner = Place {
                unit: place.unit,
                file: place.file,
                module: path,
                dir: dir.unwrap_or_default(),
                file_dir: place.file_dir.clone(),
                inline: true,
            };
            self.items(&inner, content);
            return;
        }
        let candidates = match &attribute {
            Some(relative) => {
                let base = if place.inline {
                    &place.dir
                } else {
                    &place.file_dir
                };
                join(base, relative).into_iter().collect::<Vec<_>>()
            }
            None => [format!("{name}.rs"), format!("{name}/mod.rs")]
                .iter()
                .filter_map(|relative| join(&place.dir, relative))
                .collect(),
        };
        let krate = self.index.units[place.unit].krate;
        let files = &self.workspace.crates[krate].files;
        if let Some(found) = candidates.into_iter().find(|path| files.contains_key(path)) {
            let mod_rs = attribute.is_some() || found.ends_with("/mod.rs") || found == "mod.rs";
            self.file(place.unit, &found, path, mod_rs);
        }
    }

    fn nominal(&mut self, place: &Place<'_>, ident: &syn::Ident, item: &syn::Item, local: bool) {
        let name = ident.to_string();
        let mut refs = Refs::default();
        Collector { refs: &mut refs }.visit_item(item);
        let text = canonical(item.to_token_stream());
        let id = self.push(
            place.unit,
            place.file,
            Some(name.clone()),
            ItemKind::Nominal,
            text,
            refs,
        );
        self.index.nominal.entry(name.clone()).or_default().push(id);
        if let Some(module) = place.module.last() {
            self.index
                .modules
                .entry(module.clone())
                .or_default()
                .push(id);
        }
        if local {
            self.local.insert(name);
        }
    }

    fn uses(&mut self, place: &Place<'_>, item_use: &ItemUse) {
        let mut leaves = Vec::new();
        use_leaves(&item_use.tree, &mut Vec::new(), &mut leaves);
        for (segments, name) in leaves {
            let Some(name) = name else {
                continue;
            };
            let mut refs = Refs::default();
            if let Some(first) = segments.first() {
                refs.roots.insert(first.clone());
            }
            refs.names.extend(
                segments
                    .iter()
                    .filter(|segment| !is_keyword(segment))
                    .cloned(),
            );
            let text = format!("use {} as {name}", segments.join("::"));
            let id = self.push(
                place.unit,
                place.file,
                Some(name.clone()),
                ItemKind::Use,
                text,
                refs,
            );
            self.index.nominal.entry(name).or_default().push(id);
            if let Some(module) = place.module.last() {
                self.index
                    .modules
                    .entry(module.clone())
                    .or_default()
                    .push(id);
            }
        }
    }

    fn macro_item(&mut self, place: &Place<'_>, item: &syn::Item, item_macro: &ItemMacro) {
        match &item_macro.ident {
            Some(ident) => {
                let mut body = Refs::default();
                token_refs(item_macro.mac.tokens.clone(), &mut body, false, false);
                self.macros
                    .entry(ident.to_string())
                    .or_default()
                    .extend(body.names);
                self.nominal(place, ident, item, false);
            }
            None => {
                let invoked = item_macro
                    .mac
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident.to_string());
                self.opaque(place, item, invoked);
            }
        }
    }

    fn opaque(&mut self, place: &Place<'_>, item: &syn::Item, invoked: Option<String>) {
        let tokens = item.to_token_stream();
        let mut refs = Refs::default();
        token_refs(tokens.clone(), &mut refs, true, false);
        let triggers = refs.names.clone();
        let text = canonical(tokens);
        let id = self.push(place.unit, place.file, None, ItemKind::Opaque, text, refs);
        self.triggers.push((id, invoked, triggers));
    }

    fn implementation(&mut self, place: &Place<'_>, item_impl: &ItemImpl) {
        let hashed;
        let item_impl = if is_job_impl(item_impl) {
            hashed = hashed_job(item_impl, self.index.reexport.as_ref());
            &hashed
        } else {
            item_impl
        };
        let impl_id = self.index.impls.len();
        let mut header = item_impl.clone();
        header.items.clear();
        let header_text = canonical(header.to_token_stream());
        let mut refs = Refs::default();
        header_refs(item_impl, &mut refs, true);
        let mut gate_refs = Refs::default();
        header_refs(item_impl, &mut gate_refs, false);
        let trait_gates = if is_job_impl(item_impl) {
            BTreeSet::new()
        } else {
            gate_refs.implemented
        };
        let gates = gate_refs.names.into_iter().chain(trait_gates).collect();
        let header_id = self.push(
            place.unit,
            place.file,
            None,
            ItemKind::Header(impl_id),
            header_text.clone(),
            refs,
        );
        let mut members = Vec::new();
        for impl_item in &item_impl.items {
            let name = match impl_item {
                ImplItem::Fn(function) => Some(function.sig.ident.to_string()),
                ImplItem::Const(constant) => Some(constant.ident.to_string()),
                ImplItem::Type(associated) => Some(associated.ident.to_string()),
                _ => None,
            };
            let mut refs = Refs::default();
            Collector { refs: &mut refs }.visit_impl_item(impl_item);
            let text = format!(
                "{header_text}{{ {}}} ",
                canonical(impl_item.to_token_stream())
            );
            let id = self.push(
                place.unit,
                place.file,
                name.clone(),
                ItemKind::Member(impl_id),
                text,
                refs,
            );
            if let Some(name) = name {
                self.index.members.entry(name).or_default().push(id);
            }
            members.push(id);
        }
        self.index.impls.push(Impl {
            unit: place.unit,
            is_trait: item_impl.trait_.is_some(),
            header: header_id,
            members,
        });
        self.gates.push((impl_id, gates));
    }

    fn finish(mut self) -> Index {
        for (id, invoked, mut triggers) in std::mem::take(&mut self.triggers) {
            if let Some(body) = invoked.and_then(|name| self.macros.get(&name)) {
                triggers.extend(body.iter().cloned());
            }
            for trigger in triggers {
                self.index.opaque.entry(trigger).or_default().push(id);
            }
        }
        for (impl_id, gates) in std::mem::take(&mut self.gates) {
            let local: Vec<String> = gates
                .into_iter()
                .filter(|gate| self.local.contains(gate))
                .collect();
            if local.is_empty() {
                let unit = self.index.impls[impl_id].unit;
                self.index.units[unit].open.push(impl_id);
            }
            for gate in local {
                self.index.gated.entry(gate).or_default().push(impl_id);
            }
        }
        self.index
    }
}

#[derive(Default)]
struct Scan {
    uses: BTreeSet<String>,
    globs: BTreeSet<String>,
    jobs: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for Scan {
    fn visit_item_use(&mut self, item_use: &'ast ItemUse) {
        let mut leaves = Vec::new();
        use_leaves(&item_use.tree, &mut Vec::new(), &mut leaves);
        for (segments, name) in leaves {
            if name.is_none()
                && let Some(first) = segments.first()
            {
                self.globs.insert(first.clone());
            }
            self.uses.extend(segments);
        }
    }

    fn visit_item_extern_crate(&mut self, extern_crate: &'ast ItemExternCrate) {
        self.uses.insert(extern_crate.ident.to_string());
    }

    fn visit_item_impl(&mut self, item_impl: &'ast ItemImpl) {
        if is_job_impl(item_impl) {
            self.jobs
                .insert(canonical(item_impl.self_ty.to_token_stream()));
        }
        visit::visit_item_impl(self, item_impl);
    }
}

pub(crate) fn hashed_job(item_impl: &ItemImpl, reexport: Option<&Reexport>) -> ItemImpl {
    let mut hashed = item_impl.clone();
    hashed.items.retain(|item| {
        !matches!(item, ImplItem::Const(constant)
            if UNHASHED_CONSTANTS.iter().any(|name| constant.ident == name))
    });
    for attribute in &mut hashed.attrs {
        let wrapper = reexport.is_some_and(|reexport| reexport.is_wrapper(attribute.path()));
        if !wrapper && !is_job_attribute(attribute) {
            continue;
        }
        if reexport.is_some() {
            let path = match &mut attribute.meta {
                Meta::Path(path) => path,
                Meta::List(list) => &mut list.path,
                Meta::NameValue(pair) => &mut pair.path,
            };
            if wrapper || path.segments.len() > 1 {
                *path = parse_quote!(jalari::job);
            }
        }
        let Ok(arguments) =
            attribute.parse_args_with(Punctuated::<MetaNameValue, Token![,]>::parse_terminated)
        else {
            continue;
        };
        let kept: Punctuated<MetaNameValue, Token![,]> = arguments
            .into_iter()
            .filter(|argument| {
                !UNHASHED_ARGUMENTS
                    .iter()
                    .any(|name| argument.path.is_ident(name))
            })
            .collect();
        let path = attribute.path().clone();
        attribute.meta = if kept.is_empty() {
            Meta::Path(path)
        } else {
            parse_quote!(#path(#kept))
        };
    }
    hashed
}

pub(crate) fn is_job_attribute(attribute: &Attribute) -> bool {
    attribute
        .path()
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "job")
}

fn unfold(items: &mut Vec<syn::Item>, declared: &mut Vec<(String, Vec<String>)>) {
    for item in std::mem::take(items) {
        match item {
            syn::Item::Macro(mut call) if call.ident.is_none() => {
                let (rest, jobs) = split_call(call.mac.tokens.clone());
                if jobs.is_empty() {
                    items.push(syn::Item::Macro(call));
                    continue;
                }
                let path: Vec<String> = call
                    .mac
                    .path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect();
                for job in jobs {
                    declared.push((canonical(job.self_ty.to_token_stream()), path.clone()));
                    items.push(syn::Item::Impl(job));
                }
                if !call.attrs.is_empty() || !is_blank(&rest) {
                    call.mac.tokens = rest;
                    items.push(syn::Item::Macro(call));
                }
            }
            syn::Item::Mod(mut module) => {
                if let Some((_, content)) = &mut module.content {
                    unfold(content, declared);
                }
                items.push(syn::Item::Mod(module));
            }
            other => items.push(other),
        }
    }
}

fn split_call(tokens: TokenStream) -> (TokenStream, Vec<ItemImpl>) {
    let trees: Vec<TokenTree> = tokens.into_iter().collect();
    let mut rest: Vec<TokenTree> = Vec::with_capacity(trees.len());
    let mut jobs = Vec::new();
    let mut floor = 0;
    let mut position = 0;
    while position < trees.len() {
        if let Some((start, end, job)) = job_in_call(&trees, floor, position) {
            rest.truncate(rest.len() - (position - start));
            jobs.push(job);
            position = end;
            floor = end;
            continue;
        }
        rest.push(match &trees[position] {
            TokenTree::Group(group) => {
                let (stream, inner) = split_call(group.stream());
                jobs.extend(inner);
                let mut rebuilt = Group::new(group.delimiter(), stream);
                rebuilt.set_span(group.span());
                TokenTree::Group(rebuilt)
            }
            other => other.clone(),
        });
        position += 1;
    }
    (rest.into_iter().collect(), jobs)
}

fn job_in_call(trees: &[TokenTree], floor: usize, at: usize) -> Option<(usize, usize, ItemImpl)> {
    if !matches!(&trees[at], TokenTree::Ident(ident) if ident == "impl") {
        return None;
    }
    let mut start = at;
    while start >= floor + 2
        && matches!(&trees[start - 2], TokenTree::Punct(pound) if pound.as_char() == '#')
        && matches!(&trees[start - 1], TokenTree::Group(group) if group.delimiter() == Delimiter::Bracket)
    {
        start -= 2;
    }
    let body = (at + 1..trees.len()).find(|&position| {
        matches!(&trees[position], TokenTree::Group(group) if group.delimiter() == Delimiter::Brace)
    })?;
    let end = body + 1;
    let mut job: ItemImpl = syn::parse2(trees[start..end].iter().cloned().collect()).ok()?;
    if !is_job_impl(&job) {
        return None;
    }
    if !job.attrs.iter().any(is_job_attribute) {
        let arguments = start
            .checked_sub(1)
            .filter(|&before| before >= floor)
            .and_then(|before| match &trees[before] {
                TokenTree::Group(group) if group.delimiter() == Delimiter::Parenthesis => {
                    Punctuated::<MetaNameValue, Token![,]>::parse_terminated
                        .parse2(group.stream())
                        .ok()
                }
                _ => None,
            });
        let attribute: Attribute = match &arguments {
            Some(arguments) if !arguments.is_empty() => parse_quote!(#[jalari::job(#arguments)]),
            _ => parse_quote!(#[jalari::job]),
        };
        if arguments.is_some() {
            start -= 1;
        }
        job.attrs.insert(0, attribute);
    }
    Some((start, end, job))
}

fn is_blank(tokens: &TokenStream) -> bool {
    tokens
        .clone()
        .into_iter()
        .all(|tree| matches!(tree, TokenTree::Group(group) if is_blank(&group.stream())))
}

pub(crate) fn is_job_impl(item_impl: &ItemImpl) -> bool {
    item_impl
        .trait_
        .as_ref()
        .and_then(|(_, path, _)| path.segments.last())
        .is_some_and(|segment| segment.ident == "Job")
}

pub(crate) struct Collector<'r> {
    pub(crate) refs: &'r mut Refs,
}

impl Collector<'_> {
    fn name(&mut self, ident: &syn::Ident) -> Option<String> {
        let name = ident.to_string();
        if is_keyword(&name) {
            return None;
        }
        self.refs.names.insert(name.clone());
        Some(name)
    }
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_path(&mut self, path: &'ast Path) {
        for (position, segment) in path.segments.iter().enumerate() {
            let Some(name) = self.name(&segment.ident) else {
                continue;
            };
            if position == 0 {
                self.refs.roots.insert(name);
            } else {
                self.refs.members.insert(name);
            }
        }
        visit::visit_path(self, path);
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        self.refs.members.insert(call.method.to_string());
        visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, mac: &'ast Macro) {
        self.visit_path(&mac.path);
        token_refs(mac.tokens.clone(), self.refs, true, false);
    }

    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        if attribute.path().is_ident("doc") {
            return;
        }
        self.visit_path(attribute.path());
        match &attribute.meta {
            Meta::Path(_) => {}
            Meta::List(list) => token_refs(list.tokens.clone(), self.refs, false, true),
            Meta::NameValue(pair) => {
                token_refs(pair.value.to_token_stream(), self.refs, false, true);
            }
        }
    }

    fn visit_item_use(&mut self, item_use: &'ast ItemUse) {
        let mut leaves = Vec::new();
        use_leaves(&item_use.tree, &mut Vec::new(), &mut leaves);
        for (segments, _) in leaves {
            if let Some(first) = segments.first() {
                self.refs.roots.insert(first.clone());
            }
            self.refs
                .names
                .extend(segments.into_iter().filter(|segment| !is_keyword(segment)));
        }
    }

    fn visit_item_extern_crate(&mut self, extern_crate: &'ast ItemExternCrate) {
        if let Some(name) = self.name(&extern_crate.ident) {
            self.refs.roots.insert(name);
        }
    }

    fn visit_item_impl(&mut self, item_impl: &'ast ItemImpl) {
        header_refs(item_impl, self.refs, true);
        for impl_item in &item_impl.items {
            self.visit_impl_item(impl_item);
        }
    }
}

pub(crate) fn impl_refs(item_impl: &ItemImpl) -> Refs {
    let mut refs = Refs::default();
    Collector { refs: &mut refs }.visit_item_impl(item_impl);
    refs
}

fn header_refs(item_impl: &ItemImpl, refs: &mut Refs, attributes: bool) {
    let mut collector = Collector { refs };
    if attributes {
        for attribute in &item_impl.attrs {
            collector.visit_attribute(attribute);
        }
    }
    collector.visit_generics(&item_impl.generics);
    if let Some((_, path, _)) = &item_impl.trait_ {
        for (position, segment) in path.segments.iter().enumerate() {
            let name = segment.ident.to_string();
            if !is_keyword(&name) {
                if position == 0 {
                    collector.refs.roots.insert(name.clone());
                }
                collector.refs.implemented.insert(name);
            }
            collector.visit_path_arguments(&segment.arguments);
        }
    }
    collector.visit_type(&item_impl.self_ty);
}

fn token_refs(tokens: TokenStream, refs: &mut Refs, members: bool, strings: bool) {
    let trees: Vec<TokenTree> = tokens.into_iter().collect();
    for (position, tree) in trees.iter().enumerate() {
        match tree {
            TokenTree::Ident(ident) => {
                let name = ident.to_string();
                if is_keyword(&name) {
                    continue;
                }
                if starts_path(&trees[position + 1..]) {
                    refs.roots.insert(name.clone());
                }
                if members {
                    refs.members.insert(name.clone());
                }
                refs.names.insert(name);
            }
            TokenTree::Group(group) => token_refs(group.stream(), refs, members, strings),
            TokenTree::Literal(literal) if strings => {
                for segment in path_string(literal).into_iter().flatten() {
                    refs.modules.insert(segment.clone());
                    refs.names.insert(segment);
                }
            }
            _ => {}
        }
    }
}

fn starts_path(rest: &[TokenTree]) -> bool {
    matches!(
        rest,
        [TokenTree::Punct(first), TokenTree::Punct(second), ..]
            if first.as_char() == ':' && first.spacing() == Spacing::Joint && second.as_char() == ':'
    )
}

fn path_string(literal: &Literal) -> Option<Vec<String>> {
    let parsed: syn::LitStr = syn::parse2(TokenTree::Literal(literal.clone()).into()).ok()?;
    let segments: Vec<String> = parsed.value().split("::").map(str::to_owned).collect();
    segments
        .iter()
        .all(|segment| is_identifier(segment) && !is_keyword(segment))
        .then_some(segments)
}

fn is_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_')
}

pub(crate) fn is_keyword(name: &str) -> bool {
    KEYWORDS.contains(&name)
}

fn use_leaves(
    tree: &UseTree,
    prefix: &mut Vec<String>,
    out: &mut Vec<(Vec<String>, Option<String>)>,
) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            use_leaves(&path.tree, prefix, out);
            prefix.pop();
        }
        UseTree::Name(leaf) => {
            let ident = leaf.ident.to_string();
            if ident == "self" {
                out.push((prefix.clone(), prefix.last().cloned()));
            } else {
                let mut segments = prefix.clone();
                segments.push(ident.clone());
                out.push((segments, Some(ident)));
            }
        }
        UseTree::Rename(rename) => {
            let mut segments = prefix.clone();
            segments.push(rename.ident.to_string());
            out.push((segments, Some(rename.rename.to_string())));
        }
        UseTree::Glob(_) => out.push((prefix.clone(), None)),
        UseTree::Group(group) => {
            for item in &group.items {
                use_leaves(item, prefix, out);
            }
        }
    }
}

fn path_attribute(attributes: &[Attribute]) -> Option<String> {
    attributes.iter().find_map(|attribute| {
        let Meta::NameValue(pair) = &attribute.meta else {
            return None;
        };
        if !pair.path.is_ident("path") {
            return None;
        }
        let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(value),
            ..
        }) = &pair.value
        else {
            return None;
        };
        Some(value.value())
    })
}

fn parent(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(dir, _)| dir)
}

fn stem(path: &str) -> &str {
    let name = path.rsplit_once('/').map_or(path, |(_, name)| name);
    name.strip_suffix(".rs").unwrap_or(name)
}

fn join(dir: &str, relative: &str) -> Option<String> {
    let mut parts: Vec<&str> = dir.split('/').filter(|part| !part.is_empty()).collect();
    for part in relative.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            other => parts.push(other),
        }
    }
    Some(parts.join("/"))
}
