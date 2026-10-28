use std::mem;

use proc_macro2::{Group, Ident, Spacing, Span, TokenStream, TokenTree};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit_mut::{self, VisitMut};
use syn::{
    Attribute, ExprPath, ExprStruct, File, ItemImpl, ItemUse, Macro, Meta, MetaList, MetaNameValue,
    PatStruct, PatTupleStruct, Path, PathSegment, QSelf, Token, TypePath, UseGroup, UseName,
    UsePath, UseRename, UseTree,
};

use super::index::is_job_attribute;

pub(crate) const JALARI: &str = "jalari";

const MACRO_CRATE: &str = "$crate";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Reexport {
    segments: Vec<String>,
    wrapper: Option<Vec<String>>,
}

impl Reexport {
    pub(crate) fn new(segments: Vec<String>) -> Option<Self> {
        (!segments.is_empty() && segments != [JALARI]).then_some(Self {
            segments,
            wrapper: None,
        })
    }

    pub(crate) fn from_path(path: &Path) -> Option<Self> {
        Self::new(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        )
    }

    pub(crate) fn with_macro_crate(&self, krate: &str) -> Option<Self> {
        if self.root() != MACRO_CRATE {
            return Some(self.clone());
        }
        Self::new(
            std::iter::once(krate.to_owned())
                .chain(self.segments[1..].iter().cloned())
                .collect(),
        )
    }

    pub(crate) fn with_wrapper(self, attribute: &str) -> Self {
        Self {
            wrapper: wrapper_path(attribute),
            ..self
        }
    }

    pub(crate) fn is_wrapper(&self, path: &Path) -> bool {
        self.wrapper.as_ref().is_some_and(|wrapper| {
            path.segments.len() == wrapper.len()
                && path
                    .segments
                    .iter()
                    .zip(wrapper)
                    .all(|(segment, name)| segment.ident == name)
        })
    }

    pub(crate) fn root(&self) -> &str {
        &self.segments[0]
    }

    pub(crate) fn rewrite_file(&self, file: &mut File) {
        Rewrite { reexport: self }.visit_file_mut(file);
    }

    pub(crate) fn rewrite_item(&self, item: &mut ItemImpl) {
        Rewrite { reexport: self }.visit_item_impl_mut(item);
    }

    pub(crate) fn rewrite_tokens(&self, tokens: TokenStream) -> TokenStream {
        let trees: Vec<TokenTree> = tokens.into_iter().collect();
        let mut out: Vec<TokenTree> = Vec::with_capacity(trees.len());
        let mut position = 0;
        while position < trees.len() {
            if !continues_path(&out)
                && let Some(length) = self.leading(&trees[position..])
            {
                out.push(TokenTree::Ident(Ident::new(JALARI, trees[position].span())));
                position += length;
                continue;
            }
            out.push(match &trees[position] {
                TokenTree::Group(group) => {
                    let mut rewritten =
                        Group::new(group.delimiter(), self.rewrite_tokens(group.stream()));
                    rewritten.set_span(group.span());
                    TokenTree::Group(rewritten)
                }
                other => other.clone(),
            });
            position += 1;
        }
        out.into_iter().collect()
    }

    fn leading(&self, trees: &[TokenTree]) -> Option<usize> {
        let mut position = 0;
        for (index, name) in self.segments.iter().enumerate() {
            if index > 0 {
                let [first, second] = trees.get(position..position + 2)? else {
                    return None;
                };
                if !is_separator(first, second) {
                    return None;
                }
                position += 2;
            }
            match trees.get(position)? {
                TokenTree::Ident(ident) if ident == name => position += 1,
                _ => return None,
            }
        }
        Some(position)
    }

    fn collapse(&self, path: &mut Path) {
        let length = self.segments.len();
        let through = path.segments.len() >= length
            && path
                .segments
                .iter()
                .zip(&self.segments)
                .all(|(segment, name)| segment.ident == name && segment.arguments.is_none());
        if !through {
            return;
        }
        let span = path.segments[0].ident.span();
        let rest: Vec<PathSegment> = path.segments.iter().skip(length).cloned().collect();
        path.segments = std::iter::once(PathSegment::from(Ident::new(JALARI, span)))
            .chain(rest)
            .collect();
    }

    fn use_tree(&self, tree: &mut UseTree) {
        let mut taken = Vec::new();
        let emptied = self.take(tree, 0, &mut taken);
        if taken.is_empty() {
            return;
        }
        if !emptied {
            taken.push(tree.clone());
        }
        *tree = if taken.len() == 1 {
            taken.remove(0)
        } else {
            UseTree::Group(UseGroup {
                brace_token: Default::default(),
                items: taken.into_iter().collect(),
            })
        };
    }

    fn take(&self, tree: &mut UseTree, depth: usize, taken: &mut Vec<UseTree>) -> bool {
        let last = depth + 1 == self.segments.len();
        let name = &self.segments[depth];
        match tree {
            UseTree::Path(path) if path.ident == name => {
                if !last {
                    return self.take(&mut path.tree, depth + 1, taken);
                }
                let mut rest = (*path.tree).clone();
                self.bind_self(&mut rest);
                taken.push(UseTree::Path(UsePath {
                    ident: Ident::new(JALARI, path.ident.span()),
                    colon2_token: path.colon2_token,
                    tree: Box::new(rest),
                }));
                true
            }
            UseTree::Name(leaf) if last && leaf.ident == name => {
                taken.push(self.bound(leaf.ident.span()));
                true
            }
            UseTree::Rename(rename) if last && rename.ident == name => {
                taken.push(UseTree::Rename(UseRename {
                    ident: Ident::new(JALARI, rename.ident.span()),
                    as_token: rename.as_token,
                    rename: rename.rename.clone(),
                }));
                true
            }
            UseTree::Group(group) => {
                for mut item in mem::take(&mut group.items) {
                    if !self.take(&mut item, depth, taken) {
                        group.items.push(item);
                    }
                }
                group.items.is_empty()
            }
            _ => false,
        }
    }

    fn bound(&self, span: Span) -> UseTree {
        let jalari = Ident::new(JALARI, span);
        match self.renamed(span) {
            Some(rename) => UseTree::Rename(UseRename {
                ident: jalari,
                as_token: Default::default(),
                rename,
            }),
            None => UseTree::Name(UseName { ident: jalari }),
        }
    }

    fn bind_self(&self, tree: &mut UseTree) {
        match tree {
            UseTree::Group(group) => {
                for item in &mut group.items {
                    self.bind_self(item);
                }
            }
            UseTree::Name(leaf) if leaf.ident == "self" => {
                if let Some(rename) = self.renamed(leaf.ident.span()) {
                    *tree = UseTree::Rename(UseRename {
                        ident: leaf.ident.clone(),
                        as_token: Default::default(),
                        rename,
                    });
                }
            }
            _ => {}
        }
    }

    fn renamed(&self, span: Span) -> Option<Ident> {
        let last = self.segments.last()?;
        if last == JALARI {
            return None;
        }
        Some(match last.strip_prefix("r#") {
            Some(raw) => Ident::new_raw(raw, span),
            None => Ident::new(last, span),
        })
    }
}

struct Rewrite<'r> {
    reexport: &'r Reexport,
}

impl Rewrite<'_> {
    fn qualified(&self, qself: &mut Option<QSelf>, before: usize, path: &Path) {
        if let Some(qself) = qself {
            qself.position = qself.position.saturating_sub(before - path.segments.len());
        }
    }
}

impl VisitMut for Rewrite<'_> {
    fn visit_path_mut(&mut self, path: &mut Path) {
        self.reexport.collapse(path);
        visit_mut::visit_path_mut(self, path);
    }

    fn visit_type_path_mut(&mut self, node: &mut TypePath) {
        let before = node.path.segments.len();
        visit_mut::visit_type_path_mut(self, node);
        self.qualified(&mut node.qself, before, &node.path);
    }

    fn visit_expr_path_mut(&mut self, node: &mut ExprPath) {
        let before = node.path.segments.len();
        visit_mut::visit_expr_path_mut(self, node);
        self.qualified(&mut node.qself, before, &node.path);
    }

    fn visit_expr_struct_mut(&mut self, node: &mut ExprStruct) {
        let before = node.path.segments.len();
        visit_mut::visit_expr_struct_mut(self, node);
        self.qualified(&mut node.qself, before, &node.path);
    }

    fn visit_pat_struct_mut(&mut self, node: &mut PatStruct) {
        let before = node.path.segments.len();
        visit_mut::visit_pat_struct_mut(self, node);
        self.qualified(&mut node.qself, before, &node.path);
    }

    fn visit_pat_tuple_struct_mut(&mut self, node: &mut PatTupleStruct) {
        let before = node.path.segments.len();
        visit_mut::visit_pat_tuple_struct_mut(self, node);
        self.qualified(&mut node.qself, before, &node.path);
    }

    fn visit_item_use_mut(&mut self, node: &mut ItemUse) {
        self.reexport.use_tree(&mut node.tree);
        visit_mut::visit_item_use_mut(self, node);
    }

    fn visit_macro_mut(&mut self, node: &mut Macro) {
        visit_mut::visit_macro_mut(self, node);
        node.tokens = self.reexport.rewrite_tokens(mem::take(&mut node.tokens));
    }

    fn visit_meta_list_mut(&mut self, node: &mut MetaList) {
        visit_mut::visit_meta_list_mut(self, node);
        node.tokens = self.reexport.rewrite_tokens(mem::take(&mut node.tokens));
    }
}

fn wrapper_path(attribute: &str) -> Option<Vec<String>> {
    let [attribute] =
        <[Attribute; 1]>::try_from(Attribute::parse_outer.parse_str(attribute).ok()?).ok()?;
    if is_job_attribute(&attribute) {
        return None;
    }
    match &attribute.meta {
        Meta::Path(_) => {}
        Meta::List(_) => {
            attribute
                .parse_args_with(Punctuated::<MetaNameValue, Token![,]>::parse_terminated)
                .ok()?;
        }
        Meta::NameValue(_) => return None,
    }
    Some(
        attribute
            .path()
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect(),
    )
}

fn is_separator(first: &TokenTree, second: &TokenTree) -> bool {
    matches!(
        (first, second),
        (TokenTree::Punct(first), TokenTree::Punct(second))
            if first.as_char() == ':' && first.spacing() == Spacing::Joint && second.as_char() == ':'
    )
}

fn continues_path(out: &[TokenTree]) -> bool {
    match out {
        [.., before, first, second] if is_separator(first, second) => match before {
            TokenTree::Ident(_) => true,
            TokenTree::Punct(punct) => punct.as_char() == '>',
            _ => false,
        },
        _ => false,
    }
}
