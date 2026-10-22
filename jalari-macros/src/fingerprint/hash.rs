use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};

const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

pub(crate) struct Fnv(u128);

impl Fnv {
    pub(crate) fn new() -> Self {
        Self(OFFSET)
    }

    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= u128::from(byte);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    pub(crate) fn count(&mut self, count: usize) {
        self.bytes(&(count as u64).to_le_bytes());
    }

    pub(crate) fn field(&mut self, bytes: &[u8]) {
        self.count(bytes.len());
        self.bytes(bytes);
    }

    pub(crate) fn finish(&self) -> u128 {
        self.0
    }
}

pub(crate) fn canonical(tokens: TokenStream) -> String {
    let mut out = String::new();
    write_stream(tokens, Delimiter::None, &mut out);
    out
}

fn write_stream(tokens: TokenStream, delimiter: Delimiter, out: &mut String) {
    let mut trees: Vec<TokenTree> = tokens.into_iter().collect();
    if trailing_comma_is_layout(&trees, delimiter) {
        trees.pop();
    }
    let mut position = 0;
    while position < trees.len() {
        if let Some(skip) = doc_attribute(&trees[position..]) {
            position += skip;
            continue;
        }
        write_tree(&trees[position], out);
        position += 1;
    }
}

fn trailing_comma_is_layout(trees: &[TokenTree], delimiter: Delimiter) -> bool {
    let is_comma =
        |tree: &TokenTree| matches!(tree, TokenTree::Punct(punct) if punct.as_char() == ',');
    let Some((last, rest)) = trees.split_last() else {
        return false;
    };
    if !is_comma(last) {
        return false;
    }
    match delimiter {
        Delimiter::Brace | Delimiter::Bracket => true,
        Delimiter::Parenthesis => rest.iter().any(is_comma),
        Delimiter::None => false,
    }
}

fn doc_attribute(trees: &[TokenTree]) -> Option<usize> {
    let TokenTree::Punct(pound) = trees.first()? else {
        return None;
    };
    if pound.as_char() != '#' {
        return None;
    }
    let (bang, group) = match trees.get(1)? {
        TokenTree::Punct(bang) if bang.as_char() == '!' => (1, trees.get(2)?),
        other => (0, other),
    };
    let TokenTree::Group(group) = group else {
        return None;
    };
    if group.delimiter() != Delimiter::Bracket {
        return None;
    }
    let mut inner = group.stream().into_iter();
    let is_doc = matches!(inner.next(), Some(TokenTree::Ident(ident)) if ident == "doc")
        && matches!(inner.next(), Some(TokenTree::Punct(punct)) if punct.as_char() == '=');
    is_doc.then_some(2 + bang)
}

fn write_tree(tree: &TokenTree, out: &mut String) {
    match tree {
        TokenTree::Ident(ident) => {
            out.push_str(&ident.to_string());
            out.push(' ');
        }
        TokenTree::Punct(punct) => {
            out.push(punct.as_char());
            if punct.spacing() == Spacing::Alone {
                out.push(' ');
            }
        }
        TokenTree::Literal(literal) => {
            out.push_str(&literal.to_string());
            out.push(' ');
        }
        TokenTree::Group(group) => {
            let (open, close) = match group.delimiter() {
                Delimiter::Parenthesis => ("( ", ") "),
                Delimiter::Brace => ("{ ", "} "),
                Delimiter::Bracket => ("[ ", "] "),
                Delimiter::None => ("", ""),
            };
            out.push_str(open);
            write_stream(group.stream(), group.delimiter(), out);
            out.push_str(close);
        }
    }
}
