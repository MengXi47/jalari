//! The `#[facade::job_attr]` attribute, used through the `facade` crate.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{MetaNameValue, Token, parse_macro_input, parse_quote};

/// Declares a job through `facade`, on the `facade` queue unless the arguments name a queue.
#[proc_macro_attribute]
pub fn job_attr(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut arguments =
        parse_macro_input!(attr with Punctuated::<MetaNameValue, Token![,]>::parse_terminated);
    if !arguments
        .iter()
        .any(|argument| argument.path.is_ident("queue"))
    {
        arguments.push(parse_quote!(queue = "facade"));
    }
    let item = TokenStream2::from(item);
    quote! {
        #[::facade::__jalari::job(crate = ::facade::__jalari, #arguments)]
        #item
    }
    .into()
}
