//! The `#[jalari::job]` attribute; use it through the `jalari` crate.

mod fingerprint;
#[cfg(test)]
mod tests;

use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;

use proc_macro::{Span, TokenStream};
use proc_macro2::{Ident, Span as Span2, TokenStream as TokenStream2};
use quote::{quote, quote_spanned};
use syn::ext::IdentExt;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Expr, ExprLit, ImplItem, ItemImpl, Lit, LitBool, LitInt, LitStr, MetaNameValue, Token, Type,
    parse_macro_input, parse_quote, parse_quote_spanned,
};

/// Registers an `impl Job for T` so workers can run `T`, optionally on a cron schedule.
///
/// Place it on the `impl` block. Every type registered this way and linked into a binary is run
/// by that binary's workers; nothing else needs to be listed.
///
/// # Arguments
///
/// * `name` - Sets `Job::NAME`; the type's name, as written, when neither this nor the impl
///   sets it. Renaming such a type renames the job, so add the old name to `aliases`
/// * `queue` - Default queue for the job; `default` when omitted
/// * `max_attempts` - Sets `Job::MAX_ATTEMPTS` from an integer literal
/// * `timeout` - Sets `Job::TIMEOUT` from an integer literal of seconds; a limit below one
///   second needs `const TIMEOUT` in the impl
/// * `priority` - Sets `Job::PRIORITY` from `"low"`, `"normal"`, `"high"` or `"urgent"`
/// * `aliases` - Sets `Job::ALIASES` from an array of string literals
/// * `cron` - Cron expression with five or six fields; makes the job recurring. `T` must
///   implement `Default`, which provides the arguments of every run
/// * `timezone` - IANA time zone for `cron`; `UTC` when omitted. Only valid with `cron`
/// * `missed` - What the scheduler does with a run more than a minute overdue: `"once"`
///   enqueues one catch-up run, `"skip"` enqueues none and waits for the next run. `"once"`
///   when omitted. Only valid with `cron`
/// * `fingerprint` - `true` turns the code fingerprint on for this job; `false` when omitted
/// * `checkpoints` - `true` declares that the job reaches checkpoints regularly; an abort at
///   `stop_timeout` or `shutdown_timeout` is then logged as an error. `false` when omitted
///
/// `name`, `max_attempts`, `timeout`, `priority` and `aliases` are an alternative to declaring
/// the constant in the impl. Setting one both ways is a compile error.
///
/// A schedule declared with `cron` follows the deployed code: workers sync it at startup, remove
/// it when the declaration is gone, and it cannot be changed at runtime. Use
/// `jalari::recurring::add_or_update` for schedules that change while running.
///
/// When the impl leaves out `type Context`, it is set to `()`. When `run` takes only `self`,
/// the context parameter is added for it, so jobs that need no context stay short.
///
/// # Checks
///
/// The job's constants are checked at compile time, computed values included: `NAME` must not
/// be empty, `MAX_ATTEMPTS` must be at least 1, `TIMEOUT`, when set, must be longer than zero
/// and at most `i32::MAX` milliseconds, and no entry of `ALIASES` may be empty, equal `NAME`
/// or repeat another. A violation is a compile error that names the job and points at the
/// constant, or at the argument that set it.
///
/// # Code fingerprint
///
/// With `fingerprint = true` the attribute hashes the job's code at compile time into 32 hex
/// digits. The hash covers the
/// `impl` block, the job's struct, every item that the job's code reaches by name in its crate
/// and in its `path` dependencies, the `Cargo.lock` version of each registry crate that the
/// reached code names, and the jalari version. Formatting, comments and unreachable code do not
/// change it, and neither does the directory the sources are in.
///
/// `NAME`, `ALIASES`, `PRIORITY` and `RETRY_POLICY`, and the `name`, `aliases`, `priority`,
/// `checkpoints` and `missed` arguments, are left out, in either form. Renaming a job with its old name in
/// `aliases`, or changing its priority or retry policy, therefore leaves waiting jobs runnable
/// by the new build. `MAX_ATTEMPTS` and `TIMEOUT` are covered in either form.
///
/// Every enqueue stores the fingerprint with the job, and a worker takes a job only when the
/// stored fingerprint is empty or equals its own build's. A job enqueued by another build of
/// its code waits for a worker of that build. Without `fingerprint = true` the job is enqueued
/// without a fingerprint and its workers take its jobs from any build.
///
/// ```rust,ignore
/// #[jalari::job(name = "export_report", queue = "exports", fingerprint = true)]
/// impl Job for ExportReport {
///     async fn run(self) -> JobResult {
///         Ok(())
///     }
/// }
/// ```
///
/// # Examples
///
/// ```rust,ignore
/// use jalari::{Job, JobResult};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct SendEmail {
///     to: String,
/// }
///
/// #[jalari::job(name = "send_email", queue = "emails")]
/// impl Job for SendEmail {
///     async fn run(self) -> JobResult {
///         Ok(())
///     }
/// }
///
/// #[derive(Serialize, Deserialize, Default)]
/// struct NightlyReport;
///
/// #[jalari::job(
///     name = "nightly_report",
///     cron = "0 0 3 * * *",
///     timezone = "Asia/Taipei",
///     queue = "reports"
/// )]
/// impl Job for NightlyReport {
///     async fn run(self) -> JobResult {
///         Ok(())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    let arguments =
        parse_macro_input!(attr with Punctuated::<MetaNameValue, Token![,]>::parse_terminated);
    let item_impl = parse_macro_input!(item as ItemImpl);
    match expand(arguments, item_impl, Span::call_site().local_file()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct JobArguments {
    cron: Option<LitStr>,
    timezone: Option<LitStr>,
    missed: Option<Ident>,
    queue: Option<LitStr>,
    fingerprint: Option<LitBool>,
    checkpoints: Option<LitBool>,
    constants: Vec<Constant>,
}

struct Constant {
    argument: &'static str,
    name: &'static str,
    span: Span2,
    item: ImplItem,
}

type ConstantItem = fn(&MetaNameValue, Span2) -> syn::Result<ImplItem>;

const CONSTANTS: [(&str, &str, ConstantItem); 5] = [
    ("name", "NAME", name_item),
    ("max_attempts", "MAX_ATTEMPTS", max_attempts_item),
    ("timeout", "TIMEOUT", timeout_item),
    ("priority", "PRIORITY", priority_item),
    ("aliases", "ALIASES", aliases_item),
];

const MISSED: [(&str, &str); 2] = [("once", "Once"), ("skip", "Skip")];

const PRIORITIES: [(&str, &str); 4] = [
    ("low", "Low"),
    ("normal", "Normal"),
    ("high", "High"),
    ("urgent", "Urgent"),
];

fn expand(
    arguments: Punctuated<MetaNameValue, Token![,]>,
    mut item_impl: ItemImpl,
    local_file: Option<PathBuf>,
) -> syn::Result<TokenStream2> {
    let arguments = parse_arguments(arguments)?;

    let is_job_impl = item_impl
        .trait_
        .as_ref()
        .and_then(|(_, path, _)| path.segments.last())
        .is_some_and(|segment| segment.ident == "Job");
    if !is_job_impl {
        return Err(syn::Error::new_spanned(
            &item_impl.self_ty,
            "#[jalari::job] must be placed on `impl Job for T`",
        ));
    }
    if !item_impl.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &item_impl.generics,
            "#[jalari::job] does not support generic jobs",
        ));
    }

    inject_constants(&mut item_impl, arguments.constants)?;

    let fingerprint = if arguments.fingerprint.as_ref().is_some_and(LitBool::value) {
        let hex = fingerprint::hex(fingerprint::of_call_site(&item_impl, local_file));
        quote!(::core::option::Option::Some(#hex))
    } else {
        quote!(::core::option::Option::None)
    };

    let checkpoints = arguments.checkpoints.as_ref().is_some_and(LitBool::value);

    fill_in_context(&mut item_impl);

    let job_type = &item_impl.self_ty;
    let queue = match arguments.queue {
        Some(queue) => quote!(#queue),
        None => quote!("default"),
    };
    let cron = match arguments.cron {
        Some(expression) => {
            let timezone = match arguments.timezone {
                Some(timezone) => quote!(#timezone),
                None => quote!("UTC"),
            };
            let missed = arguments
                .missed
                .unwrap_or_else(|| Ident::new("Once", Span2::call_site()));
            quote! {
                ::core::option::Option::Some(
                    ::jalari::__private::CronRegistration::new::<#job_type>(
                        #expression,
                        #timezone,
                        ::jalari::recurring::Missed::#missed,
                    )
                )
            }
        }
        None => quote!(::core::option::Option::None),
    };

    let checks = constant_checks(&item_impl);

    Ok(quote! {
        #item_impl

        ::jalari::__private::inventory::submit! {
            ::jalari::__private::JobRegistration::new::<#job_type>(
                #queue, #cron, #fingerprint, #checkpoints
            )
        }

        #checks
    })
}

fn declares(item_impl: &ItemImpl, name: &str) -> bool {
    item_impl
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Const(constant) if constant.ident == name))
}

fn inject_constants(item_impl: &mut ItemImpl, constants: Vec<Constant>) -> syn::Result<()> {
    for constant in constants {
        if declares(item_impl, constant.name) {
            return Err(syn::Error::new(
                constant.span,
                format!(
                    "`{}` is set both here and by `const {}` in the impl; keep one",
                    constant.argument, constant.name
                ),
            ));
        }
        item_impl.items.push(constant.item);
    }
    if !declares(item_impl, "NAME") {
        let name = default_name(&item_impl.self_ty)?;
        let span = item_impl.self_ty.span();
        item_impl.items.push(parse_quote_spanned! {span=>
            const NAME: &'static str = #name;
        });
    }
    Ok(())
}

fn default_name(self_ty: &Type) -> syn::Result<LitStr> {
    match self_ty {
        Type::Group(group) => default_name(&group.elem),
        Type::Path(path) if path.qself.is_none() => match path.path.segments.last() {
            Some(segment) if segment.arguments.is_none() => Ok(LitStr::new(
                &segment.ident.unraw().to_string(),
                segment.ident.span(),
            )),
            _ => Err(unnamed(self_ty)),
        },
        _ => Err(unnamed(self_ty)),
    }
}

fn unnamed(self_ty: &Type) -> syn::Error {
    syn::Error::new_spanned(
        self_ty,
        "#[jalari::job] takes the job's name from a plain type name; set `name = \"...\"` for \
         this type",
    )
}

fn constant_checks(item_impl: &ItemImpl) -> TokenStream2 {
    let job_type = &item_impl.self_ty;
    let label = quote!(#job_type).to_string();
    let span_of = |name: &str| {
        item_impl
            .items
            .iter()
            .find_map(|item| match item {
                ImplItem::Const(constant) if constant.ident == name => Some(constant.span()),
                _ => None,
            })
            .unwrap_or_else(|| job_type.span())
    };
    let job = quote!(<#job_type as ::jalari::Job>);

    let message = format!("{label}: Job::MAX_ATTEMPTS must be at least 1");
    let max_attempts = quote_spanned! {span_of("MAX_ATTEMPTS")=>
        ::core::assert!(#job::MAX_ATTEMPTS >= 1, #message);
    };
    let message = format!("{label}: Job::NAME must not be empty");
    let name = quote_spanned! {span_of("NAME")=>
        ::core::assert!(!#job::NAME.is_empty(), #message);
    };
    let message =
        format!("{label}: Job::TIMEOUT must be longer than zero and at most i32::MAX milliseconds");
    let timeout = quote_spanned! {span_of("TIMEOUT")=>
        ::core::assert!(
            match #job::TIMEOUT {
                ::core::option::Option::Some(timeout) => {
                    !timeout.is_zero()
                        && timeout.as_millis()
                            <= ::core::primitive::i32::MAX as ::core::primitive::u128
                }
                ::core::option::Option::None => true,
            },
            #message
        );
    };
    let message =
        format!("{label}: Job::ALIASES must not contain an empty name, NAME or a repeated name");
    let aliases = quote_spanned! {span_of("ALIASES")=>
        ::core::assert!(
            ::jalari::__private::aliases_are_distinct(#job::NAME, #job::ALIASES),
            #message
        );
    };

    quote! {
        const _: () = {
            #max_attempts
            #name
            #timeout
            #aliases
        };
    }
}

fn fill_in_context(item_impl: &mut ItemImpl) {
    let declares_context = item_impl
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Type(declared) if declared.ident == "Context"));
    if !declares_context {
        item_impl.items.push(parse_quote!(
            type Context = ();
        ));
    }
    for item in &mut item_impl.items {
        if let ImplItem::Fn(function) = item
            && function.sig.ident == "run"
            && function.sig.inputs.len() == 1
        {
            function
                .sig
                .inputs
                .push(parse_quote!(_context: &Self::Context));
        }
    }
}

fn parse_arguments(arguments: Punctuated<MetaNameValue, Token![,]>) -> syn::Result<JobArguments> {
    let mut parsed = JobArguments {
        cron: None,
        timezone: None,
        missed: None,
        queue: None,
        fingerprint: None,
        checkpoints: None,
        constants: Vec::new(),
    };
    for argument in arguments {
        let span = argument.path.span();
        if let Some(&(key, name, item)) = CONSTANTS
            .iter()
            .find(|(key, _, _)| argument.path.is_ident(key))
        {
            if parsed
                .constants
                .iter()
                .any(|constant| constant.name == name)
            {
                return Err(syn::Error::new(span, "duplicate argument"));
            }
            parsed.constants.push(Constant {
                argument: key,
                name,
                span,
                item: item(&argument, span)?,
            });
            continue;
        }
        if argument.path.is_ident("fingerprint") || argument.path.is_ident("checkpoints") {
            let slot = if argument.path.is_ident("fingerprint") {
                &mut parsed.fingerprint
            } else {
                &mut parsed.checkpoints
            };
            if slot.is_some() {
                return Err(syn::Error::new(span, "duplicate argument"));
            }
            *slot = Some(bool_value(&argument)?);
            continue;
        }
        if argument.path.is_ident("missed") {
            if parsed.missed.is_some() {
                return Err(syn::Error::new(span, "duplicate argument"));
            }
            parsed.missed = Some(missed_variant(&argument, span)?);
            continue;
        }
        let slot = if argument.path.is_ident("cron") {
            &mut parsed.cron
        } else if argument.path.is_ident("timezone") {
            &mut parsed.timezone
        } else if argument.path.is_ident("queue") {
            &mut parsed.queue
        } else {
            return Err(syn::Error::new_spanned(
                &argument.path,
                "expected `name`, `queue`, `max_attempts`, `timeout`, `priority`, `aliases`, \
                 `cron`, `timezone`, `missed`, `fingerprint` or `checkpoints`",
            ));
        };
        if slot.is_some() {
            return Err(syn::Error::new(span, "duplicate argument"));
        }
        *slot = Some(string_value(&argument)?);
    }
    if let (None, Some(timezone)) = (&parsed.cron, &parsed.timezone) {
        return Err(syn::Error::new_spanned(
            timezone,
            "`timezone` requires `cron`",
        ));
    }
    if let (None, Some(missed)) = (&parsed.cron, &parsed.missed) {
        return Err(syn::Error::new(missed.span(), "`missed` requires `cron`"));
    }
    Ok(parsed)
}

fn name_item(argument: &MetaNameValue, span: Span2) -> syn::Result<ImplItem> {
    let name = string_value(argument)?;
    Ok(parse_quote_spanned! {span=> const NAME: &'static str = #name; })
}

fn max_attempts_item(argument: &MetaNameValue, span: Span2) -> syn::Result<ImplItem> {
    let max_attempts = LitInt::new(&integer_value::<i32>(argument)?.to_string(), span);
    Ok(parse_quote_spanned! {span=> const MAX_ATTEMPTS: i32 = #max_attempts; })
}

fn timeout_item(argument: &MetaNameValue, span: Span2) -> syn::Result<ImplItem> {
    let seconds = LitInt::new(&integer_value::<u64>(argument)?.to_string(), span);
    Ok(parse_quote_spanned! {span=>
        const TIMEOUT: ::core::option::Option<::core::time::Duration> =
            ::core::option::Option::Some(::core::time::Duration::from_secs(#seconds));
    })
}

fn priority_item(argument: &MetaNameValue, span: Span2) -> syn::Result<ImplItem> {
    let level = string_value(argument)?;
    let variant = PRIORITIES
        .iter()
        .find(|(text, _)| level.value() == *text)
        .map(|(_, variant)| Ident::new(variant, span))
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &level,
                "expected \"low\", \"normal\", \"high\" or \"urgent\"",
            )
        })?;
    Ok(parse_quote_spanned! {span=>
        const PRIORITY: ::jalari::Priority = ::jalari::Priority::#variant;
    })
}

fn missed_variant(argument: &MetaNameValue, span: Span2) -> syn::Result<Ident> {
    let policy = string_value(argument)?;
    MISSED
        .iter()
        .find(|(text, _)| policy.value() == *text)
        .map(|(_, variant)| Ident::new(variant, span))
        .ok_or_else(|| syn::Error::new_spanned(&policy, "expected \"once\" or \"skip\""))
}

fn aliases_item(argument: &MetaNameValue, span: Span2) -> syn::Result<ImplItem> {
    let Expr::Array(array) = &argument.value else {
        return Err(syn::Error::new_spanned(
            &argument.value,
            "expected an array of string literals",
        ));
    };
    let aliases = array
        .elems
        .iter()
        .map(|alias| match alias {
            Expr::Lit(ExprLit {
                lit: Lit::Str(alias),
                ..
            }) => Ok(alias),
            other => Err(syn::Error::new_spanned(other, "expected a string literal")),
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(parse_quote_spanned! {span=>
        const ALIASES: &'static [&'static str] = &[#(#aliases),*];
    })
}

fn string_value(argument: &MetaNameValue) -> syn::Result<LitStr> {
    match &argument.value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.clone()),
        other => Err(syn::Error::new_spanned(other, "expected a string literal")),
    }
}

fn bool_value(argument: &MetaNameValue) -> syn::Result<LitBool> {
    match &argument.value {
        Expr::Lit(ExprLit {
            lit: Lit::Bool(value),
            ..
        }) => Ok(value.clone()),
        other => Err(syn::Error::new_spanned(other, "expected `true` or `false`")),
    }
}

fn integer_value<N>(argument: &MetaNameValue) -> syn::Result<N>
where
    N: FromStr,
    N::Err: Display,
{
    match &argument.value {
        Expr::Lit(ExprLit {
            lit: Lit::Int(value),
            ..
        }) => value.base10_parse(),
        other => Err(syn::Error::new_spanned(
            other,
            "expected an integer literal",
        )),
    }
}
