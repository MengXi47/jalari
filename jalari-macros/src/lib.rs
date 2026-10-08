use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Expr, ExprLit, ItemImpl, Lit, LitStr, MetaNameValue, Token, parse_macro_input};

/// Registers an `impl Job for T` so workers can run `T`, optionally on a cron schedule.
///
/// Place it on the `impl` block. Every type registered this way and linked into a binary is run
/// by that binary's workers; nothing else needs to be listed.
///
/// # Arguments
///
/// * `queue` - Default queue for the job; `default` when omitted
/// * `cron` - Cron expression with five or six fields; makes the job recurring. `T` must
///   implement `Default`, which provides the arguments of every run
/// * `timezone` - IANA time zone for `cron`; `UTC` when omitted. Only valid with `cron`
///
/// A schedule declared with `cron` follows the deployed code: workers sync it at startup, remove
/// it when the declaration is gone, and it cannot be changed at runtime. Use
/// `jalari::recurring::add_or_update` for schedules that change while running.
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
/// #[jalari::job(queue = "emails")]
/// impl Job for SendEmail {
///     const NAME: &'static str = "send_email";
///
///     async fn run(self) -> JobResult {
///         Ok(())
///     }
/// }
///
/// #[derive(Serialize, Deserialize, Default)]
/// struct NightlyReport;
///
/// #[jalari::job(cron = "0 0 3 * * *", timezone = "Asia/Taipei", queue = "reports")]
/// impl Job for NightlyReport {
///     const NAME: &'static str = "nightly_report";
///
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
    match expand(arguments, item_impl) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct JobArguments {
    cron: Option<LitStr>,
    timezone: Option<LitStr>,
    queue: Option<LitStr>,
}

fn expand(
    arguments: Punctuated<MetaNameValue, Token![,]>,
    item_impl: ItemImpl,
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
            quote! {
                ::core::option::Option::Some(
                    ::jalari::__private::CronRegistration::new::<#job_type>(#expression, #timezone)
                )
            }
        }
        None => quote!(::core::option::Option::None),
    };

    Ok(quote! {
        #item_impl

        ::jalari::__private::inventory::submit! {
            ::jalari::__private::JobRegistration::new::<#job_type>(#queue, #cron)
        }
    })
}

fn parse_arguments(arguments: Punctuated<MetaNameValue, Token![,]>) -> syn::Result<JobArguments> {
    let mut parsed = JobArguments {
        cron: None,
        timezone: None,
        queue: None,
    };
    for argument in arguments {
        let value = string_value(&argument)?;
        let slot = if argument.path.is_ident("cron") {
            &mut parsed.cron
        } else if argument.path.is_ident("timezone") {
            &mut parsed.timezone
        } else if argument.path.is_ident("queue") {
            &mut parsed.queue
        } else {
            return Err(syn::Error::new_spanned(
                &argument.path,
                "expected `cron`, `timezone` or `queue`",
            ));
        };
        if slot.is_some() {
            return Err(syn::Error::new_spanned(
                &argument.path,
                "duplicate argument",
            ));
        }
        *slot = Some(value);
    }
    if let (None, Some(timezone)) = (&parsed.cron, &parsed.timezone) {
        return Err(syn::Error::new_spanned(
            timezone,
            "`timezone` requires `cron`",
        ));
    }
    Ok(parsed)
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
