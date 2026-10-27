use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{ItemImpl, MetaNameValue, Token};

use super::{expand, parse_arguments};

fn arguments(text: &str) -> Punctuated<MetaNameValue, Token![,]> {
    Punctuated::<MetaNameValue, Token![,]>::parse_terminated
        .parse_str(text)
        .unwrap()
}

fn expanded(attribute: &str, item: &str) -> String {
    let item: ItemImpl = syn::parse_str(item).unwrap();
    expand(arguments(attribute), item, None)
        .unwrap()
        .to_string()
}

fn stamped(expansion: &str) -> Option<String> {
    let registration = expansion.split("JobRegistration :: new").nth(1)?;
    let start = registration.find("Some (\"")? + "Some (\"".len();
    Some(registration[start..start + 32].to_owned())
}

const SHORT: &str = "impl Job for J {
    const NAME: &'static str = \"j\";
    async fn run(self) -> JobResult { Ok(()) }
}";

const FILLED: &str = "impl Job for J {
    const NAME: &'static str = \"j\";
    async fn run(self, _context: &Self::Context) -> JobResult { Ok(()) }
    type Context = ();
}";

#[test]
fn test_fingerprint_argument_takes_a_boolean() {
    assert!(parse_arguments(arguments("fingerprint = false")).is_ok());
    assert!(parse_arguments(arguments("queue = \"q\", fingerprint = true")).is_ok());
    for text in [
        "fingerprint = \"false\"",
        "fingerprint = 0",
        "fingerprint = true, fingerprint = false",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
}

#[test]
fn test_expansion_stamps_a_fingerprint_only_when_turned_on() {
    let on = stamped(&expanded("fingerprint = true", SHORT)).expect("a fingerprint when on");
    assert_eq!(on.len(), 32);
    assert!(on.chars().all(|c| c.is_ascii_hexdigit()));
    for text in ["", "fingerprint = false"] {
        let off = expanded(text, SHORT);
        assert_eq!(stamped(&off), None, "{text:?} stamps no fingerprint");
        assert!(off.contains(
            ":: core :: option :: Option :: None , :: core :: option :: Option :: None)"
        ));
    }
}

#[test]
fn test_fingerprint_covers_the_impl_as_written() {
    assert_ne!(
        stamped(&expanded("fingerprint = true", SHORT)),
        stamped(&expanded("fingerprint = true", FILLED))
    );
}
