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
            ":: core :: option :: Option :: None , :: core :: option :: Option :: None , false)"
        ));
    }
}

#[test]
fn test_expansion_asserts_job_constants() {
    let expansion = expanded("", SHORT);
    let checks = expansion
        .split("const _ : () =")
        .nth(1)
        .expect("a const block that checks the constants");
    for expected in [
        "< J as :: jalari :: Job > :: MAX_ATTEMPTS >= 1",
        "\"J: Job::MAX_ATTEMPTS must be at least 1\"",
        "! < J as :: jalari :: Job > :: NAME . is_empty ()",
        "\"J: Job::NAME must not be empty\"",
        "< J as :: jalari :: Job > :: TIMEOUT",
        "timeout . is_zero ()",
        "timeout . as_millis () <= :: core :: primitive :: i32 :: MAX",
    ] {
        assert!(checks.contains(expected), "the checks contain {expected}");
    }
}

#[test]
fn test_fingerprint_covers_the_impl_as_written() {
    assert_ne!(
        stamped(&expanded("fingerprint = true", SHORT)),
        stamped(&expanded("fingerprint = true", FILLED))
    );
}

fn registration(expansion: &str) -> String {
    let call = expansion
        .split("JobRegistration :: new")
        .nth(1)
        .expect("a registration");
    call[..call.find('}').expect("the end of the submit block")]
        .trim()
        .to_owned()
}

#[test]
fn test_checkpoints_argument_takes_a_boolean() {
    for text in [
        "checkpoints = true",
        "checkpoints = false",
        "queue = \"q\", fingerprint = true, checkpoints = true",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_ok(),
            "{text} is accepted"
        );
    }
}

#[test]
fn test_checkpoints_argument_rejects_a_string() {
    for text in ["checkpoints = \"true\"", "checkpoints = 1"] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
}

#[test]
fn test_checkpoints_argument_rejects_duplicates() {
    assert!(parse_arguments(arguments("checkpoints = true, checkpoints = false")).is_err());
}

#[test]
fn test_checkpoints_flag_reaches_the_registration() {
    assert!(registration(&expanded("checkpoints = true", SHORT)).ends_with(", true)"));
    for text in ["", "checkpoints = false"] {
        assert!(
            registration(&expanded(text, SHORT)).ends_with(", false)"),
            "{text:?} registers no checkpoints"
        );
    }
}

#[test]
fn test_checkpoints_flag_leaves_the_fingerprint_alone() {
    assert_eq!(
        stamped(&expanded("fingerprint = true, checkpoints = true", SHORT)),
        stamped(&expanded("fingerprint = true", SHORT))
    );
}
