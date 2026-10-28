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
        ":: jalari :: __private :: aliases_are_distinct (< J as :: jalari :: Job > :: NAME , \
         < J as :: jalari :: Job > :: ALIASES)",
        "\"J: Job::ALIASES must not contain an empty name, NAME or a repeated name\"",
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

#[test]
fn test_missed_argument_takes_once_or_skip() {
    for text in [
        "cron = \"0 0 3 * * *\", missed = \"once\"",
        "missed = \"skip\", cron = \"0 0 3 * * *\", timezone = \"Asia/Taipei\"",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_ok(),
            "{text} is accepted"
        );
    }
    for text in [
        "cron = \"0 0 3 * * *\", missed = \"all\"",
        "cron = \"0 0 3 * * *\", missed = \"Skip\"",
        "cron = \"0 0 3 * * *\", missed = true",
        "cron = \"0 0 3 * * *\", missed = \"skip\", missed = \"once\"",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
    let message = parse_arguments(arguments("cron = \"0 0 3 * * *\", missed = \"all\""))
        .err()
        .unwrap()
        .to_string();
    assert_eq!(message, "expected \"once\" or \"skip\"");
}

#[test]
fn test_missed_argument_requires_cron() {
    for text in ["missed = \"skip\"", "queue = \"q\", missed = \"once\""] {
        let message = parse_arguments(arguments(text)).err().unwrap().to_string();
        assert_eq!(message, "`missed` requires `cron`", "{text}");
    }
    let message = parse_arguments(arguments("misssed = \"skip\""))
        .err()
        .unwrap()
        .to_string();
    assert!(message.contains("`missed`"), "{message}");
}

#[test]
fn test_missed_policy_reaches_the_registration() {
    let skip = expanded("cron = \"0 0 3 * * *\", missed = \"skip\"", SHORT);
    assert!(
        skip.contains(":: jalari :: recurring :: Missed :: Skip"),
        "{skip}"
    );
    let default = expanded("cron = \"0 0 3 * * *\"", SHORT);
    assert!(
        default.contains(":: jalari :: recurring :: Missed :: Once"),
        "{default}"
    );
}

const UNNAMED: &str = "impl Job for SendEmail {
    async fn run(self) -> JobResult { Ok(()) }
}";

fn rejected(attribute: &str, item: &str) -> String {
    let item: ItemImpl = syn::parse_str(item).unwrap();
    expand(arguments(attribute), item, None)
        .expect_err("the expansion is rejected")
        .to_string()
}

fn impl_block(expansion: &str) -> &str {
    &expansion[..expansion
        .find(":: jalari :: __private :: inventory :: submit !")
        .expect("the registration follows the impl")]
}

#[test]
fn test_default_name_is_the_type_identifier() {
    assert!(
        impl_block(&expanded("", UNNAMED)).contains("const NAME : & 'static str = \"SendEmail\" ;")
    );
    let pathed = "impl Job for crate::jobs::SendEmail {
        async fn run(self) -> JobResult { Ok(()) }
    }";
    assert!(
        impl_block(&expanded("", pathed)).contains("const NAME : & 'static str = \"SendEmail\" ;")
    );
}

#[test]
fn test_explicit_name_sets_the_const() {
    let expansion = expanded("name = \"send_email\"", UNNAMED);
    let block = impl_block(&expansion);
    assert!(block.contains("const NAME : & 'static str = \"send_email\" ;"));
    assert!(!block.contains("\"SendEmail\""));
}

#[test]
fn test_impl_name_is_kept_without_the_attribute() {
    let block = impl_block(&expanded("", SHORT)).to_owned();
    assert_eq!(block.matches("const NAME").count(), 1);
    assert!(block.contains("const NAME : & 'static str = \"j\" ;"));
}

#[test]
fn test_name_in_attribute_and_impl_is_an_error() {
    assert_eq!(
        rejected("name = \"k\"", SHORT),
        "`name` is set both here and by `const NAME` in the impl; keep one"
    );
}

#[test]
fn test_every_injected_const_conflicts_with_the_impl() {
    for (attribute, constant, declared) in [
        (
            "max_attempts = 3",
            "MAX_ATTEMPTS",
            "const MAX_ATTEMPTS: i32 = 3;",
        ),
        (
            "timeout = 30",
            "TIMEOUT",
            "const TIMEOUT: Option<Duration> = None;",
        ),
        (
            "priority = \"high\"",
            "PRIORITY",
            "const PRIORITY: Priority = Priority::Low;",
        ),
        (
            "aliases = [\"old\"]",
            "ALIASES",
            "const ALIASES: &'static [&'static str] = &[];",
        ),
    ] {
        let item = format!(
            "impl Job for J {{
                const NAME: &'static str = \"j\";
                {declared}
                async fn run(self) -> JobResult {{ Ok(()) }}
            }}"
        );
        let argument = attribute.split(' ').next().unwrap();
        assert_eq!(
            rejected(attribute, &item),
            format!(
                "`{argument}` is set both here and by `const {constant}` in the impl; keep one"
            ),
        );
    }
}

#[test]
fn test_max_attempts_attribute_injects_the_const() {
    assert!(
        impl_block(&expanded("max_attempts = 5", SHORT)).contains("const MAX_ATTEMPTS : i32 = 5 ;")
    );
    for text in [
        "max_attempts = \"5\"",
        "max_attempts = 3000000000",
        "max_attempts = 2.5",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
}

#[test]
fn test_timeout_attribute_injects_seconds() {
    assert!(impl_block(&expanded("timeout = 30", SHORT)).contains(
        "const TIMEOUT : :: core :: option :: Option < :: core :: time :: Duration > = \
         :: core :: option :: Option :: Some (:: core :: time :: Duration :: from_secs (30)) ;"
    ));
    for text in ["timeout = \"30s\"", "timeout = 1.5", "timeout = -1"] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
}

#[test]
fn test_priority_attribute_injects_the_variant() {
    for (text, variant) in [
        ("low", "Low"),
        ("normal", "Normal"),
        ("high", "High"),
        ("urgent", "Urgent"),
    ] {
        let expansion = expanded(&format!("priority = \"{text}\""), SHORT);
        assert!(
            impl_block(&expansion).contains(&format!(
                "const PRIORITY : :: jalari :: Priority = :: jalari :: Priority :: {variant} ;"
            )),
            "{text} injects {variant}"
        );
    }
}

#[test]
fn test_priority_attribute_rejects_unknown_levels() {
    for text in [
        "priority = \"High\"",
        "priority = \"critical\"",
        "priority = 1",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
    let message = parse_arguments(arguments("priority = \"critical\""))
        .err()
        .unwrap()
        .to_string();
    assert_eq!(
        message,
        "expected \"low\", \"normal\", \"high\" or \"urgent\""
    );
}

#[test]
fn test_aliases_attribute_injects_the_slice() {
    assert!(
        impl_block(&expanded("aliases = [\"old\", \"older\"]", SHORT))
            .contains("const ALIASES : & 'static [& 'static str] = & [\"old\" , \"older\"] ;")
    );
    assert!(
        impl_block(&expanded("aliases = []", SHORT))
            .contains("const ALIASES : & 'static [& 'static str] = & [] ;")
    );
    for text in [
        "aliases = \"old\"",
        "aliases = [old]",
        "aliases = [\"old\", 1]",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
}

#[test]
fn test_metadata_arguments_reject_duplicates() {
    for text in [
        "name = \"a\", name = \"b\"",
        "max_attempts = 1, max_attempts = 2",
        "timeout = 1, timeout = 2",
        "priority = \"low\", priority = \"high\"",
        "aliases = [], aliases = []",
    ] {
        assert!(
            parse_arguments(arguments(text)).is_err(),
            "{text} is rejected"
        );
    }
}

#[test]
fn test_non_path_type_requires_a_name() {
    for item in [
        "impl Job for Wrapper<u8> { async fn run(self) -> JobResult { Ok(()) } }",
        "impl Job for &'static Ping { async fn run(self) -> JobResult { Ok(()) } }",
        "impl Job for (A, B) { async fn run(self) -> JobResult { Ok(()) } }",
    ] {
        assert!(
            rejected("", item).contains("set `name = \"...\"`"),
            "{item} asks for a name"
        );
        assert!(
            expand(
                arguments("name = \"wrapped\""),
                syn::parse_str(item).unwrap(),
                None
            )
            .is_ok()
        );
    }
}

#[test]
fn test_fingerprint_covers_attribute_max_attempts_and_timeout() {
    let base = stamped(&expanded("fingerprint = true", SHORT));
    let five = stamped(&expanded("fingerprint = true, max_attempts = 5", SHORT));
    let six = stamped(&expanded("fingerprint = true, max_attempts = 6", SHORT));
    assert_ne!(base, five);
    assert_ne!(five, six);
    let thirty = stamped(&expanded("fingerprint = true, timeout = 30", SHORT));
    let sixty = stamped(&expanded("fingerprint = true, timeout = 60", SHORT));
    assert_ne!(base, thirty);
    assert_ne!(thirty, sixty);
}

#[test]
fn test_fingerprint_ignores_name_aliases_and_priority() {
    let base = stamped(&expanded("fingerprint = true", SHORT));
    assert_eq!(
        base,
        stamped(&expanded(
            "fingerprint = true",
            &SHORT.replace("\"j\"", "\"k\"")
        ))
    );
    assert_eq!(
        base,
        stamped(&expanded(
            "fingerprint = true, aliases = [\"old\"], priority = \"urgent\"",
            SHORT
        ))
    );
    assert_eq!(
        stamped(&expanded("fingerprint = true", UNNAMED)),
        stamped(&expanded(
            "fingerprint = true, name = \"send_email\"",
            UNNAMED
        ))
    );
}

#[test]
fn test_fingerprint_ignores_retry_policy() {
    let with_policy = SHORT.replace(
        "const NAME: &'static str = \"j\";",
        "const NAME: &'static str = \"j\";
         const RETRY_POLICY: Option<&'static dyn RetryPolicy> = Some(&BACKOFF);",
    );
    assert_eq!(
        stamped(&expanded("fingerprint = true", SHORT)),
        stamped(&expanded("fingerprint = true", &with_policy))
    );
}

#[test]
fn test_fingerprint_still_covers_max_attempts() {
    let declared = |attempts: u32| {
        SHORT.replace(
            "const NAME: &'static str = \"j\";",
            &format!("const NAME: &'static str = \"j\"; const MAX_ATTEMPTS: i32 = {attempts};"),
        )
    };
    let base = stamped(&expanded("fingerprint = true", SHORT));
    assert_ne!(base, stamped(&expanded("fingerprint = true", &declared(3))));
    assert_ne!(
        stamped(&expanded("fingerprint = true", &declared(3))),
        stamped(&expanded("fingerprint = true", &declared(4)))
    );
}
