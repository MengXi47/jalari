use crate::{Error, ErrorKind, Result};

const MAX_IDENTIFIER_LENGTH: usize = 63;
const LONGEST_OBJECT_NAME: &str = "job_history_created_at_idx";
const DEFAULT_SCHEMA_NAME: &str = "jalari";

pub(crate) const JOB_CHANNEL: &str = "job";
pub(crate) const RECURRING_CHANNEL: &str = "recurring";
pub(crate) const CONFIG_CHANNEL: &str = "config";

/// Where jalari's tables live: a dedicated schema, or a table prefix inside an existing one.
///
/// The default is the `jalari` schema. Several independent jalari installations can share a
/// database as long as each uses its own schema or prefix.
///
/// # Examples
///
/// ```rust
/// let dedicated = jalari::Schema::named("jobs")?;
/// let prefixed = jalari::Schema::prefixed("public", "jalari_")?;
/// assert_eq!(prefixed.prefix(), "jalari_");
/// # Ok::<(), jalari::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    name: String,
    prefix: String,
}

impl Schema {
    /// Puts the tables in schema `name` with their plain names (`name.job`, `name.recurring`...).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `name` does not match `[a-z_][a-z0-9_]*`
    ///   ([`InvalidSchemaName`](ErrorKind::InvalidSchemaName))
    /// - `name` starts with `pg_` ([`ReservedSchemaName`](ErrorKind::ReservedSchemaName))
    /// - `name` is longer than 63 bytes ([`SchemaNameTooLong`](ErrorKind::SchemaNameTooLong))
    pub fn named(name: &str) -> Result<Self> {
        validate_schema_name(name)?;
        Ok(Self {
            name: name.to_owned(),
            prefix: String::new(),
        })
    }

    /// Puts the tables in schema `name` with `prefix` added to each table and index name
    /// (`public.jalari_job`...).
    ///
    /// Use it when jalari must share an existing schema such as `public`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `name` is invalid, as for [`named`](Self::named)
    /// - `prefix` is not a lowercase identifier
    ///   ([`InvalidTablePrefix`](ErrorKind::InvalidTablePrefix))
    /// - `prefix` is longer than 37 bytes, which would push some names past PostgreSQL's limit
    ///   ([`TablePrefixTooLong`](ErrorKind::TablePrefixTooLong))
    pub fn prefixed(name: &str, prefix: &str) -> Result<Self> {
        validate_schema_name(name)?;
        validate_table_prefix(prefix)?;
        Ok(Self {
            name: name.to_owned(),
            prefix: prefix.to_owned(),
        })
    }

    /// Schema that holds the tables.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Prefix added to every table and index name; empty for [`named`](Self::named).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn quoted_name(&self) -> String {
        format!("\"{}\"", self.name)
    }

    pub(crate) fn table(&self, table: &str) -> String {
        format!("\"{}\".\"{}{}\"", self.name, self.prefix, table)
    }

    pub(crate) fn lock_key(&self, purpose: &str) -> String {
        format!("jalari:{purpose}:{}.{}", self.name, self.prefix)
    }

    pub(crate) fn notify_channel(&self, kind: &str) -> String {
        let channel = format!("{}.{}{kind}", self.name, self.prefix);
        if channel.len() <= MAX_IDENTIFIER_LENGTH {
            channel
        } else {
            format!("jalari_{:016x}", fnv1a(channel.as_bytes()))
        }
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            name: DEFAULT_SCHEMA_NAME.to_owned(),
            prefix: String::new(),
        }
    }
}

fn validate_schema_name(name: &str) -> Result<()> {
    if !is_identifier(name) {
        return Err(Error::new(
            ErrorKind::InvalidSchemaName,
            format!("schema name {name:?} must match [a-z_][a-z0-9_]*"),
        ));
    }
    if name.len() > MAX_IDENTIFIER_LENGTH {
        return Err(Error::new(
            ErrorKind::SchemaNameTooLong,
            format!("schema name {name:?} is longer than {MAX_IDENTIFIER_LENGTH} characters"),
        ));
    }
    if name.starts_with("pg_") {
        return Err(Error::new(
            ErrorKind::ReservedSchemaName,
            format!("schema name {name:?} uses the reserved pg_ prefix"),
        ));
    }
    Ok(())
}

fn validate_table_prefix(prefix: &str) -> Result<()> {
    if !is_identifier(prefix) {
        return Err(Error::new(
            ErrorKind::InvalidTablePrefix,
            format!("table prefix {prefix:?} must match [a-z_][a-z0-9_]*"),
        ));
    }
    let max_prefix_length = MAX_IDENTIFIER_LENGTH - LONGEST_OBJECT_NAME.len();
    if prefix.len() > max_prefix_length {
        return Err(Error::new(
            ErrorKind::TablePrefixTooLong,
            format!("table prefix {prefix:?} is longer than {max_prefix_length} characters"),
        ));
    }
    Ok(())
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some('a'..='z' | '_'))
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_schema_is_jalari_without_prefix() {
        let schema = Schema::default();
        assert_eq!(schema.name(), "jalari");
        assert_eq!(schema.prefix(), "");
        assert_eq!(schema.table("job"), "\"jalari\".\"job\"");
    }

    #[test]
    fn test_prefixed_schema_prefixes_table_names() {
        let schema = Schema::prefixed("public", "jalari_").unwrap();
        assert_eq!(schema.table("job"), "\"public\".\"jalari_job\"");
        assert_eq!(schema.quoted_name(), "\"public\"");
    }

    #[test]
    fn test_lock_key_differs_per_schema_and_prefix() {
        let named = Schema::named("jalari").unwrap();
        let prefixed = Schema::prefixed("jalari", "a_").unwrap();
        assert_ne!(named.lock_key("migrate"), prefixed.lock_key("migrate"));
    }

    #[test]
    fn test_notify_channel_follows_schema_and_prefix() {
        assert_eq!(Schema::default().notify_channel(JOB_CHANNEL), "jalari.job");
        assert_eq!(
            Schema::default().notify_channel(RECURRING_CHANNEL),
            "jalari.recurring"
        );
        assert_eq!(
            Schema::prefixed("public", "jalari_")
                .unwrap()
                .notify_channel(JOB_CHANNEL),
            "public.jalari_job"
        );
    }

    #[test]
    fn test_long_notify_channel_is_hashed_within_limit() {
        let long = Schema::prefixed(&"s".repeat(63), "p_").unwrap();
        let other = Schema::prefixed(&"t".repeat(63), "p_").unwrap();
        let channel = long.notify_channel(JOB_CHANNEL);
        assert!(channel.len() <= MAX_IDENTIFIER_LENGTH, "{channel}");
        assert!(channel.starts_with("jalari_"));
        assert_eq!(channel, long.notify_channel(JOB_CHANNEL));
        assert_ne!(channel, other.notify_channel(JOB_CHANNEL));
        assert_ne!(channel, long.notify_channel(RECURRING_CHANNEL));
    }

    #[test]
    fn test_rejects_invalid_schema_names() {
        for name in ["", "1abc", "Abc", "a-b", "a b", "a\"b", "ä"] {
            let err = Schema::named(name).unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidSchemaName, "{name:?}");
        }
    }

    #[test]
    fn test_rejects_reserved_schema_names() {
        let err = Schema::named("pg_catalog").unwrap_err();
        assert_eq!(err.kind, ErrorKind::ReservedSchemaName);
    }

    #[test]
    fn test_rejects_invalid_prefixes() {
        for prefix in ["", "1_", "A_", "a-", "a."] {
            let err = Schema::prefixed("public", prefix).unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidTablePrefix, "{prefix:?}");
        }
    }

    #[test]
    fn test_rejects_overlong_names() {
        assert!(Schema::named(&"a".repeat(63)).is_ok());
        let err = Schema::named(&"a".repeat(64)).unwrap_err();
        assert_eq!(err.kind, ErrorKind::SchemaNameTooLong);

        let longest_prefix = "p".repeat(MAX_IDENTIFIER_LENGTH - LONGEST_OBJECT_NAME.len());
        assert!(Schema::prefixed("public", &longest_prefix).is_ok());
        let err = Schema::prefixed("public", &format!("{longest_prefix}p")).unwrap_err();
        assert_eq!(err.kind, ErrorKind::TablePrefixTooLong);
    }
}
