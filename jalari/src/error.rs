use std::fmt;

/// What went wrong, for code that needs to decide how to react.
///
/// Database errors are classified by their SQLSTATE. [`ConnectionFailed`](Self::ConnectionFailed),
/// [`ConnectionTerminated`](Self::ConnectionTerminated), [`PoolTimedOut`](Self::PoolTimedOut),
/// [`SerializationFailure`](Self::SerializationFailure), [`Deadlock`](Self::Deadlock) and
/// [`LockTimeout`](Self::LockTimeout) are usually temporary and worth retrying.
///
/// New kinds may be added in minor releases, so a `match` needs a `_` arm.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// The database could not be reached or the connection broke (I/O errors, SQLSTATE class
    /// `08`, `57P03`).
    ConnectionFailed,
    /// The server ended the session, for example an administrator shutdown or an idle timeout
    /// (`57P01`, `57P02`, `57P05`, `25P03`).
    ConnectionTerminated,
    /// The TLS handshake failed.
    TlsFailed,
    /// The server sent something the client did not expect.
    ProtocolViolation,
    /// The server rejected the credentials (SQLSTATE class `28`).
    AuthenticationFailed,
    /// The role lacks a privilege the statement needs (`42501`), such as `CREATE` on the database
    /// when migrating.
    PermissionDenied,
    /// The connection URL or options are invalid.
    InvalidConnectionOptions,
    /// No pooled connection became free within the pool's acquire timeout.
    PoolTimedOut,
    /// The pool was closed.
    PoolClosed,
    /// A serializable transaction could not be committed (`40001`).
    SerializationFailure,
    /// The statement was chosen as a deadlock victim (`40P01`).
    Deadlock,
    /// A lock could not be acquired within `lock_timeout` (`55P03`).
    LockTimeout,
    /// The statement was cancelled, for example by `statement_timeout` (`57014`).
    QueryCanceled,
    /// A query that must return a row returned none.
    RowNotFound,
    /// A row could not be decoded into the expected Rust type.
    RowDecodeFailed,
    /// A value could not be encoded as a query argument.
    ArgumentEncodeFailed,
    /// Any other database error; the message starts with its SQLSTATE.
    QueryFailed,
    /// A schema name is not a lowercase identifier matching `[a-z_][a-z0-9_]*`.
    InvalidSchemaName,
    /// A schema name starts with the reserved `pg_` prefix.
    ReservedSchemaName,
    /// A schema name is longer than PostgreSQL's 63-byte identifier limit.
    SchemaNameTooLong,
    /// A table prefix is not a lowercase identifier.
    InvalidTablePrefix,
    /// A table prefix would push some table or index name past 63 bytes.
    TablePrefixTooLong,
    /// The tables are missing or at a different version than this release of jalari needs;
    /// returned by [`init`](crate::init). Run a migration to fix it.
    SchemaVersionMismatch,
    /// A migration target is outside `1..=`[`SCHEMA_VERSION`](crate::SCHEMA_VERSION).
    UnknownSchemaVersion,
    /// [`init`](crate::init) was called a second time.
    AlreadyInitialized,
    /// An operation that needs [`init`](crate::init) ran before it completed.
    NotInitialized,
    /// A job could not be serialized with `serde_json`.
    PayloadEncodeFailed,
    /// A timeout does not fit in an `i32` of milliseconds (about 24 days).
    TimeoutTooLong,
    /// An enqueue with [`OnConflict::Replace`](crate::OnConflict::Replace) kept racing with
    /// other writers for the same `job_key` and gave up after several attempts.
    JobKeyContention,
    /// Two `#[jalari::job]` types in the same binary use the same [`Job::NAME`](crate::Job::NAME).
    DuplicateJobName,
    /// [`WorkerBuilder::build`](crate::WorkerBuilder::build) found no `#[jalari::job]` linked
    /// into the binary.
    NoJobRegistered,
    /// [`WorkerBuilder::build`](crate::WorkerBuilder::build) was called without any
    /// [`queue`](crate::WorkerBuilder::queue).
    NoQueueConfigured,
    /// The same queue was configured twice on one worker.
    DuplicateQueue,
    /// A queue was configured with a concurrency of zero.
    InvalidConcurrency,
    /// The pool's `max_connections` is lower than the worker needs: the sum of all queue
    /// concurrencies plus two.
    InsufficientConnections,
    /// The worker's heartbeat interval is zero or not shorter than the cluster's
    /// [`worker_timeout`](crate::ClusterConfig::worker_timeout).
    InvalidHeartbeatInterval,
    /// [`cluster::update`](crate::cluster::update) was given a value outside its allowed range.
    InvalidConfigValue,
    /// The single row of the `config` table was deleted.
    ConfigMissing,
    /// A cron expression could not be parsed.
    InvalidCronExpression,
    /// A time zone is not a known IANA name such as `Asia/Taipei`.
    UnknownTimezone,
    /// No recurring schedule has the given name.
    RecurringNotFound,
    /// The recurring schedule is declared with `#[jalari::job(cron = ...)]` and cannot be
    /// changed, paused or removed at runtime.
    RecurringManagedByCode,
    /// An error that fits no other kind; the payload describes it.
    Unknown(String),
}

/// Error category and diagnostic message returned by every fallible jalari operation.
///
/// Match on [`kind`](Self::kind) to decide what to do; [`msg`](Self::msg) is for people and logs,
/// may be empty and has no stable format. `Display` prints `Kind: msg`.
///
/// # Examples
///
/// ```rust
/// let err = jalari::Schema::named("Bad-Name").unwrap_err();
/// assert_eq!(err.kind, jalari::ErrorKind::InvalidSchemaName);
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    /// The category to match on.
    pub kind: ErrorKind,
    /// A human-readable description; database errors start with their SQLSTATE.
    pub msg: String,
}

/// `Result` with jalari's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Creates an error of the given kind with a message.
    pub fn new(kind: ErrorKind, msg: String) -> Self {
        Self { kind, msg }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self::new(kind, String::new())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.msg.is_empty() {
            write!(f, "{:?}", self.kind)
        } else {
            write!(f, "{:?}: {}", self.kind, self.msg)
        }
    }
}

impl std::error::Error for Error {}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        let kind = match &e {
            sqlx::Error::Database(database_error) => {
                let code = database_error.code().unwrap_or_default();
                return Self::new(
                    kind_for_sqlstate(&code),
                    format!("{code}: {}", database_error.message()),
                );
            }
            sqlx::Error::Io(_) | sqlx::Error::WorkerCrashed => ErrorKind::ConnectionFailed,
            sqlx::Error::Tls(_) => ErrorKind::TlsFailed,
            sqlx::Error::Protocol(_) => ErrorKind::ProtocolViolation,
            sqlx::Error::Configuration(_) => ErrorKind::InvalidConnectionOptions,
            sqlx::Error::PoolTimedOut => ErrorKind::PoolTimedOut,
            sqlx::Error::PoolClosed => ErrorKind::PoolClosed,
            sqlx::Error::RowNotFound => ErrorKind::RowNotFound,
            sqlx::Error::TypeNotFound { .. }
            | sqlx::Error::ColumnNotFound(_)
            | sqlx::Error::ColumnIndexOutOfBounds { .. }
            | sqlx::Error::ColumnDecode { .. }
            | sqlx::Error::Decode(_) => ErrorKind::RowDecodeFailed,
            sqlx::Error::Encode(_) => ErrorKind::ArgumentEncodeFailed,
            _ => ErrorKind::QueryFailed,
        };
        Self::new(kind, e.to_string())
    }
}

fn kind_for_sqlstate(code: &str) -> ErrorKind {
    match code {
        "42501" => ErrorKind::PermissionDenied,
        "40001" => ErrorKind::SerializationFailure,
        "40P01" => ErrorKind::Deadlock,
        "55P03" => ErrorKind::LockTimeout,
        "57014" => ErrorKind::QueryCanceled,
        "57P01" | "57P02" | "57P05" | "25P03" => ErrorKind::ConnectionTerminated,
        "57P03" => ErrorKind::ConnectionFailed,
        _ if code.starts_with("08") => ErrorKind::ConnectionFailed,
        _ if code.starts_with("28") => ErrorKind::AuthenticationFailed,
        _ => ErrorKind::QueryFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_includes_message_when_present() {
        let error = Error::new(ErrorKind::InvalidSchemaName, "bad schema".to_owned());
        assert_eq!(error.to_string(), "InvalidSchemaName: bad schema");
        assert_eq!(
            Error::from(ErrorKind::RecurringNotFound).to_string(),
            "RecurringNotFound"
        );
    }

    #[test]
    fn test_sqlstate_classification() {
        let cases = [
            ("42501", ErrorKind::PermissionDenied),
            ("28P01", ErrorKind::AuthenticationFailed),
            ("40001", ErrorKind::SerializationFailure),
            ("40P01", ErrorKind::Deadlock),
            ("55P03", ErrorKind::LockTimeout),
            ("57014", ErrorKind::QueryCanceled),
            ("57P01", ErrorKind::ConnectionTerminated),
            ("57P02", ErrorKind::ConnectionTerminated),
            ("57P05", ErrorKind::ConnectionTerminated),
            ("25P03", ErrorKind::ConnectionTerminated),
            ("57P03", ErrorKind::ConnectionFailed),
            ("08006", ErrorKind::ConnectionFailed),
            ("23505", ErrorKind::QueryFailed),
            ("42601", ErrorKind::QueryFailed),
            ("", ErrorKind::QueryFailed),
        ];
        for (code, kind) in cases {
            assert_eq!(kind_for_sqlstate(code), kind, "{code}");
        }
    }

    #[test]
    fn test_sqlx_errors_without_database_map_to_kinds() {
        let cases = [
            (sqlx::Error::PoolTimedOut, ErrorKind::PoolTimedOut),
            (sqlx::Error::PoolClosed, ErrorKind::PoolClosed),
            (sqlx::Error::WorkerCrashed, ErrorKind::ConnectionFailed),
            (sqlx::Error::RowNotFound, ErrorKind::RowNotFound),
            (
                sqlx::Error::ColumnNotFound("id".to_owned()),
                ErrorKind::RowDecodeFailed,
            ),
            (
                sqlx::Error::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
                ErrorKind::ConnectionFailed,
            ),
            (
                sqlx::Error::Protocol("bad".to_owned()),
                ErrorKind::ProtocolViolation,
            ),
        ];
        for (source, kind) in cases {
            let error = Error::from(source);
            assert_eq!(error.kind, kind);
            assert!(!error.msg.is_empty());
        }
    }
}
