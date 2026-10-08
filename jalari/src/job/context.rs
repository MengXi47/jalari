use std::future::Future;

use serde::Serialize;
use serde_json::Value;
use tokio::task::futures::TaskLocalFuture;

use crate::{Error, ErrorKind, Result};

tokio::task_local! {
    static CONTEXT: Captured;
}

#[derive(Debug, Clone, Default)]
pub(crate) enum Captured {
    #[default]
    Empty,
    Value(Value),
    Invalid(String),
}

impl Captured {
    pub(crate) fn encode<C: Serialize>(context: &C) -> Self {
        match serde_json::to_value(context) {
            Ok(value) => Self::Value(value),
            Err(e) => Self::Invalid(e.to_string()),
        }
    }

    pub(crate) fn from_stored(context: Option<Value>) -> Self {
        context.map_or(Self::Empty, Self::Value)
    }

    pub(crate) fn into_value(self, task: &str) -> Result<Option<Value>> {
        match self {
            Self::Empty => Ok(None),
            Self::Value(value) => Ok(Some(value)),
            Self::Invalid(msg) => Err(Error::new(
                ErrorKind::PayloadEncodeFailed,
                format!("{task}: context: {msg}"),
            )),
        }
    }
}

/// Runs `future` with `context` attached to every job it enqueues.
///
/// The context is serialized once, here, and stored with each job enqueued inside `future`
/// unless [`EnqueueOptions::context`](crate::EnqueueOptions::context) or
/// [`EnqueueOptions::no_context`](crate::EnqueueOptions::no_context) says otherwise. Workers
/// hand it to their [`JobMiddleware`](crate::JobMiddleware) through
/// [`JobMeta::context`](crate::JobMeta::context). Jobs enqueued while a job runs inherit that
/// job's context.
///
/// Store identifiers only, such as a tenant or user id: the context is saved as plain JSON
/// next to the job.
///
/// # Examples
///
/// ```rust,no_run
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendEmail { to: String }
/// # #[jalari::job]
/// # impl jalari::Job for SendEmail {
/// #     const NAME: &'static str = "send_email";
/// #     async fn run(self) -> jalari::JobResult { Ok(()) }
/// # }
/// #[derive(Serialize, Deserialize)]
/// struct Caller {
///     tenant_id: String,
/// }
///
/// # async fn example() -> jalari::Result<()> {
/// let caller = Caller { tenant_id: "acme".to_owned() };
/// jalari::scope(&caller, async {
///     jalari::enqueue(&SendEmail { to: "a@example.com".to_owned() }).await
/// })
/// .await?;
/// # Ok(())
/// # }
/// ```
pub fn scope<C: Serialize, F: Future>(context: &C, future: F) -> impl Future<Output = F::Output> {
    within(Captured::encode(context), future)
}

pub(crate) fn within<F: Future>(captured: Captured, future: F) -> TaskLocalFuture<Captured, F> {
    CONTEXT.scope(captured, future)
}

pub(crate) fn current() -> Captured {
    CONTEXT.try_with(Clone::clone).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scope_is_visible_inside_and_gone_outside() {
        assert!(matches!(current(), Captured::Empty));
        let inside = scope(&"acme", async { current() }).await;
        assert!(matches!(inside, Captured::Value(Value::String(tenant)) if tenant == "acme"));
        assert!(matches!(current(), Captured::Empty));
    }

    #[tokio::test]
    async fn test_inner_scope_wins() {
        let inner = scope(&1, scope(&2, async { current() })).await;
        assert!(matches!(inner, Captured::Value(value) if value == 2));
    }

    #[test]
    fn test_invalid_context_becomes_encode_error() {
        let mut map = std::collections::HashMap::new();
        map.insert(vec![1_u8], 1);
        let err = Captured::encode(&map).into_value("task").unwrap_err();
        assert_eq!(err.kind, ErrorKind::PayloadEncodeFailed);
        assert!(err.msg.starts_with("task: context: "));
    }
}
