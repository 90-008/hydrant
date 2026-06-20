use crate::resolver::ResolverError;
use crate::util::throttle::ThrottleHandle;
use jacquard_common::error::{ClientError, ClientErrorKind};
use miette::Diagnostic;
use reqwest::StatusCode;
use smol_str::{SmolStr, ToSmolStr};
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Diagnostic, Error)]
pub enum BackfillError {
    #[error("{0}")]
    Generic(miette::Report),
    #[error("too many requests")]
    Ratelimited,
    #[error("pds is throttled")]
    PreemptivelyThrottled,
    #[error("transport error: {0}")]
    Transport(SmolStr),
    #[error("repo was concurrently deleted")]
    Deleted,
}

impl From<ClientError> for BackfillError {
    fn from(e: ClientError) -> Self {
        match e.kind() {
            ClientErrorKind::Http {
                status: StatusCode::TOO_MANY_REQUESTS,
            } => Self::Ratelimited,
            ClientErrorKind::Transport => Self::Transport(
                e.source_err()
                    .expect("transport error without source")
                    .to_smolstr(),
            ),
            _ => Self::Generic(e.into()),
        }
    }
}

impl BackfillError {
    pub(crate) fn from_sparse_client(e: ClientError, throttle: &ThrottleHandle) -> Self {
        match e.kind() {
            ClientErrorKind::Http {
                status: StatusCode::TOO_MANY_REQUESTS,
            } => Self::Ratelimited,
            ClientErrorKind::Transport => {
                let reason = e
                    .source_err()
                    .expect("transport error without source")
                    .to_smolstr();
                if let Some(secs) = throttle.record_failure_detail("transport", reason.to_string())
                {
                    debug!(secs, reason = %reason, "throttling pds after sparse transport error");
                }
                Self::Transport(reason)
            }
            _ => Self::Generic(e.into()),
        }
    }
}

impl From<miette::Report> for BackfillError {
    fn from(e: miette::Report) -> Self {
        Self::Generic(e)
    }
}

impl From<ResolverError> for BackfillError {
    fn from(e: ResolverError) -> Self {
        match e {
            ResolverError::Ratelimited => Self::Ratelimited,
            ResolverError::Transport(s) => Self::Transport(s),
            ResolverError::Generic(e) => Self::Generic(e),
        }
    }
}
