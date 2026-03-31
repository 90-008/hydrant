use std::time::Duration;

use jacquard_common::{deps::fluent_uri, types::string::Handle};
use rand::RngExt;
use reqwest::StatusCode;
use serde::{Deserialize, Deserializer, Serializer};
use tokio::sync::watch;
use tracing::info;
use url::Url;

use crate::{db::types::DidKey, types::RepoStatus};

pub mod throttle;

/// outcome of [`RetryWithBackoff::retry`] when the operation does not succeed.
pub enum RetryOutcome<E> {
    /// ratelimited after exhausting all retries
    Ratelimited,
    /// non-ratelimit failure, carrying the last error
    Failed(E),
}

/// extension trait that adds `.retry()` to async `FnMut` closures.
///
/// `on_ratelimit` receives the error and current attempt number.
/// returning `Some(duration)` signals a transient failure and provides the backoff;
/// returning `None` signals a terminal failure.
pub trait RetryWithBackoff<T, E, Fut>: FnMut() -> Fut
where
    Fut: Future<Output = Result<T, E>>,
{
    #[allow(async_fn_in_trait)]
    async fn retry(
        &mut self,
        max_retries: u32,
        on_ratelimit: impl Fn(&E, u32) -> Option<Duration>,
    ) -> Result<T, RetryOutcome<E>> {
        let mut attempt = 0u32;
        loop {
            match self().await {
                Ok(val) => return Ok(val),
                Err(e) => match on_ratelimit(&e, attempt) {
                    Some(_) if attempt >= max_retries => return Err(RetryOutcome::Ratelimited),
                    Some(backoff) => {
                        // jitter the backoff
                        let backoff = rand::rng().random_range((backoff / 2)..backoff);
                        tokio::time::sleep(backoff).await;
                        attempt += 1;
                    }
                    None => return Err(RetryOutcome::Failed(e)),
                },
            }
        }
    }
}

impl<T, E, F, Fut> RetryWithBackoff<T, E, Fut> for F
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
}

/// extension trait that adds `.wait_enabled()` to `watch::Receiver<bool>`.
///
/// waits until the value becomes `true`, logging once when paused and once when resumed.
pub trait WatchEnabledExt {
    #[allow(async_fn_in_trait)]
    async fn wait_enabled(&mut self, component: &'static str);
}

impl WatchEnabledExt for watch::Receiver<bool> {
    async fn wait_enabled(&mut self, component: &'static str) {
        if !*self.borrow() {
            info!("{component} paused");
            while !*self.borrow() {
                let _ = self.changed().await;
            }
            info!("{component} resumed");
        }
    }
}

/// extension trait that adds `.error_for_status()` to futures returning a reqwest `Response`.
pub trait ErrorForStatus: Future<Output = Result<reqwest::Response, reqwest::Error>> {
    fn error_for_status(self) -> impl Future<Output = Result<reqwest::Response, reqwest::Error>>
    where
        Self: Sized,
    {
        futures::FutureExt::map(self, |r| r.and_then(|r| r.error_for_status()))
    }
}

impl<F: Future<Output = Result<reqwest::Response, reqwest::Error>>> ErrorForStatus for F {}

/// extracts a retry delay in seconds from rate limit response headers.
///
/// checks in priority order:
/// - `retry-after: <seconds>` (relative)
/// - `ratelimit-reset: <unix timestamp>` (absolute) (ref pds sends this)
pub fn parse_retry_after(resp: &reqwest::Response) -> Option<u64> {
    let headers = resp.headers();

    let retry_after = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let rate_limit_reset = headers
        .get("ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .map(|ts| {
            let now = chrono::Utc::now().timestamp();
            (ts - now).max(1) as u64
        });

    retry_after.or(rate_limit_reset)
}

// cloudflare-specific status codes
pub const CONNECTION_TIMEOUT: StatusCode = unsafe {
    match StatusCode::from_u16(522) {
        Ok(s) => s,
        _ => std::hint::unreachable_unchecked(),
    }
};
pub const SITE_FROZEN: StatusCode = unsafe {
    match StatusCode::from_u16(530) {
        Ok(s) => s,
        _ => std::hint::unreachable_unchecked(),
    }
};

pub fn ser_status_code<S: Serializer>(s: &Option<StatusCode>, ser: S) -> Result<S::Ok, S::Error> {
    match s {
        Some(code) => ser.serialize_some(&code.as_u16()),
        None => ser.serialize_none(),
    }
}

pub fn deser_status_code<'de, D: Deserializer<'de>>(
    deser: D,
) -> Result<Option<StatusCode>, D::Error> {
    Option::<u16>::deserialize(deser)?
        .map(StatusCode::from_u16)
        .transpose()
        .map_err(serde::de::Error::custom)
}

pub fn opt_cid_serialize_str<S: Serializer>(v: &Option<cid::Cid>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(cid) => s.serialize_some(cid.to_string().as_str()),
        None => s.serialize_none(),
    }
}

pub fn did_key_serialize_str<S: Serializer>(v: &DidKey<'_>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.encode())
}

pub fn opt_did_key_serialize_str<S: Serializer>(
    v: &Option<DidKey<'_>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match v {
        Some(k) => s.serialize_some(k.encode().as_str()),
        None => s.serialize_none(),
    }
}

pub fn repo_status_serialize_str<S: Serializer>(v: &RepoStatus, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

pub fn url_to_fluent_uri(url: &Url) -> fluent_uri::Uri<String> {
    fluent_uri::Uri::parse(url.as_str())
        .expect("that url is validated")
        .to_owned()
}

pub(crate) fn invalid_handle() -> Handle<'static> {
    unsafe { Handle::unchecked("handle.invalid") }
}
