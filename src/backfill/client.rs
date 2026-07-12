use crate::util::throttle::Throttler;
use bytes::{Bytes, BytesMut};
use jacquard_common::http_client::HttpClient;
use reqwest::StatusCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// an http client that spreads requests across a pool of egress connections (the direct
/// connection plus any configured proxies) via round-robin, and records 429 rate-limit
/// responses into the shared [`Throttler`] before jacquard discards the headers.
#[derive(Clone)]
pub struct ThrottledHttpClient {
    clients: Arc<Vec<reqwest::Client>>,
    next: Arc<AtomicUsize>,
    pub throttler: Throttler,
}

impl ThrottledHttpClient {
    /// builds a client from a non-empty pool. panics if `clients` is empty.
    pub fn new(clients: Vec<reqwest::Client>, throttler: Throttler) -> Self {
        assert!(
            !clients.is_empty(),
            "throttled client pool must be non-empty"
        );
        Self {
            clients: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
            throttler,
        }
    }

    /// picks the next client in round-robin order.
    fn pick(&self) -> &reqwest::Client {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }

    /// starts a raw GET request on the next client in the pool. used by the sparse getBlocks
    /// path, which fetches CAR bytes directly rather than through the xrpc layer.
    pub fn get(&self, url: url::Url) -> reqwest::RequestBuilder {
        self.pick().get(url)
    }
}

impl HttpClient for ThrottledHttpClient {
    type Error = reqwest::Error;

    async fn send_http(
        &self,
        request: http::Request<Vec<u8>>,
    ) -> Result<http::Response<Vec<u8>>, Self::Error> {
        let uri = request.uri().clone();
        let host_url = if let Some(host) = uri.host() {
            let scheme = uri.scheme_str().unwrap_or("https");
            let port_str = uri.port_u16().map(|p| format!(":{p}")).unwrap_or_default();
            let url_str = format!("{scheme}://{host}{port_str}");
            url::Url::parse(&url_str).ok()
        } else {
            None
        };

        let res = self.pick().send_http(request).await;

        if let Ok(ref resp) = res {
            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                if let Some(ref host_url) = host_url {
                    let headers = resp.headers();
                    let retry_after = headers
                        .get(http::header::RETRY_AFTER)
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

                    let delay_secs = retry_after.or(rate_limit_reset);
                    self.throttler
                        .get_handle(host_url)
                        .await
                        .record_ratelimit(delay_secs);
                }
            }
        }

        res
    }
}

/// streams a response body into a single contiguous [`Bytes`], enforcing a hard byte ceiling.
///
/// returns `Ok(None)` when the body exceeds `max_bytes`: the response is dropped without
/// buffering the remainder, so per-task memory stays bounded regardless of what the PDS sends
/// (a chunked response with no `Content-Length`, or a decompression bomb — `chunk` yields
/// already-decoded bytes, so the ceiling applies to the decompressed size). the buffer is grown
/// in place and never double-buffered: at most `buf + one chunk` is resident at any point.
pub(crate) async fn collect_body_bounded(
    mut resp: reqwest::Response,
    max_bytes: usize,
) -> Result<Option<Bytes>, reqwest::Error> {
    // reject early when a declared (uncompressed) length already exceeds the ceiling. reqwest
    // strips `Content-Length` from decoded responses, so a present value is the real body size.
    let content_length = resp.content_length();
    if content_length.is_some_and(|len| len > max_bytes as u64) {
        return Ok(None);
    }
    let reserve = content_length.unwrap_or(0).min(max_bytes as u64) as usize;
    let mut buf = BytesMut::with_capacity(reserve);
    while let Some(chunk) = resp.chunk().await? {
        if buf.len() + chunk.len() > max_bytes {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Some(buf.freeze()))
}
