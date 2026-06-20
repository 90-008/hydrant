use crate::util::throttle::Throttler;
use jacquard_common::http_client::HttpClient;
use reqwest::StatusCode;

#[derive(Clone)]
pub struct ThrottledHttpClient {
    pub client: reqwest::Client,
    pub throttler: Throttler,
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

        let res = self.client.send_http(request).await;

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
