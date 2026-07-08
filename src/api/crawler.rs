use std::net::IpAddr;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, get, post},
};
use serde::Deserialize;
use url::Url;

use crate::config::{CrawlerMode, CrawlerSource};
use crate::control::{Hydrant, crawler::CrawlerSourceInfo};

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/crawler/sources", get(list_sources))
        .route("/crawler/sources", post(add_source))
        .route("/crawler/sources", delete(remove_source))
        .route("/crawler/cursors", delete(reset_cursor))
}

pub async fn list_sources(State(hydrant): State<Hydrant>) -> Json<Vec<CrawlerSourceInfo>> {
    Json(hydrant.crawler.list_sources().await)
}

#[derive(Deserialize)]
pub struct AddSourceRequest {
    pub url: Url,
    pub mode: CrawlerMode,
}

fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback() || ip.is_multicast() || ip.is_unspecified() || {
                let octets = ip.octets();
                (octets[0] & 0xfe) == 0xfc || (octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80)
            }
        }
    }
}

async fn validate_source_url(url: &Url) -> Result<(), String> {
    use std::net::IpAddr;
    let scheme = url.scheme();
    if !matches!(scheme, "ws" | "wss" | "http" | "https") {
        return Err(format!(
            "invalid scheme `{scheme}`: only ws, wss, http, https are allowed"
        ));
    }
    let Some(host) = url.host_str() else {
        return Err("missing host in URL".to_string());
    };
    if host.is_empty() {
        return Err("empty host in URL".to_string());
    }
    if host == "localhost" || host.ends_with(".local") || host.ends_with(".internal") {
        return Err("local/internal hostnames are not allowed".to_string());
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_private_ip(ip) {
            return Err("private IP addresses are not allowed".to_string());
        }
    } else {
        match tokio::net::lookup_host(format!("{}:80", host)).await {
            Ok(addrs) => {
                for addr in addrs {
                    if is_private_ip(addr.ip()) {
                        return Err("host resolves to a private IP address".to_string());
                    }
                }
            }
            Err(e) => {
                return Err(format!("failed to resolve host `{host}`: {e}"));
            }
        }
    }
    Ok(())
}

async fn validate_reachability(url: &Url) -> Result<(), String> {
    let mut base = url.clone();
    match base.scheme() {
        "wss" => {
            let _ = base.set_scheme("https");
        }
        "ws" => {
            let _ = base.set_scheme("http");
        }
        _ => {}
    }
    let check_url = base
        .join("/xrpc/com.atproto.sync.listRepos?limit=1")
        .map_err(|e| e.to_string())?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?;

    match client.get(check_url).send().await {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("host is unreachable: {e}")),
    }
}

pub async fn add_source(
    State(hydrant): State<Hydrant>,
    Json(body): Json<AddSourceRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let sources = hydrant.crawler.list_sources().await;
    if sources.len() >= 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Too many crawler sources configured (limit 100)".to_string(),
        ));
    }

    if let Err(e) = validate_source_url(&body.url).await {
        return Err((StatusCode::BAD_REQUEST, format!("Invalid URL: {e}")));
    }

    if let Err(e) = validate_reachability(&body.url).await {
        return Err((StatusCode::BAD_REQUEST, format!("Unreachable: {e}")));
    }

    hydrant
        .crawler
        .add_source(CrawlerSource {
            url: body.url,
            mode: body.mode,
        })
        .await
        .map(|_| StatusCode::CREATED)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Deserialize)]
pub struct RemoveSourceRequest {
    pub url: Url,
}

pub async fn remove_source(
    State(hydrant): State<Hydrant>,
    Json(body): Json<RemoveSourceRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .crawler
        .remove_source(&body.url)
        .await
        .map(|found| {
            found
                .then_some(StatusCode::OK)
                .unwrap_or(StatusCode::NOT_FOUND)
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Deserialize)]
pub struct ResetCursorBody {
    pub key: String,
}

pub async fn reset_cursor(
    State(hydrant): State<Hydrant>,
    Json(body): Json<ResetCursorBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .crawler
        .reset_cursor(&body.key)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
