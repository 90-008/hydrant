use std::str::FromStr;

use crate::control::{Hydrant, RepoInfo};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use jacquard_common::types::did::Did;
use miette::IntoDiagnostic;
use serde::Deserialize;

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/repos", get(handle_get_repos))
        .route("/repos/resync", post(handle_post_resync))
        .route("/repos/{did}", get(handle_get_repo))
        .route("/repos", put(handle_put_repos))
        .route("/repos", delete(handle_delete_repos))
}

#[derive(Deserialize, Debug)]
pub struct RepoRequest {
    pub did: String,
}

#[derive(Deserialize)]
pub struct GetReposParams {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

pub async fn handle_get_repos(
    State(hydrant): State<Hydrant>,
    Query(params): Query<GetReposParams>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(100).min(1000);
    let cursor = params
        .cursor
        .map(|c| Did::from_str(&c))
        .transpose()
        .map_err(bad_request)?;

    let items = tokio::task::spawn_blocking(move || {
        hydrant
            .repos
            .iter(cursor.as_ref())
            .take(limit)
            .collect::<miette::Result<Vec<_>>>()
    })
    .await
    .into_diagnostic()
    .flatten()
    .map_err(internal)?;

    if prefers_json(&headers) {
        return Ok(Json(items).into_response());
    }

    use futures::StreamExt;

    let stream = futures::stream::iter(items.into_iter().map(|item| {
        let json = serde_json::to_string(&item).ok()?;
        Some(Ok::<_, std::io::Error>(format!("{json}\n")))
    }))
    .filter_map(|x| futures::future::ready(x));

    let body = Body::from_stream(stream);

    Ok(([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response())
}

pub async fn handle_get_repo(
    State(hydrant): State<Hydrant>,
    Path(did_str): Path<String>,
) -> Result<Json<RepoInfo>, (StatusCode, String)> {
    let did = Did::new(&did_str).map_err(bad_request)?;

    let item = hydrant
        .repos
        .info(&did)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    item.map(Json)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "repository not found".to_string()))
}

pub async fn handle_put_repos(
    State(hydrant): State<Hydrant>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, (StatusCode, String)> {
    let items = parse_body(body, &headers).await?;

    let dids: Vec<Did<'static>> = items
        .into_iter()
        .filter_map(|item| Did::new_owned(&item.did).ok())
        .collect();

    let queued = hydrant
        .repos
        .track(dids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(did_list_response(queued, &headers))
}

pub async fn handle_delete_repos(
    State(hydrant): State<Hydrant>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, (StatusCode, String)> {
    let items = parse_body(body, &headers).await?;

    let dids: Vec<Did<'static>> = items
        .into_iter()
        .filter_map(|item| Did::new_owned(&item.did).ok())
        .collect();

    let untracked = hydrant
        .repos
        .untrack(dids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(did_list_response(untracked, &headers))
}

pub async fn handle_post_resync(
    State(hydrant): State<Hydrant>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, (StatusCode, String)> {
    let items = parse_body(body, &headers).await?;

    let dids: Vec<Did<'static>> = items
        .into_iter()
        .filter_map(|item| Did::new_owned(&item.did).ok())
        .collect();

    let queued = hydrant.repos.resync(dids).await.map_err(internal)?;

    Ok(did_list_response(queued, &headers))
}

fn prefers_json(headers: &HeaderMap) -> bool {
    let contains_json = |h: axum::http::HeaderName| {
        headers
            .get(h)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("application/json"))
    };
    contains_json(header::ACCEPT) || contains_json(header::CONTENT_TYPE)
}

fn did_list_response(dids: Vec<Did<'static>>, headers: &HeaderMap) -> Response {
    if prefers_json(headers) {
        let body: Vec<String> = dids.into_iter().map(|d| d.to_string()).collect();
        Json(body).into_response()
    } else {
        let body = dids
            .iter()
            .filter_map(|d| serde_json::to_string(&d.as_str()).ok())
            .map(|s| format!("{s}\n"))
            .collect::<String>();
        ([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response()
    }
}

async fn parse_body(
    body: Body,
    headers: &HeaderMap,
) -> Result<Vec<RepoRequest>, (StatusCode, String)> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();

    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(bad_request)?;

    let text = std::str::from_utf8(&body_bytes).map_err(bad_request)?;
    let trimmed = text.trim();

    if content_type.contains("application/json") {
        serde_json::from_str::<Vec<RepoRequest>>(trimmed)
            .map_err(|e| bad_request(format!("invalid JSON array: {e}")))
    } else {
        trimmed
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<RepoRequest>(line)
                    .map_err(|e| bad_request(format!("invalid NDJSON line: {e}")))
            })
            .collect()
    }
}

fn bad_request<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

fn internal<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
