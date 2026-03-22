use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::control::Hydrant;

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route(
            "/xrpc/blue.microcosm.links.getBacklinks",
            get(handle_get_backlinks),
        )
        .route(
            "/xrpc/blue.microcosm.links.getBacklinksCount",
            get(handle_get_backlinks_count),
        )
}

#[derive(Deserialize)]
pub struct GetBacklinksParams {
    pub subject: String,
    /// filter by source collection, optionally with a path suffix `collection:path`
    pub source: Option<String>,
    pub limit: Option<u64>,
    pub cursor: Option<String>,
    pub reverse: Option<bool>,
}

#[derive(Serialize)]
pub struct Backlink {
    pub uri: SmolStr,
    pub cid: SmolStr,
}

#[derive(Serialize)]
pub struct GetBacklinksOutput {
    pub backlinks: Vec<Backlink>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Deserialize)]
pub struct GetBacklinksCountParams {
    pub subject: String,
    pub source: Option<String>,
}

#[derive(Serialize)]
pub struct GetBacklinksCountOutput {
    pub count: u64,
}

pub async fn handle_get_backlinks(
    State(hydrant): State<Hydrant>,
    axum::extract::Query(params): axum::extract::Query<GetBacklinksParams>,
) -> Result<Json<GetBacklinksOutput>, StatusCode> {
    let limit = params.limit.unwrap_or(50).min(100) as usize;
    let reverse = params.reverse.unwrap_or(false);

    let cursor_bytes = params
        .cursor
        .as_deref()
        .and_then(|c| data_encoding::BASE64URL_NOPAD.decode(c.as_bytes()).ok());

    let mut fetch = hydrant
        .backlinks
        .fetch(params.subject)
        .limit(limit)
        .reverse(reverse);
    if let Some(ref src) = params.source {
        fetch = fetch.source(src);
    }
    if let Some(c) = cursor_bytes {
        fetch = fetch.cursor(c);
    }

    let page = fetch
        .run()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let next_cursor = page
        .next_cursor
        .map(|b| data_encoding::BASE64URL_NOPAD.encode(&b));
    let backlinks = page
        .backlinks
        .into_iter()
        .map(|e| Backlink {
            uri: e.uri,
            cid: e.cid,
        })
        .collect();

    Ok(Json(GetBacklinksOutput {
        backlinks,
        cursor: next_cursor,
    }))
}

pub async fn handle_get_backlinks_count(
    State(hydrant): State<Hydrant>,
    axum::extract::Query(params): axum::extract::Query<GetBacklinksCountParams>,
) -> Result<Json<GetBacklinksCountOutput>, StatusCode> {
    let mut count_q = hydrant.backlinks.count(params.subject);
    if let Some(ref src) = params.source {
        count_q = count_q.source(src);
    }

    let count = count_q
        .run()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(GetBacklinksCountOutput { count }))
}
