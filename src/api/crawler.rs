use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, get, post},
};
use serde::Deserialize;
use url::Url;

use crate::config::{CrawlerMode, CrawlerSource};
use crate::control::{CrawlerSourceInfo, Hydrant};

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

pub async fn add_source(
    State(hydrant): State<Hydrant>,
    Json(body): Json<AddSourceRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
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
