use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, get, post},
};
use serde::Deserialize;
use url::Url;

use crate::control::{FirehoseSourceInfo, Hydrant};

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/firehose/sources", get(list_sources))
        .route("/firehose/sources", post(add_source))
        .route("/firehose/sources", delete(remove_source))
        .route("/firehose/cursors", delete(reset_cursor))
}

pub async fn list_sources(State(hydrant): State<Hydrant>) -> Json<Vec<FirehoseSourceInfo>> {
    Json(hydrant.firehose.list_sources().await)
}

#[derive(Deserialize)]
pub struct AddSourceRequest {
    pub url: Url,
    /// true to treat this as a direct PDS connection; enables host authority enforcement.
    /// defaults to false (aggregating relay).
    #[serde(default)]
    pub is_pds: bool,
}

pub async fn add_source(
    State(hydrant): State<Hydrant>,
    Json(body): Json<AddSourceRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .firehose
        .add_source(body.url, body.is_pds)
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
        .firehose
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
        .firehose
        .reset_cursor(&body.key)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
