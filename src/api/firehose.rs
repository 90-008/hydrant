use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use serde::Deserialize;
use url::Url;

use crate::control::{FirehoseSourceInfo, Hydrant};

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/firehose/source", get(get_source))
        .route("/firehose/sources", get(list_sources))
        .route("/firehose/sources", post(add_source))
        .route("/firehose/sources", delete(remove_source))
        .route("/firehose/cursors", delete(reset_cursor))
}

#[derive(Debug, Deserialize)]
pub struct GetSourceQuery {
    pub url: Option<String>,
    pub host: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListSourcesQuery {
    pub host: Option<String>,
    pub url: Option<String>,
    pub is_pds: Option<bool>,
    pub running: Option<bool>,
    pub failing: Option<bool>,
    pub throttled: Option<bool>,
}

pub async fn get_source(
    State(hydrant): State<Hydrant>,
    Query(query): Query<GetSourceQuery>,
) -> Result<Json<FirehoseSourceInfo>, (StatusCode, String)> {
    let sources = hydrant.firehose.list_sources().await;
    let source = match (query.url.as_deref(), query.host.as_deref()) {
        (Some(url), None) => sources
            .into_iter()
            .find(|source| source.url.as_str() == url)
            .ok_or_else(|| (StatusCode::NOT_FOUND, "source does not exist".to_string()))?,
        (None, Some(host)) => {
            let mut matches = sources
                .into_iter()
                .filter(|source| source.url.host_str() == Some(host));
            let Some(source) = matches.next() else {
                return Err((StatusCode::NOT_FOUND, "source does not exist".to_string()));
            };
            if matches.next().is_some() {
                return Err((
                    StatusCode::CONFLICT,
                    "multiple sources match host; query by url".to_string(),
                ));
            }
            source
        }
        (Some(_), Some(_)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "query by either url or host, not both".to_string(),
            ));
        }
        (None, None) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "missing query param: url or host".to_string(),
            ));
        }
    };
    Ok(Json(source))
}

pub async fn list_sources(
    State(hydrant): State<Hydrant>,
    Query(query): Query<ListSourcesQuery>,
) -> Json<Vec<FirehoseSourceInfo>> {
    let mut sources = hydrant.firehose.list_sources().await;
    sources.retain(|source| {
        query
            .host
            .as_deref()
            .is_none_or(|host| source.url.host_str() == Some(host))
            && query
                .url
                .as_deref()
                .is_none_or(|url| source.url.as_str() == url)
            && query.is_pds.is_none_or(|is_pds| source.is_pds == is_pds)
            && query
                .running
                .is_none_or(|running| source.running == running)
            && query
                .failing
                .is_none_or(|failing| source.failing == failing)
            && query
                .throttled
                .is_none_or(|throttled| source.throttled == throttled)
    });
    Json(sources)
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
