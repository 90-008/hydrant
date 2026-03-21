use crate::control::Hydrant;
use crate::filter::{FilterMode, SetUpdate};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, patch},
};
use serde::Deserialize;

type FilterSnapshot = crate::control::FilterSnapshot;

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/filter", get(handle_get_filter))
        .route("/filter", patch(handle_patch_filter))
}

pub async fn handle_get_filter(
    State(hydrant): State<Hydrant>,
) -> Result<Json<FilterSnapshot>, (StatusCode, String)> {
    hydrant
        .filter
        .get()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .map(Json)
}

#[derive(Deserialize)]
pub struct FilterPatch {
    pub mode: Option<FilterMode>,
    pub signals: Option<SetUpdate>,
    pub collections: Option<SetUpdate>,
    pub excludes: Option<SetUpdate>,
}

pub async fn handle_patch_filter(
    State(hydrant): State<Hydrant>,
    Json(patch): Json<FilterPatch>,
) -> Result<Json<FilterSnapshot>, (StatusCode, String)> {
    hydrant
        .filter
        .patch(patch.mode, patch.signals, patch.collections, patch.excludes)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .map(Json)
}
