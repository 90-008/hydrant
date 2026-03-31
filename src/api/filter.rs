use crate::control::{FilterPatch, Hydrant};
use crate::filter::FilterMode;
use crate::patch::SetUpdate;
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

use tracing::error;
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
pub struct PatchFilterBody {
    pub mode: Option<FilterMode>,
    pub signals: Option<SetUpdate>,
    pub collections: Option<SetUpdate>,
    pub excludes: Option<SetUpdate>,
}

pub async fn handle_patch_filter(
    State(hydrant): State<Hydrant>,
    Json(body): Json<PatchFilterBody>,
) -> Result<Json<FilterSnapshot>, (StatusCode, String)> {
    let mut p = FilterPatch::new(&hydrant.filter);
    p.mode = body.mode;
    p.signals = body.signals;
    p.collections = body.collections;
    p.excludes = body.excludes;
    p.apply()
        .await
        .map_err(|e| {
            error!(err = %e, "failed to patch filter");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })
        .map(Json)
}
