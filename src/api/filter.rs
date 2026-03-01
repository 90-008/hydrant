use std::sync::Arc;

use crate::api::AppState;
use crate::db;
use crate::db::filter::EXCLUDE_PREFIX;
use crate::filter::{FilterMode, SetUpdate};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, patch},
};
use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/filter", get(handle_get_filter))
        .route("/filter", patch(handle_patch_filter))
}

#[derive(Serialize)]
pub struct FilterResponse {
    pub mode: FilterMode,
    pub signals: Vec<String>,
    pub collections: Vec<String>,
    pub excludes: Vec<String>,
}

pub async fn handle_get_filter(
    State(state): State<Arc<AppState>>,
) -> Result<Json<FilterResponse>, (StatusCode, String)> {
    let filter_ks = state.db.filter.clone();
    let resp = tokio::task::spawn_blocking(move || {
        let hot = db::filter::load(&filter_ks).map_err(|e| e.to_string())?;
        let excludes =
            db::filter::read_set(&filter_ks, EXCLUDE_PREFIX).map_err(|e| e.to_string())?;
        Ok::<_, String>(FilterResponse {
            mode: hot.mode,
            signals: hot.signals.iter().map(|s| s.to_string()).collect(),
            collections: hot.collections.iter().map(|s| s.to_string()).collect(),
            excludes,
        })
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(resp))
}

#[derive(Deserialize)]
pub struct FilterPatch {
    pub mode: Option<FilterMode>,
    pub signals: Option<SetUpdate>,
    pub collections: Option<SetUpdate>,
    pub excludes: Option<SetUpdate>,
}

pub async fn handle_patch_filter(
    State(state): State<Arc<AppState>>,
    Json(patch): Json<FilterPatch>,
) -> Result<StatusCode, (StatusCode, String)> {
    let db = &state.db;

    let filter_ks = db.filter.clone();
    let inner = db.inner.clone();

    let patch_mode = patch.mode;
    let patch_signals = patch.signals;
    let patch_collections = patch.collections;
    let patch_excludes = patch.excludes;

    let new_filter = tokio::task::spawn_blocking(move || {
        let mut batch = inner.batch();

        db::filter::apply_patch(
            &mut batch,
            &filter_ks,
            patch_mode,
            patch_signals,
            patch_collections,
            patch_excludes,
        )
        .map_err(|e| e.to_string())?;

        batch
            .commit()
            .into_diagnostic()
            .map_err(|e| e.to_string())?;

        let new_filter = db::filter::load(&filter_ks).map_err(|e| e.to_string())?;
        Ok::<_, String>(new_filter)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    state.filter.store(Arc::new(new_filter));

    Ok(StatusCode::OK)
}
