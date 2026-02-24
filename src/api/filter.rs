use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, patch},
};
use miette::IntoDiagnostic;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::db::{self, keys, ser_repo_state};
use crate::filter::{DID_PREFIX, EXCLUDE_PREFIX, FilterConfig, FilterMode, SetUpdate};
use crate::types::{GaugeState, RepoState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/filter", get(handle_get_filter))
        .route("/filter", patch(handle_patch_filter))
}

#[derive(Serialize)]
pub struct FilterResponse {
    pub mode: FilterMode,
    pub dids: Vec<String>,
    pub signals: Vec<String>,
    pub collections: Vec<String>,
    pub excludes: Vec<String>,
}

pub async fn handle_get_filter(
    State(state): State<Arc<AppState>>,
) -> Result<Json<FilterResponse>, (StatusCode, String)> {
    let filter_ks = state.db.filter.clone();
    let resp = tokio::task::spawn_blocking(move || {
        let hot = FilterConfig::load(&filter_ks).map_err(|e| e.to_string())?;
        let dids = db::filter::read_set(&filter_ks, DID_PREFIX).map_err(|e| e.to_string())?;
        let excludes =
            db::filter::read_set(&filter_ks, EXCLUDE_PREFIX).map_err(|e| e.to_string())?;
        Ok::<_, String>(FilterResponse {
            mode: hot.mode,
            dids,
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
    pub dids: Option<SetUpdate>,
    pub signals: Option<SetUpdate>,
    pub collections: Option<SetUpdate>,
    pub excludes: Option<SetUpdate>,
}

pub async fn handle_patch_filter(
    State(state): State<Arc<AppState>>,
    Json(patch): Json<FilterPatch>,
) -> Result<StatusCode, (StatusCode, String)> {
    let db = &state.db;

    let new_dids: Option<Vec<String>> = match &patch.dids {
        Some(SetUpdate::Set(dids)) => Some(dids.clone()),
        Some(SetUpdate::Patch(map)) => {
            let added: Vec<String> = map
                .iter()
                .filter(|(_, add)| **add)
                .map(|(d, _)| d.clone())
                .collect();
            (!added.is_empty()).then_some(added)
        }
        None => None,
    };

    let filter_ks = db.filter.clone();
    let repos_ks = db.repos.clone();
    let pending_ks = db.pending.clone();
    let inner = db.inner.clone();

    let patch_mode = patch.mode;
    let patch_dids = patch.dids;
    let patch_signals = patch.signals;
    let patch_collections = patch.collections;
    let patch_excludes = patch.excludes;

    let (new_repo_count, new_filter) = tokio::task::spawn_blocking(move || {
        let mut batch = inner.batch();

        db::filter::apply_patch(
            &mut batch,
            &filter_ks,
            patch_mode,
            patch_dids,
            patch_signals,
            patch_collections,
            patch_excludes,
        )
        .map_err(|e| e.to_string())?;

        let mut added = 0i64;

        if let Some(dids) = new_dids {
            for did_str in &dids {
                let did =
                    jacquard::types::did::Did::new_owned(did_str).map_err(|e| e.to_string())?;
                let did_key = keys::repo_key(&did);
                let exists = repos_ks
                    .contains_key(&did_key)
                    .into_diagnostic()
                    .map_err(|e| e.to_string())?;
                if !exists {
                    let repo_state = RepoState::backfilling(rand::rng().next_u64());
                    let bytes = ser_repo_state(&repo_state).map_err(|e| e.to_string())?;
                    batch.insert(&repos_ks, &did_key, bytes);
                    batch.insert(
                        &pending_ks,
                        keys::pending_key(repo_state.index_id),
                        &did_key,
                    );
                    added += 1;
                }
            }
        }

        batch
            .commit()
            .into_diagnostic()
            .map_err(|e| e.to_string())?;

        let new_filter = db::filter::load(&filter_ks).map_err(|e| e.to_string())?;
        Ok::<_, String>((added, new_filter))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    state.filter.store(Arc::new(new_filter));

    if new_repo_count > 0 {
        state.db.update_count_async("repos", new_repo_count).await;
        for _ in 0..new_repo_count {
            state
                .db
                .update_gauge_diff_async(&GaugeState::Synced, &GaugeState::Pending)
                .await;
        }
        state.notify_backfill();
    }

    Ok(StatusCode::OK)
}
