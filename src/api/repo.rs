use crate::api::AppState;
use crate::db::{Db, keys, ser_repo_state};
use crate::ops;
use crate::types::{GaugeState, RepoState};
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use jacquard::types::did::Did;
use rand::Rng;
use serde::Deserialize;
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/repo/add", post(handle_repo_add))
        .route("/repo/remove", post(handle_repo_remove))
}

#[derive(Deserialize)]
pub struct RepoAddRequest {
    pub dids: Vec<String>,
}

pub async fn handle_repo_add(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RepoAddRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let db = &state.db;
    let mut batch = db.inner.batch();
    let mut added = 0;

    for did_str in req.dids {
        let did = Did::new_owned(did_str.as_str())
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        let did_key = keys::repo_key(&did);
        if !Db::contains_key(db.repos.clone(), &did_key)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        {
            let repo_state = RepoState::backfilling(rand::rng().next_u64());
            let bytes = ser_repo_state(&repo_state)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

            batch.insert(&db.repos, &did_key, bytes);
            batch.insert(
                &db.pending,
                keys::pending_key(repo_state.index_id),
                &did_key,
            );

            added += 1;
        }
    }

    if added > 0 {
        tokio::task::spawn_blocking(move || batch.commit().map_err(|e| e.to_string()))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

        state.db.update_count_async("repos", added).await;
        for _ in 0..added {
            state
                .db
                .update_gauge_diff_async(&GaugeState::Synced, &GaugeState::Pending)
                .await;
        }

        // trigger backfill worker
        state.notify_backfill();
    }
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct RepoRemoveRequest {
    pub dids: Vec<String>,
}

pub async fn handle_repo_remove(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RepoRemoveRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let db = &state.db;
    let mut batch = db.inner.batch();
    let mut removed_repos = 0;
    let mut old_gauges = Vec::new();

    for did_str in req.dids {
        let did = Did::new_owned(did_str.as_str())
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        let did_key = keys::repo_key(&did);

        if let Some(state_bytes) = Db::get(db.repos.clone(), &did_key)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        {
            let repo_state = crate::db::deser_repo_state(&state_bytes)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

            let was_pending = matches!(repo_state.status, crate::types::RepoStatus::Backfilling);
            // todo: idk
            // let was_resync = matches!(
            //     repo_state.status,
            //     crate::types::RepoStatus::Error(_)
            //         | crate::types::RepoStatus::Deactivated
            //         | crate::types::RepoStatus::Takendown
            //         | crate::types::RepoStatus::Suspended
            // );

            let old_gauge = if was_pending {
                GaugeState::Pending
            } else if let Some(resync_bytes) = Db::get(db.resync.clone(), &did_key)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            {
                let resync_state: crate::types::ResyncState = rmp_serde::from_slice(&resync_bytes)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                let kind = if let crate::types::ResyncState::Error { kind, .. } = resync_state {
                    Some(kind)
                } else {
                    None
                };
                GaugeState::Resync(kind)
            } else {
                GaugeState::Synced
            };

            ops::delete_repo(&mut batch, db, &did, repo_state)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

            old_gauges.push(old_gauge);

            removed_repos -= 1;
        }
    }

    tokio::task::spawn_blocking(move || batch.commit().map_err(|e| e.to_string()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    if removed_repos != 0 {
        state.db.update_count_async("repos", removed_repos).await;
        for old_gauge in old_gauges {
            state
                .db
                .update_gauge_diff_async(&old_gauge, &GaugeState::Synced)
                .await;
        }
    }

    Ok(StatusCode::OK)
}
