use crate::api::AppState;
use crate::db::{Db, keys, ser_repo_state};
use crate::types::RepoState;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use jacquard::types::did::Did;
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
            let repo_state = RepoState::backfilling(&did);
            let bytes = ser_repo_state(&repo_state)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

            batch.insert(&db.repos, &did_key, bytes);
            batch.insert(&db.pending, &did_key, Vec::new());

            added += 1;
        }
    }

    if added > 0 {
        tokio::task::spawn_blocking(move || batch.commit().map_err(|e| e.to_string()))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

        state.db.update_count_async("repos", added).await;
        state.db.update_count_async("pending", added).await;

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
    let mut removed = 0;

    for did_str in req.dids {
        let did = Did::new_owned(did_str.as_str())
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        let did_key = keys::repo_key(&did);
        if Db::contains_key(db.repos.clone(), &did_key)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        {
            batch.remove(&db.repos, &did_key);
            batch.remove(&db.pending, &did_key);
            batch.remove(&db.resync, &did_key);
            removed -= 1;
        }
    }

    tokio::task::spawn_blocking(move || batch.commit().map_err(|e| e.to_string()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    state.db.update_count_async("repos", removed).await;
    state.db.update_count_async("pending", removed).await;

    Ok(StatusCode::OK)
}
