use crate::api::AppState;
use crate::db::{keys, ser_repo_state, Db};
use crate::types::RepoState;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use jacquard::{types::did::Did, IntoStatic};
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
    let mut to_backfill = Vec::new();

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

            let jacquard_did = Did::new_owned(did.as_str())
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
            to_backfill.push(jacquard_did);
        }
    }

    if added > 0 {
        tokio::task::spawn_blocking(move || batch.commit().map_err(|e| e.to_string()))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
        state.db.update_count_async("repos", added).await;
        state.db.update_count_async("pending", added).await;

        // trigger backfill
        for did in to_backfill {
            let _ = state.backfill_tx.send(did.into_static());
        }
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
