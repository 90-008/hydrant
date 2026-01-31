use crate::api::AppState;
use crate::db::keys;
use axum::{
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    Json,
};
use jacquard::types::ident::AtIdentifier;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct DebugCountRequest {
    pub did: String,
    pub collection: String,
}

#[derive(Serialize)]
pub struct DebugCountResponse {
    pub count: usize,
}

pub async fn handle_debug_count(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(req): Query<DebugCountRequest>,
) -> Result<Json<DebugCountResponse>, StatusCode> {
    if !addr.ip().is_loopback() {
        return Err(StatusCode::FORBIDDEN);
    }

    let did = state
        .resolver
        .resolve_did(&AtIdentifier::new(req.did.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let db = &state.db;
    let ks = db.records.clone();

    // {did_prefix}\x00{collection}\x00
    let mut prefix = Vec::new();
    prefix.extend_from_slice(keys::did_prefix(&did).as_bytes());
    prefix.push(keys::SEP);
    prefix.extend_from_slice(req.collection.as_bytes());
    prefix.push(keys::SEP);

    let count = tokio::task::spawn_blocking(move || {
        let mut count = 0;
        let start_key = prefix.clone();
        let mut end_key = prefix.clone();
        if let Some(msg) = end_key.last_mut() {
            *msg += 1;
        }

        for item in ks.range(start_key..end_key) {
            if item.into_inner().is_ok() {
                count += 1;
            }
        }
        count
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(DebugCountResponse { count }))
}
