use crate::db::keys;
use crate::{api::AppState, db::types::TrimmedDid};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use jacquard::types::ident::AtIdentifier;
use serde::{Deserialize, Serialize};
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

pub fn router() -> axum::Router<Arc<AppState>> {
    axum::Router::new().route("/debug/count", axum::routing::get(handle_debug_count))
}

pub async fn handle_debug_count(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugCountRequest>,
) -> Result<Json<DebugCountResponse>, StatusCode> {
    let did = state
        .resolver
        .resolve_did(&AtIdentifier::new(req.did.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let db = &state.db;
    let ks = db.records.clone();

    // {did_prefix}\x00{collection}\x00
    let mut prefix = Vec::new();
    prefix.extend_from_slice(TrimmedDid::from(&did).as_bytes());
    prefix.push(keys::SEP);
    prefix.extend_from_slice(req.collection.as_bytes());
    prefix.push(keys::SEP);

    let count = tokio::task::spawn_blocking(move || {
        let start_key = prefix.clone();
        let mut end_key = prefix.clone();
        if let Some(msg) = end_key.last_mut() {
            *msg += 1;
        }

        ks.range(start_key..end_key).count()
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(DebugCountResponse { count }))
}
