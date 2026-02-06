use crate::api::AppState;
use axum::{Json, extract::State, response::Result};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};

#[derive(Serialize)]
pub struct StatsResponse {
    pub counts: HashMap<&'static str, u64>,
}

pub async fn get_stats(State(state): State<Arc<AppState>>) -> Result<Json<StatsResponse>> {
    let db = &state.db;

    let mut counts: HashMap<&'static str, u64> = futures::future::join_all(
        ["repos", "records", "blocks", "pending", "resync"]
            .into_iter()
            .map(|name| async move { (name, db.get_count(name).await) }),
    )
    .await
    .into_iter()
    .collect();
    // this should be accurate since we dont remove events
    counts.insert("events", db.events.approximate_len() as u64);

    Ok(Json(StatsResponse { counts }))
}
