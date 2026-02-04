use crate::api::AppState;
use axum::{extract::State, response::Result, Json};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};

#[derive(Serialize)]
pub struct StatsResponse {
    pub counts: HashMap<&'static str, u64>,
}

pub async fn get_stats(State(state): State<Arc<AppState>>) -> Result<Json<StatsResponse>> {
    let db = &state.db;

    let counts = futures::future::join_all(
        ["repos", "records", "blocks", "pending", "resync"]
            .into_iter()
            .map(|name| async move { (name, db.get_count(name).await) }),
    )
    .await
    .into_iter()
    .collect();

    Ok(Json(StatsResponse { counts }))
}
