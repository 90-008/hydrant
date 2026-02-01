use crate::api::AppState;
use crate::db::keys;
use axum::{extract::State, response::Result, Json};
use serde::Serialize;
use smol_str::SmolStr;
use std::sync::Arc;

#[derive(Serialize)]
pub struct StatsResponse {
    pub keyspace_stats: Vec<KeyspaceStat>,
}

#[derive(Serialize)]
pub struct KeyspaceStat {
    pub name: SmolStr,
    pub count: i64,
}

pub async fn get_stats(State(state): State<Arc<AppState>>) -> Result<Json<StatsResponse>> {
    let db = &state.db;

    let stats = futures::future::try_join_all(
        [
            "repos", "records", "blocks", "events", "buffer", "pending", "resync",
        ]
        .into_iter()
        .map(|name| async move {
            Ok::<_, miette::Report>(KeyspaceStat {
                name: name.into(),
                count: db.get_count(keys::count_keyspace_key(name)).await?,
            })
        }),
    )
    .await
    .map_err(|e| e.to_string())?;

    Ok(Json(StatsResponse {
        keyspace_stats: stats,
    }))
}
