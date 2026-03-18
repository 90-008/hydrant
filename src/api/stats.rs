use crate::api::AppState;
use axum::{Json, extract::State, response::Result};
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Serialize)]
pub struct StatsResponse {
    pub counts: BTreeMap<&'static str, u64>,
    pub size: BTreeMap<&'static str, u64>,
}

pub async fn get_stats(State(state): State<Arc<AppState>>) -> Result<Json<StatsResponse>> {
    let db = state.db.clone();

    let mut counts: BTreeMap<&'static str, u64> = futures::future::join_all(
        [
            "repos",
            "pending",
            "resync",
            "records",
            "blocks",
            "error_ratelimited",
            "error_transport",
            "error_generic",
        ]
        .into_iter()
        .map(|name| {
            let db = db.clone();
            async move { (name, db.get_count(name).await) }
        }),
    )
    .await
    .into_iter()
    .collect();
    // this should be accurate since we dont remove events
    // todo: ...unless in ephemeral mode
    counts.insert("events", db.events.approximate_len() as u64);

    let size = tokio::task::spawn_blocking(move || {
        let mut size = BTreeMap::new();
        size.insert("repos", db.repos.disk_space());
        size.insert("records", db.records.disk_space());
        size.insert("blocks", db.blocks.disk_space());
        size.insert("cursors", db.cursors.disk_space());
        size.insert("pending", db.pending.disk_space());
        size.insert("resync", db.resync.disk_space());
        size.insert("resync_buffer", db.resync_buffer.disk_space());
        size.insert("events", db.events.disk_space());
        size.insert("counts", db.counts.disk_space());
        size.insert("filter", db.filter.disk_space());
        size.insert("crawler", db.crawler.disk_space());
        size
    })
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(StatsResponse { counts, size }))
}
