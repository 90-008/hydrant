use std::sync::Arc;

use crate::state::AppState;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, patch},
};
use serde::{Deserialize, Serialize};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/ingestion", get(get_ingestion))
        .route("/ingestion", patch(patch_ingestion))
}

#[derive(Serialize)]
pub struct IngestionStatus {
    pub crawler: bool,
    pub firehose: bool,
    pub backfill: bool,
}

pub async fn get_ingestion(State(state): State<Arc<AppState>>) -> Json<IngestionStatus> {
    Json(IngestionStatus {
        crawler: *state.crawler_enabled.borrow(),
        firehose: *state.firehose_enabled.borrow(),
        backfill: *state.backfill_enabled.borrow(),
    })
}

#[derive(Deserialize)]
pub struct IngestionPatch {
    #[serde(default)]
    pub crawler: Option<bool>,
    #[serde(default)]
    pub firehose: Option<bool>,
    #[serde(default)]
    pub backfill: Option<bool>,
}

pub async fn patch_ingestion(
    State(state): State<Arc<AppState>>,
    Json(body): Json<IngestionPatch>,
) -> StatusCode {
    if let Some(crawler) = body.crawler {
        state.crawler_enabled.send_replace(crawler);
    }
    if let Some(firehose) = body.firehose {
        state.firehose_enabled.send_replace(firehose);
    }
    if let Some(backfill) = body.backfill {
        state.backfill_enabled.send_replace(backfill);
    }
    StatusCode::OK
}
