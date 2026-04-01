use crate::control::Hydrant;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, patch},
};
use serde::{Deserialize, Serialize};

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/ingestion", get(get_ingestion))
        .route("/ingestion", patch(patch_ingestion))
}

#[derive(Serialize)]
pub struct IngestionStatus {
    #[cfg(feature = "indexer")]
    pub crawler: bool,
    pub firehose: bool,
    #[cfg(feature = "indexer")]
    pub backfill: bool,
}

pub async fn get_ingestion(State(hydrant): State<Hydrant>) -> Json<IngestionStatus> {
    Json(IngestionStatus {
        #[cfg(feature = "indexer")]
        crawler: hydrant.crawler.is_enabled(),
        firehose: hydrant.firehose.is_enabled(),
        #[cfg(feature = "indexer")]
        backfill: hydrant.backfill.is_enabled(),
    })
}

#[derive(Deserialize)]
pub struct IngestionPatch {
    #[cfg(feature = "indexer")]
    #[serde(default)]
    pub crawler: Option<bool>,
    #[serde(default)]
    pub firehose: Option<bool>,
    #[cfg(feature = "indexer")]
    #[serde(default)]
    pub backfill: Option<bool>,
}

pub async fn patch_ingestion(
    State(hydrant): State<Hydrant>,
    Json(body): Json<IngestionPatch>,
) -> StatusCode {
    #[cfg(feature = "indexer")]
    if let Some(crawler) = body.crawler {
        if crawler {
            hydrant.crawler.enable();
        } else {
            hydrant.crawler.disable();
        }
    }
    if let Some(firehose) = body.firehose {
        if firehose {
            hydrant.firehose.enable();
        } else {
            hydrant.firehose.disable();
        }
    }
    #[cfg(feature = "indexer")]
    if let Some(backfill) = body.backfill {
        if backfill {
            hydrant.backfill.enable();
        } else {
            hydrant.backfill.disable();
        }
    }
    StatusCode::OK
}
