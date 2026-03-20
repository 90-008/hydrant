use std::sync::Arc;

use crate::state::AppState;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use futures::FutureExt;
use miette::IntoDiagnostic;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/db/train", post(handle_train_dict))
        .route("/db/compact", post(handle_compact))
}

pub async fn handle_train_dict(
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    state
        .with_ingestion_paused(async || {
            let train = |name: &'static str| {
                let db = state.db.clone();
                tokio::task::spawn_blocking(move || db.train_dict(name))
                    .map(|res| res.into_diagnostic().flatten())
            };
            let repos = train("repos");
            let blocks = train("blocks");
            let events = train("events");

            tokio::try_join!(repos, blocks, events)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok(StatusCode::OK)
        })
        .await
}

pub async fn handle_compact(State(state): State<Arc<AppState>>) -> Result<StatusCode, StatusCode> {
    state
        .with_ingestion_paused(async || {
            state
                .db
                .compact()
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok(StatusCode::OK)
        })
        .await
}
