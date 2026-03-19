use std::sync::Arc;

use crate::state::AppState;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use futures::FutureExt;
use miette::IntoDiagnostic;

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/train_dict", post(handle_train_dict))
}

pub async fn handle_train_dict(
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    let train = |name: &'static str| {
        let db = state.db.clone();
        tokio::task::spawn_blocking(move || db.train_dict(name))
            .map(|res| res.into_diagnostic().flatten())
    };
    let repos = train("repos");
    let blocks = train("blocks");
    let events = train("events");

    tokio::try_join!(repos, blocks, events).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}
