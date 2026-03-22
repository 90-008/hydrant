use crate::control::Hydrant;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, post},
};
use serde::Deserialize;

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/db/train", post(handle_train_dict))
        .route("/db/compact", post(handle_compact))
        .route("/cursors", delete(handle_reset_cursor))
}

pub async fn handle_train_dict(
    State(hydrant): State<Hydrant>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .db
        .train_dicts()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct ResetCursorBody {
    pub key: String,
}

pub async fn handle_reset_cursor(
    State(hydrant): State<Hydrant>,
    Json(body): Json<ResetCursorBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .crawler
        .reset_cursor(&body.key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    hydrant
        .firehose
        .reset_cursor(&body.key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}

pub async fn handle_compact(
    State(hydrant): State<Hydrant>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .db
        .compact()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}
