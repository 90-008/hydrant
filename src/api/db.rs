use crate::control::Hydrant;
use axum::{Router, extract::State, http::StatusCode, routing::post};

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/db/train", post(handle_train_dict))
        .route("/db/compact", post(handle_compact))
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
