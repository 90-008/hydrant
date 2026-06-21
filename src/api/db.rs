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
    if let Err(e) = hydrant.db.compact().await {
        let err_msg = e.to_string();
        if err_msg.contains("already in progress") {
            return Err((StatusCode::CONFLICT, err_msg));
        }
        return Err((StatusCode::INTERNAL_SERVER_ERROR, err_msg));
    }
    Ok(StatusCode::OK)
}
