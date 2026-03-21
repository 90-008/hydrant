use crate::control::Hydrant;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse, response::Response};

pub async fn get_stats(State(hydrant): State<Hydrant>) -> Response {
    match hydrant.stats().await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
