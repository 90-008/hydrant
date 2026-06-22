use crate::control::Hydrant;
use axum::Router;
use axum::routing::get;
use axum::{
    extract::{Query, State},
    response::IntoResponse,
};
use axum_tws::{Message, WebSocket, WebSocketUpgrade};
use serde::Deserialize;
use tracing::error;

use super::ws::{WsAction, control_frame_only_limits, run_socket};

pub fn router() -> Router<Hydrant> {
    Router::new().route("/", get(handle_stream))
}

#[derive(Deserialize)]
pub struct StreamQuery {
    pub cursor: Option<u64>,
}

pub async fn handle_stream(
    State(hydrant): State<Hydrant>,
    Query(query): Query<StreamQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.limits(control_frame_only_limits())
        .on_upgrade(move |socket| handle_socket(socket, hydrant, query))
}

async fn handle_socket(socket: WebSocket, hydrant: Hydrant, query: StreamQuery) {
    let send_timeout = hydrant.stream_send_timeout();
    let events = hydrant.subscribe(query.cursor);
    run_socket(
        socket,
        events,
        |item| match item {
            Ok(evt) => match serde_json::to_string(&evt) {
                Ok(json) => WsAction::Send(Message::text(json)),
                Err(e) => {
                    error!(err = %e, "failed to serialize event");
                    WsAction::Skip
                }
            },
            Err(err) => {
                let json = serde_json::json!({
                    "type": "error",
                    "error": err.code(),
                    "message": err.to_string(),
                });
                WsAction::Close(Some(Message::text(json.to_string())))
            }
        },
        send_timeout,
        |timeout_dur| {
            let json = serde_json::json!({
                "type": "error",
                "error": "ConsumerTooSlow",
                "message": format!(
                    "stream socket send blocked for at least {} seconds",
                    timeout_dur.as_secs()
                ),
            });
            Some(Message::text(json.to_string()))
        },
    )
    .await;
}
