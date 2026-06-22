use axum::{
    extract::{Query, State},
    response::IntoResponse,
};
use axum_tws::{Message, WebSocket, WebSocketUpgrade};
use serde::Deserialize;
use tracing::error;

use crate::api::ws::{WsAction, control_frame_only_limits, run_socket};
use crate::control::{Hydrant, RelayStreamError};
use crate::ingest::stream::encode_error_frame;

#[derive(Deserialize)]
pub struct SubscribeReposQuery {
    pub cursor: Option<u64>,
}

pub async fn handle(
    State(hydrant): State<Hydrant>,
    Query(query): Query<SubscribeReposQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.limits(control_frame_only_limits())
        .on_upgrade(move |socket| handle_socket(socket, hydrant, query))
}

async fn handle_socket(socket: WebSocket, hydrant: Hydrant, query: SubscribeReposQuery) {
    let send_timeout = hydrant.stream_send_timeout();
    let stream = hydrant.subscribe_repos(query.cursor);
    run_socket(
        socket,
        stream,
        |item| match item {
            Ok(frame) => WsAction::Send(Message::binary(frame)),
            Err(err) => match encode_error_frame(err.code(), Some(&err.to_string())) {
                Ok(frame) => WsAction::Close(Some(Message::binary(frame))),
                Err(e) => {
                    error!(err = %e, "failed to encode relay stream error frame");
                    WsAction::Close(None)
                }
            },
        },
        send_timeout,
        |timeout_dur| {
            let err = RelayStreamError::ConsumerTooSlow {
                reason: format!(
                    "relay stream socket send blocked for at least {} seconds",
                    timeout_dur.as_secs()
                ),
            };
            match encode_error_frame(err.code(), Some(&err.to_string())) {
                Ok(frame) => Some(Message::binary(frame)),
                Err(e) => {
                    error!(err = %e, "failed to encode relay stream error frame");
                    None
                }
            }
        },
    )
    .await;
}
