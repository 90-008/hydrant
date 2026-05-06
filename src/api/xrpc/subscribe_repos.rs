use axum::{
    extract::{Query, State},
    response::IntoResponse,
};
use axum_tws::{Message, WebSocket, WebSocketUpgrade};
use futures::StreamExt;
use serde::Deserialize;
use tracing::error;

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
    ws.on_upgrade(move |socket| handle_socket(socket, hydrant, query))
}

async fn handle_socket(mut socket: WebSocket, hydrant: Hydrant, query: SubscribeReposQuery) {
    let send_timeout = hydrant.stream_send_timeout();
    let mut stream = hydrant.subscribe_repos(query.cursor);

    while let Some(item) = stream.next().await {
        let frame = match item {
            Ok(frame) => frame,
            Err(err) => {
                send_error_frame(&mut socket, send_timeout, &err).await;
                break;
            }
        };

        match tokio::time::timeout(send_timeout, socket.send(Message::binary(frame))).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => break,
            Err(_) => {
                let err = RelayStreamError::ConsumerTooSlow {
                    reason: format!(
                        "relay stream socket send blocked for at least {} seconds",
                        send_timeout.as_secs()
                    ),
                };
                send_error_frame(&mut socket, std::time::Duration::from_secs(1), &err).await;
                break;
            }
        }
    }
}

async fn send_error_frame(
    socket: &mut WebSocket,
    timeout: std::time::Duration,
    err: &RelayStreamError,
) {
    match encode_error_frame(err.code(), Some(&err.to_string())) {
        Ok(frame) => {
            let _ = tokio::time::timeout(timeout, socket.send(Message::binary(frame))).await;
        }
        Err(e) => {
            error!(err = %e, "failed to encode relay stream error frame");
        }
    }
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), socket.close()).await;
}
