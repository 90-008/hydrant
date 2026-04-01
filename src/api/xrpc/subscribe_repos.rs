use axum::{
    extract::{Query, State},
    response::IntoResponse,
};
use axum_tws::{Message, WebSocket, WebSocketUpgrade};
use futures::StreamExt;
use serde::Deserialize;

use crate::control::Hydrant;

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
    let mut stream = hydrant.subscribe_repos(query.cursor);

    while let Some(frame) = stream.next().await {
        if socket.send(Message::binary(frame)).await.is_err() {
            break;
        }
    }
}
