use crate::control::Hydrant;
use axum::Router;
use axum::routing::get;
use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures::StreamExt;
use serde::Deserialize;
use tracing::error;

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
    ws.on_upgrade(move |socket| handle_socket(socket, hydrant, query))
}

async fn handle_socket(mut socket: WebSocket, hydrant: Hydrant, query: StreamQuery) {
    let mut stream = hydrant.subscribe(query.cursor);

    while let Some(evt) = stream.next().await {
        match serde_json::to_string(&evt) {
            Ok(json) => {
                if socket.send(Message::Text(json.into())).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                error!(err = %e, "failed to serialize event");
            }
        }
    }
}
