use crate::control::Hydrant;
use axum::Router;
use axum::routing::get;
use axum::{
    extract::{Query, State},
    response::IntoResponse,
};
use axum_tws::{Message, WebSocket, WebSocketUpgrade};
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
    let send_timeout = hydrant.stream_send_timeout();
    let mut stream = hydrant.subscribe(query.cursor);

    while let Some(item) = stream.next().await {
        let evt = match item {
            Ok(evt) => evt,
            Err(err) => {
                let json = serde_json::json!({
                    "type": "error",
                    "error": err.code(),
                    "message": err.to_string(),
                });
                let _ = tokio::time::timeout(
                    send_timeout,
                    socket.send(Message::text(json.to_string())),
                )
                .await;
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(1), socket.close()).await;
                break;
            }
        };

        match serde_json::to_string(&evt) {
            Ok(json) => {
                match tokio::time::timeout(send_timeout, socket.send(Message::text(json))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => break,
                    Err(_) => {
                        let err = serde_json::json!({
                            "type": "error",
                            "error": "ConsumerTooSlow",
                            "message": format!(
                                "stream socket send blocked for at least {} seconds",
                                send_timeout.as_secs()
                            ),
                        });
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(1),
                            socket.send(Message::text(err.to_string())),
                        )
                        .await;
                        let _ =
                            tokio::time::timeout(std::time::Duration::from_secs(1), socket.close())
                                .await;
                        break;
                    }
                }
            }
            Err(e) => {
                error!(err = %e, "failed to serialize event");
            }
        }
    }
}
