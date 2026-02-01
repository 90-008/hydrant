use crate::api::AppState;
use crate::db::keys;
use crate::types::{BroadcastEvent, MarshallableEvt, RecordEvt, StoredEvent};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
};
use jacquard_common::types::value::RawData;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

#[derive(Deserialize)]
pub struct StreamQuery {
    pub cursor: Option<u64>,
}

pub async fn handle_stream(
    State(state): State<Arc<AppState>>,
    Query(query): Query<StreamQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, query))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, query: StreamQuery) {
    let (tx, mut rx) = mpsc::channel(500);

    std::thread::Builder::new()
        .name(format!(
            "stream-handler-{}",
            std::time::SystemTime::UNIX_EPOCH
                .elapsed()
                .unwrap()
                .as_secs()
        ))
        .spawn(move || {
            let db = &state.db;
            let mut event_rx = db.event_tx.subscribe();
            let ks = db.events.clone();
            let mut current_id = match query.cursor {
                Some(cursor) => cursor.saturating_sub(1),
                None => {
                    let max_id = db.next_event_id.load(std::sync::atomic::Ordering::SeqCst);
                    max_id.saturating_sub(1)
                }
            };

            loop {
                // 1. catch up from DB
                loop {
                    let mut found = false;
                    for item in ks.range(keys::event_key((current_id + 1) as i64)..) {
                        if let Ok((k, v)) = item.into_inner() {
                            let mut buf = [0u8; 8];
                            buf.copy_from_slice(&k);
                            let id = u64::from_be_bytes(buf);
                            current_id = id;

                            let stored_evt: StoredEvent = match rmp_serde::from_slice(&v) {
                                Ok(e) => e,
                                Err(_) => continue,
                            };

                            let marshallable = match stored_evt {
                                StoredEvent::Record {
                                    live,
                                    did,
                                    rev,
                                    collection,
                                    rkey,
                                    action,
                                    cid,
                                } => {
                                    let mut record_val = None;
                                    if let Some(cid_str) = &cid {
                                        if let Ok(Some(block_bytes)) =
                                            db.blocks.get(keys::block_key(cid_str))
                                        {
                                            if let Ok(raw_data) =
                                                serde_ipld_dagcbor::from_slice::<RawData>(
                                                    &block_bytes,
                                                )
                                            {
                                                record_val = serde_json::to_value(raw_data).ok();
                                            }
                                        }
                                    }

                                    MarshallableEvt {
                                        id,
                                        event_type: "record".into(),
                                        record: Some(RecordEvt {
                                            live,
                                            did,
                                            rev,
                                            collection,
                                            rkey,
                                            action,
                                            record: record_val,
                                            cid,
                                        }),
                                        identity: None,
                                    }
                                }
                                StoredEvent::Identity(identity) => MarshallableEvt {
                                    id,
                                    event_type: "identity".into(),
                                    record: None,
                                    identity: Some(identity),
                                },
                            };

                            let json_str = match serde_json::to_string(&marshallable) {
                                Ok(s) => s,
                                Err(_) => continue,
                            };

                            if tx.blocking_send(Message::Text(json_str.into())).is_err() {
                                return;
                            }
                            found = true;
                        } else {
                            break;
                        }
                    }
                    if !found {
                        break;
                    }
                }

                // 2. wait for live events
                match event_rx.blocking_recv() {
                    Ok(BroadcastEvent::Persisted(_)) => {
                        // just wake up and run catch-up loop again
                    }
                    Ok(BroadcastEvent::Ephemeral(evt)) => {
                        // send ephemeral event directly
                        let json_str = match serde_json::to_string(&evt) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        if tx.blocking_send(Message::Text(json_str.into())).is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // continue to catch up
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        })
        .expect("failed to spawn stream handler thread");

    while let Some(msg) = rx.recv().await {
        if socket.send(msg).await.is_err() {
            break;
        }
    }
}
