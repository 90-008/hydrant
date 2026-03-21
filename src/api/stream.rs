use crate::api::AppState;
use crate::db::keys;
use crate::types::{BroadcastEvent, MarshallableEvt, RecordEvt, StoredData, StoredEvent};
use axum::Router;
use axum::routing::get;
use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use cid::multihash::Multihash;
use jacquard_common::types::cid::{ATP_CID_HASH, IpldCid};
use jacquard_common::{CowStr, RawData};
use jacquard_repo::DAG_CBOR_CID_CODEC;
use miette::{Context, IntoDiagnostic};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info_span};

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/", get(handle_stream))
}

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
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

    let runtime = tokio::runtime::Handle::current();
    let id = std::time::SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap()
        .as_secs();

    let thread = std::thread::Builder::new()
        .name(format!("stream-handler-{id}"))
        .spawn(move || {
            let _runtime_guard = runtime.enter();
            stream(state, cancel_rx, tx, query, id);
        })
        .expect("failed to spawn stream handler thread");

    while let Some(msg) = rx.recv().await {
        if let Err(e) = socket.send(msg).await {
            error!(err = %e, "failed to send ws message");
            break;
        }
    }

    let _ = cancel_tx.send(());
    if let Err(e) = thread.join() {
        error!(err = ?e, "stream handler thread panicked");
    }
}

fn stream(
    state: Arc<AppState>,
    mut cancel: oneshot::Receiver<()>,
    tx: mpsc::Sender<Message>,
    query: StreamQuery,
    id: u64,
) {
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
    let runtime = tokio::runtime::Handle::current();

    let span = info_span!("stream", id);
    let _entered_span = span.enter();

    loop {
        // 1. catch up from DB
        loop {
            let mut found = false;
            for item in ks.range(keys::event_key(current_id + 1)..) {
                let (k, v) = match item.into_inner() {
                    Ok((k, v)) => (k, v),
                    Err(e) => {
                        error!(err = %e, "failed to read event from db");
                        break;
                    }
                };
                let id = match k
                    .as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("expected event id to be 8 bytes")
                    .map(u64::from_be_bytes)
                {
                    Ok(id) => id,
                    Err(e) => {
                        error!(err = %e, "failed to parse event id");
                        continue;
                    }
                };
                current_id = id;

                let StoredEvent {
                    live,
                    did,
                    rev,
                    collection,
                    rkey,
                    action,
                    data,
                } = match rmp_serde::from_slice(&v) {
                    Ok(e) => e,
                    Err(e) => {
                        error!(err = %e, "failed to deserialize stored event");
                        continue;
                    }
                };

                let _entered = info_span!("record", data = ?data).entered();

                let record = match data {
                    StoredData::Ptr(cid) => {
                        let block = db
                            .blocks
                            .get(&keys::block_key(collection.as_str(), &cid.to_bytes()));
                        match block {
                            Ok(Some(bytes)) => {
                                match serde_ipld_dagcbor::from_slice::<RawData>(&bytes) {
                                    Ok(val) => Some((
                                        cid,
                                        serde_json::to_value(val)
                                            .expect("that cbor raw data is valid json"),
                                    )),
                                    Err(e) => {
                                        error!(err = %e, "cant parse block, must be corrupted?");
                                        return;
                                    }
                                }
                            }
                            Ok(None) => {
                                error!("block not found? this is a bug!!");
                                continue;
                            }
                            Err(e) => {
                                error!(err = %e, "can't get block");
                                crate::db::check_poisoned(&e);
                                return;
                            }
                        }
                    }
                    StoredData::Block(block) => {
                        let digest = Sha256::digest(&block);
                        let hash =
                            Multihash::wrap(ATP_CID_HASH, &digest).expect("that its valid sha256");
                        let cid = IpldCid::new_v1(DAG_CBOR_CID_CODEC, hash);
                        match serde_ipld_dagcbor::from_slice::<RawData>(&block) {
                            Ok(val) => Some((
                                cid,
                                serde_json::to_value(val)
                                    .expect("that cbor raw data is valid json"),
                            )),
                            Err(e) => {
                                error!(err = %e, "cant parse block, must be corrupted?");
                                return;
                            }
                        }
                    }
                    StoredData::Nothing => None,
                };

                let (cid, record) = record
                    .map(|(c, r)| (Some(c), Some(r)))
                    .unwrap_or((None, None));
                let marshallable = MarshallableEvt {
                    id,
                    event_type: "record".into(),
                    record: Some(RecordEvt {
                        live,
                        did: did.to_did(),
                        rev: CowStr::Owned(rev.to_tid().into()),
                        collection,
                        rkey: CowStr::Owned(rkey.to_smolstr().into()),
                        action: CowStr::Borrowed(action.as_str()),
                        record,
                        cid: cid.map(|c| jacquard_common::types::cid::Cid::ipld(c).into()),
                    }),
                    identity: None,
                    account: None,
                };

                let json_str = match serde_json::to_string(&marshallable) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(err = %e, "failed to serialize ws event");
                        continue;
                    }
                };

                if let Err(e) = tx.blocking_send(Message::Text(json_str.into())) {
                    error!(err = %e, "failed to send ws message");
                    return;
                }

                found = true;
            }
            if !found {
                break;
            }
        }

        // 2. wait for live events
        let next_event = runtime.block_on(async {
            tokio::select! {
                res = event_rx.recv() => Some(res),
                _ = &mut cancel => None,
            }
        });

        let Some(next_event) = next_event else {
            break;
        };

        match next_event {
            Ok(BroadcastEvent::Persisted(_)) => {
                // just wake up and run catch-up loop again
            }
            Ok(BroadcastEvent::Ephemeral(evt)) => {
                // send ephemeral event directly
                let json_str = match serde_json::to_string(&evt) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(err = %e, "failed to serialize ws event");
                        continue;
                    }
                };
                if let Err(e) = tx.blocking_send(Message::Text(json_str.into())) {
                    error!(err = %e, "failed to send ws message");
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
}
