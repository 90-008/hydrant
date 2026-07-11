use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tracing::error;

use crate::db::{self, keys};
use crate::state::AppState;
use crate::types::{BroadcastEvent, MarshallableEvt, RecordEvt, StoredData, StoredEvent};
use jacquard_common::types::cid::{ATP_CID_HASH, IpldCid};
use jacquard_common::types::nsid::Nsid;
use jacquard_common::types::string::Rkey;
use jacquard_common::{CowStr, IntoStatic, RawData};
use jacquard_repo::DAG_CBOR_CID_CODEC;
use sha2::{Digest, Sha256};

use super::{ReplayChunk, StreamBroadcast, StreamOptions, run_ordered_stream, stream_seq_after};
use crate::control::{Event, StreamError};

pub(crate) fn event_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<Result<Event, StreamError>>,
    cursor: Option<u64>,
    opts: StreamOptions,
) {
    let db = &state.db;
    let event_rx = db.stream.event_tx.subscribe();
    let ks = db.stream.events.clone();
    let current_id = match cursor {
        Some(c) => c.checked_sub(1),
        None => db.stream.next_event_id.load(Ordering::SeqCst).checked_sub(1),
    };
    let catch_up_target = cursor
        .and_then(|_| db.stream.next_event_id.load(Ordering::SeqCst).checked_sub(1))
        .filter(|target| stream_seq_after(*target, current_id));
    let replay_state = state.clone();

    run_ordered_stream(
        tx,
        event_rx,
        current_id,
        catch_up_target,
        opts,
        move |current_id, target, chunk_size| {
            read_event_replay_chunk(&replay_state, &ks, current_id, target, chunk_size)
        },
        move |event| broadcast_to_event(&state, event),
    );
}

fn read_event_replay_chunk(
    state: &AppState,
    ks: &fjall::Keyspace,
    current_id: Option<u64>,
    target: u64,
    chunk_size: usize,
) -> ReplayChunk<Event> {
    let start = current_id.map(|id| id.saturating_add(1)).unwrap_or(0);
    if start > target {
        return ReplayChunk {
            events: Vec::new(),
            last_seen_seq: current_id,
            exhausted: true,
        };
    }

    let mut events = Vec::with_capacity(chunk_size);
    let mut last_seen_seq = current_id;
    let mut exhausted = false;
    let max_scanned = chunk_size.saturating_mul(4).max(chunk_size);
    let mut scanned = 0usize;
    let mut iter = ks.range(keys::event_key(start)..=keys::event_key(target));

    while events.len() < chunk_size && scanned < max_scanned {
        let Some(item) = iter.next() else {
            exhausted = true;
            break;
        };
        scanned += 1;

        let (k, v) = match item.into_inner() {
            Ok(kv) => kv,
            Err(e) => {
                error!(err = %e, "failed to read event from db");
                exhausted = true;
                break;
            }
        };

        let id = match k.as_ref().try_into().map(u64::from_be_bytes) {
            Ok(id) => id,
            Err(_) => {
                error!("failed to parse event id");
                continue;
            }
        };
        last_seen_seq = Some(id);

        let stored: StoredEvent = match rmp_serde::from_slice(&v) {
            Ok(e) => e,
            Err(e) => {
                error!(err = %e, "failed to deserialize stored event");
                continue;
            }
        };

        let Some(out_evt) = stored_to_event(state, id, stored, None) else {
            continue;
        };

        events.push(out_evt);
    }

    ReplayChunk {
        events,
        last_seen_seq,
        exhausted,
    }
}

fn broadcast_to_event(state: &AppState, event: BroadcastEvent) -> Option<Event> {
    match event {
        BroadcastEvent::Persisted(_) => None,
        BroadcastEvent::LiveRecord(evt) => {
            let stored = evt.stored.clone();
            stored_to_event(state, evt.id, stored, evt.inline_block.clone())
        }
        BroadcastEvent::Ephemeral(evt) => Some(*evt),
    }
}

pub(crate) fn stored_to_event(
    state: &AppState,
    id: u64,
    stored: StoredEvent<'_>,
    inline_block: Option<bytes::Bytes>,
) -> Option<Event> {
    let StoredEvent {
        live,
        did,
        rev,
        collection,
        rkey,
        action,
        data,
    } = stored;

    let record = match data {
        StoredData::Ptr(cid) => {
            if let Some(bytes) = inline_block {
                match serde_ipld_dagcbor::from_slice::<RawData>(&bytes) {
                    Ok(val) => Some((cid, Arc::from(serde_json::value::to_raw_value(&val).ok()?))),
                    Err(e) => {
                        error!(err = %e, "cant parse block");
                        return None;
                    }
                }
            } else {
                let block = state
                    .db
                    .indexer.blocks
                    .get(keys::block_key(collection.as_str(), &cid.to_bytes()));
                match block {
                    Ok(Some(bytes)) => {
                        match serde_ipld_dagcbor::from_slice::<RawData>(bytes.as_ref()) {
                            Ok(val) => {
                                Some((cid, Arc::from(serde_json::value::to_raw_value(&val).ok()?)))
                            }
                            Err(e) => {
                                error!(err = %e, "cant parse block");
                                return None;
                            }
                        }
                    }
                    Ok(None) => {
                        error!("block not found, this is a bug");
                        return None;
                    }
                    Err(e) => {
                        error!(err = %e, "cant get block");
                        db::check_poisoned(&e);
                        return None;
                    }
                }
            }
        }
        StoredData::Block(block) => {
            let digest = Sha256::digest(&block);
            let hash =
                cid::multihash::Multihash::wrap(ATP_CID_HASH, &digest).expect("valid sha256 hash");
            let cid = IpldCid::new_v1(DAG_CBOR_CID_CODEC, hash);
            match serde_ipld_dagcbor::from_slice::<RawData>(&block) {
                Ok(val) => Some((cid, Arc::from(serde_json::value::to_raw_value(&val).ok()?))),
                Err(e) => {
                    error!(err = %e, "cant parse block");
                    return None;
                }
            }
        }
        StoredData::Nothing => None,
    };

    let (cid, record) = record
        .map(|(c, r)| (Some(c), Some(r)))
        .unwrap_or((None, None));

    Some(MarshallableEvt {
        id,
        kind: crate::types::EventType::Record,
        record: Some(RecordEvt {
            live,
            did: did.to_did(),
            rev: rev.to_tid(),
            collection: match Nsid::new_cow(collection.clone().into_static()) {
                Ok(nsid) => nsid,
                Err(e) => {
                    error!(err = %e, ?collection, "stored event has invalid collection NSID");
                    return None;
                }
            },
            rkey: match Rkey::new_cow(CowStr::Owned(rkey.to_smolstr())) {
                Ok(rk) => rk,
                Err(e) => {
                    error!(err = %e, ?rkey, "stored event has invalid rkey");
                    return None;
                }
            },
            action: CowStr::Borrowed(action.as_str()),
            record,
            cid,
        }),
        identity: None,
        account: None,
    })
}
