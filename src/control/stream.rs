use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::error;

use crate::db::keys;
use crate::state::AppState;
use std::sync::atomic::Ordering;

#[cfg(feature = "indexer_stream")]
use {
    super::Event,
    crate::db,
    crate::types::{BroadcastEvent, MarshallableEvt, RecordEvt, StoredData, StoredEvent},
    jacquard_common::types::cid::{ATP_CID_HASH, IpldCid},
    jacquard_common::types::nsid::Nsid,
    jacquard_common::types::string::Rkey,
    jacquard_common::{CowStr, IntoStatic, RawData},
    jacquard_repo::DAG_CBOR_CID_CODEC,
    sha2::{Digest, Sha256},
};

#[cfg(feature = "indexer_stream")]
pub(super) fn event_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<Event>,
    cursor: Option<u64>,
) {
    let db = &state.db;
    let mut event_rx = db.event_tx.subscribe();
    let ks = db.events.clone();
    let mut current_id = match cursor {
        Some(c) => c.checked_sub(1),
        None => db.next_event_id.load(Ordering::SeqCst).checked_sub(1),
    };
    let mut needs_catch_up = cursor.is_some();

    loop {
        if needs_catch_up {
            // catch up from db (record events only; ids are sparse due to ephemeral events)
            let start = current_id.map(|id| id.saturating_add(1)).unwrap_or(0);
            for item in ks.range(keys::event_key(start)..) {
                let (k, v) = match item.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => {
                        error!(err = %e, "failed to read event from db");
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
                current_id = Some(id);

                let stored: StoredEvent = match rmp_serde::from_slice(&v) {
                    Ok(e) => e,
                    Err(e) => {
                        error!(err = %e, "failed to deserialize stored event");
                        continue;
                    }
                };

                let Some(out_evt) = stored_to_event(&state, id, stored, None) else {
                    continue;
                };

                if tx.blocking_send(out_evt).is_err() {
                    return; // receiver dropped
                }
            }
            needs_catch_up = false;
        }

        // wait for live events
        match event_rx.blocking_recv() {
            Ok(BroadcastEvent::Persisted(_)) => needs_catch_up = true,
            Ok(BroadcastEvent::LiveRecord(evt)) => {
                let expected = current_id.map(|id| id.saturating_add(1)).unwrap_or(0);
                if needs_catch_up || evt.id != expected {
                    needs_catch_up = true;
                    continue;
                }

                let stored = evt.stored.clone();
                let Some(out_evt) =
                    stored_to_event(&state, evt.id, stored, evt.inline_block.clone())
                else {
                    needs_catch_up = true;
                    continue;
                };
                let out_id = out_evt.id;
                if tx.blocking_send(out_evt).is_err() {
                    return;
                }
                current_id = Some(out_id);
            }
            Ok(BroadcastEvent::Ephemeral(evt)) => {
                let evt_id = evt.id;
                if tx.blocking_send(*evt).is_err() {
                    return;
                }
                current_id = Some(current_id.unwrap_or(0).max(evt_id));
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => needs_catch_up = true,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(feature = "relay")]
pub(super) fn relay_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<bytes::Bytes>,
    cursor: Option<u64>,
) {
    use crate::types::RelayBroadcast;
    use std::sync::atomic::Ordering;

    let mut relay_rx = state.db.relay_broadcast_tx.subscribe();
    let ks = state.db.relay_events.clone();
    let mut current_seq = match cursor {
        Some(c) => c.saturating_sub(1),
        None => ks
            .iter()
            .next_back()
            .and_then(|guard| {
                guard
                    .key()
                    .ok()
                    .and_then(|k| k.as_ref().try_into().ok())
                    .map(u64::from_be_bytes)
            })
            .unwrap_or(0),
    };
    let mut head_seq = current_seq;
    let mut needs_catch_up = true;

    loop {
        if needs_catch_up {
            // catch up from db: send all stored frames from current_seq+1 onward
            for item in ks.range(crate::db::keys::relay_event_key(current_seq + 1)..) {
                let (k, v) = match item.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => {
                        error!(err = %e, "relay stream: failed to read relay_events");
                        break;
                    }
                };
                let seq = match k.as_ref().try_into().map(u64::from_be_bytes) {
                    Ok(s) => s,
                    Err(_) => {
                        error!("relay stream: failed to parse relay event seq");
                        continue;
                    }
                };
                if seq != current_seq + 1 {
                    break;
                }
                if tx.blocking_send(bytes::Bytes::copy_from_slice(&v)).is_err() {
                    return; // subscriber dropped
                }
                current_seq = seq;
                if current_seq >= head_seq {
                    break;
                }
            }
            needs_catch_up = false;
        }

        // wait for live events
        match relay_rx.blocking_recv() {
            Ok(RelayBroadcast::Persisted(seq)) => {
                head_seq = head_seq.max(seq);
                needs_catch_up = current_seq < head_seq;
            }
            Ok(RelayBroadcast::Ephemeral(seq, frame)) => {
                head_seq = head_seq.max(seq);
                if seq != current_seq + 1 {
                    // out-of-order or gap: fall back to db catch-up to preserve ordering.
                    needs_catch_up = true;
                    continue;
                }
                if tx.blocking_send(frame).is_err() {
                    return;
                }
                current_seq = seq;
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => needs_catch_up = true,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(feature = "indexer_stream")]
fn stored_to_event(
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
                    Ok(val) => Some((cid, serde_json::to_value(val).ok()?)),
                    Err(e) => {
                        error!(err = %e, "cant parse block");
                        return None;
                    }
                }
            } else {
                let block = state
                    .db
                    .blocks
                    .get(&keys::block_key(collection.as_str(), &cid.to_bytes()));
                match block {
                    Ok(Some(bytes)) => {
                        match serde_ipld_dagcbor::from_slice::<RawData>(bytes.as_ref()) {
                            Ok(val) => Some((cid, serde_json::to_value(val).ok()?)),
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
                Ok(val) => Some((cid, serde_json::to_value(val).ok()?)),
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
            collection: Nsid::new_cow(collection.clone().into_static())
                .expect("that collection is already validated"),
            rkey: Rkey::new_cow(CowStr::Owned(rkey.to_smolstr()))
                .expect("that rkey is already validated"),
            action: CowStr::Borrowed(action.as_str()),
            record,
            cid,
        }),
        identity: None,
        account: None,
    })
}
