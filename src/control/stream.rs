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
        Some(c) => c.saturating_sub(1),
        None => db.next_event_id.load(Ordering::SeqCst).saturating_sub(1),
    };

    loop {
        // catch up from db
        loop {
            let mut found = false;
            for item in ks.range(keys::event_key(current_id + 1)..) {
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
                current_id = id;

                let stored: StoredEvent = match rmp_serde::from_slice(&v) {
                    Ok(e) => e,
                    Err(e) => {
                        error!(err = %e, "failed to deserialize stored event");
                        continue;
                    }
                };

                let Some(evt) = stored_to_event(&state, id, stored) else {
                    continue;
                };

                if tx.blocking_send(evt).is_err() {
                    return; // receiver dropped
                }
                found = true;
            }
            if !found {
                break;
            }
        }

        // wait for live events
        match event_rx.blocking_recv() {
            Ok(BroadcastEvent::Persisted(_)) => {} // re-run catch-up
            Ok(BroadcastEvent::Ephemeral(evt)) => {
                if tx.blocking_send(*evt).is_err() {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
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
        None => state
            .db
            .next_relay_seq
            .load(Ordering::Relaxed)
            .saturating_sub(1),
    };

    loop {
        // catch up from db: send all stored frames from current_seq+1 onward
        loop {
            let mut found = false;
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
                current_seq = seq;
                if tx.blocking_send(bytes::Bytes::copy_from_slice(&v)).is_err() {
                    return; // subscriber dropped
                }
                found = true;
            }
            if !found {
                break;
            }
        }

        // wait for live events
        match relay_rx.blocking_recv() {
            Ok(RelayBroadcast::Persisted(_)) => {} // re-run catch-up
            Ok(RelayBroadcast::Ephemeral(frame)) => {
                if tx.blocking_send(frame).is_err() {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {} // re-run catch-up
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(feature = "indexer_stream")]
fn stored_to_event(state: &AppState, id: u64, stored: StoredEvent<'_>) -> Option<Event> {
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
            let block = state
                .db
                .blocks
                .get(&keys::block_key(collection.as_str(), &cid.to_bytes()));
            match block {
                Ok(Some(bytes)) => match serde_ipld_dagcbor::from_slice::<RawData>(&bytes) {
                    Ok(val) => Some((cid, serde_json::to_value(val).ok()?)),
                    Err(e) => {
                        error!(err = %e, "cant parse block");
                        return None;
                    }
                },
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
