use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use fjall::OwnedWriteBatch;
use miette::{IntoDiagnostic, Result};
use serde_json::value::RawValue;

use crate::db::{Db, keys};
use crate::types::{JetstreamBroadcast, StoredJetstreamEvent};

/// pre-built commit data for live jetstream tailing.
/// `record` holds pre-serialized json so it can be embedded raw into each op's
/// event bytes without re-encoding, and arc-shared across ops that reference the
/// same cid so the serialization happens exactly once per unique block.
#[derive(Clone)]
pub(crate) struct JetstreamEphemeral {
    pub did: String,
    pub rev: String,
    pub operation: String,
    pub collection: String,
    pub rkey: String,
    pub record: Option<Arc<RawValue>>,
    pub cid: Option<String>,
    pub live: bool,
}

pub(crate) fn stage_event(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    event: StoredJetstreamEvent<'_>,
    ephemeral: Option<JetstreamEphemeral>,
) -> Result<JetstreamBroadcast> {
    let id = db.jetstream.next_id.fetch_add(1, Ordering::SeqCst);
    let time_us = next_time_us(db);
    let ephemeral = ephemeral.and_then(|data| {
        let json_event = crate::control::stream::JetstreamEvent {
            did: &data.did,
            time_us,
            payload: crate::control::stream::JetstreamPayload::Commit {
                commit: crate::control::stream::JetstreamCommit {
                    rev: &data.rev,
                    operation: &data.operation,
                    collection: &data.collection,
                    rkey: &data.rkey,
                    record: data.record.as_deref(),
                    cid: data.cid,
                    live: data.live,
                },
            },
        };
        serde_json::to_vec(&json_event).ok().map(Bytes::from)
    });
    let event = event.into_static();
    let bytes = rmp_serde::to_vec(&event).into_diagnostic()?;
    batch.insert(
        &db.jetstream.events,
        keys::jetstream_event_key(time_us as u64, id),
        bytes,
    );
    Ok(JetstreamBroadcast {
        id,
        time_us,
        event,
        ephemeral,
    })
}

fn next_time_us(db: &Db) -> i64 {
    loop {
        let last = db.jetstream.last_time_us.load(Ordering::SeqCst);
        let now = chrono::Utc::now().timestamp_micros();
        let next = now.max(last.saturating_add(1));
        if db
            .jetstream
            .last_time_us
            .compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return next;
        }
    }
}

#[cfg(all(feature = "indexer_stream", feature = "jetstream"))]
pub(crate) fn build_ephemeral_from_stored(
    did: &str,
    rev: &str,
    operation: &str,
    collection: &str,
    rkey: &str,
    data: &crate::types::StoredData,
    inline_block: Option<&Bytes>,
    live: bool,
) -> Option<JetstreamEphemeral> {
    use crate::types::StoredData;
    use jacquard_common::types::cid::{ATP_CID_HASH, IpldCid};
    use jacquard_repo::DAG_CBOR_CID_CODEC;
    use sha2::{Digest, Sha256};

    let (cid, record) = match data {
        StoredData::Ptr(cid) => {
            if let Some(bytes) = inline_block {
                match serde_ipld_dagcbor::from_slice::<jacquard_common::RawData>(bytes) {
                    Ok(val) => (
                        Some(cid.to_string()),
                        Some(Arc::from(serde_json::value::to_raw_value(&val).ok()?)),
                    ),
                    Err(_) => return None,
                }
            } else {
                return None;
            }
        }
        StoredData::Block(block) => {
            let digest = Sha256::digest(block);
            let hash = cid::multihash::Multihash::wrap(ATP_CID_HASH, &digest).ok()?;
            let cid = IpldCid::new_v1(DAG_CBOR_CID_CODEC, hash);
            match serde_ipld_dagcbor::from_slice::<jacquard_common::RawData>(block) {
                Ok(val) => (
                    Some(cid.to_string()),
                    Some(Arc::from(serde_json::value::to_raw_value(&val).ok()?)),
                ),
                Err(_) => return None,
            }
        }
        StoredData::Nothing => (None, None),
    };

    Some(JetstreamEphemeral {
        did: did.to_string(),
        rev: rev.to_string(),
        operation: operation.to_string(),
        collection: collection.to_string(),
        rkey: rkey.to_string(),
        record,
        cid,
        live,
    })
}
