use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, warn};

use crate::db::keys;
use crate::state::AppState;
use crate::types::{JetstreamBroadcast, StoredJetstreamEvent};

#[cfg(feature = "relay")]
use crate::ingest::stream::{SubscribeReposMessage, decode_frame};
#[cfg(feature = "indexer_stream")]
use crate::types::StoredEvent;

use super::{
    ReplayChunk, STREAM_SEND_RETRY_PAUSE, StreamOptions, StreamTooSlow, clear_replay_blocked,
    note_replay_blocked, replay_chunk_size_for, send_stream_error,
};
use crate::control::{JetstreamFilter, JetstreamStreamError};

#[cfg(feature = "indexer_stream")]
use super::indexer::stored_to_event;

pub(crate) fn jetstream_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<Result<Bytes, JetstreamStreamError>>,
    cursor: Option<i64>,
    filter: JetstreamFilter,
    opts: StreamOptions,
) {
    let mut event_rx = state.db.jetstream_tx.subscribe();
    let head = latest_jetstream_head(&state);

    let replay = cursor
        .filter(|cursor| *cursor <= chrono::Utc::now().timestamp_micros())
        .map(|cursor| cursor.max(0) as u64);

    // for cursor replay: skip live events with id <= head to avoid duplicating
    // events already covered by the replay window.
    let mut current_id = replay.and(head).map(|(_, id)| id);

    if let (Some(cursor_us), Some((target_time_us, _))) = (replay, head) {
        let start_key = keys::jetstream_event_key(cursor_us, 0);
        match send_jetstream_replay_window(
            &state,
            &tx,
            &mut event_rx,
            start_key.to_vec(),
            target_time_us,
            &filter,
            opts,
        ) {
            None => return,
            Some(max_id) => {
                if let Some(max_id) = max_id {
                    current_id = Some(max_id);
                }
            }
        }
    }

    // live tail: read directly from the broadcast channel without a buffered
    // pending queue. this avoids the pending queue overflow that occurs when
    // most events are filtered (e.g. wantedCollections) and send_stream_event
    // is rarely called during replay, leaving pending unbounded.
    loop {
        match event_rx.blocking_recv() {
            Ok(event) => {
                let id = event.id;
                if current_id.is_some_and(|c| id <= c) {
                    continue;
                }
                let out = jetstream_event_to_bytes(&state, event, &filter);
                if let Some(bytes) = out {
                    match send_jetstream_live_event(&tx, &mut event_rx, bytes, opts) {
                        Ok(()) => current_id = Some(id),
                        Err(()) => return,
                    }
                } else {
                    current_id = Some(id);
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                let err = StreamTooSlow::lagged(skipped);
                warn!(%err, "closing slow Jetstream subscriber");
                send_stream_error(&tx, err.into());
                return;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn latest_jetstream_head(state: &AppState) -> Option<(u64, u64)> {
    let guard = state.db.jetstream_events.iter().next_back()?;
    let key = match guard.key() {
        Ok(key) => key,
        Err(e) => {
            error!(err = %e, "failed to read latest Jetstream event key");
            return None;
        }
    };
    match keys::parse_jetstream_event_key(&key) {
        Ok(parsed) => Some(parsed),
        Err(e) => {
            error!(err = %e, "failed to parse latest Jetstream event key");
            None
        }
    }
}

// drain the Jetstream broadcast channel without buffering, to prevent the
// receiver from lagging during long replay windows.
fn drain_jetstream_broadcast(event_rx: &mut broadcast::Receiver<JetstreamBroadcast>) {
    loop {
        match event_rx.try_recv() {
            Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                break;
            }
        }
    }
}

// send one live Jetstream event, retrying until the channel drains or timeout.
// drains the broadcast channel during retries to keep the receiver from lagging.
fn send_jetstream_live_event(
    tx: &mpsc::Sender<Result<Bytes, JetstreamStreamError>>,
    event_rx: &mut broadcast::Receiver<JetstreamBroadcast>,
    bytes: Bytes,
    opts: StreamOptions,
) -> Result<(), ()> {
    let mut item = Ok(bytes);
    let started = Instant::now();
    loop {
        match tx.try_send(item) {
            Ok(()) => return Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return Err(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                item = returned;
                drain_jetstream_broadcast(event_rx);
                if started.elapsed() >= opts.send_timeout {
                    send_stream_error(tx, StreamTooSlow::send_timeout(opts.send_timeout).into());
                    return Err(());
                }
                std::thread::sleep(STREAM_SEND_RETRY_PAUSE);
            }
        }
    }
}

fn send_jetstream_replay_window(
    state: &AppState,
    tx: &mpsc::Sender<Result<Bytes, JetstreamStreamError>>,
    event_rx: &mut broadcast::Receiver<JetstreamBroadcast>,
    mut next_key: Vec<u8>,
    target_time_us: u64,
    filter: &JetstreamFilter,
    opts: StreamOptions,
) -> Option<Option<u64>> {
    let mut max_id_seen: Option<u64> = None;
    let mut replay_blocked_since: Option<Instant> = None;
    loop {
        // drain live events without buffering so the broadcast receiver never
        // lags regardless of how many events arrive during the replay window.
        drain_jetstream_broadcast(event_rx);

        // skip the DB read when the output channel is already saturated.
        let Some(chunk_size) = replay_chunk_size_for(tx, opts.replay_chunk_size) else {
            drain_jetstream_broadcast(event_rx);
            if let Err(err) = note_replay_blocked(&mut replay_blocked_since, opts.send_timeout) {
                send_stream_error(tx, err.into());
                return None;
            }
            std::thread::sleep(STREAM_SEND_RETRY_PAUSE);
            continue;
        };
        clear_replay_blocked(&mut replay_blocked_since);

        let chunk = read_jetstream_replay_chunk(state, &next_key, target_time_us, chunk_size);
        for event in chunk.events {
            let event_id = event.id;
            next_key = keys::jetstream_event_key(event.time_us as u64, event_id.saturating_add(1))
                .to_vec();
            max_id_seen = Some(max_id_seen.unwrap_or(0).max(event_id));
            if let Some(bytes) = jetstream_event_to_bytes(state, event, filter) {
                match send_jetstream_live_event(tx, event_rx, bytes, opts) {
                    Ok(()) => {}
                    Err(()) => return None,
                }
            }
        }

        if chunk.exhausted {
            return Some(max_id_seen);
        }
        if !opts.replay_chunk_pause.is_zero() {
            // emergency compatibility knob; zero by default.
            std::thread::sleep(opts.replay_chunk_pause);
        }
    }
}

fn read_jetstream_replay_chunk(
    state: &AppState,
    start_key: &[u8],
    target_time_us: u64,
    chunk_size: usize,
) -> ReplayChunk<JetstreamBroadcast> {
    let end_key = keys::jetstream_event_key(target_time_us, u64::MAX);
    let mut events = Vec::with_capacity(chunk_size);
    let mut exhausted = false;
    let mut iter = state.db.jetstream_events.range(start_key..=&end_key[..]);

    while events.len() < chunk_size {
        let Some(item) = iter.next() else {
            exhausted = true;
            break;
        };

        let (k, v) = match item.into_inner() {
            Ok(kv) => kv,
            Err(e) => {
                error!(err = %e, "failed to read Jetstream event from db");
                exhausted = true;
                break;
            }
        };
        let (time_us, id) = match keys::parse_jetstream_event_key(&k) {
            Ok(parsed) => parsed,
            Err(e) => {
                error!(err = %e, "failed to parse Jetstream event key");
                continue;
            }
        };

        let event: StoredJetstreamEvent = match rmp_serde::from_slice(&v) {
            Ok(event) => event,
            Err(e) => {
                error!(err = %e, "failed to deserialize Jetstream event");
                continue;
            }
        };
        events.push(JetstreamBroadcast {
            id,
            time_us: time_us as i64,
            event: event.into_static(),
            ephemeral: None,
        });
    }

    ReplayChunk {
        events,
        last_seen_seq: None,
        exhausted,
    }
}

#[derive(serde::Serialize)]
pub(crate) struct JetstreamEvent<'a> {
    pub(crate) did: &'a str,
    pub(crate) time_us: i64,
    #[serde(flatten)]
    pub(crate) payload: JetstreamPayload<'a>,
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub(crate) enum JetstreamPayload<'a> {
    Commit {
        commit: JetstreamCommit<'a>,
    },
    Identity {
        #[serde(borrow)]
        identity: JetstreamIdentity<'a>,
    },
    Account {
        #[serde(borrow)]
        account: JetstreamAccount<'a>,
    },
}

#[derive(serde::Serialize)]
pub(crate) struct JetstreamCommit<'a> {
    pub(crate) rev: &'a str,
    pub(crate) operation: &'a str,
    pub(crate) collection: &'a str,
    pub(crate) rkey: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) record: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cid: Option<String>,
    pub(crate) live: bool,
}

#[derive(serde::Serialize)]
pub(crate) struct JetstreamIdentity<'a> {
    pub(crate) did: String,
    pub(crate) seq: i64,
    pub(crate) time: &'a crate::ingest::stream::Datetime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) handle: Option<String>,
}

#[derive(serde::Serialize)]
pub(crate) struct JetstreamAccount<'a> {
    pub(crate) active: bool,
    pub(crate) did: String,
    pub(crate) seq: i64,
    pub(crate) time: &'a crate::ingest::stream::Datetime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
}

fn jetstream_event_to_bytes(
    state: &AppState,
    event: JetstreamBroadcast,
    filter: &JetstreamFilter,
) -> Option<Bytes> {
    if !filter.wants(&event.event) {
        return None;
    }

    // live tailing: use pre-serialized ephemeral bytes to avoid db reads.
    if let Some(bytes) = event.ephemeral {
        return Some(bytes);
    }

    match &event.event {
        #[cfg(feature = "indexer_stream")]
        StoredJetstreamEvent::Commit { event_id, live, .. } => {
            let bytes = state.db.events.get(keys::event_key(*event_id)).ok()??;
            let stored: StoredEvent = rmp_serde::from_slice(&bytes).ok()?;
            let evt = stored_to_event(state, *event_id, stored, None)?;
            let rec = evt.record?;
            let did_str = rec.did.as_str();

            let json_event = JetstreamEvent {
                did: did_str,
                time_us: event.time_us,
                payload: JetstreamPayload::Commit {
                    commit: JetstreamCommit {
                        rev: rec.rev.as_str(),
                        operation: rec.action.as_str(),
                        collection: rec.collection.as_str(),
                        rkey: rec.rkey.as_str(),
                        record: rec.record.as_ref(),
                        cid: rec.cid.map(|cid| cid.to_string()),
                        live: *live,
                    },
                },
            };
            serde_json::to_vec(&json_event).ok().map(Bytes::from)
        }
        #[cfg(feature = "relay")]
        StoredJetstreamEvent::RelayCommit {
            relay_seq,
            op_index,
            ..
        } => {
            let frame = state
                .db
                .relay_events
                .get(keys::relay_event_key(*relay_seq))
                .ok()??;
            let SubscribeReposMessage::Commit(commit) = decode_frame(frame.as_ref()).ok()? else {
                return None;
            };
            let op = commit.ops.get(*op_index as usize)?;
            let (collection, rkey) = op.path.split_once('/')?;
            let action = op.action.as_str();

            let mut record_owned = None;
            let cid = if matches!(action, "create" | "update") {
                let cid = op.cid.as_ref()?;
                let cid_ipld = cid.to_ipld().ok()?;
                let parsed = tokio::runtime::Handle::current()
                    .block_on(jacquard_repo::car::reader::parse_car_bytes(
                        commit.blocks.as_ref(),
                    ))
                    .ok()?;
                let block = parsed.blocks.get(&cid_ipld)?;
                let val = serde_ipld_dagcbor::from_slice::<jacquard_common::RawData>(block).ok()?;
                record_owned = Some(serde_json::to_value(val).ok()?);
                Some(cid.to_string())
            } else {
                None
            };

            let json_event = JetstreamEvent {
                did: commit.repo.as_str(),
                time_us: event.time_us,
                payload: JetstreamPayload::Commit {
                    commit: JetstreamCommit {
                        rev: commit.rev.as_str(),
                        operation: action,
                        collection,
                        rkey,
                        record: record_owned.as_ref(),
                        cid,
                        live: true,
                    },
                },
            };
            serde_json::to_vec(&json_event).ok().map(Bytes::from)
        }
        #[cfg(feature = "relay")]
        StoredJetstreamEvent::RelayAccount { relay_seq, .. } => {
            let frame = state
                .db
                .relay_events
                .get(keys::relay_event_key(*relay_seq))
                .ok()??;
            let SubscribeReposMessage::Account(account) = decode_frame(frame.as_ref()).ok()? else {
                return None;
            };
            let did_str = account.did.as_str();
            let status = account.status.as_ref().map(|s| {
                use jacquard_common::{IntoStatic, cowstr::ToCowStr};
                s.to_cowstr().into_static().as_str().to_string()
            });

            let json_event = JetstreamEvent {
                did: did_str,
                time_us: event.time_us,
                payload: JetstreamPayload::Account {
                    account: JetstreamAccount {
                        active: account.active,
                        did: did_str.to_string(),
                        seq: account.seq,
                        time: &account.time,
                        status,
                    },
                },
            };
            serde_json::to_vec(&json_event).ok().map(Bytes::from)
        }
        #[cfg(feature = "relay")]
        StoredJetstreamEvent::RelayIdentity { relay_seq, .. } => {
            let frame = state
                .db
                .relay_events
                .get(keys::relay_event_key(*relay_seq))
                .ok()??;
            let SubscribeReposMessage::Identity(identity) = decode_frame(frame.as_ref()).ok()?
            else {
                return None;
            };
            let did_str = identity.did.as_str();
            let handle = identity.handle.as_ref().map(|h| h.as_str().to_string());

            let json_event = JetstreamEvent {
                did: did_str,
                time_us: event.time_us,
                payload: JetstreamPayload::Identity {
                    identity: JetstreamIdentity {
                        did: did_str.to_string(),
                        seq: identity.seq,
                        time: &identity.time,
                        handle,
                    },
                },
            };
            serde_json::to_vec(&json_event).ok().map(Bytes::from)
        }
        StoredJetstreamEvent::Account {
            did,
            active,
            status,
            seq,
            time,
        } => {
            let did_str = did.to_did();
            let status = status.as_ref().map(|s| s.as_str().to_string());

            let json_event = JetstreamEvent {
                did: did_str.as_str(),
                time_us: event.time_us,
                payload: JetstreamPayload::Account {
                    account: JetstreamAccount {
                        active: *active,
                        did: did_str.as_str().to_string(),
                        seq: *seq,
                        time,
                        status,
                    },
                },
            };
            serde_json::to_vec(&json_event).ok().map(Bytes::from)
        }
        StoredJetstreamEvent::Identity {
            did,
            handle,
            seq,
            time,
        } => {
            let did_str = did.to_did();
            let handle = handle.as_ref().map(|h| h.as_str().to_string());

            let json_event = JetstreamEvent {
                did: did_str.as_str(),
                time_us: event.time_us,
                payload: JetstreamPayload::Identity {
                    identity: JetstreamIdentity {
                        did: did_str.as_str().to_string(),
                        seq: *seq,
                        time,
                        handle,
                    },
                },
            };
            serde_json::to_vec(&json_event).ok().map(Bytes::from)
        }
    }
}
