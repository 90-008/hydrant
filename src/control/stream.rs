use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc};
use tracing::{error, warn};

use crate::db::keys;
use crate::state::AppState;
use std::sync::atomic::Ordering;

#[cfg(feature = "indexer_stream")]
use {
    super::{Event, StreamError},
    crate::config::Config,
    crate::db,
    crate::types::{BroadcastEvent, MarshallableEvt, RecordEvt, StoredData, StoredEvent},
    jacquard_common::types::cid::{ATP_CID_HASH, IpldCid},
    jacquard_common::types::nsid::Nsid,
    jacquard_common::types::string::Rkey,
    jacquard_common::{CowStr, IntoStatic, RawData},
    jacquard_repo::DAG_CBOR_CID_CODEC,
    sha2::{Digest, Sha256},
    tokio::sync::mpsc::error::TrySendError,
};

#[cfg(feature = "indexer_stream")]
const STREAM_SEND_RETRY_PAUSE: Duration = Duration::from_millis(10);

#[cfg(feature = "indexer_stream")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct EventStreamOptions {
    replay_chunk_size: usize,
    replay_chunk_pause: Duration,
    pending_event_limit: usize,
    send_timeout: Duration,
}

#[cfg(feature = "indexer_stream")]
impl EventStreamOptions {
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            replay_chunk_size: config.stream_replay_chunk_size.max(1),
            replay_chunk_pause: config.stream_replay_chunk_pause,
            pending_event_limit: config.stream_pending_event_limit.max(1),
            send_timeout: config.stream_send_timeout,
        }
    }
}

#[cfg(feature = "indexer_stream")]
struct PendingEvents {
    queue: VecDeque<BroadcastEvent>,
    persisted_head: Option<u64>,
    limit: usize,
}

#[cfg(feature = "indexer_stream")]
impl PendingEvents {
    fn new(limit: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            persisted_head: None,
            limit,
        }
    }

    fn push(&mut self, event: BroadcastEvent) -> Result<(), StreamError> {
        match event {
            BroadcastEvent::Persisted(id) => {
                self.persisted_head = Some(self.persisted_head.unwrap_or(0).max(id));
                Ok(())
            }
            event => {
                if self.queue.len() >= self.limit {
                    return Err(StreamError::ConsumerTooSlow {
                        reason: format!(
                            "pending stream event buffer exceeded {} events",
                            self.limit
                        ),
                    });
                }
                self.queue.push_back(event);
                Ok(())
            }
        }
    }

    fn next_event_id(&self) -> Option<u64> {
        self.queue.front().map(broadcast_event_id)
    }

    fn pop_front(&mut self) -> Option<BroadcastEvent> {
        self.queue.pop_front()
    }

    fn take_persisted_after(&mut self, current_id: Option<u64>) -> Option<u64> {
        let head = self.persisted_head.take()?;
        is_after_current(head, current_id).then_some(head)
    }
}

#[cfg(feature = "indexer_stream")]
pub(super) fn event_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<Result<Event, StreamError>>,
    cursor: Option<u64>,
    opts: EventStreamOptions,
) {
    let db = &state.db;
    let mut event_rx = db.event_tx.subscribe();
    let ks = db.events.clone();
    let mut current_id = match cursor {
        Some(c) => c.checked_sub(1),
        None => db.next_event_id.load(Ordering::SeqCst).checked_sub(1),
    };
    let mut catch_up_target = cursor
        .and_then(|_| db.next_event_id.load(Ordering::SeqCst).checked_sub(1))
        .filter(|target| is_after_current(*target, current_id));
    let mut replay_gap_target = None;
    let mut pending = PendingEvents::new(opts.pending_event_limit);

    loop {
        if let Err(err) = drain_pending_broadcasts(&mut event_rx, &mut pending) {
            send_stream_error(&tx, err);
            return;
        }

        drop_delivered_pending(&mut pending, current_id);

        if let Some(target) = replay_gap_target {
            advance_replay_gap(&mut current_id, target, &pending);
            if current_id.is_some_and(|id| id >= target) {
                replay_gap_target = None;
            }
        }

        if catch_up_target.is_none() {
            catch_up_target = pending.take_persisted_after(current_id);
        }

        let pending_next_id = pending.next_event_id();
        let pending_is_ready = pending_next_id.is_some_and(|id| id == next_expected_id(current_id));

        if let Some(target) = catch_up_target.filter(|_| !pending_is_ready) {
            let effective_target = pending_next_id
                .and_then(|id| id.checked_sub(1))
                .map(|before_pending| before_pending.min(target))
                .unwrap_or(target);
            let chunk = read_replay_chunk(
                &state,
                &ks,
                current_id,
                effective_target,
                opts.replay_chunk_size,
            );
            current_id = chunk.last_seen_id.or(current_id);

            for event in chunk.events {
                match send_stream_event(&tx, event, &mut event_rx, &mut pending, opts) {
                    Ok(SendOutcome::Sent) => {}
                    Ok(SendOutcome::ReceiverDropped) => return,
                    Err(err) => {
                        send_stream_error(&tx, err);
                        return;
                    }
                }
            }

            if chunk.exhausted || current_id.is_some_and(|id| id >= effective_target) {
                if effective_target == target {
                    catch_up_target = None;
                }
                replay_gap_target = Some(effective_target);
            } else if !opts.replay_chunk_pause.is_zero() {
                std::thread::sleep(opts.replay_chunk_pause);
            }

            continue;
        }

        if let Some(event) = pending.pop_front() {
            let id = broadcast_event_id(&event);
            if !is_after_current(id, current_id) {
                continue;
            }

            let expected = next_expected_id(current_id);
            if id != expected {
                catch_up_target = id.checked_sub(1);
                pending.queue.push_front(event);
                continue;
            }

            let Some(out_event) = broadcast_to_event(&state, event) else {
                catch_up_target = Some(id);
                continue;
            };

            match send_stream_event(&tx, out_event, &mut event_rx, &mut pending, opts) {
                Ok(SendOutcome::Sent) => {
                    current_id = Some(id);
                }
                Ok(SendOutcome::ReceiverDropped) => return,
                Err(err) => {
                    send_stream_error(&tx, err);
                    return;
                }
            }
            continue;
        }

        match event_rx.blocking_recv() {
            Ok(BroadcastEvent::Persisted(id)) => {
                if is_after_current(id, current_id) {
                    catch_up_target = Some(id);
                }
            }
            Ok(event) => {
                if let Err(err) = pending.push(event) {
                    send_stream_error(&tx, err);
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                let err = StreamError::ConsumerTooSlow {
                    reason: format!("subscriber lagged past {skipped} broadcast events"),
                };
                warn!(%err, "closing slow stream subscriber");
                send_stream_error(&tx, err);
                return;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(feature = "indexer_stream")]
struct ReplayChunk {
    events: Vec<Event>,
    last_seen_id: Option<u64>,
    exhausted: bool,
}

#[cfg(feature = "indexer_stream")]
fn read_replay_chunk(
    state: &AppState,
    ks: &fjall::Keyspace,
    current_id: Option<u64>,
    target: u64,
    chunk_size: usize,
) -> ReplayChunk {
    let start = current_id.map(|id| id.saturating_add(1)).unwrap_or(0);
    if start > target {
        return ReplayChunk {
            events: Vec::new(),
            last_seen_id: current_id,
            exhausted: true,
        };
    }

    let mut events = Vec::with_capacity(chunk_size);
    let mut last_seen_id = current_id;
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
        last_seen_id = Some(id);

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
        last_seen_id,
        exhausted,
    }
}

#[cfg(feature = "indexer_stream")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendOutcome {
    Sent,
    ReceiverDropped,
}

#[cfg(feature = "indexer_stream")]
fn send_stream_event(
    tx: &mpsc::Sender<Result<Event, StreamError>>,
    event: Event,
    event_rx: &mut broadcast::Receiver<BroadcastEvent>,
    pending: &mut PendingEvents,
    opts: EventStreamOptions,
) -> Result<SendOutcome, StreamError> {
    let mut item = Ok(event);
    let started = Instant::now();

    loop {
        match tx.try_send(item) {
            Ok(()) => return Ok(SendOutcome::Sent),
            Err(TrySendError::Closed(_)) => return Ok(SendOutcome::ReceiverDropped),
            Err(TrySendError::Full(returned)) => {
                item = returned;
                drain_pending_broadcasts(event_rx, pending)?;
                if started.elapsed() >= opts.send_timeout {
                    return Err(StreamError::ConsumerTooSlow {
                        reason: format!(
                            "stream delivery blocked for at least {} seconds",
                            opts.send_timeout.as_secs()
                        ),
                    });
                }
                std::thread::sleep(STREAM_SEND_RETRY_PAUSE);
            }
        }
    }
}

#[cfg(feature = "indexer_stream")]
fn send_stream_error(tx: &mpsc::Sender<Result<Event, StreamError>>, err: StreamError) {
    warn!(%err, "closing stream subscriber");
    let _ = tx.blocking_send(Err(err));
}

#[cfg(feature = "indexer_stream")]
fn drain_pending_broadcasts(
    event_rx: &mut broadcast::Receiver<BroadcastEvent>,
    pending: &mut PendingEvents,
) -> Result<(), StreamError> {
    loop {
        match event_rx.try_recv() {
            Ok(event) => pending.push(event)?,
            Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
            Err(broadcast::error::TryRecvError::Closed) => return Ok(()),
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                return Err(StreamError::ConsumerTooSlow {
                    reason: format!("subscriber lagged past {skipped} broadcast events"),
                });
            }
        }
    }
}

#[cfg(feature = "indexer_stream")]
fn drop_delivered_pending(pending: &mut PendingEvents, current_id: Option<u64>) {
    while pending
        .next_event_id()
        .is_some_and(|id| !is_after_current(id, current_id))
    {
        pending.pop_front();
    }
}

#[cfg(feature = "indexer_stream")]
fn advance_replay_gap(current_id: &mut Option<u64>, target: u64, pending: &PendingEvents) {
    let next_pending_id = pending.next_event_id().filter(|id| *id <= target);
    let advance_to = next_pending_id
        .and_then(|id| id.checked_sub(1))
        .unwrap_or(target);

    if is_after_current(advance_to, *current_id) {
        *current_id = Some(advance_to);
    }
}

#[cfg(feature = "indexer_stream")]
fn broadcast_event_id(event: &BroadcastEvent) -> u64 {
    match event {
        BroadcastEvent::Persisted(id) => *id,
        BroadcastEvent::LiveRecord(evt) => evt.id,
        BroadcastEvent::Ephemeral(evt) => evt.id,
    }
}

#[cfg(feature = "indexer_stream")]
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

#[cfg(feature = "indexer_stream")]
fn is_after_current(id: u64, current_id: Option<u64>) -> bool {
    current_id.is_none_or(|current| id > current)
}

#[cfg(feature = "indexer_stream")]
fn next_expected_id(current_id: Option<u64>) -> u64 {
    current_id.map(|id| id.saturating_add(1)).unwrap_or(0)
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
