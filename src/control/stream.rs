use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, warn};

use crate::config::Config;
use crate::db::keys;
use crate::state::AppState;
use std::sync::atomic::Ordering;

#[cfg(feature = "indexer_stream")]
use {
    super::{Event, StreamError},
    crate::db,
    crate::types::{BroadcastEvent, MarshallableEvt, RecordEvt, StoredData, StoredEvent},
    jacquard_common::types::cid::{ATP_CID_HASH, IpldCid},
    jacquard_common::types::nsid::Nsid,
    jacquard_common::types::string::Rkey,
    jacquard_common::{CowStr, IntoStatic, RawData},
    jacquard_repo::DAG_CBOR_CID_CODEC,
    sha2::{Digest, Sha256},
};

#[cfg(feature = "relay")]
use super::RelayStreamError;

#[cfg(feature = "jetstream")]
use {
    super::{JetstreamStreamError, JetstreamSubscriberOptions},
    crate::types::{JetstreamBroadcast, StoredJetstreamEvent},
    bytes::Bytes,
};

#[cfg(all(feature = "relay", feature = "jetstream"))]
use crate::ingest::stream::{SubscribeReposMessage, decode_frame};

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
const STREAM_SEND_RETRY_PAUSE: Duration = Duration::from_millis(10);

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamOptions {
    replay_chunk_size: usize,
    replay_chunk_pause: Duration,
    pending_event_limit: usize,
    send_timeout: Duration,
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
impl StreamOptions {
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            replay_chunk_size: config.stream_replay_chunk_size.max(1),
            replay_chunk_pause: config.stream_replay_chunk_pause,
            pending_event_limit: config.stream_pending_event_limit.max(1),
            send_timeout: config.stream_send_timeout,
        }
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
#[derive(Debug, Clone)]
struct StreamTooSlow {
    reason: String,
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
impl StreamTooSlow {
    fn pending_limit(limit: usize) -> Self {
        Self {
            reason: format!("pending stream event buffer exceeded {limit} events"),
        }
    }

    fn lagged(skipped: u64) -> Self {
        Self {
            reason: format!("subscriber lagged past {skipped} broadcast events"),
        }
    }

    fn send_timeout(timeout: Duration) -> Self {
        Self {
            reason: format!(
                "stream delivery blocked for at least {} seconds",
                timeout.as_secs()
            ),
        }
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
impl fmt::Display for StreamTooSlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream consumer too slow: {}", self.reason)
    }
}

#[cfg(feature = "indexer_stream")]
impl From<StreamTooSlow> for StreamError {
    fn from(err: StreamTooSlow) -> Self {
        Self::ConsumerTooSlow { reason: err.reason }
    }
}

#[cfg(feature = "relay")]
impl From<StreamTooSlow> for RelayStreamError {
    fn from(err: StreamTooSlow) -> Self {
        Self::ConsumerTooSlow { reason: err.reason }
    }
}

#[cfg(feature = "jetstream")]
impl From<StreamTooSlow> for JetstreamStreamError {
    fn from(err: StreamTooSlow) -> Self {
        Self::ConsumerTooSlow { reason: err.reason }
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
trait StreamBroadcast {
    fn sequence(&self) -> u64;
    fn is_persisted_marker(&self) -> bool;
}

#[cfg(feature = "indexer_stream")]
impl StreamBroadcast for BroadcastEvent {
    fn sequence(&self) -> u64 {
        match self {
            BroadcastEvent::Persisted(id) => *id,
            BroadcastEvent::LiveRecord(evt) => evt.id,
            BroadcastEvent::Ephemeral(evt) => evt.id,
        }
    }

    fn is_persisted_marker(&self) -> bool {
        matches!(self, BroadcastEvent::Persisted(_))
    }
}

#[cfg(feature = "relay")]
impl StreamBroadcast for crate::types::RelayBroadcast {
    fn sequence(&self) -> u64 {
        match self {
            Self::Persisted(seq) | Self::Ephemeral(seq, _) => *seq,
        }
    }

    fn is_persisted_marker(&self) -> bool {
        matches!(self, Self::Persisted(_))
    }
}

#[cfg(feature = "jetstream")]
impl StreamBroadcast for JetstreamBroadcast {
    fn sequence(&self) -> u64 {
        self.id
    }

    fn is_persisted_marker(&self) -> bool {
        false
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
struct PendingLiveEvents<T> {
    queue: VecDeque<T>,
    persisted_head: Option<u64>,
    limit: usize,
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
impl<T> PendingLiveEvents<T>
where
    T: StreamBroadcast,
{
    fn new(limit: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            persisted_head: None,
            limit,
        }
    }

    fn push(&mut self, event: T) -> Result<(), StreamTooSlow> {
        if event.is_persisted_marker() {
            let seq = event.sequence();
            self.persisted_head = Some(self.persisted_head.unwrap_or(0).max(seq));
            return Ok(());
        }

        if self.queue.len() >= self.limit {
            return Err(StreamTooSlow::pending_limit(self.limit));
        }
        self.queue.push_back(event);
        Ok(())
    }

    fn next_sequence(&self) -> Option<u64> {
        self.queue.front().map(StreamBroadcast::sequence)
    }

    fn pop_front(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    fn push_front(&mut self, event: T) {
        self.queue.push_front(event);
    }

    fn take_persisted_after(&mut self, current_id: Option<u64>) -> Option<u64> {
        let head = self.persisted_head.take()?;
        stream_seq_after(head, current_id).then_some(head)
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
struct ReplayChunk<T> {
    events: Vec<T>,
    last_seen_seq: Option<u64>,
    exhausted: bool,
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendOutcome {
    Sent,
    ReceiverDropped,
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn run_ordered_stream<B, O, E>(
    tx: mpsc::Sender<Result<O, E>>,
    mut event_rx: broadcast::Receiver<B>,
    mut current_seq: Option<u64>,
    mut catch_up_target: Option<u64>,
    opts: StreamOptions,
    mut read_replay_chunk: impl FnMut(Option<u64>, u64, usize) -> ReplayChunk<O>,
    mut live_to_output: impl FnMut(B) -> Option<O>,
) where
    B: StreamBroadcast + Clone,
    E: From<StreamTooSlow> + fmt::Display,
{
    let mut replay_gap_target = None;
    let mut pending = PendingLiveEvents::new(opts.pending_event_limit);

    loop {
        if let Err(err) = drain_pending_broadcasts(&mut event_rx, &mut pending) {
            send_stream_error(&tx, err.into());
            return;
        }

        drop_delivered_pending(&mut pending, current_seq);

        if let Some(target) = replay_gap_target {
            advance_replay_gap(&mut current_seq, target, &pending);
            if current_seq.is_some_and(|seq| seq >= target) {
                replay_gap_target = None;
            }
        }

        if catch_up_target.is_none() {
            catch_up_target = pending.take_persisted_after(current_seq);
        }

        let pending_next_seq = pending.next_sequence();
        let pending_is_ready =
            pending_next_seq.is_some_and(|seq| seq == next_expected_seq(current_seq));

        if let Some(target) = catch_up_target.filter(|_| !pending_is_ready) {
            let effective_target = pending_next_seq
                .and_then(|seq| seq.checked_sub(1))
                .map(|before_pending| before_pending.min(target))
                .unwrap_or(target);
            let chunk = read_replay_chunk(current_seq, effective_target, opts.replay_chunk_size);
            current_seq = chunk.last_seen_seq.or(current_seq);

            for event in chunk.events {
                match send_stream_event(&tx, event, &mut event_rx, &mut pending, opts) {
                    Ok(SendOutcome::Sent) => {}
                    Ok(SendOutcome::ReceiverDropped) => return,
                    Err(err) => {
                        send_stream_error(&tx, err.into());
                        return;
                    }
                }
            }

            if chunk.exhausted || current_seq.is_some_and(|seq| seq >= effective_target) {
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
            let seq = event.sequence();
            if !stream_seq_after(seq, current_seq) {
                continue;
            }

            let expected = next_expected_seq(current_seq);
            if seq != expected {
                catch_up_target = seq.checked_sub(1);
                pending.push_front(event);
                continue;
            }

            let Some(out_event) = live_to_output(event) else {
                catch_up_target = Some(seq);
                continue;
            };

            match send_stream_event(&tx, out_event, &mut event_rx, &mut pending, opts) {
                Ok(SendOutcome::Sent) => {
                    current_seq = Some(seq);
                }
                Ok(SendOutcome::ReceiverDropped) => return,
                Err(err) => {
                    send_stream_error(&tx, err.into());
                    return;
                }
            }
            continue;
        }

        match event_rx.blocking_recv() {
            Ok(event) => {
                if event.is_persisted_marker() {
                    let seq = event.sequence();
                    if stream_seq_after(seq, current_seq) {
                        catch_up_target = Some(seq);
                    }
                    continue;
                }

                if let Err(err) = pending.push(event) {
                    send_stream_error(&tx, err.into());
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                let err = StreamTooSlow::lagged(skipped);
                warn!(%err, "closing slow stream subscriber");
                send_stream_error(&tx, err.into());
                return;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(feature = "indexer_stream")]
pub(super) fn event_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<Result<Event, StreamError>>,
    cursor: Option<u64>,
    opts: StreamOptions,
) {
    let db = &state.db;
    let event_rx = db.event_tx.subscribe();
    let ks = db.events.clone();
    let current_id = match cursor {
        Some(c) => c.checked_sub(1),
        None => db.next_event_id.load(Ordering::SeqCst).checked_sub(1),
    };
    let catch_up_target = cursor
        .and_then(|_| db.next_event_id.load(Ordering::SeqCst).checked_sub(1))
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

#[cfg(feature = "indexer_stream")]
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

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn send_stream_event<O, E, B>(
    tx: &mpsc::Sender<Result<O, E>>,
    event: O,
    event_rx: &mut broadcast::Receiver<B>,
    pending: &mut PendingLiveEvents<B>,
    opts: StreamOptions,
) -> Result<SendOutcome, StreamTooSlow>
where
    B: StreamBroadcast + Clone,
{
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
                    return Err(StreamTooSlow::send_timeout(opts.send_timeout));
                }
                std::thread::sleep(STREAM_SEND_RETRY_PAUSE);
            }
        }
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn send_stream_error<O, E>(tx: &mpsc::Sender<Result<O, E>>, err: E)
where
    E: fmt::Display,
{
    warn!(%err, "closing stream subscriber");
    let _ = tx.try_send(Err(err));
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn drain_pending_broadcasts<B>(
    event_rx: &mut broadcast::Receiver<B>,
    pending: &mut PendingLiveEvents<B>,
) -> Result<(), StreamTooSlow>
where
    B: StreamBroadcast + Clone,
{
    loop {
        match event_rx.try_recv() {
            Ok(event) => pending.push(event)?,
            Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
            Err(broadcast::error::TryRecvError::Closed) => return Ok(()),
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                return Err(StreamTooSlow::lagged(skipped));
            }
        }
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn drop_delivered_pending<B>(pending: &mut PendingLiveEvents<B>, current_id: Option<u64>)
where
    B: StreamBroadcast,
{
    while pending
        .next_sequence()
        .is_some_and(|id| !stream_seq_after(id, current_id))
    {
        pending.pop_front();
    }
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn advance_replay_gap<B>(current_id: &mut Option<u64>, target: u64, pending: &PendingLiveEvents<B>)
where
    B: StreamBroadcast,
{
    let next_pending_id = pending.next_sequence().filter(|id| *id <= target);
    let advance_to = next_pending_id
        .and_then(|id| id.checked_sub(1))
        .unwrap_or(target);

    if stream_seq_after(advance_to, *current_id) {
        *current_id = Some(advance_to);
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

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn stream_seq_after(id: u64, current_id: Option<u64>) -> bool {
    current_id.is_none_or(|current| id > current)
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn next_expected_seq(current_id: Option<u64>) -> u64 {
    current_id.map(|id| id.saturating_add(1)).unwrap_or(0)
}

#[cfg(feature = "relay")]
pub(super) fn relay_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<Result<bytes::Bytes, RelayStreamError>>,
    cursor: Option<u64>,
    opts: StreamOptions,
) {
    let relay_rx = state.db.relay_broadcast_tx.subscribe();
    let ks = state.db.relay_events.clone();
    let current_seq = match cursor {
        Some(c) => Some(c.saturating_sub(1)),
        None => Some(
            state
                .db
                .next_relay_seq
                .load(Ordering::SeqCst)
                .saturating_sub(1),
        ),
    };
    let catch_up_target = cursor
        .and_then(|_| {
            state
                .db
                .next_relay_seq
                .load(Ordering::SeqCst)
                .checked_sub(1)
        })
        .filter(|target| stream_seq_after(*target, current_seq));

    run_ordered_stream(
        tx,
        relay_rx,
        current_seq,
        catch_up_target,
        opts,
        move |current_seq, target, chunk_size| {
            read_relay_replay_chunk(&ks, current_seq, target, chunk_size)
        },
        relay_broadcast_to_frame,
    );
}

#[cfg(feature = "relay")]
fn read_relay_replay_chunk(
    ks: &fjall::Keyspace,
    current_seq: Option<u64>,
    target: u64,
    chunk_size: usize,
) -> ReplayChunk<bytes::Bytes> {
    let start = current_seq.map(|seq| seq.saturating_add(1)).unwrap_or(0);
    if start > target {
        return ReplayChunk {
            events: Vec::new(),
            last_seen_seq: current_seq,
            exhausted: true,
        };
    }

    let mut events = Vec::with_capacity(chunk_size);
    let mut last_seen_seq = current_seq;
    let mut exhausted = false;
    let max_scanned = chunk_size.saturating_mul(4).max(chunk_size);
    let mut scanned = 0usize;
    let mut iter = ks.range(keys::relay_event_key(start)..=keys::relay_event_key(target));

    while events.len() < chunk_size && scanned < max_scanned {
        let Some(item) = iter.next() else {
            exhausted = true;
            break;
        };
        scanned += 1;

        let (k, v) = match item.into_inner() {
            Ok(kv) => kv,
            Err(e) => {
                error!(err = %e, "relay stream: failed to read relay_events");
                exhausted = true;
                break;
            }
        };
        let seq = match k.as_ref().try_into().map(u64::from_be_bytes) {
            Ok(seq) => seq,
            Err(_) => {
                error!("relay stream: failed to parse relay event seq");
                continue;
            }
        };
        last_seen_seq = Some(seq);
        events.push(bytes::Bytes::copy_from_slice(&v));
    }

    ReplayChunk {
        events,
        last_seen_seq,
        exhausted,
    }
}

#[cfg(feature = "relay")]
fn relay_broadcast_to_frame(event: crate::types::RelayBroadcast) -> Option<bytes::Bytes> {
    match event {
        crate::types::RelayBroadcast::Persisted(_) => None,
        crate::types::RelayBroadcast::Ephemeral(_, frame) => Some(frame),
    }
}

#[cfg(feature = "jetstream")]
pub(super) fn jetstream_stream_thread(
    state: Arc<AppState>,
    tx: mpsc::Sender<Result<Bytes, JetstreamStreamError>>,
    cursor: Option<i64>,
    filter: crate::control::JetstreamFilter,
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

#[cfg(feature = "jetstream")]
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
#[cfg(feature = "jetstream")]
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
#[cfg(feature = "jetstream")]
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
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[cfg(feature = "jetstream")]
fn send_jetstream_replay_window(
    state: &AppState,
    tx: &mpsc::Sender<Result<Bytes, JetstreamStreamError>>,
    event_rx: &mut broadcast::Receiver<JetstreamBroadcast>,
    mut next_key: Vec<u8>,
    target_time_us: u64,
    filter: &crate::control::JetstreamFilter,
    opts: StreamOptions,
) -> Option<Option<u64>> {
    let mut max_id_seen: Option<u64> = None;
    loop {
        // drain live events without buffering so the broadcast receiver never
        // lags regardless of how many events arrive during the replay window.
        drain_jetstream_broadcast(event_rx);

        let chunk =
            read_jetstream_replay_chunk(state, &next_key, target_time_us, opts.replay_chunk_size);
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
            std::thread::sleep(opts.replay_chunk_pause);
        }
    }
}

#[cfg(feature = "jetstream")]
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
        });
    }

    ReplayChunk {
        events,
        last_seen_seq: None,
        exhausted,
    }
}

#[cfg(feature = "jetstream")]
#[derive(serde::Serialize)]
struct JetstreamEvent<'a> {
    did: &'a str,
    time_us: i64,
    #[serde(flatten)]
    payload: JetstreamPayload<'a>,
}

#[cfg(feature = "jetstream")]
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum JetstreamPayload<'a> {
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

#[cfg(feature = "jetstream")]
#[derive(serde::Serialize)]
struct JetstreamCommit<'a> {
    rev: &'a str,
    operation: &'a str,
    collection: &'a str,
    rkey: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    record: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cid: Option<String>,
    live: bool,
}

#[cfg(feature = "jetstream")]
#[derive(serde::Serialize)]
struct JetstreamIdentity<'a> {
    did: String,
    seq: i64,
    time: &'a crate::ingest::stream::Datetime,
    #[serde(skip_serializing_if = "Option::is_none")]
    handle: Option<String>,
}

#[cfg(feature = "jetstream")]
#[derive(serde::Serialize)]
struct JetstreamAccount<'a> {
    active: bool,
    did: String,
    seq: i64,
    time: &'a crate::ingest::stream::Datetime,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

#[cfg(feature = "jetstream")]
fn jetstream_event_to_bytes(
    state: &AppState,
    event: JetstreamBroadcast,
    filter: &crate::control::JetstreamFilter,
) -> Option<Bytes> {
    if !filter.wants(&event.event) {
        return None;
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

#[cfg(all(test, any(feature = "indexer_stream", feature = "relay")))]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    enum TestBroadcast {
        Persisted(u64),
        Live(u64),
    }

    impl StreamBroadcast for TestBroadcast {
        fn sequence(&self) -> u64 {
            match self {
                Self::Persisted(seq) | Self::Live(seq) => *seq,
            }
        }

        fn is_persisted_marker(&self) -> bool {
            matches!(self, Self::Persisted(_))
        }
    }

    #[test]
    fn ordered_stream_replays_chunks_then_live_tail() {
        let opts = StreamOptions {
            replay_chunk_size: 2,
            replay_chunk_pause: Duration::ZERO,
            pending_event_limit: 4,
            send_timeout: Duration::from_secs(1),
        };
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (broadcast_tx, broadcast_rx) = broadcast::channel(16);

        let handle = std::thread::spawn(move || {
            run_ordered_stream::<TestBroadcast, u64, StreamTooSlow>(
                out_tx,
                broadcast_rx,
                Some(0),
                Some(5),
                opts,
                |current, target, chunk_size| {
                    let start = current.map(|seq| seq.saturating_add(1)).unwrap_or(0);
                    if start > target {
                        return ReplayChunk {
                            events: Vec::new(),
                            last_seen_seq: current,
                            exhausted: true,
                        };
                    }

                    let end = target.min(start.saturating_add(chunk_size as u64).saturating_sub(1));
                    let events = (start..=end).collect::<Vec<_>>();
                    ReplayChunk {
                        last_seen_seq: events.last().copied().or(current),
                        exhausted: end >= target,
                        events,
                    }
                },
                |event| match event {
                    TestBroadcast::Persisted(_) => None,
                    TestBroadcast::Live(seq) => Some(seq),
                },
            );
        });

        broadcast_tx.send(TestBroadcast::Live(6)).unwrap();
        broadcast_tx.send(TestBroadcast::Persisted(6)).unwrap();
        drop(broadcast_tx);

        let mut out = Vec::new();
        while let Some(item) = out_rx.blocking_recv() {
            out.push(item.unwrap());
        }
        handle.join().unwrap();

        assert_eq!(out, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn ordered_stream_closes_when_output_queue_is_full() {
        let opts = StreamOptions {
            replay_chunk_size: 2,
            replay_chunk_pause: Duration::ZERO,
            pending_event_limit: 4,
            send_timeout: Duration::ZERO,
        };
        let (out_tx, _out_rx) = mpsc::channel(1);
        let (_broadcast_tx, broadcast_rx) = broadcast::channel(16);

        run_ordered_stream::<TestBroadcast, u64, StreamTooSlow>(
            out_tx,
            broadcast_rx,
            Some(0),
            Some(2),
            opts,
            |_, _, _| ReplayChunk {
                events: vec![1, 2],
                last_seen_seq: Some(2),
                exhausted: true,
            },
            |event| match event {
                TestBroadcast::Persisted(_) => None,
                TestBroadcast::Live(seq) => Some(seq),
            },
        );
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
                    .get(keys::block_key(collection.as_str(), &cid.to_bytes()));
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
