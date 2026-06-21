use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tracing::error;

use crate::db::keys;
use crate::state::AppState;
use crate::types::RelayBroadcast;

use crate::control::RelayStreamError;
use super::{ReplayChunk, StreamOptions, run_ordered_stream, stream_seq_after};

pub(crate) fn relay_stream_thread(
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

fn relay_broadcast_to_frame(event: RelayBroadcast) -> Option<bytes::Bytes> {
    match event {
        RelayBroadcast::Persisted(_) => None,
        RelayBroadcast::Ephemeral(_, frame) => Some(frame),
    }
}
