#![cfg(any(feature = "indexer_stream", feature = "relay", feature = "jetstream"))]

use std::fmt;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

use super::types::{
    PendingLiveEvents, ReplayChunk, SendOutcome, StreamBroadcast, StreamOptions, StreamTooSlow,
    STREAM_SEND_RETRY_PAUSE,
};

pub(crate) fn run_ordered_stream<B, O, E>(
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
    let mut replay_blocked_since: Option<Instant> = None;

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

            let Some(chunk_size) = replay_chunk_size_for(&tx, opts.replay_chunk_size) else {
                if let Err(err) = drain_pending_broadcasts(&mut event_rx, &mut pending) {
                    send_stream_error(&tx, err.into());
                    return;
                }
                if let Err(err) = note_replay_blocked(&mut replay_blocked_since, opts.send_timeout)
                {
                    send_stream_error(&tx, err.into());
                    return;
                }
                std::thread::sleep(STREAM_SEND_RETRY_PAUSE);
                continue;
            };
            clear_replay_blocked(&mut replay_blocked_since);

            let chunk = read_replay_chunk(current_seq, effective_target, chunk_size);
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

pub(crate) fn send_stream_error<O, E>(tx: &mpsc::Sender<Result<O, E>>, err: E)
where
    E: fmt::Display,
{
    warn!(%err, "closing stream subscriber");
    let _ = tx.try_send(Err(err));
}

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

pub(crate) fn stream_seq_after(id: u64, current_id: Option<u64>) -> bool {
    current_id.is_none_or(|current| id > current)
}

pub(crate) fn next_expected_seq(current_id: Option<u64>) -> u64 {
    current_id.map(|id| id.saturating_add(1)).unwrap_or(0)
}

pub(crate) fn replay_chunk_size_for<T>(
    tx: &mpsc::Sender<T>,
    configured: Option<NonZeroUsize>,
) -> Option<usize> {
    let available = tx.capacity();

    if available == 0 {
        return None;
    }

    let max_cap = tx.max_capacity().max(1);
    let desired = configured
        .map(NonZeroUsize::get)
        .unwrap_or_else(|| (max_cap / 2).max(1))
        .min(max_cap);

    Some(desired.min(available).max(1))
}

pub(crate) fn note_replay_blocked(
    blocked_since: &mut Option<Instant>,
    timeout: Duration,
) -> Result<(), StreamTooSlow> {
    let started = blocked_since.get_or_insert_with(Instant::now);

    if started.elapsed() >= timeout {
        return Err(StreamTooSlow::send_timeout(timeout));
    }

    Ok(())
}

pub(crate) fn clear_replay_blocked(blocked_since: &mut Option<Instant>) {
    *blocked_since = None;
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

    fn chunk_size(n: usize) -> Option<NonZeroUsize> {
        NonZeroUsize::new(n)
    }

    #[test]
    fn ordered_stream_replays_chunks_then_live_tail() {
        let opts = StreamOptions {
            replay_chunk_size: chunk_size(2),
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
            replay_chunk_size: chunk_size(2),
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

    #[test]
    fn replay_chunk_auto_uses_half_max_capacity() {
        let (tx, _rx) = mpsc::channel::<()>(500);
        assert_eq!(replay_chunk_size_for(&tx, None), Some(250));
    }

    #[test]
    fn replay_chunk_manual_is_respected() {
        let (tx, _rx) = mpsc::channel::<()>(500);
        assert_eq!(replay_chunk_size_for(&tx, chunk_size(64)), Some(64));
    }

    #[test]
    fn replay_chunk_manual_is_capped_to_max_capacity() {
        let (tx, _rx) = mpsc::channel::<()>(500);
        assert_eq!(replay_chunk_size_for(&tx, chunk_size(1000)), Some(500));
    }

    #[test]
    fn replay_chunk_is_capped_to_current_available_capacity() {
        let (tx, mut rx) = mpsc::channel::<()>(4);

        tx.try_send(()).unwrap();
        tx.try_send(()).unwrap();
        tx.try_send(()).unwrap();

        assert_eq!(replay_chunk_size_for(&tx, None), Some(1));

        let _ = rx.try_recv();
    }

    #[test]
    fn replay_chunk_none_when_channel_full() {
        let (tx, _rx) = mpsc::channel::<()>(2);

        tx.try_send(()).unwrap();
        tx.try_send(()).unwrap();

        assert_eq!(replay_chunk_size_for(&tx, None), None);
    }

    #[test]
    fn replay_chunk_manual_none_when_channel_full() {
        let (tx, _rx) = mpsc::channel::<()>(2);

        tx.try_send(()).unwrap();
        tx.try_send(()).unwrap();

        assert_eq!(replay_chunk_size_for(&tx, chunk_size(1)), None);
    }

    #[test]
    fn replay_chunk_auto_min_one_for_small_channel() {
        let (tx, _rx) = mpsc::channel::<()>(1);
        assert_eq!(replay_chunk_size_for(&tx, None), Some(1));
    }
}
