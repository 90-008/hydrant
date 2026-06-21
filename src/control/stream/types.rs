use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::config::Config;

pub(crate) const STREAM_SEND_RETRY_PAUSE: Duration = Duration::from_millis(10);
pub(crate) const STREAM_CHANNEL_CAPACITY: usize = 500;

#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamOptions {
    pub(crate) replay_chunk_size: Option<NonZeroUsize>,
    pub(crate) replay_chunk_pause: Duration,
    pub(crate) pending_event_limit: usize,
    pub(crate) send_timeout: Duration,
}

impl StreamOptions {
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            replay_chunk_size: NonZeroUsize::new(config.stream_replay_chunk_size),
            replay_chunk_pause: config.stream_replay_chunk_pause,
            pending_event_limit: config.stream_pending_event_limit.max(1),
            send_timeout: config.stream_send_timeout,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StreamTooSlow {
    pub(crate) reason: String,
}

impl StreamTooSlow {
    pub(crate) fn pending_limit(limit: usize) -> Self {
        Self {
            reason: format!("pending stream event buffer exceeded {limit} events"),
        }
    }

    pub(crate) fn lagged(skipped: u64) -> Self {
        Self {
            reason: format!("subscriber lagged past {skipped} broadcast events"),
        }
    }

    pub(crate) fn send_timeout(timeout: Duration) -> Self {
        Self {
            reason: format!(
                "stream delivery blocked for at least {} seconds",
                timeout.as_secs()
            ),
        }
    }
}

impl fmt::Display for StreamTooSlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream consumer too slow: {}", self.reason)
    }
}

#[cfg(feature = "indexer_stream")]
impl From<StreamTooSlow> for super::super::StreamError {
    fn from(err: StreamTooSlow) -> Self {
        Self::ConsumerTooSlow { reason: err.reason }
    }
}

#[cfg(feature = "relay")]
impl From<StreamTooSlow> for super::super::RelayStreamError {
    fn from(err: StreamTooSlow) -> Self {
        Self::ConsumerTooSlow { reason: err.reason }
    }
}

#[cfg(feature = "jetstream")]
impl From<StreamTooSlow> for super::super::JetstreamStreamError {
    fn from(err: StreamTooSlow) -> Self {
        Self::ConsumerTooSlow { reason: err.reason }
    }
}

pub(crate) trait StreamBroadcast {
    fn sequence(&self) -> u64;
    fn is_persisted_marker(&self) -> bool;
}

#[cfg(feature = "indexer_stream")]
impl StreamBroadcast for crate::types::BroadcastEvent {
    fn sequence(&self) -> u64 {
        match self {
            crate::types::BroadcastEvent::Persisted(id) => *id,
            crate::types::BroadcastEvent::LiveRecord(evt) => evt.id,
            crate::types::BroadcastEvent::Ephemeral(evt) => evt.id,
        }
    }

    fn is_persisted_marker(&self) -> bool {
        matches!(self, crate::types::BroadcastEvent::Persisted(_))
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
impl StreamBroadcast for crate::types::JetstreamBroadcast {
    fn sequence(&self) -> u64 {
        self.id
    }

    fn is_persisted_marker(&self) -> bool {
        false
    }
}

pub(crate) struct PendingLiveEvents<T> {
    pub(crate) queue: VecDeque<T>,
    pub(crate) persisted_head: Option<u64>,
    pub(crate) limit: usize,
}

impl<T> PendingLiveEvents<T>
where
    T: StreamBroadcast,
{
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            persisted_head: None,
            limit,
        }
    }

    pub(crate) fn push(&mut self, event: T) -> Result<(), StreamTooSlow> {
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

    pub(crate) fn next_sequence(&self) -> Option<u64> {
        self.queue.front().map(StreamBroadcast::sequence)
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    pub(crate) fn push_front(&mut self, event: T) {
        self.queue.push_front(event);
    }

    pub(crate) fn take_persisted_after(&mut self, current_id: Option<u64>) -> Option<u64> {
        let head = self.persisted_head.take()?;
        stream_seq_after(head, current_id).then_some(head)
    }
}

pub(crate) struct ReplayChunk<T> {
    pub(crate) events: Vec<T>,
    pub(crate) last_seen_seq: Option<u64>,
    pub(crate) exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Sent,
    ReceiverDropped,
}

fn stream_seq_after(id: u64, current_id: Option<u64>) -> bool {
    current_id.is_none_or(|current| id > current)
}
