use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use parking_lot::Mutex;
use serde::Serialize;
use url::Url;

use super::{now_ts, duration_micros, nonzero_i64, RelayWorkerStats, RelayWorkerStatsSnapshot, RelayShardStats};

#[derive(Default)]
pub struct FirehoseStats {
    sources: scc::HashMap<Url, Arc<FirehoseSourceStats>>,
    relay_worker: RelayWorkerStats,
}

impl FirehoseStats {
    pub async fn handle(&self, url: &Url) -> Arc<FirehoseSourceStats> {
        self.sources
            .entry_async(url.clone())
            .await
            .or_insert_with(|| Arc::new(FirehoseSourceStats::default()))
            .get()
            .clone()
    }

    pub fn snapshot(&self, url: &Url) -> Option<FirehoseStatsSnapshot> {
        self.sources.read_sync(url, |_, stats| stats.snapshot())
    }

    pub fn relay_shard(&self, id: usize) -> Arc<RelayShardStats> {
        self.relay_worker.shard(id)
    }

    pub fn relay_worker_snapshot(&self) -> RelayWorkerStatsSnapshot {
        self.relay_worker.snapshot()
    }
}

#[derive(Default)]
pub struct FirehoseSourceStats {
    connection_attempts: AtomicU64,
    successful_connections: AtomicU64,
    connect_errors: AtomicU64,
    stream_errors: AtomicU64,
    frames_read: AtomicU64,
    bytes_read: AtomicU64,
    messages_decoded: AtomicU64,
    messages_forwarded: AtomicU64,
    messages_skipped: AtomicU64,
    commit_messages: AtomicU64,
    sync_messages: AtomicU64,
    identity_messages: AtomicU64,
    account_messages: AtomicU64,
    info_messages: AtomicU64,
    forward_errors: AtomicU64,
    throttle_waits: AtomicU64,
    throttle_wait_micros: AtomicU64,
    should_process_micros: AtomicU64,
    send_waits: AtomicU64,
    send_wait_micros: AtomicU64,
    connect_elapsed_micros: AtomicU64,
    max_send_wait_micros: AtomicU64,
    max_should_process_micros: AtomicU64,
    max_throttle_wait_micros: AtomicU64,
    last_connect_attempt_at: AtomicI64,
    last_connected_at: AtomicI64,
    last_frame_at: AtomicI64,
    last_decoded_at: AtomicI64,
    last_forwarded_at: AtomicI64,
    last_error_at: AtomicI64,
    last_start_cursor: AtomicI64,
    last_seq: AtomicI64,
    max_seq: AtomicI64,
    last_error_kind: Mutex<Option<&'static str>>,
}

impl FirehoseSourceStats {
    pub fn record_connect_attempt(&self, cursor: Option<i64>) {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);
        self.last_connect_attempt_at
            .store(now_ts(), Ordering::Relaxed);
        self.last_start_cursor
            .store(cursor.unwrap_or(0), Ordering::Relaxed);
    }

    pub fn record_connected(&self, elapsed: Duration) {
        self.successful_connections.fetch_add(1, Ordering::Relaxed);
        self.last_connected_at.store(now_ts(), Ordering::Relaxed);
        self.connect_elapsed_micros
            .fetch_add(duration_micros(elapsed), Ordering::Relaxed);
    }

    pub fn record_connect_error(&self, kind: &'static str) {
        self.connect_errors.fetch_add(1, Ordering::Relaxed);
        self.record_error_kind(kind);
    }

    pub fn record_stream_error(&self, kind: &'static str) {
        self.stream_errors.fetch_add(1, Ordering::Relaxed);
        self.record_error_kind(kind);
    }

    pub fn record_frame(&self, len: usize) {
        self.frames_read.fetch_add(1, Ordering::Relaxed);
        self.bytes_read
            .fetch_add(len.try_into().unwrap_or(u64::MAX), Ordering::Relaxed);
        self.last_frame_at.store(now_ts(), Ordering::Relaxed);
    }

    pub fn record_decoded(&self, kind: &'static str, seq: Option<i64>) {
        self.messages_decoded.fetch_add(1, Ordering::Relaxed);
        self.last_decoded_at.store(now_ts(), Ordering::Relaxed);
        match kind {
            "commit" => self.commit_messages.fetch_add(1, Ordering::Relaxed),
            "sync" => self.sync_messages.fetch_add(1, Ordering::Relaxed),
            "identity" => self.identity_messages.fetch_add(1, Ordering::Relaxed),
            "account" => self.account_messages.fetch_add(1, Ordering::Relaxed),
            "info" => self.info_messages.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
        if let Some(seq) = seq {
            self.last_seq.store(seq, Ordering::Relaxed);
            self.max_seq.fetch_max(seq, Ordering::Relaxed);
        }
    }

    pub fn record_throttle_wait(&self, elapsed: Duration) {
        let micros = duration_micros(elapsed);
        if micros == 0 {
            return;
        }
        self.throttle_waits.fetch_add(1, Ordering::Relaxed);
        self.throttle_wait_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.max_throttle_wait_micros
            .fetch_max(micros, Ordering::Relaxed);
    }

    pub fn record_should_process(&self, elapsed: Duration) {
        let micros = duration_micros(elapsed);
        self.should_process_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.max_should_process_micros
            .fetch_max(micros, Ordering::Relaxed);
    }

    pub fn record_skipped(&self) {
        self.messages_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_forwarded(&self, elapsed: Duration) {
        self.messages_forwarded.fetch_add(1, Ordering::Relaxed);
        self.last_forwarded_at.store(now_ts(), Ordering::Relaxed);
        self.record_send_wait(elapsed);
    }

    pub fn record_forward_error(&self, elapsed: Duration) {
        self.forward_errors.fetch_add(1, Ordering::Relaxed);
        self.record_send_wait(elapsed);
    }

    fn record_send_wait(&self, elapsed: Duration) {
        let micros = duration_micros(elapsed);
        self.send_waits.fetch_add(1, Ordering::Relaxed);
        self.send_wait_micros.fetch_add(micros, Ordering::Relaxed);
        self.max_send_wait_micros
            .fetch_max(micros, Ordering::Relaxed);
    }

    fn record_error_kind(&self, kind: &'static str) {
        self.last_error_at.store(now_ts(), Ordering::Relaxed);
        *self.last_error_kind.lock() = Some(kind);
    }

    fn snapshot(&self) -> FirehoseStatsSnapshot {
        FirehoseStatsSnapshot {
            connection_attempts: self.load_u64(&self.connection_attempts),
            successful_connections: self.load_u64(&self.successful_connections),
            connect_errors: self.load_u64(&self.connect_errors),
            stream_errors: self.load_u64(&self.stream_errors),
            frames_read: self.load_u64(&self.frames_read),
            bytes_read: self.load_u64(&self.bytes_read),
            messages_decoded: self.load_u64(&self.messages_decoded),
            messages_forwarded: self.load_u64(&self.messages_forwarded),
            messages_skipped: self.load_u64(&self.messages_skipped),
            message_kinds: FirehoseMessageStats {
                commit: self.load_u64(&self.commit_messages),
                sync: self.load_u64(&self.sync_messages),
                identity: self.load_u64(&self.identity_messages),
                account: self.load_u64(&self.account_messages),
                info: self.load_u64(&self.info_messages),
            },
            forward_errors: self.load_u64(&self.forward_errors),
            throttle_waits: self.load_u64(&self.throttle_waits),
            throttle_wait_micros: self.load_u64(&self.throttle_wait_micros),
            should_process_micros: self.load_u64(&self.should_process_micros),
            send_waits: self.load_u64(&self.send_waits),
            send_wait_micros: self.load_u64(&self.send_wait_micros),
            connect_elapsed_micros: self.load_u64(&self.connect_elapsed_micros),
            max_send_wait_micros: self.load_u64(&self.max_send_wait_micros),
            max_should_process_micros: self.load_u64(&self.max_should_process_micros),
            max_throttle_wait_micros: self.load_u64(&self.max_throttle_wait_micros),
            last_connect_attempt_at: nonzero_i64(&self.last_connect_attempt_at),
            last_connected_at: nonzero_i64(&self.last_connected_at),
            last_frame_at: nonzero_i64(&self.last_frame_at),
            last_decoded_at: nonzero_i64(&self.last_decoded_at),
            last_forwarded_at: nonzero_i64(&self.last_forwarded_at),
            last_error_at: nonzero_i64(&self.last_error_at),
            last_start_cursor: nonzero_i64(&self.last_start_cursor),
            last_seq: nonzero_i64(&self.last_seq),
            max_seq: nonzero_i64(&self.max_seq),
            last_error_kind: *self.last_error_kind.lock(),
        }
    }

    fn load_u64(&self, atomic: &AtomicU64) -> u64 {
        atomic.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FirehoseStatsSnapshot {
    pub connection_attempts: u64,
    pub successful_connections: u64,
    pub connect_errors: u64,
    pub stream_errors: u64,
    pub frames_read: u64,
    pub bytes_read: u64,
    pub messages_decoded: u64,
    pub messages_forwarded: u64,
    pub messages_skipped: u64,
    pub message_kinds: FirehoseMessageStats,
    pub forward_errors: u64,
    pub throttle_waits: u64,
    pub throttle_wait_micros: u64,
    pub should_process_micros: u64,
    pub send_waits: u64,
    pub send_wait_micros: u64,
    pub connect_elapsed_micros: u64,
    pub max_send_wait_micros: u64,
    pub max_should_process_micros: u64,
    pub max_throttle_wait_micros: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_connect_attempt_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_connected_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_frame_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_decoded_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_forwarded_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_start_cursor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_kind: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FirehoseMessageStats {
    pub commit: u64,
    pub sync: u64,
    pub identity: u64,
    pub account: u64,
    pub info: u64,
}
