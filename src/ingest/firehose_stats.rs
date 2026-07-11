mod kinds;
#[cfg(not(feature = "firehose-diagnostics"))]
mod noop;
#[cfg(feature = "firehose-diagnostics")]
mod relay;
#[cfg(feature = "firehose-diagnostics")]
mod source;

pub use kinds::*;
#[cfg(not(feature = "firehose-diagnostics"))]
pub use noop::*;
#[cfg(feature = "firehose-diagnostics")]
pub use relay::{RelayShardStats, RelayWorkerStats, RelayWorkerStatsSnapshot};
#[cfg(feature = "firehose-diagnostics")]
pub use source::{FirehoseSourceStats, FirehoseStats, FirehoseStatsSnapshot};

#[cfg(feature = "firehose-diagnostics")]
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
#[cfg(feature = "firehose-diagnostics")]
use std::time::Duration;

#[cfg(feature = "firehose-diagnostics")]
fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

#[cfg(feature = "firehose-diagnostics")]
fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

#[cfg(feature = "firehose-diagnostics")]
fn add_duration(total: &AtomicU64, duration: Duration) {
    let micros = duration_micros(duration);
    total.fetch_add(micros, Ordering::Relaxed);
}

#[cfg(feature = "firehose-diagnostics")]
fn add_duration_with_max(total: &AtomicU64, max: &AtomicU64, duration: Duration) {
    let micros = duration_micros(duration);
    total.fetch_add(micros, Ordering::Relaxed);
    max.fetch_max(micros, Ordering::Relaxed);
}

#[cfg(feature = "firehose-diagnostics")]
fn nonzero_i64(atomic: &AtomicI64) -> Option<i64> {
    let value = atomic.load(Ordering::Relaxed);
    (value != 0).then_some(value)
}

#[cfg(all(test, feature = "firehose-diagnostics"))]
mod tests {
    use super::*;

    #[test]
    fn snapshot_records_firehose_progress() {
        let stats = FirehoseSourceStats::default();

        stats.record_connect_attempt(Some(100));
        stats.record_connected(Duration::from_millis(12));
        stats.record_frame(42);
        stats.record_decoded("commit", Some(101));
        stats.record_forwarded(Duration::from_micros(7));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.connection_attempts, 1);
        assert_eq!(snapshot.successful_connections, 1);
        assert_eq!(snapshot.frames_read, 1);
        assert_eq!(snapshot.bytes_read, 42);
        assert_eq!(snapshot.messages_decoded, 1);
        assert_eq!(snapshot.messages_forwarded, 1);
        assert_eq!(snapshot.last_start_cursor, Some(100));
        assert_eq!(snapshot.last_seq, Some(101));
        assert_eq!(snapshot.max_seq, Some(101));
        assert_eq!(snapshot.message_kinds.commit, 1);
    }

    #[test]
    fn snapshot_records_relay_worker_progress() {
        let stats = RelayShardStats::default();

        stats.record_received(200);
        stats.record_process_error();
        stats.record_processed(RelayShardTimings {
            process_message: Duration::from_micros(10),
            stage_counts: Duration::from_micros(20),
            stage_and_commit: Duration::from_micros(30),
            apply_counts: Duration::from_micros(40),
            broadcast: Duration::from_micros(50),
            cursor: Duration::from_micros(60),
            total: Duration::from_micros(210),
        });

        let snapshot = stats.snapshot(3);
        assert_eq!(snapshot.id, 3);
        assert_eq!(snapshot.received_messages, 1);
        assert_eq!(snapshot.processed_messages, 1);
        assert_eq!(snapshot.process_errors, 1);
        assert_eq!(snapshot.last_seq, Some(200));
        assert_eq!(snapshot.max_seq, Some(200));
        assert_eq!(snapshot.process_message_micros, 10);
        assert_eq!(snapshot.stage_counts_micros, 20);
        assert_eq!(snapshot.stage_and_commit_micros, 30);
        assert_eq!(snapshot.apply_counts_micros, 40);
        assert_eq!(snapshot.broadcast_micros, 50);
        assert_eq!(snapshot.cursor_micros, 60);
        assert_eq!(snapshot.total_micros, 210);
    }
}
