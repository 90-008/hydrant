//! zero-cost mirror of the diagnostics stats surface, used when the
//! `firehose-diagnostics` feature is disabled. every recorder is an inline
//! no-op so ingest call sites stay unconditional.

use std::sync::Arc;
use std::time::Duration;
use url::Url;

use super::{
    HostAuthorityStatsOutcome, RelayMessageKind, RelayShardTimings, RepoStateLoadOutcome,
    ValidationStatsOutcome,
};

#[derive(Default)]
pub struct FirehoseStats;

impl FirehoseStats {
    #[inline(always)]
    pub async fn handle(&self, _url: &Url) -> Arc<FirehoseSourceStats> {
        Arc::new(FirehoseSourceStats)
    }

    #[inline(always)]
    pub fn relay_shard(&self, _id: usize) -> Arc<RelayShardStats> {
        Arc::new(RelayShardStats)
    }
}

#[derive(Default)]
pub struct FirehoseSourceStats;

impl FirehoseSourceStats {
    #[inline(always)]
    pub fn record_connect_attempt(&self, _cursor: Option<i64>) {}
    #[inline(always)]
    pub fn record_connected(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_connect_error(&self, _kind: &'static str) {}
    #[inline(always)]
    pub fn record_stream_error(&self, _kind: &'static str) {}
    #[inline(always)]
    pub fn record_frame(&self, _len: usize) {}
    #[inline(always)]
    pub fn record_decoded(&self, _kind: &'static str, _seq: Option<i64>) {}
    #[inline(always)]
    pub fn record_throttle_wait(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_should_process(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_skipped(&self) {}
    #[inline(always)]
    pub fn record_forwarded(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_forward_error(&self, _elapsed: Duration) {}
}

#[derive(Default)]
pub struct RelayShardStats;

// mirrors the full diagnostics surface; per-combo unused recorders are expected
#[allow(dead_code)]
impl RelayShardStats {
    #[inline(always)]
    pub fn record_received(&self, _seq: i64) {}
    #[inline(always)]
    pub fn record_info(&self) {}
    #[inline(always)]
    pub fn record_message_kind(&self, _kind: RelayMessageKind) {}
    #[inline(always)]
    pub fn record_repo_state_load(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_repo_state_outcome(&self, _outcome: RepoStateLoadOutcome) {}
    #[inline(always)]
    pub fn record_host_authority(&self, _elapsed: Duration, _outcome: HostAuthorityStatsOutcome) {}
    #[inline(always)]
    pub fn record_handle_message(&self, _kind: RelayMessageKind, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_fetch_key(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_validate_commit(&self, _elapsed: Duration, _outcome: ValidationStatsOutcome) {}
    #[inline(always)]
    pub fn record_validate_sync(&self, _elapsed: Duration, _outcome: ValidationStatsOutcome) {}
    #[inline(always)]
    pub fn record_refresh_doc(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_new_account(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_resolve_doc(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_repo_status_probe(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_queue_emit(&self, _elapsed: Duration) {}
    #[inline(always)]
    pub fn record_process_error(&self) {}
    #[inline(always)]
    pub fn record_commit_error(&self) {}
    #[inline(always)]
    pub fn record_processed(&self, _timings: RelayShardTimings) {}
}
