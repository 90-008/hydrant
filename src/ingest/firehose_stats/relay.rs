use parking_lot::Mutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use super::{
    HostAuthorityStatsOutcome, RelayMessageKind, RelayShardTimings, RepoStateLoadOutcome,
    ValidationStatsOutcome, add_duration, add_duration_with_max, nonzero_i64, now_ts,
};

#[derive(Default)]
pub struct RelayWorkerStats {
    shards: Mutex<BTreeMap<usize, Arc<RelayShardStats>>>,
}

impl RelayWorkerStats {
    pub(crate) fn shard(&self, id: usize) -> Arc<RelayShardStats> {
        let mut shards = self.shards.lock();
        shards
            .entry(id)
            .or_insert_with(|| Arc::new(RelayShardStats::default()))
            .clone()
    }

    pub(crate) fn snapshot(&self) -> RelayWorkerStatsSnapshot {
        let shards = self
            .shards
            .lock()
            .iter()
            .map(|(&id, stats)| stats.snapshot(id))
            .collect();
        RelayWorkerStatsSnapshot { shards }
    }
}

#[derive(Default)]
pub struct RelayShardStats {
    received_messages: AtomicU64,
    info_messages: AtomicU64,
    processed_messages: AtomicU64,
    process_errors: AtomicU64,
    commit_errors: AtomicU64,
    commit_messages: AtomicU64,
    sync_messages: AtomicU64,
    identity_messages: AtomicU64,
    account_messages: AtomicU64,
    repo_state_hits: AtomicU64,
    repo_state_misses: AtomicU64,
    repo_state_drops: AtomicU64,
    host_authority_checks: AtomicU64,
    host_authority_authorized: AtomicU64,
    host_authority_stale: AtomicU64,
    host_authority_wrong: AtomicU64,
    host_authority_errors: AtomicU64,
    fetch_key_calls: AtomicU64,
    validate_commit_calls: AtomicU64,
    validate_commit_accepted: AtomicU64,
    validate_commit_stale: AtomicU64,
    validate_commit_sig_failures: AtomicU64,
    validate_commit_rejected: AtomicU64,
    validate_sync_calls: AtomicU64,
    validate_sync_accepted: AtomicU64,
    validate_sync_sig_failures: AtomicU64,
    validate_sync_rejected: AtomicU64,
    refresh_doc_calls: AtomicU64,
    new_account_calls: AtomicU64,
    resolve_doc_calls: AtomicU64,
    repo_status_probe_calls: AtomicU64,
    queue_emit_calls: AtomicU64,
    process_message_micros: AtomicU64,
    load_repo_state_micros: AtomicU64,
    host_authority_micros: AtomicU64,
    handle_commit_micros: AtomicU64,
    handle_sync_micros: AtomicU64,
    handle_identity_micros: AtomicU64,
    handle_account_micros: AtomicU64,
    fetch_key_micros: AtomicU64,
    validate_commit_micros: AtomicU64,
    validate_sync_micros: AtomicU64,
    refresh_doc_micros: AtomicU64,
    new_account_micros: AtomicU64,
    resolve_doc_micros: AtomicU64,
    repo_status_probe_micros: AtomicU64,
    queue_emit_micros: AtomicU64,
    stage_counts_micros: AtomicU64,
    stage_and_commit_micros: AtomicU64,
    apply_counts_micros: AtomicU64,
    broadcast_micros: AtomicU64,
    cursor_micros: AtomicU64,
    total_micros: AtomicU64,
    max_process_message_micros: AtomicU64,
    max_load_repo_state_micros: AtomicU64,
    max_host_authority_micros: AtomicU64,
    max_handle_commit_micros: AtomicU64,
    max_fetch_key_micros: AtomicU64,
    max_validate_commit_micros: AtomicU64,
    max_refresh_doc_micros: AtomicU64,
    max_new_account_micros: AtomicU64,
    max_queue_emit_micros: AtomicU64,
    max_stage_and_commit_micros: AtomicU64,
    max_total_micros: AtomicU64,
    last_received_at: AtomicI64,
    last_processed_at: AtomicI64,
    last_error_at: AtomicI64,
    last_seq: AtomicI64,
    max_seq: AtomicI64,
}

impl RelayShardStats {
    pub fn record_received(&self, seq: i64) {
        self.received_messages.fetch_add(1, Ordering::Relaxed);
        self.last_received_at.store(now_ts(), Ordering::Relaxed);
        self.last_seq.store(seq, Ordering::Relaxed);
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
    }

    pub fn record_info(&self) {
        self.info_messages.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_message_kind(&self, kind: RelayMessageKind) {
        match kind {
            RelayMessageKind::Commit => self.commit_messages.fetch_add(1, Ordering::Relaxed),
            RelayMessageKind::Sync => self.sync_messages.fetch_add(1, Ordering::Relaxed),
            RelayMessageKind::Identity => self.identity_messages.fetch_add(1, Ordering::Relaxed),
            RelayMessageKind::Account => self.account_messages.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn record_repo_state_load(&self, elapsed: Duration) {
        add_duration_with_max(
            &self.load_repo_state_micros,
            &self.max_load_repo_state_micros,
            elapsed,
        );
    }

    pub fn record_repo_state_outcome(&self, outcome: RepoStateLoadOutcome) {
        match outcome {
            RepoStateLoadOutcome::Hit => self.repo_state_hits.fetch_add(1, Ordering::Relaxed),
            RepoStateLoadOutcome::Miss => self.repo_state_misses.fetch_add(1, Ordering::Relaxed),
            RepoStateLoadOutcome::Drop => self.repo_state_drops.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn record_host_authority(&self, elapsed: Duration, outcome: HostAuthorityStatsOutcome) {
        self.host_authority_checks.fetch_add(1, Ordering::Relaxed);
        match outcome {
            HostAuthorityStatsOutcome::Authorized => self
                .host_authority_authorized
                .fetch_add(1, Ordering::Relaxed),
            HostAuthorityStatsOutcome::WasStale => {
                self.host_authority_stale.fetch_add(1, Ordering::Relaxed)
            }
            HostAuthorityStatsOutcome::WrongHost => {
                self.host_authority_wrong.fetch_add(1, Ordering::Relaxed)
            }
            HostAuthorityStatsOutcome::Error => {
                self.host_authority_errors.fetch_add(1, Ordering::Relaxed)
            }
        };
        add_duration_with_max(
            &self.host_authority_micros,
            &self.max_host_authority_micros,
            elapsed,
        );
    }

    pub fn record_handle_message(&self, kind: RelayMessageKind, elapsed: Duration) {
        match kind {
            RelayMessageKind::Commit => add_duration_with_max(
                &self.handle_commit_micros,
                &self.max_handle_commit_micros,
                elapsed,
            ),
            RelayMessageKind::Sync => add_duration(&self.handle_sync_micros, elapsed),
            RelayMessageKind::Identity => add_duration(&self.handle_identity_micros, elapsed),
            RelayMessageKind::Account => add_duration(&self.handle_account_micros, elapsed),
        }
    }

    pub fn record_fetch_key(&self, elapsed: Duration) {
        self.fetch_key_calls.fetch_add(1, Ordering::Relaxed);
        add_duration_with_max(&self.fetch_key_micros, &self.max_fetch_key_micros, elapsed);
    }

    pub fn record_validate_commit(&self, elapsed: Duration, outcome: ValidationStatsOutcome) {
        self.validate_commit_calls.fetch_add(1, Ordering::Relaxed);
        match outcome {
            ValidationStatsOutcome::Accepted => self
                .validate_commit_accepted
                .fetch_add(1, Ordering::Relaxed),
            ValidationStatsOutcome::Stale => {
                self.validate_commit_stale.fetch_add(1, Ordering::Relaxed)
            }
            ValidationStatsOutcome::SigFailure => self
                .validate_commit_sig_failures
                .fetch_add(1, Ordering::Relaxed),
            ValidationStatsOutcome::Rejected => self
                .validate_commit_rejected
                .fetch_add(1, Ordering::Relaxed),
        };
        add_duration_with_max(
            &self.validate_commit_micros,
            &self.max_validate_commit_micros,
            elapsed,
        );
    }

    pub fn record_validate_sync(&self, elapsed: Duration, outcome: ValidationStatsOutcome) {
        self.validate_sync_calls.fetch_add(1, Ordering::Relaxed);
        match outcome {
            ValidationStatsOutcome::Accepted => {
                self.validate_sync_accepted.fetch_add(1, Ordering::Relaxed)
            }
            ValidationStatsOutcome::SigFailure => self
                .validate_sync_sig_failures
                .fetch_add(1, Ordering::Relaxed),
            ValidationStatsOutcome::Stale | ValidationStatsOutcome::Rejected => {
                self.validate_sync_rejected.fetch_add(1, Ordering::Relaxed)
            }
        };
        add_duration(&self.validate_sync_micros, elapsed);
    }

    pub fn record_refresh_doc(&self, elapsed: Duration) {
        self.refresh_doc_calls.fetch_add(1, Ordering::Relaxed);
        add_duration_with_max(
            &self.refresh_doc_micros,
            &self.max_refresh_doc_micros,
            elapsed,
        );
    }

    pub fn record_new_account(&self, elapsed: Duration) {
        self.new_account_calls.fetch_add(1, Ordering::Relaxed);
        add_duration_with_max(
            &self.new_account_micros,
            &self.max_new_account_micros,
            elapsed,
        );
    }

    pub fn record_resolve_doc(&self, elapsed: Duration) {
        self.resolve_doc_calls.fetch_add(1, Ordering::Relaxed);
        add_duration(&self.resolve_doc_micros, elapsed);
    }

    #[cfg_attr(all(feature = "relay", not(feature = "indexer")), allow(dead_code))]
    pub fn record_repo_status_probe(&self, elapsed: Duration) {
        self.repo_status_probe_calls.fetch_add(1, Ordering::Relaxed);
        add_duration(&self.repo_status_probe_micros, elapsed);
    }

    #[cfg_attr(not(feature = "relay"), allow(dead_code))]
    pub fn record_queue_emit(&self, elapsed: Duration) {
        self.queue_emit_calls.fetch_add(1, Ordering::Relaxed);
        add_duration_with_max(
            &self.queue_emit_micros,
            &self.max_queue_emit_micros,
            elapsed,
        );
    }

    pub fn record_process_error(&self) {
        self.process_errors.fetch_add(1, Ordering::Relaxed);
        self.last_error_at.store(now_ts(), Ordering::Relaxed);
    }

    pub fn record_commit_error(&self) {
        self.commit_errors.fetch_add(1, Ordering::Relaxed);
        self.last_error_at.store(now_ts(), Ordering::Relaxed);
    }

    pub fn record_processed(&self, timings: RelayShardTimings) {
        self.processed_messages.fetch_add(1, Ordering::Relaxed);
        self.last_processed_at.store(now_ts(), Ordering::Relaxed);
        add_duration_with_max(
            &self.process_message_micros,
            &self.max_process_message_micros,
            timings.process_message,
        );
        add_duration(&self.stage_counts_micros, timings.stage_counts);
        add_duration_with_max(
            &self.stage_and_commit_micros,
            &self.max_stage_and_commit_micros,
            timings.stage_and_commit,
        );
        add_duration(&self.apply_counts_micros, timings.apply_counts);
        add_duration(&self.broadcast_micros, timings.broadcast);
        add_duration(&self.cursor_micros, timings.cursor);
        add_duration_with_max(&self.total_micros, &self.max_total_micros, timings.total);
    }

    pub(crate) fn snapshot(&self, id: usize) -> RelayShardStatsSnapshot {
        RelayShardStatsSnapshot {
            id,
            received_messages: self.received_messages.load(Ordering::Relaxed),
            info_messages: self.info_messages.load(Ordering::Relaxed),
            processed_messages: self.processed_messages.load(Ordering::Relaxed),
            process_errors: self.process_errors.load(Ordering::Relaxed),
            commit_errors: self.commit_errors.load(Ordering::Relaxed),
            commit_messages: self.commit_messages.load(Ordering::Relaxed),
            sync_messages: self.sync_messages.load(Ordering::Relaxed),
            identity_messages: self.identity_messages.load(Ordering::Relaxed),
            account_messages: self.account_messages.load(Ordering::Relaxed),
            repo_state_hits: self.repo_state_hits.load(Ordering::Relaxed),
            repo_state_misses: self.repo_state_misses.load(Ordering::Relaxed),
            repo_state_drops: self.repo_state_drops.load(Ordering::Relaxed),
            host_authority_checks: self.host_authority_checks.load(Ordering::Relaxed),
            host_authority_authorized: self.host_authority_authorized.load(Ordering::Relaxed),
            host_authority_stale: self.host_authority_stale.load(Ordering::Relaxed),
            host_authority_wrong: self.host_authority_wrong.load(Ordering::Relaxed),
            host_authority_errors: self.host_authority_errors.load(Ordering::Relaxed),
            fetch_key_calls: self.fetch_key_calls.load(Ordering::Relaxed),
            validate_commit_calls: self.validate_commit_calls.load(Ordering::Relaxed),
            validate_commit_accepted: self.validate_commit_accepted.load(Ordering::Relaxed),
            validate_commit_stale: self.validate_commit_stale.load(Ordering::Relaxed),
            validate_commit_sig_failures: self.validate_commit_sig_failures.load(Ordering::Relaxed),
            validate_commit_rejected: self.validate_commit_rejected.load(Ordering::Relaxed),
            validate_sync_calls: self.validate_sync_calls.load(Ordering::Relaxed),
            validate_sync_accepted: self.validate_sync_accepted.load(Ordering::Relaxed),
            validate_sync_sig_failures: self.validate_sync_sig_failures.load(Ordering::Relaxed),
            validate_sync_rejected: self.validate_sync_rejected.load(Ordering::Relaxed),
            refresh_doc_calls: self.refresh_doc_calls.load(Ordering::Relaxed),
            new_account_calls: self.new_account_calls.load(Ordering::Relaxed),
            resolve_doc_calls: self.resolve_doc_calls.load(Ordering::Relaxed),
            repo_status_probe_calls: self.repo_status_probe_calls.load(Ordering::Relaxed),
            queue_emit_calls: self.queue_emit_calls.load(Ordering::Relaxed),
            process_message_micros: self.process_message_micros.load(Ordering::Relaxed),
            load_repo_state_micros: self.load_repo_state_micros.load(Ordering::Relaxed),
            host_authority_micros: self.host_authority_micros.load(Ordering::Relaxed),
            handle_commit_micros: self.handle_commit_micros.load(Ordering::Relaxed),
            handle_sync_micros: self.handle_sync_micros.load(Ordering::Relaxed),
            handle_identity_micros: self.handle_identity_micros.load(Ordering::Relaxed),
            handle_account_micros: self.handle_account_micros.load(Ordering::Relaxed),
            fetch_key_micros: self.fetch_key_micros.load(Ordering::Relaxed),
            validate_commit_micros: self.validate_commit_micros.load(Ordering::Relaxed),
            validate_sync_micros: self.validate_sync_micros.load(Ordering::Relaxed),
            refresh_doc_micros: self.refresh_doc_micros.load(Ordering::Relaxed),
            new_account_micros: self.new_account_micros.load(Ordering::Relaxed),
            resolve_doc_micros: self.resolve_doc_micros.load(Ordering::Relaxed),
            repo_status_probe_micros: self.repo_status_probe_micros.load(Ordering::Relaxed),
            queue_emit_micros: self.queue_emit_micros.load(Ordering::Relaxed),
            stage_counts_micros: self.stage_counts_micros.load(Ordering::Relaxed),
            stage_and_commit_micros: self.stage_and_commit_micros.load(Ordering::Relaxed),
            apply_counts_micros: self.apply_counts_micros.load(Ordering::Relaxed),
            broadcast_micros: self.broadcast_micros.load(Ordering::Relaxed),
            cursor_micros: self.cursor_micros.load(Ordering::Relaxed),
            total_micros: self.total_micros.load(Ordering::Relaxed),
            max_process_message_micros: self.max_process_message_micros.load(Ordering::Relaxed),
            max_load_repo_state_micros: self.max_load_repo_state_micros.load(Ordering::Relaxed),
            max_host_authority_micros: self.max_host_authority_micros.load(Ordering::Relaxed),
            max_handle_commit_micros: self.max_handle_commit_micros.load(Ordering::Relaxed),
            max_fetch_key_micros: self.max_fetch_key_micros.load(Ordering::Relaxed),
            max_validate_commit_micros: self.max_validate_commit_micros.load(Ordering::Relaxed),
            max_refresh_doc_micros: self.max_refresh_doc_micros.load(Ordering::Relaxed),
            max_new_account_micros: self.max_new_account_micros.load(Ordering::Relaxed),
            max_queue_emit_micros: self.max_queue_emit_micros.load(Ordering::Relaxed),
            max_stage_and_commit_micros: self.max_stage_and_commit_micros.load(Ordering::Relaxed),
            max_total_micros: self.max_total_micros.load(Ordering::Relaxed),
            last_received_at: nonzero_i64(&self.last_received_at),
            last_processed_at: nonzero_i64(&self.last_processed_at),
            last_error_at: nonzero_i64(&self.last_error_at),
            last_seq: nonzero_i64(&self.last_seq),
            max_seq: nonzero_i64(&self.max_seq),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayWorkerStatsSnapshot {
    pub shards: Vec<RelayShardStatsSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayShardStatsSnapshot {
    pub id: usize,
    pub received_messages: u64,
    pub info_messages: u64,
    pub processed_messages: u64,
    pub process_errors: u64,
    pub commit_errors: u64,
    pub commit_messages: u64,
    pub sync_messages: u64,
    pub identity_messages: u64,
    pub account_messages: u64,
    pub repo_state_hits: u64,
    pub repo_state_misses: u64,
    pub repo_state_drops: u64,
    pub host_authority_checks: u64,
    pub host_authority_authorized: u64,
    pub host_authority_stale: u64,
    pub host_authority_wrong: u64,
    pub host_authority_errors: u64,
    pub fetch_key_calls: u64,
    pub validate_commit_calls: u64,
    pub validate_commit_accepted: u64,
    pub validate_commit_stale: u64,
    pub validate_commit_sig_failures: u64,
    pub validate_commit_rejected: u64,
    pub validate_sync_calls: u64,
    pub validate_sync_accepted: u64,
    pub validate_sync_sig_failures: u64,
    pub validate_sync_rejected: u64,
    pub refresh_doc_calls: u64,
    pub new_account_calls: u64,
    pub resolve_doc_calls: u64,
    pub repo_status_probe_calls: u64,
    pub queue_emit_calls: u64,
    pub process_message_micros: u64,
    pub load_repo_state_micros: u64,
    pub host_authority_micros: u64,
    pub handle_commit_micros: u64,
    pub handle_sync_micros: u64,
    pub handle_identity_micros: u64,
    pub handle_account_micros: u64,
    pub fetch_key_micros: u64,
    pub validate_commit_micros: u64,
    pub validate_sync_micros: u64,
    pub refresh_doc_micros: u64,
    pub new_account_micros: u64,
    pub resolve_doc_micros: u64,
    pub repo_status_probe_micros: u64,
    pub queue_emit_micros: u64,
    pub stage_counts_micros: u64,
    pub stage_and_commit_micros: u64,
    pub apply_counts_micros: u64,
    pub broadcast_micros: u64,
    pub cursor_micros: u64,
    pub total_micros: u64,
    pub max_process_message_micros: u64,
    pub max_load_repo_state_micros: u64,
    pub max_host_authority_micros: u64,
    pub max_handle_commit_micros: u64,
    pub max_fetch_key_micros: u64,
    pub max_validate_commit_micros: u64,
    pub max_refresh_doc_micros: u64,
    pub max_new_account_micros: u64,
    pub max_queue_emit_micros: u64,
    pub max_stage_and_commit_micros: u64,
    pub max_total_micros: u64,
    pub last_received_at: Option<i64>,
    pub last_processed_at: Option<i64>,
    pub last_error_at: Option<i64>,
    pub last_seq: Option<i64>,
    pub max_seq: Option<i64>,
}
