use std::time::Duration;

use jacquard_api::com_atproto::sync::get_repo_status::{
    GetRepoStatusOutput, GetRepoStatusOutputStatus,
};
use smol_str::SmolStr;

#[cfg(feature = "firehose-diagnostics")]
use crate::ingest::firehose_stats::{RelayMessageKind, ValidationStatsOutcome};
#[cfg(feature = "firehose-diagnostics")]
use crate::ingest::stream::SubscribeReposMessage;
#[cfg(feature = "firehose-diagnostics")]
use crate::ingest::validation::{
    CommitValidationError, SyncValidationError, ValidatedCommit, ValidatedSync,
};
use crate::types::{RepoState, RepoStatus};

pub mod context;
pub mod handlers;
pub mod worker;

pub(crate) use context::WorkerContext;
pub use worker::RelayWorker;
pub(crate) use worker::WorkerMessage;

pub(crate) const WRONG_HOST_AUTHORITY_RECHECK_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const WRONG_HOST_AUTHORITY_CACHE_PRUNE_AT: usize = 8192;

pub(crate) fn map_repo_status_probe(
    output: Option<GetRepoStatusOutput<'_>>,
) -> Option<RepoState<'static>> {
    let output = output?;

    let mut repo_state = RepoState::backfilling();
    repo_state.active = output.active;
    repo_state.status = match output.status {
        Some(GetRepoStatusOutputStatus::Takendown) => RepoStatus::Takendown,
        Some(GetRepoStatusOutputStatus::Suspended) => RepoStatus::Suspended,
        Some(GetRepoStatusOutputStatus::Deactivated) => RepoStatus::Deactivated,
        Some(GetRepoStatusOutputStatus::Deleted) => RepoStatus::Deleted,
        Some(GetRepoStatusOutputStatus::Desynchronized) => RepoStatus::Desynchronized,
        Some(GetRepoStatusOutputStatus::Throttled) => RepoStatus::Throttled,
        Some(GetRepoStatusOutputStatus::Other(s)) => RepoStatus::Error(s.into()),
        None => output
            .active
            .then_some(RepoStatus::Synced)
            .unwrap_or_else(|| RepoStatus::Error("unknown".into())),
    };

    Some(repo_state)
}

#[cfg(feature = "firehose-diagnostics")]
pub(crate) fn relay_message_kind(msg: &SubscribeReposMessage<'_>) -> Option<RelayMessageKind> {
    match msg {
        SubscribeReposMessage::Commit(_) => Some(RelayMessageKind::Commit),
        SubscribeReposMessage::Sync(_) => Some(RelayMessageKind::Sync),
        SubscribeReposMessage::Identity(_) => Some(RelayMessageKind::Identity),
        SubscribeReposMessage::Account(_) => Some(RelayMessageKind::Account),
        SubscribeReposMessage::Info(_) => None,
    }
}

#[cfg(feature = "firehose-diagnostics")]
pub(crate) fn commit_validation_outcome(
    res: &std::result::Result<ValidatedCommit<'_>, CommitValidationError>,
) -> ValidationStatsOutcome {
    match res {
        Ok(_) => ValidationStatsOutcome::Accepted,
        Err(CommitValidationError::StaleRev) => ValidationStatsOutcome::Stale,
        Err(CommitValidationError::SigFailure) => ValidationStatsOutcome::SigFailure,
        Err(_) => ValidationStatsOutcome::Rejected,
    }
}

#[cfg(feature = "firehose-diagnostics")]
pub(crate) fn sync_validation_outcome(
    res: &std::result::Result<ValidatedSync, SyncValidationError>,
) -> ValidationStatsOutcome {
    match res {
        Ok(_) => ValidationStatsOutcome::Accepted,
        Err(SyncValidationError::SigFailure) => ValidationStatsOutcome::SigFailure,
        Err(_) => ValidationStatsOutcome::Rejected,
    }
}

/// outcome of a host authority check.
pub(crate) enum AuthorityOutcome {
    /// stored pds matched the source host immediately.
    Authorized,
    /// pds migrated: doc now points to this host, but our stored state was stale.
    WasStale,
    /// host did not match even after doc resolution.
    WrongHost { expected: SmolStr },
}

#[cfg(test)]
mod tests {
    use super::*;
    use jacquard_common::types::did::Did;

    #[test]
    fn missing_repo_status_probe_falls_back_to_live_discovery() {
        assert!(map_repo_status_probe(None).is_none());
    }

    #[test]
    fn active_repo_status_probe_maps_to_synced_repo_state() {
        let repo_state = map_repo_status_probe(Some(GetRepoStatusOutput {
            did: Did::new("did:plc:testrepo").expect("valid did"),
            active: true,
            status: None,
            rev: None,
            extra_data: None,
        }))
        .expect("probe should map");

        assert!(repo_state.active);
        assert_eq!(repo_state.status, RepoStatus::Synced);
    }

    #[test]
    fn throttled_repo_status_probe_preserves_host_status() {
        let repo_state = map_repo_status_probe(Some(GetRepoStatusOutput {
            did: Did::new("did:plc:testrepo").expect("valid did"),
            active: true,
            status: Some(GetRepoStatusOutputStatus::Throttled),
            rev: None,
            extra_data: None,
        }))
        .expect("probe should map");

        assert!(repo_state.active);
        assert_eq!(repo_state.status, RepoStatus::Throttled);
    }
}
