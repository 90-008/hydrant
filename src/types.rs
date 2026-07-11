use std::fmt::Display;

use jacquard_common::types::string::Did;
use jacquard_common::{CowStr, IntoStatic};
use jacquard_repo::commit::Commit as AtpCommit;
#[cfg(feature = "indexer")]
use serde::{Deserialize, Serialize};
use smol_str::ToSmolStr;

use crate::db::types::DbTid;
use crate::resolver::MiniDoc;

#[cfg(any(feature = "indexer_stream", feature = "relay", feature = "jetstream"))]
pub(crate) mod event;
pub(crate) mod v2;
pub(crate) mod v4;
pub(crate) mod v7;

#[cfg(any(feature = "indexer_stream", feature = "relay", feature = "jetstream"))]
pub(crate) use event::*;
pub(crate) use v7::*;

impl<'c> From<AtpCommit<'c>> for Commit {
    fn from(value: AtpCommit<'c>) -> Self {
        Self {
            data: value.data,
            prev: value.prev,
            rev: DbTid::from(&value.rev),
            sig: value.sig,
            version: value.version,
        }
    }
}

impl Commit {
    pub(crate) fn into_atp_commit<'i>(self, did: Did<'i>) -> Option<AtpCommit<'i>> {
        // version < 0 is a sentinel used in v2 migration for repos with no commit data
        if self.version < 0 {
            return None;
        }
        Some(AtpCommit {
            did,
            rev: self.rev.to_tid(),
            data: self.data,
            prev: self.prev,
            sig: self.sig,
            version: self.version,
        })
    }
}

impl Display for RepoStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoStatus::Synced => write!(f, "synced"),
            RepoStatus::Error(e) => write!(f, "error({e})"),
            RepoStatus::Deactivated => write!(f, "deactivated"),
            RepoStatus::Takendown => write!(f, "takendown"),
            RepoStatus::Suspended => write!(f, "suspended"),
            RepoStatus::Deleted => write!(f, "deleted"),
            RepoStatus::Desynchronized => write!(f, "desynchronized"),
            RepoStatus::Throttled => write!(f, "throttled"),
        }
    }
}


#[cfg(feature = "indexer")]
mod indexer {
    use super::*;

    impl RepoMetadata {
        pub fn backfilling(index_id: u64) -> Self {
            Self {
                index_id,
                tracked: true,
            }
        }
    }

    impl ResyncState {
        pub fn next_backoff(retry_count: u32) -> i64 {
            // exponential backoff: 1m, 2m, 4m, 8m... up to 1h
            let base = 60;
            let cap = 3600;
            let mult = 2u64.pow(retry_count.min(10)) as i64;
            let delay = (base * mult).min(cap);

            // add +/- 10% jitter
            let jitter = (rand::random::<f64>() * 0.2 - 0.1) * delay as f64;
            let delay = (delay as f64 + jitter) as i64;

            chrono::Utc::now().timestamp() + delay
        }
    }

    #[cfg(feature = "indexer_stream")]
    #[derive(Clone, Debug)]
    pub(crate) enum BroadcastEvent {
        Persisted(#[allow(dead_code)] u64),
        /// a durable record event with optional inline block bytes for live tailing.
        ///
        /// used to avoid re-reading `events`/`blocks` from the database when tailing.
        LiveRecord(std::sync::Arc<super::LiveRecordEvent>),
        Ephemeral(Box<MarshallableEvt<'static>>),
    }

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    pub(crate) enum GaugeState {
        Synced,
        Pending,
        Resync(Option<ResyncErrorKind>),
    }

    impl GaugeState {
        pub fn is_resync(&self) -> bool {
            matches!(self, GaugeState::Resync(_))
        }
    }
}

#[cfg(feature = "indexer")]
pub(crate) use indexer::*;

impl<'i> RepoState<'i> {
    pub fn backfilling() -> Self {
        Self {
            active: true,
            status: RepoStatus::Desynchronized,
            root: None,
            last_updated_at: chrono::Utc::now().timestamp(),
            handle: None,
            pds: None,
            signing_key: None,
            last_message_time: None,
            last_identity_time: None,
            last_account_time: None,
        }
    }

    #[cfg(any(test, feature = "relay"))]
    pub fn synced() -> Self {
        Self {
            status: RepoStatus::Synced,
            ..Self::backfilling()
        }
    }

    // advances the high-water mark to event_ms if it's newer than what we've seen
    pub fn advance_message_time(&mut self, event_ms: i64) {
        self.last_message_time = Some(event_ms.max(self.last_message_time.unwrap_or(0)));
    }

    pub fn should_process_identity_time(&self, event_ms: i64) -> bool {
        self.last_identity_time.is_none_or(|t| event_ms > t)
    }

    pub fn advance_identity_time(&mut self, event_ms: i64) {
        self.last_identity_time = Some(event_ms.max(self.last_identity_time.unwrap_or(0)));
        self.advance_message_time(event_ms);
    }

    pub fn should_process_account_time(&self, event_ms: i64) -> bool {
        self.last_account_time.is_none_or(|t| event_ms > t)
    }

    pub fn advance_account_time(&mut self, event_ms: i64) {
        self.last_account_time = Some(event_ms.max(self.last_account_time.unwrap_or(0)));
        self.advance_message_time(event_ms);
    }

    // updates last_updated_at to now
    pub fn touch(&mut self) {
        self.last_updated_at = chrono::Utc::now().timestamp();
    }

    pub fn update_from_doc(&mut self, doc: MiniDoc) -> bool {
        let new_signing_key = doc.key.map(From::from);
        let changed = self.pds.as_deref() != Some(doc.pds.as_str())
            || self.handle != doc.handle
            || self.signing_key != new_signing_key;
        self.pds = Some(CowStr::Owned(doc.pds.to_smolstr()));
        self.handle = doc.handle;
        self.signing_key = new_signing_key;
        changed
    }
}

impl<'i> IntoStatic for RepoState<'i> {
    type Output = RepoState<'static>;

    fn into_static(self) -> Self::Output {
        RepoState {
            active: self.active,
            status: self.status,
            root: self.root,
            last_updated_at: self.last_updated_at,
            handle: self.handle.map(IntoStatic::into_static),
            pds: self.pds.map(IntoStatic::into_static),
            signing_key: self.signing_key.map(IntoStatic::into_static),
            last_message_time: self.last_message_time,
            last_identity_time: self.last_identity_time,
            last_account_time: self.last_account_time,
        }
    }
}

#[cfg(feature = "indexer")]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ResyncErrorKind {
    Ratelimited,
    Transport,
    Generic,
}

#[cfg(feature = "indexer")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ResyncState {
    Error {
        kind: ResyncErrorKind,
        retry_count: u32,
        next_retry: i64, // unix timestamp
    },
    Gone {
        status: RepoStatus, // deactivated, takendown, suspended
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::types::DidKey;
    use jacquard_common::types::string::Handle;
    use miette::IntoDiagnostic;

    #[test]
    fn identity_dedupe_does_not_depend_on_commit_clock() {
        let mut state = RepoState::backfilling();

        state.advance_message_time(2_000);

        assert!(state.should_process_identity_time(1_500));
        state.advance_identity_time(1_500);
        assert_eq!(state.last_message_time, Some(2_000));
        assert_eq!(state.last_identity_time, Some(1_500));

        assert!(!state.should_process_identity_time(1_500));
        assert!(!state.should_process_identity_time(1_499));
        assert!(state.should_process_identity_time(1_501));
    }

    #[test]
    fn account_dedupe_does_not_depend_on_commit_clock() {
        let mut state = RepoState::backfilling();

        state.advance_message_time(3_000);

        assert!(state.should_process_account_time(2_500));
        state.advance_account_time(2_500);
        assert_eq!(state.last_message_time, Some(3_000));
        assert_eq!(state.last_account_time, Some(2_500));

        assert!(!state.should_process_account_time(2_500));
        assert!(!state.should_process_account_time(2_499));
        assert!(state.should_process_account_time(2_501));
    }

    #[test]
    fn synced_state_is_active_without_backfilling_status() {
        let state = RepoState::synced();
        assert!(state.active);
        assert_eq!(state.status, RepoStatus::Synced);
        assert!(state.root.is_none());
    }

    #[test]
    fn into_static_preserves_per_event_clocks() -> miette::Result<()> {
        let mut state = RepoState::backfilling();
        state.last_message_time = Some(10);
        state.last_identity_time = Some(20);
        state.last_account_time = Some(30);
        state.handle = Some(Handle::new("alice.test").into_diagnostic()?);
        state.pds = Some(CowStr::Borrowed("https://pds.example"));
        state.signing_key = Some(DidKey::from_did_key(
            "did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme",
        )?);

        let static_state = state.into_static();

        assert_eq!(static_state.last_message_time, Some(10));
        assert_eq!(static_state.last_identity_time, Some(20));
        assert_eq!(static_state.last_account_time, Some(30));

        Ok(())
    }
}
