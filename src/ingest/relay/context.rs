use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use fjall::OwnedWriteBatch;
use jacquard_api::com_atproto::sync::get_repo_status::{GetRepoStatus, GetRepoStatusError};
use jacquard_common::IntoStatic;
use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::did::Did;
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use miette::{IntoDiagnostic, Result};
use smol_str::{SmolStr, ToSmolStr};
use tokio::runtime::Handle;
use tracing::{debug, trace, warn};
use url::Url;

use crate::db::{self, CountDeltas, keys};
use crate::state::AppState;
use crate::types::{RepoState, RepoStatus};
use crate::util;

use super::{
    AuthorityOutcome, RelayWorker, WRONG_HOST_AUTHORITY_CACHE_PRUNE_AT,
    WRONG_HOST_AUTHORITY_RECHECK_INTERVAL, WorkerMessage, map_repo_status_probe,
};
use crate::ingest::stream::AccountStatus;
#[cfg(feature = "indexer")]
use crate::ingest::stream::SubscribeReposMessage;
use crate::ingest::validation::{
    CommitValidationError, SyncValidationError, ValidatedCommit, ValidatedSync, ValidationContext,
};

use super::{commit_validation_outcome, sync_validation_outcome};
use crate::ingest::firehose_stats::StatsInstant;

pub(crate) struct WorkerContext<'a> {
    pub(crate) verify_signatures: bool,
    pub(crate) state: &'a AppState,
    pub(crate) vctx: ValidationContext<'a>,
    pub(crate) stats: Arc<crate::ingest::firehose_stats::RelayShardStats>,
    pub(crate) batch: OwnedWriteBatch,
    pub(crate) count_deltas: CountDeltas,
    pub(crate) sink: super::sink::EventSink,
    pub(crate) http: reqwest::Client,
    pub(crate) error_counts: HashMap<u64, u32, nohash_hasher::BuildNoHashHasher<u64>>,
    pub(crate) wrong_host_authority: HashMap<u64, Instant, nohash_hasher::BuildNoHashHasher<u64>>,
}

impl WorkerContext<'_> {
    /// increments host error counter, returns if host is suppressed or not
    pub(crate) fn inc_error(&mut self, host: &str) -> bool {
        let error_count = self.error_counts.entry(util::hash(&host)).or_default();
        let is_suppressed = *error_count > 50;
        *error_count += 1;
        is_suppressed
    }

    pub(crate) fn reset_error(&mut self, host: &str) {
        if let Some(count) = self.error_counts.get_mut(&util::hash(&host)) {
            *count = 0;
        }
    }

    fn wrong_host_authority_key(did: &Did, source_host: &str) -> u64 {
        util::hash(&(did.as_str(), source_host))
    }

    fn recently_rejected_wrong_host_authority(&mut self, cache_key: u64) -> bool {
        if self
            .wrong_host_authority
            .get(&cache_key)
            .is_some_and(|checked_at| checked_at.elapsed() < WRONG_HOST_AUTHORITY_RECHECK_INTERVAL)
        {
            return true;
        }

        self.wrong_host_authority.remove(&cache_key);
        false
    }

    fn remember_wrong_host_authority(&mut self, cache_key: u64) {
        if self.wrong_host_authority.len() >= WRONG_HOST_AUTHORITY_CACHE_PRUNE_AT {
            self.wrong_host_authority.retain(|_, checked_at| {
                checked_at.elapsed() < WRONG_HOST_AUTHORITY_RECHECK_INTERVAL
            });
            if self.wrong_host_authority.len() >= WRONG_HOST_AUTHORITY_CACHE_PRUNE_AT {
                self.wrong_host_authority.clear();
            }
        }
        self.wrong_host_authority.insert(cache_key, Instant::now());
    }

    pub(crate) fn check_host_authority(
        &mut self,
        did: &Did,
        repo_state: &mut RepoState,
        source_host: &str,
    ) -> Result<AuthorityOutcome> {
        let pds_host = |pds: &str| {
            Url::parse(pds)
                .ok()
                .and_then(|u| u.host_str().map(SmolStr::new))
        };

        let cache_key = Self::wrong_host_authority_key(did, source_host);
        let expected = repo_state.pds.as_deref().and_then(pds_host);
        if expected.as_deref() == Some(source_host) {
            self.wrong_host_authority.remove(&cache_key);
            return Ok(AuthorityOutcome::Authorized);
        }
        if let Some(expected) = expected.clone()
            && self.recently_rejected_wrong_host_authority(cache_key)
        {
            return Ok(AuthorityOutcome::WrongHost { expected });
        }

        // try again once
        self.refresh_doc(did, repo_state)?;
        let Some(expected) = repo_state.pds.as_deref().and_then(pds_host) else {
            miette::bail!("can't get pds host???");
        };

        if expected.as_str() == source_host {
            self.wrong_host_authority.remove(&cache_key);
            Ok(AuthorityOutcome::WasStale)
        } else {
            self.remember_wrong_host_authority(cache_key);
            Ok(AuthorityOutcome::WrongHost { expected })
        }
    }

    pub(crate) fn refresh_doc(&mut self, did: &Did, repo_state: &mut RepoState) -> Result<()> {
        let refresh_started = StatsInstant::now();
        let result = (|| {
            let db = &self.state.db;
            self.state.resolver.invalidate_sync(did);
            let resolve_started = StatsInstant::now();
            let doc = Handle::current()
                .block_on(self.state.resolver.resolve_doc(did))
                .map_err(|e| miette::miette!("{e}"));
            self.stats.record_resolve_doc(resolve_started.elapsed());
            let doc = doc?;
            repo_state.update_from_doc(doc);
            repo_state.touch();

            self.batch.insert(
                &db.repos,
                keys::repo_key(did),
                db::ser_repo_state(repo_state)?,
            );
            Ok(())
        })();
        self.stats.record_refresh_doc(refresh_started.elapsed());
        result
    }

    pub(crate) fn validate_commit<'c>(
        &mut self,
        repo_state: &mut RepoState,
        commit: &'c crate::ingest::stream::Commit<'c>,
    ) -> Result<Option<ValidatedCommit<'c>>> {
        let did = &commit.repo;
        let key = self.fetch_key(did)?;
        let validate_started = StatsInstant::now();
        let validation = self.vctx.validate_commit(commit, repo_state, key.as_ref());
        self.stats.record_validate_commit(
            validate_started.elapsed(),
            commit_validation_outcome(&validation),
        );
        match validation {
            Ok(v) => return Ok(Some(v)),
            Err(CommitValidationError::StaleRev) => {
                trace!("skipping replayed commit");
                return Ok(None);
            }
            Err(CommitValidationError::SigFailure) => {}
            Err(e) => {
                debug!(err = %e, "commit rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(did, repo_state)?;
        let key = self.fetch_key(did)?;
        let validate_started = StatsInstant::now();
        let validation = self.vctx.validate_commit(commit, repo_state, key.as_ref());
        self.stats.record_validate_commit(
            validate_started.elapsed(),
            commit_validation_outcome(&validation),
        );
        match validation {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                debug!(err = %e, "commit rejected after key refresh");
                Ok(None)
            }
        }
    }

    pub(crate) fn validate_sync(
        &mut self,
        repo_state: &mut RepoState,
        sync: &crate::ingest::stream::Sync<'_>,
    ) -> Result<Option<ValidatedSync>> {
        let did = &sync.did;
        let key = self.fetch_key(did)?;
        let validate_started = StatsInstant::now();
        let validation = self.vctx.validate_sync(sync, key.as_ref());
        self.stats.record_validate_sync(
            validate_started.elapsed(),
            sync_validation_outcome(&validation),
        );
        match validation {
            Ok(v) => return Ok(Some(v)),
            Err(SyncValidationError::SigFailure) => {}
            Err(e) => {
                debug!(err = %e, "sync rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(did, repo_state)?;
        let key = self.fetch_key(did)?;
        let validate_started = StatsInstant::now();
        let validation = self.vctx.validate_sync(sync, key.as_ref());
        self.stats.record_validate_sync(
            validate_started.elapsed(),
            sync_validation_outcome(&validation),
        );
        match validation {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                debug!(err = %e, "sync rejected after key refresh");
                Ok(None)
            }
        }
    }

    fn fetch_key(&self, did: &Did) -> Result<Option<PublicKey<'static>>> {
        let started = StatsInstant::now();
        let result = if self.verify_signatures {
            Handle::current()
                .block_on(self.state.resolver.resolve_signing_key(did))
                .map(Some)
                .map_err(|e| miette::miette!("{e}"))
        } else {
            Ok(None)
        };
        self.stats.record_fetch_key(started.elapsed());
        result
    }

    /// maps an inactive account status to the corresponding `RepoStatus`.
    /// panics on `AccountStatus::Deleted`, caller must handle that
    pub(crate) fn inactive_account_repo_status(
        &self,
        did: &Did,
        status: &Option<AccountStatus<'_>>,
    ) -> RepoStatus {
        match status {
            Some(AccountStatus::Takendown) => RepoStatus::Takendown,
            Some(AccountStatus::Suspended) => RepoStatus::Suspended,
            Some(AccountStatus::Deactivated) => RepoStatus::Deactivated,
            Some(AccountStatus::Throttled) => RepoStatus::Throttled,
            Some(AccountStatus::Desynchronized) => RepoStatus::Desynchronized,
            Some(AccountStatus::Other(s)) => {
                warn!(did = %did, status = %s, "unknown account status");
                RepoStatus::Error(s.to_smolstr())
            }
            Some(AccountStatus::Deleted) => {
                unreachable!("deleted is handled before status mapping")
            }
            None => {
                warn!(did = %did, "account inactive but no status provided");
                RepoStatus::Error("unknown".into())
            }
        }
    }

    #[cfg_attr(all(feature = "relay", not(feature = "indexer")), allow(dead_code))]
    pub(crate) async fn check_repo_status(
        &self,
        did: &Did<'_>,
        pds: &Url,
    ) -> Result<Option<RepoState<'static>>> {
        let req = GetRepoStatus::new().did(did.clone().into_static()).build();
        let resp = self
            .http
            .xrpc(crate::util::url_to_fluent_uri(pds))
            .send(&req)
            .await;

        let output = match resp {
            Err(_) => return Ok(None),
            Ok(r) => match r.into_output() {
                Ok(o) => o,
                Err(XrpcError::Xrpc(GetRepoStatusError::RepoNotFound(_))) => {
                    // treat probe-time 404s like any other transient probe failure.
                    // we already have a live event from the authoritative host, so
                    // inserting an inactive placeholder here can wedge the repo until
                    // a later account event arrives.
                    return Ok(map_repo_status_probe(None));
                }
                Err(_) => return Ok(None),
            },
        };

        Ok(map_repo_status_probe(Some(output)))
    }

    pub(crate) fn load_repo_state(
        &mut self,
        msg: &WorkerMessage,
    ) -> Result<Option<RepoState<'static>>> {
        let db = &self.state.db;
        let did = msg.msg.did().expect("we checked if valid");
        let repo_key = keys::repo_key(did);
        let metadata_key = keys::repo_metadata_key(did);

        let metadata = db
            .repo_metadata
            .get(&metadata_key)
            .into_diagnostic()?
            .map(|bytes| db::deser_repo_meta(&bytes))
            .transpose()?;

        if metadata.is_some_and(|m| !m.tracked) {
            trace!(did = %did, "ignoring message, repo is explicitly untracked");
            self.stats.record_repo_state_outcome(
                crate::ingest::firehose_stats::RepoStateLoadOutcome::Drop,
            );
            return Ok(None);
        }

        let repo_state_opt = db
            .repos
            .get(&repo_key)
            .into_diagnostic()?
            .map(|bytes| db::deser_repo_state(bytes.as_ref()).map(|s| s.into_static()))
            .transpose()?;

        if let Some(repo_state) = repo_state_opt {
            self.stats.record_repo_state_outcome(
                crate::ingest::firehose_stats::RepoStateLoadOutcome::Hit,
            );
            return Ok(Some(repo_state));
        }

        #[cfg(feature = "indexer")]
        {
            let filter = self.state.filter.load();
            if filter.mode == crate::filter::FilterMode::Filter && !filter.signals.is_empty() {
                let commit = match &msg.msg {
                    SubscribeReposMessage::Commit(c) => c,
                    _ => {
                        self.stats.record_repo_state_outcome(
                            crate::ingest::firehose_stats::RepoStateLoadOutcome::Drop,
                        );
                        return Ok(None);
                    }
                };
                let touches_signal = commit.ops.iter().any(|op| {
                    op.path
                        .split_once('/')
                        .map(|(col, _)| {
                            let m = filter.matches_signal(col);
                            debug!(
                                did = %did, path = %op.path, col = %col,
                                signals = ?filter.signals, matched = m,
                                "signal check"
                            );
                            m
                        })
                        .unwrap_or(false)
                });
                if !touches_signal {
                    trace!(did = %did, "dropping commit, no signal-matching ops");
                    self.stats.record_repo_state_outcome(
                        crate::ingest::firehose_stats::RepoStateLoadOutcome::Drop,
                    );
                    return Ok(None);
                }
            }
        }

        debug!(did = %did, "discovered new account from firehose, queueing backfill");
        let new_account_started = StatsInstant::now();

        // resolve doc to initialize repo state
        self.state.resolver.invalidate_sync(did);
        let resolve_started = StatsInstant::now();
        let doc = tokio::runtime::Handle::current()
            .block_on(self.state.resolver.resolve_doc(did))
            .into_diagnostic();
        self.stats.record_resolve_doc(resolve_started.elapsed());
        let doc = doc?;

        // if it's a PDS, verify it's the authoritative one
        if msg.is_pds {
            let pds_host = doc.pds.host_str().map(|h| h.to_string());
            if pds_host.as_deref() != msg.firehose.host_str() {
                warn!(did = %did, got = ?pds_host, expected = ?msg.firehose.host_str(), "message rejected: wrong host for new account");
                self.stats.record_repo_state_outcome(
                    crate::ingest::firehose_stats::RepoStateLoadOutcome::Drop,
                );
                return Ok(None);
            }

            if let Some(host) = msg.firehose.host_str() {
                let count = self
                    .state
                    .db
                    .get_count_sync(&keys::pds_account_count_key(host));
                if self.state.is_over_account_limit(host, count) {
                    warn!(did = %did, host, count, "account limit reached for host, dropping new account");
                    self.stats.record_repo_state_outcome(
                        crate::ingest::firehose_stats::RepoStateLoadOutcome::Drop,
                    );
                    return Ok(None);
                }
            }
        }

        #[cfg(any(not(feature = "relay"), feature = "indexer"))]
        let mut repo_state = {
            // try to get upstream status
            let probe_started = StatsInstant::now();
            let repo_state =
                tokio::runtime::Handle::current().block_on(self.check_repo_status(did, &doc.pds));
            self.stats.record_repo_status_probe(probe_started.elapsed());
            repo_state
                .ok()
                .flatten()
                .unwrap_or_else(RepoState::backfilling)
        };

        #[cfg(all(feature = "relay", not(feature = "indexer")))]
        let mut repo_state = RepoState::synced();

        repo_state.update_from_doc(doc);
        RelayWorker::update_pds_account_count(
            self,
            false,
            None,
            repo_state.active,
            RelayWorker::pds_host(repo_state.pds.as_deref()).as_deref(),
        );

        self.batch.insert(
            &db.repos,
            &repo_key,
            crate::db::ser_repo_state(&repo_state)?,
        );

        self.sink.new_repo(did.clone().into_static());

        self.count_deltas.add_repos(1);

        self.stats.record_repo_state_outcome(
            crate::ingest::firehose_stats::RepoStateLoadOutcome::Miss,
        );
        self.stats.record_new_account(new_account_started.elapsed());

        Ok(Some(repo_state))
    }
}
