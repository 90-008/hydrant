//! sink for builds with neither indexer nor relay: validated events update
//! repo state but are not forwarded anywhere.

use fjall::OwnedWriteBatch;
use miette::Result;
use url::Url;

use crate::ingest::stream::{Account, Commit, Identity, Sync};
use crate::state::AppState;
use crate::types::RepoState;

/// per-worker seed from which each shard builds its sink.
#[derive(Clone, Default)]
pub(crate) struct SinkSeed;

pub(crate) struct EventSink;

pub(crate) struct Staged;

pub(crate) struct IdentitySnapshot;

pub(crate) struct AccountSnapshot;

impl EventSink {
    pub(crate) fn new(
        _seed: &SinkSeed,
        _stats: std::sync::Arc<crate::ingest::firehose_stats::RelayShardStats>,
    ) -> Self {
        Self
    }

    pub(crate) fn new_repo(&mut self, _did: jacquard_common::types::did::Did<'static>) {}

    pub(crate) fn identity_snapshot(&self, _repo_state: &RepoState) -> IdentitySnapshot {
        IdentitySnapshot
    }

    pub(crate) fn account_snapshot(&self, _repo_state: &RepoState) -> AccountSnapshot {
        AccountSnapshot
    }

    pub(crate) fn commit(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        _commit: Commit<'static>,
        _chain_break: bool,
        _parsed_blocks: jacquard_repo::car::reader::ParsedCar,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) fn sync(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        _sync: Sync<'static>,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) fn identity(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        _identity: Identity<'static>,
        _repo_state: &RepoState,
        _snapshot: IdentitySnapshot,
    ) -> Result<()> {
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn account(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        _account: Account<'static>,
        _repo_state: &RepoState,
        _snapshot: AccountSnapshot,
        _was_active: bool,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) fn commit_txn(
        &mut self,
        _state: &AppState,
        txn: crate::db::Txn<'_>,
    ) -> Result<(Staged, crate::db::TxnCommitTimings)> {
        let (_, timings) = txn.commit_with(|_| Ok(()))?;
        Ok((Staged, timings))
    }

    pub(crate) fn flush(&mut self, _state: &AppState, _staged: Staged) {}

    pub(crate) fn advance_cursor(&self, _state: &AppState, _firehose: &Url, _seq: i64) {}
}
