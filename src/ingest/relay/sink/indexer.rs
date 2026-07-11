use fjall::OwnedWriteBatch;
use jacquard_common::IntoStatic;
use jacquard_common::types::string::Handle;
use miette::Result;
use url::Url;

use crate::db::types::DidKey;
use crate::ingest::indexer::{
    IndexerAccountData, IndexerCommitData, IndexerEvent, IndexerEventData, IndexerIdentityData,
    IndexerMessage, IndexerTx,
};
use crate::ingest::stream::{Account, Commit, Identity, Sync};
use crate::state::AppState;
use crate::types::{RepoState, RepoStatus};

/// per-worker seed from which each shard builds its sink.
#[derive(Clone)]
pub(crate) struct SinkSeed {
    hook: IndexerTx,
}

impl SinkSeed {
    pub(crate) fn new(hook: IndexerTx) -> Self {
        Self { hook }
    }
}

/// forwards validated events to the indexer hook after batch commit.
pub(crate) struct EventSink {
    hook: IndexerTx,
    pending: Vec<IndexerMessage>,
}

pub(crate) struct Staged;

pub(crate) struct IdentitySnapshot {
    handle: Option<Handle<'static>>,
    signing_key: Option<DidKey<'static>>,
}

pub(crate) struct AccountSnapshot {
    status: RepoStatus,
}

impl EventSink {
    pub(crate) fn new(
        seed: &SinkSeed,
        _stats: std::sync::Arc<crate::ingest::firehose_stats::RelayShardStats>,
    ) -> Self {
        Self {
            hook: seed.hook.clone(),
            pending: Vec::with_capacity(2),
        }
    }

    pub(crate) fn new_repo(&mut self, did: jacquard_common::types::did::Did<'static>) {
        self.pending.push(IndexerMessage::NewRepo(did));
    }

    pub(crate) fn identity_snapshot(&self, repo_state: &RepoState) -> IdentitySnapshot {
        IdentitySnapshot {
            handle: repo_state.handle.clone().map(IntoStatic::into_static),
            signing_key: repo_state.signing_key.clone().map(IntoStatic::into_static),
        }
    }

    pub(crate) fn account_snapshot(&self, repo_state: &RepoState) -> AccountSnapshot {
        AccountSnapshot {
            status: repo_state.status.clone(),
        }
    }

    pub(crate) fn commit(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        firehose: &Url,
        commit: Commit<'static>,
        chain_break: bool,
        parsed_blocks: jacquard_repo::car::reader::ParsedCar,
    ) -> Result<()> {
        self.pending
            .push(IndexerMessage::Event(Box::new(IndexerEvent {
                seq: commit.seq,
                firehose: firehose.clone(),
                data: IndexerEventData::Commit(IndexerCommitData {
                    commit,
                    chain_break,
                    parsed_blocks,
                }),
            })));
        Ok(())
    }

    pub(crate) fn sync(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        firehose: &Url,
        sync: Sync<'static>,
    ) -> Result<()> {
        self.pending
            .push(IndexerMessage::Event(Box::new(IndexerEvent {
                seq: sync.seq,
                firehose: firehose.clone(),
                data: IndexerEventData::Sync(sync.did.into_static()),
            })));
        Ok(())
    }

    pub(crate) fn identity(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        firehose: &Url,
        identity: Identity<'static>,
        repo_state: &RepoState,
        snapshot: IdentitySnapshot,
    ) -> Result<()> {
        let changed =
            repo_state.handle != snapshot.handle || repo_state.signing_key != snapshot.signing_key;
        self.pending
            .push(IndexerMessage::Event(Box::new(IndexerEvent {
                seq: identity.seq,
                firehose: firehose.clone(),
                data: IndexerEventData::Identity(IndexerIdentityData { identity, changed }),
            })));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn account(
        &mut self,
        _state: &AppState,
        _batch: &mut OwnedWriteBatch,
        firehose: &Url,
        account: Account<'static>,
        repo_state: &RepoState,
        snapshot: AccountSnapshot,
        was_active: bool,
    ) -> Result<()> {
        let changed = repo_state.active != was_active || repo_state.status != snapshot.status;
        self.pending
            .push(IndexerMessage::Event(Box::new(IndexerEvent {
                seq: account.seq,
                firehose: firehose.clone(),
                data: IndexerEventData::Account(IndexerAccountData {
                    account,
                    was_active,
                    changed,
                }),
            })));
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

    pub(crate) fn flush(&mut self, _state: &AppState, _staged: Staged) {
        for msg in self.pending.drain(..) {
            let _ = self.hook.blocking_send(msg);
        }
    }

    pub(crate) fn advance_cursor(&self, _state: &AppState, _firehose: &Url, _seq: i64) {
        // in indexer mode the FirehoseWorker advances the cursor after processing
    }
}
