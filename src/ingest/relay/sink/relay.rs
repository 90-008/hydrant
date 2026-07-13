use fjall::OwnedWriteBatch;
use miette::Result;
#[cfg(feature = "jetstream")]
use smol_str::ToSmolStr;
use std::sync::atomic::Ordering;
use url::Url;

use crate::db::keys;
#[cfg(feature = "jetstream")]
use crate::db::types::TrimmedDid;
use crate::ingest::stream::{Account, Commit, Identity, Sync, encode_frame};
use crate::state::AppState;
#[cfg(feature = "jetstream")]
use crate::types::StoredJetstreamEvent;
use crate::types::{RelayBroadcast, RepoState};
#[cfg(feature = "jetstream")]
use jacquard_common::CowStr;

/// per-worker seed from which each shard builds its sink.
#[derive(Clone, Default)]
pub(crate) struct SinkSeed;

/// re-sequences validated events and re-emits them on the relay stream,
/// plus jetstream events when that feature is enabled.
pub(crate) struct EventSink {
    stats: std::sync::Arc<crate::ingest::firehose_stats::RelayShardStats>,
    broadcasts: Vec<RelayBroadcast>,
    #[cfg(feature = "jetstream")]
    jetstream_events: Vec<(
        StoredJetstreamEvent<'static>,
        Option<crate::jetstream::JetstreamEphemeral>,
    )>,
}

pub(crate) struct Staged {
    #[cfg(feature = "jetstream")]
    jetstream_broadcasts: Vec<crate::types::JetstreamBroadcast>,
}

pub(crate) struct IdentitySnapshot;

pub(crate) struct AccountSnapshot;

impl EventSink {
    pub(crate) fn new(
        _seed: &SinkSeed,
        stats: std::sync::Arc<crate::ingest::firehose_stats::RelayShardStats>,
    ) -> Self {
        Self {
            stats,
            broadcasts: Vec::with_capacity(2),
            #[cfg(feature = "jetstream")]
            jetstream_events: Vec::with_capacity(2),
        }
    }

    pub(crate) fn new_repo(&mut self, _did: jacquard_common::types::did::Did<'static>) {}

    pub(crate) fn identity_snapshot(&self, _repo_state: &RepoState) -> IdentitySnapshot {
        IdentitySnapshot
    }

    pub(crate) fn account_snapshot(&self, _repo_state: &RepoState) -> AccountSnapshot {
        AccountSnapshot
    }

    /// assigns the next relay sequence number, persists the re-encoded frame,
    /// and queues its broadcast.
    fn queue_emit(
        &mut self,
        state: &AppState,
        batch: &mut OwnedWriteBatch,
        make_frame: impl FnOnce(i64) -> Result<bytes::Bytes>,
    ) -> Result<u64> {
        let started = crate::ingest::firehose_stats::StatsInstant::now();
        let result = (|| {
            let db = &state.db;
            let seq = db.relay.next_seq.fetch_add(1, Ordering::SeqCst);
            let frame = make_frame(seq as i64)?;
            batch.insert(&db.relay.events, keys::relay_event_key(seq), frame.as_ref());
            self.broadcasts.push(RelayBroadcast::Ephemeral(seq, frame));
            self.broadcasts.push(RelayBroadcast::Persisted(seq));
            Ok(seq)
        })();
        self.stats.record_queue_emit(started.elapsed());
        result
    }

    pub(crate) fn commit(
        &mut self,
        state: &AppState,
        batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        mut commit: Commit<'static>,
        _chain_break: bool,
        parsed_blocks: jacquard_repo::car::reader::ParsedCar,
    ) -> Result<()> {
        let _ = &parsed_blocks;
        #[cfg(feature = "jetstream")]
        let jetstream_ops = commit
            .ops
            .iter()
            .enumerate()
            .filter_map(|(idx, op)| {
                matches!(op.action.as_str(), "create" | "update" | "delete")
                    .then_some(op)
                    .and_then(|op| split_collection(&op.path))
                    .map(|collection| (idx as u32, collection))
            })
            .collect::<Vec<_>>();
        #[cfg(feature = "jetstream")]
        let jetstream_did = TrimmedDid::from(&commit.repo).into_static();

        let _relay_seq = self.queue_emit(state, batch, |seq| {
            commit.seq = seq;
            encode_frame("#commit", &commit)
        })?;
        #[cfg(feature = "jetstream")]
        {
            // skip car parse and record materialization when no jetstream subscribers are
            // connected — the stream thread re-reads from relay_events as a fallback.
            let has_subscribers = state.db.jetstream.tx.receiver_count() > 0;
            for (op_index, collection) in &jetstream_ops {
                let ephemeral = has_subscribers.then_some(()).and_then(|()| {
                    build_relay_commit_ephemeral(&commit, *op_index, collection, &parsed_blocks)
                });
                self.jetstream_events.push((
                    StoredJetstreamEvent::RelayCommit {
                        did: jetstream_did.clone(),
                        collection: collection.clone(),
                        relay_seq: _relay_seq,
                        op_index: *op_index,
                    },
                    ephemeral,
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn sync(
        &mut self,
        state: &AppState,
        batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        mut sync: Sync<'static>,
    ) -> Result<()> {
        self.queue_emit(state, batch, |seq| {
            sync.seq = seq;
            encode_frame("#sync", &sync)
        })?;
        Ok(())
    }

    pub(crate) fn identity(
        &mut self,
        state: &AppState,
        batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        mut identity: Identity<'static>,
        _repo_state: &RepoState,
        _snapshot: IdentitySnapshot,
    ) -> Result<()> {
        let _relay_seq = self.queue_emit(state, batch, |seq| {
            identity.seq = seq;
            encode_frame("#identity", &identity)
        })?;
        #[cfg(feature = "jetstream")]
        self.jetstream_events.push((
            StoredJetstreamEvent::RelayIdentity {
                did: TrimmedDid::from(&identity.did).into_static(),
                relay_seq: _relay_seq,
            },
            None,
        ));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn account(
        &mut self,
        state: &AppState,
        batch: &mut OwnedWriteBatch,
        _firehose: &Url,
        mut account: Account<'static>,
        _repo_state: &RepoState,
        _snapshot: AccountSnapshot,
        _was_active: bool,
    ) -> Result<()> {
        let _relay_seq = self.queue_emit(state, batch, |seq| {
            account.seq = seq;
            encode_frame("#account", &account)
        })?;
        #[cfg(feature = "jetstream")]
        self.jetstream_events.push((
            StoredJetstreamEvent::RelayAccount {
                did: TrimmedDid::from(&account.did).into_static(),
                relay_seq: _relay_seq,
            },
            None,
        ));
        Ok(())
    }

    pub(crate) fn commit_txn(
        &mut self,
        state: &AppState,
        txn: crate::db::Txn<'_>,
    ) -> Result<(Staged, crate::db::TxnCommitTimings)> {
        #[cfg(feature = "jetstream")]
        {
            let _lock = state.db.jetstream.lock.lock();
            let (jetstream_broadcasts, timings) = txn.commit_with(|batch| {
                self.jetstream_events
                    .drain(..)
                    .map(|(event, ephemeral)| {
                        crate::jetstream::stage_event(batch, &state.db, event, ephemeral)
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            Ok((
                Staged {
                    jetstream_broadcasts,
                },
                timings,
            ))
        }
        #[cfg(not(feature = "jetstream"))]
        {
            let _ = state;
            let (_, timings) = txn.commit_with(|_| Ok(()))?;
            Ok((Staged {}, timings))
        }
    }

    pub(crate) fn flush(&mut self, state: &AppState, staged: Staged) {
        for broadcast in self.broadcasts.drain(..) {
            let _ = state.db.relay.broadcast_tx.send(broadcast);
        }
        #[cfg(feature = "jetstream")]
        for broadcast in staged.jetstream_broadcasts {
            let _ = state.db.jetstream.tx.send(broadcast);
        }
        #[cfg(not(feature = "jetstream"))]
        let _ = staged;
    }

    /// the relay is the terminal consumer, so it advances the firehose cursor
    /// itself once a message is fully processed.
    pub(crate) fn advance_cursor(&self, state: &AppState, firehose: &Url, seq: i64) {
        state
            .firehose_cursors
            .peek_with(firehose, |_, c| c.store(seq, Ordering::SeqCst));
    }
}

#[cfg(feature = "jetstream")]
fn split_collection(path: &str) -> Option<CowStr<'static>> {
    path.split_once('/')
        .map(|(collection, _)| CowStr::Owned(collection.to_smolstr()))
}

#[cfg(feature = "jetstream")]
fn build_relay_commit_ephemeral(
    commit: &Commit,
    op_index: u32,
    collection: &CowStr<'static>,
    car: &jacquard_repo::car::reader::ParsedCar,
) -> Option<crate::jetstream::JetstreamEphemeral> {
    let op = commit.ops.get(op_index as usize)?;
    let (_, rkey) = op.path.split_once('/')?;
    let action = op.action.as_str();

    let (record, cid) = if matches!(action, "create" | "update") {
        let cid_link = op.cid.as_ref()?;
        let cid_ipld = cid_link.to_ipld().ok()?;
        let block = car.blocks.get(&cid_ipld)?;
        let val = serde_ipld_dagcbor::from_slice::<jacquard_common::RawData>(block).ok()?;
        let record = serde_json::value::to_raw_value(&val)
            .ok()
            .map(std::sync::Arc::from);
        (record, Some(cid_link.to_string()))
    } else {
        (None, None)
    };

    Some(crate::jetstream::JetstreamEphemeral {
        did: commit.repo.as_str().to_string(),
        rev: commit.rev.as_str().to_string(),
        operation: action.to_string(),
        collection: collection.as_str().to_string(),
        rkey: rkey.to_string(),
        record,
        cid,
        live: true,
    })
}
