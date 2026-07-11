use std::collections::HashMap;

use bytes::Bytes;
use jacquard_common::IntoStatic;
use jacquard_common::types::cid::IpldCid;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};

use crate::db::types::{DbAction, DbRkey, DbTid};
use crate::db::{CountDeltas, Db, keys};
use crate::ops::record_events::{EmitOp, RecordEmitter, RecordEventOrigin, RecordEvents};
use crate::state::AppState;
#[cfg(feature = "indexer")]
use crate::types::GaugeState;
use crate::types::RepoState;

/// one atomic database write, including its in-memory count projections.
///
/// count deltas are persisted in the same fjall batch and applied in memory only
/// after that batch commits. dropping a transaction discards both.
pub(crate) struct Txn<'db> {
    pub(crate) batch: fjall::OwnedWriteBatch,
    pub(crate) db: &'db Db,
    pub(crate) counts: CountDeltas,
    #[cfg(feature = "indexer")]
    lifecycle_transitions: Vec<(Did<'static>, GaugeState)>,
}

impl<'db> Txn<'db> {
    pub(crate) fn new(db: &'db Db) -> Self {
        Self {
            batch: db.inner.batch(),
            db,
            counts: CountDeltas::default(),
            #[cfg(feature = "indexer")]
            lifecycle_transitions: Vec::new(),
        }
    }

    pub(crate) fn records<'txn, 'did, 'repo>(
        &'txn mut self,
        state: &AppState,
        commit_rev: &DbTid,
        did: &'did Did<'repo>,
    ) -> RecordTxn<'txn, 'db, 'did, 'repo> {
        self.record_scope(state, commit_rev, did, RecordEventOrigin::Live, false)
    }

    pub(crate) fn backfill_records<'txn, 'did, 'repo>(
        &'txn mut self,
        state: &AppState,
        commit_rev: &DbTid,
        did: &'did Did<'repo>,
    ) -> RecordTxn<'txn, 'db, 'did, 'repo> {
        self.record_scope(state, commit_rev, did, RecordEventOrigin::Backfill, true)
    }

    fn record_scope<'txn, 'did, 'repo>(
        &'txn mut self,
        state: &AppState,
        commit_rev: &DbTid,
        did: &'did Did<'repo>,
        origin: RecordEventOrigin,
        count_ephemeral_records: bool,
    ) -> RecordTxn<'txn, 'db, 'did, 'repo> {
        RecordTxn {
            txn: self,
            emitter: RecordEmitter::new(state, commit_rev, origin),
            did,
            ephemeral: state.ephemeral,
            only_index_links: state.only_index_links,
            count_ephemeral_records,
            records_delta: 0,
            blocks_count: 0,
            collection_deltas: HashMap::new(),
        }
    }

    #[cfg(feature = "indexer")]
    pub(crate) fn transition_lifecycle(&mut self, did: &Did<'_>, gauge: GaugeState) {
        self.lifecycle_transitions
            .push((did.clone().into_static(), gauge));
    }

    pub(crate) fn commit(mut self) -> Result<()> {
        #[cfg(feature = "indexer")]
        let lifecycle_reservation = {
            let mut lifecycle = self.db.lifecycle_counts();
            for (did, gauge) in self.lifecycle_transitions {
                lifecycle.transition(&mut self.batch, &did, gauge)?;
            }
            lifecycle.stage(&mut self.batch)
        };
        let count_reservation = self.db.stage_count_deltas(&mut self.batch, &self.counts);

        self.batch.commit().into_diagnostic()?;

        self.db.apply_count_deltas(&self.counts);
        drop(count_reservation);
        #[cfg(feature = "indexer")]
        self.db.apply_lifecycle_counts(lifecycle_reservation);
        Ok(())
    }
}

/// one commit's record mutations within a larger atomic transaction.
pub(crate) struct RecordTxn<'txn, 'db, 'did, 'repo> {
    txn: &'txn mut Txn<'db>,
    emitter: RecordEmitter,
    did: &'did Did<'repo>,
    ephemeral: bool,
    only_index_links: bool,
    count_ephemeral_records: bool,
    records_delta: i64,
    blocks_count: i64,
    collection_deltas: HashMap<String, i64>,
}

impl RecordTxn<'_, '_, '_, '_> {
    pub(crate) fn put_record(
        &mut self,
        collection: &str,
        rkey: &DbRkey,
        cid: IpldCid,
        block: &Bytes,
        action: DbAction,
    ) -> Result<()> {
        self.blocks_count += 1;
        if action == DbAction::Create && (!self.ephemeral || self.count_ephemeral_records) {
            self.records_delta += 1;
        }

        #[cfg(feature = "indexer")]
        if !self.ephemeral {
            let cid_bytes = cid.to_bytes();
            if !self.only_index_links {
                self.txn.batch.insert(
                    &self.txn.db.indexer.blocks,
                    keys::block_key(collection, &cid_bytes),
                    block.as_ref(),
                );
            }
            self.txn.batch.insert(
                &self.txn.db.indexer.records,
                keys::record_key(self.did, collection, rkey),
                cid_bytes,
            );
            if action == DbAction::Create {
                *self
                    .collection_deltas
                    .entry(collection.to_owned())
                    .or_default() += 1;
            }
            crate::ops::backlink_ops::index_record(
                &mut self.txn.batch,
                self.txn.db,
                self.did,
                collection,
                &rkey.to_smolstr(),
                block,
            )?;
        }

        self.emitter.emit(
            &mut self.txn.batch,
            self.txn.db,
            EmitOp {
                did: self.did,
                collection,
                rkey,
                action,
                cid: Some(cid),
                block: Some(block),
            },
        )
    }

    pub(crate) fn delete_record(&mut self, collection: &str, rkey: &DbRkey) -> Result<()> {
        if !self.ephemeral || self.count_ephemeral_records {
            self.records_delta -= 1;
        }

        #[cfg(feature = "indexer")]
        if !self.ephemeral {
            self.txn.batch.remove(
                &self.txn.db.indexer.records,
                keys::record_key(self.did, collection, rkey),
            );
            *self
                .collection_deltas
                .entry(collection.to_owned())
                .or_default() -= 1;
            crate::ops::backlink_ops::delete_record(
                &mut self.txn.batch,
                self.txn.db,
                self.did,
                collection,
                &rkey.to_smolstr(),
            )?;
        }

        self.emitter.emit(
            &mut self.txn.batch,
            self.txn.db,
            EmitOp {
                did: self.did,
                collection,
                rkey,
                action: DbAction::Delete,
                cid: None,
                block: None,
            },
        )
    }

    pub(crate) fn update_repo_state(&mut self, state: &RepoState<'_>) -> Result<()> {
        self.txn.batch.insert(
            &self.txn.db.repos,
            keys::repo_key(self.did),
            crate::db::ser_repo_state(state)?,
        );
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<RecordEvents> {
        #[cfg(feature = "indexer")]
        if !self.ephemeral {
            for (collection, delta) in &self.collection_deltas {
                crate::db::update_record_count(
                    &mut self.txn.batch,
                    self.txn.db,
                    self.did,
                    collection,
                    *delta,
                )?;
            }
        }
        self.txn.counts.add_records(self.records_delta);
        self.txn.counts.add_blocks(self.blocks_count);

        Ok(self.emitter.finish())
    }
}
