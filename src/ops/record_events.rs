//! per-op stream/jetstream event emission for `apply_commit`.
//!
//! when `indexer_stream` is enabled the emitter persists a `StoredEvent` per
//! record op, stages jetstream events, and collects live broadcasts; without
//! it the emitter is a zero-sized no-op, so `apply_commit` stays unconditional.

#[cfg(feature = "indexer_stream")]
mod enabled {
    use bytes::Bytes;
    use fjall::OwnedWriteBatch;
    use jacquard_common::CowStr;
    use jacquard_common::IntoStatic;
    use jacquard_common::types::cid::IpldCid;
    use jacquard_common::types::did::Did;
    use miette::{IntoDiagnostic, Result};
    use std::sync::atomic::Ordering;

    use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
    use crate::db::{Db, keys};
    use crate::state::AppState;
    use crate::types::{LiveRecordEvent, StoredData, StoredEvent};

    /// one record op, as seen by the event emitter.
    pub(crate) struct EmitOp<'a> {
        pub(crate) did: &'a Did<'a>,
        pub(crate) collection: &'a str,
        pub(crate) rkey: &'a DbRkey,
        pub(crate) action: DbAction,
        /// record cid for create/update ops
        pub(crate) cid: Option<IpldCid>,
        /// raw record block for create/update ops
        pub(crate) block: Option<&'a Bytes>,
    }

    #[derive(Clone, Copy)]
    pub(crate) enum RecordEventOrigin {
        Live,
        Backfill,
    }

    pub(crate) struct RecordEmitter {
        rev: DbTid,
        ephemeral: bool,
        only_index_links: bool,
        live: bool,
        should_broadcast_live: bool,
        live_events: Vec<LiveRecordEvent>,
        last_event_id: Option<u64>,
        #[cfg(feature = "jetstream")]
        should_stage_jetstream: bool,
        #[cfg(feature = "jetstream")]
        jetstream_events: Vec<crate::types::JetstreamBroadcast>,
    }

    impl RecordEmitter {
        pub(crate) fn new(state: &AppState, commit_rev: &DbTid, origin: RecordEventOrigin) -> Self {
            let live = matches!(origin, RecordEventOrigin::Live);
            Self {
                rev: *commit_rev,
                ephemeral: state.ephemeral,
                only_index_links: state.only_index_links,
                live,
                should_broadcast_live: live && state.db.stream.event_tx.receiver_count() > 0,
                live_events: Vec::new(),
                last_event_id: None,
                #[cfg(feature = "jetstream")]
                should_stage_jetstream: live && state.db.jetstream.tx.receiver_count() > 0,
                #[cfg(feature = "jetstream")]
                jetstream_events: Vec::new(),
            }
        }

        pub(crate) fn emit(
            &mut self,
            batch: &mut OwnedWriteBatch,
            db: &Db,
            op: EmitOp<'_>,
        ) -> Result<()> {
            // in ephemeral mode, the event payload is the only place we persist the record.
            let block_inline_for_event = (self.ephemeral).then(|| op.block.cloned()).flatten();
            // inline record bytes for live tailing so we don't have to load from blocks.
            let inline_block =
                (!self.ephemeral && self.should_broadcast_live && !self.only_index_links)
                    .then(|| op.block.cloned())
                    .flatten();

            let data = block_inline_for_event
                .map(StoredData::Block)
                .or_else(|| {
                    (!self.only_index_links)
                        .then(|| op.cid.map(StoredData::Ptr))
                        .flatten()
                })
                .unwrap_or(StoredData::Nothing);

            let event_id = db.stream.next_event_id.fetch_add(1, Ordering::SeqCst);
            self.last_event_id = Some(event_id);
            let did_trimmed = TrimmedDid::from(op.did);
            let collection = CowStr::Borrowed(op.collection);

            let evt = StoredEvent {
                live: self.live,
                did: did_trimmed.clone(),
                rev: self.rev,
                collection: collection.clone(),
                rkey: op.rkey.clone(),
                action: op.action,
                data: data.clone(),
            };
            let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
            db.stream
                .stage_event(batch, keys::event_key(event_id), bytes);

            #[cfg(feature = "jetstream")]
            {
                let jetstream = crate::types::StoredJetstreamEvent::Commit {
                    did: did_trimmed.clone().into_static(),
                    collection: collection.clone().into_static(),
                    event_id,
                    live: self.live,
                };
                let ephemeral = self
                    .should_stage_jetstream
                    .then(|| {
                        crate::jetstream::build_ephemeral_from_stored(
                            did_trimmed.to_did().as_str(),
                            self.rev.to_tid().as_str(),
                            op.action.as_str(),
                            collection.as_str(),
                            op.rkey.to_smolstr().as_str(),
                            &data,
                            inline_block.as_ref(),
                            self.live,
                        )
                    })
                    .flatten();
                let broadcast = crate::jetstream::stage_event(batch, db, jetstream, ephemeral)?;
                if self.live {
                    self.jetstream_events.push(broadcast);
                }
            }

            if self.should_broadcast_live {
                self.live_events.push(LiveRecordEvent {
                    id: event_id,
                    stored: StoredEvent {
                        live: evt.live,
                        did: did_trimmed.into_static(),
                        rev: evt.rev,
                        collection: collection.into_static(),
                        rkey: op.rkey.clone(),
                        action: evt.action,
                        data,
                    },
                    inline_block,
                });
            }

            Ok(())
        }

        pub(crate) fn finish(self) -> RecordEvents {
            RecordEvents {
                live_events: self.live_events,
                last_event_id: self.last_event_id,
                #[cfg(feature = "jetstream")]
                jetstream_events: self.jetstream_events,
            }
        }
    }

    /// events collected while applying one commit.
    pub(crate) struct RecordEvents {
        pub(crate) live_events: Vec<LiveRecordEvent>,
        pub(crate) last_event_id: Option<u64>,
        #[cfg(feature = "jetstream")]
        pub(crate) jetstream_events: Vec<crate::types::JetstreamBroadcast>,
    }
}

#[cfg(feature = "indexer_stream")]
pub(crate) use enabled::*;

#[cfg(not(feature = "indexer_stream"))]
mod noop {
    use bytes::Bytes;
    use fjall::OwnedWriteBatch;
    use jacquard_common::types::cid::IpldCid;
    use jacquard_common::types::did::Did;
    use miette::Result;

    use crate::db::Db;
    use crate::db::types::{DbAction, DbRkey};
    use crate::state::AppState;

    #[derive(Clone, Copy)]
    pub(crate) enum RecordEventOrigin {
        Live,
        Backfill,
    }

    #[allow(dead_code)]
    pub(crate) struct EmitOp<'a> {
        pub(crate) did: &'a Did<'a>,
        pub(crate) collection: &'a str,
        pub(crate) rkey: &'a DbRkey,
        pub(crate) action: DbAction,
        pub(crate) cid: Option<IpldCid>,
        pub(crate) block: Option<&'a Bytes>,
    }

    pub(crate) struct RecordEmitter;

    impl RecordEmitter {
        #[inline(always)]
        pub(crate) fn new(
            _state: &AppState,
            _commit_rev: &crate::db::types::DbTid,
            _origin: RecordEventOrigin,
        ) -> Self {
            Self
        }

        #[inline(always)]
        pub(crate) fn emit(
            &mut self,
            _batch: &mut OwnedWriteBatch,
            _db: &Db,
            _op: EmitOp<'_>,
        ) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        pub(crate) fn finish(self) -> RecordEvents {
            RecordEvents
        }
    }

    pub(crate) struct RecordEvents;
}

#[cfg(not(feature = "indexer_stream"))]
pub(crate) use noop::*;
