use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use tokio::runtime::Handle as TokioHandle;
use tracing::{debug, error, warn};

use crate::db::{self, Txn, keys, ser_repo_meta};
use crate::ingest::stream::types::AccountStatus;
use crate::ingest::stream::{Account, Commit, Identity};
use crate::ingest::validation;
use crate::ops;
use crate::resolver::ResolverError;
use crate::state::AppState;
use crate::types::{GaugeState, RepoMetadata, RepoState, RepoStatus};

#[cfg(feature = "jetstream")]
use crate::{
    db::types::TrimmedDid,
    types::{JetstreamBroadcast, StoredJetstreamEvent},
};
#[cfg(feature = "indexer_stream")]
use {
    crate::types::{AccountEvt, BroadcastEvent, IdentityEvt},
    jacquard_common::cowstr::ToCowStr,
};

use super::message::{
    IndexerAccountData, IndexerCommitData, IndexerEvent, IndexerEventData, IndexerIdentityData,
    IndexerMessage, IndexerRx,
};
use super::worker::{FirehoseWorker, IngestError, RepoProcessResult};

struct WorkerContext<'a> {
    state: &'a AppState,
    txn: Txn<'a>,
    lifecycle_transitions: &'a mut Vec<(Did<'static>, GaugeState)>,
    #[cfg(feature = "indexer_stream")]
    broadcast_events: &'a mut Vec<BroadcastEvent>,
    #[cfg(feature = "jetstream")]
    jetstream_events: &'a mut Vec<JetstreamBroadcast>,
}

impl FirehoseWorker {
    pub(crate) fn shard(id: usize, mut rx: IndexerRx, state: Arc<AppState>, handle: TokioHandle) {
        let _guard = handle.enter();
        debug!(shard = id, "shard started");

        #[cfg(feature = "indexer_stream")]
        let mut broadcast_events = Vec::new();
        #[cfg(feature = "jetstream")]
        let mut jetstream_events = Vec::new();
        let mut lifecycle_transitions = Vec::new();

        while let Some(msg) = rx.blocking_recv() {
            let txn = Txn::new(&state.db);
            #[cfg(feature = "indexer_stream")]
            broadcast_events.clear();
            #[cfg(feature = "jetstream")]
            jetstream_events.clear();
            lifecycle_transitions.clear();

            let mut ctx = WorkerContext {
                state: &state,
                txn,
                lifecycle_transitions: &mut lifecycle_transitions,
                #[cfg(feature = "indexer_stream")]
                broadcast_events: &mut broadcast_events,
                #[cfg(feature = "jetstream")]
                jetstream_events: &mut jetstream_events,
            };

            match msg {
                IndexerMessage::BackfillFinished(did) => {
                    let _span = tracing::info_span!("ingest", did = %did).entered();
                    debug!("backfill finished, verifying state and draining buffer");

                    let repo_key = keys::repo_key(&did);
                    if let Ok(Some(state_bytes)) = state.db.repos.get(&repo_key).into_diagnostic() {
                        match crate::db::deser_repo_state(&state_bytes) {
                            Ok(repo_state) => {
                                let repo_state = repo_state.into_static();

                                match Self::drain_resync_buffer(&mut ctx, &did, repo_state) {
                                    Ok(RepoProcessResult::Ok(s)) => {
                                        let res = ops::transition_repo(
                                            &mut ctx.txn.batch,
                                            &state.db,
                                            ctx.lifecycle_transitions,
                                            &did,
                                            s,
                                            RepoStatus::Synced,
                                        );
                                        if let Err(e) = res {
                                            error!(err = %e, "failed to transition to synced");
                                        }
                                    }
                                    Ok(RepoProcessResult::NeedsBackfill(_)) => {}
                                    Ok(RepoProcessResult::Deleted) => {}
                                    Err(e) => {
                                        error!(err = %e, "failed to drain resync buffer")
                                    }
                                };
                            }
                            Err(e) => error!(err = %e, "failed to deser repo state"),
                        }
                    }
                }
                IndexerMessage::NewRepo(did) => {
                    let _span = tracing::info_span!("ingest", did = %did).entered();
                    debug!("new repo discovered, triggering backfill");

                    let repo_key = keys::repo_key(&did);
                    if let Ok(Some(state_bytes)) = state.db.repos.get(&repo_key).into_diagnostic() {
                        match crate::db::deser_repo_state(&state_bytes) {
                            Ok(repo_state) => {
                                if let Err(e) = Self::trigger_backfill(&mut ctx, &did, repo_state) {
                                    error!(err = %e, "failed to trigger backfill for new repo");
                                }
                            }
                            Err(e) => error!(err = %e, "failed to deser repo state"),
                        }
                    }
                }
                IndexerMessage::Event(e) => {
                    let IndexerEvent {
                        seq,
                        firehose,
                        data,
                    } = *e;
                    let _span = tracing::info_span!("ingest", hose = %firehose, did = tracing::field::Empty).entered();

                    let repo_bytes = {
                        let did = match &data {
                            IndexerEventData::Commit(m) => &m.commit.repo,
                            IndexerEventData::Identity(m) => &m.identity.did,
                            IndexerEventData::Account(m) => &m.account.did,
                            IndexerEventData::Sync(did) => did,
                        };
                        _span.record("did", did.as_ref());

                        let repo_key = keys::repo_key(did);
                        match state.db.repos.get(&repo_key).into_diagnostic() {
                            Ok(Some(b)) => b,
                            Ok(None) => {
                                state
                                    .firehose_cursors
                                    .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                                continue;
                            }
                            Err(e) => {
                                error!(err = %e, "failed to get repo state");
                                state
                                    .firehose_cursors
                                    .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                                continue;
                            }
                        }
                    };
                    let repo_state = match crate::db::deser_repo_state(&repo_bytes) {
                        Ok(s) => s,
                        Err(e) => {
                            error!(err = %e, "failed to deser repo state");
                            state
                                .firehose_cursors
                                .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                            continue;
                        }
                    };

                    match data {
                        IndexerEventData::Commit(msg) => {
                            let IndexerCommitData {
                                commit,
                                chain_break,
                                parsed_blocks,
                            } = msg;

                            let try_persist = |commit: &Commit| {
                                if let Err(e) =
                                    ops::persist_to_resync_buffer(&state.db, &commit.repo, commit)
                                {
                                    error!(err = %e, "failed to persist commit to resync_buffer");
                                }
                            };

                            match Self::handle_commit(
                                &mut ctx,
                                repo_state,
                                &commit,
                                chain_break,
                                parsed_blocks,
                            ) {
                                Ok(RepoProcessResult::Ok(_)) => {}
                                Ok(RepoProcessResult::Deleted) => {
                                    ctx.txn.counts.add_repos(-1);
                                }
                                Ok(RepoProcessResult::NeedsBackfill(Some(commit))) => {
                                    try_persist(commit);
                                }
                                Ok(RepoProcessResult::NeedsBackfill(None)) => {}
                                Err(e) => {
                                    if let IngestError::Generic(ref r) = e {
                                        db::check_poisoned_report(r);
                                    }
                                    error!(err = %e, "error processing commit");
                                    try_persist(&commit);
                                }
                            }
                        }
                        IndexerEventData::Identity(msg) => {
                            let IndexerIdentityData { identity, changed } = msg;

                            if let Err(e) =
                                Self::handle_identity(&mut ctx, repo_state, &identity, changed)
                            {
                                error!(err = %e, "error processing identity");
                            }
                        }
                        IndexerEventData::Account(msg) => {
                            let IndexerAccountData {
                                account,
                                was_active,
                                changed,
                            } = msg;

                            if let Err(e) = Self::handle_account(
                                &mut ctx, repo_state, changed, &account, was_active,
                            ) {
                                error!(err = %e, "error processing account");
                            }
                        }
                        IndexerEventData::Sync(did) => {
                            warn!("sync event, triggering backfill");
                            if let Err(e) = Self::trigger_backfill(&mut ctx, &did, repo_state) {
                                error!(err = %e, "failed to trigger backfill on sync");
                            }
                        }
                    }
                    state
                        .firehose_cursors
                        .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                }
            }

            let lifecycle_result = ctx
                .lifecycle_transitions
                .drain(..)
                .try_for_each(|(did, gauge)| ctx.txn.transition_lifecycle(&did, gauge).map(|_| ()));
            if let Err(e) = lifecycle_result {
                error!(shard = id, err = %e, "failed to stage lifecycle transitions");
                continue;
            }
            if let Err(e) = ctx.txn.commit() {
                error!(shard = id, err = %e, "failed to commit transaction");
                continue;
            }
            #[cfg(feature = "indexer_stream")]
            for evt in ctx.broadcast_events.drain(..) {
                let _ = state.db.stream.event_tx.send(evt);
            }
            #[cfg(feature = "jetstream")]
            for evt in ctx.jetstream_events.drain(..) {
                let _ = state.db.jetstream.tx.send(evt);
            }

            // state.db.inner.persist(fjall::PersistMode::Buffer).ok();
        }
    }

    // don't retry commit or sync on key fetch errors
    // since we'll just try again later if we get commit or sync again
    fn check_if_retriable_failure(e: &IngestError) -> bool {
        match e {
            IngestError::Commit(_) | IngestError::NoSigningKey(_) => false,
            IngestError::Generic(report) => {
                report
                    .downcast_ref::<jacquard_repo::error::CommitError>()
                    .is_none()
                    && report
                        .downcast_ref::<crate::resolver::NoSigningKeyError>()
                        .is_none()
            }
            IngestError::Resolver(ResolverError::Ratelimited) => true,
            IngestError::Resolver(ResolverError::Transport(_)) => true,
            _ => false,
        }
    }

    fn handle_commit<'s, 'c>(
        ctx: &mut WorkerContext,
        mut repo_state: RepoState<'s>,
        commit: &'c Commit<'c>,
        chain_break: bool,
        parsed_blocks: jacquard_repo::car::reader::ParsedCar,
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        let db = &ctx.state.db;
        let did = &commit.repo;
        repo_state.advance_message_time(commit.time.0.timestamp_millis());

        let metadata_key = keys::repo_metadata_key(did);
        let metadata_bytes = db.repo_metadata.get(&metadata_key).into_diagnostic()?;
        let is_backfilling = if let Some(metadata_bytes) = metadata_bytes {
            let metadata = crate::db::deser_repo_meta(metadata_bytes.as_ref())?;
            db.indexer
                .pending
                .get(keys::pending_key(metadata.index_id))
                .into_diagnostic()?
                .is_some()
        } else {
            false
        };

        if chain_break {
            warn!("chain break detected, triggering backfill");
            Self::trigger_backfill(ctx, did, repo_state)?;
            return Ok(RepoProcessResult::NeedsBackfill(Some(commit)));
        }

        if is_backfilling {
            return Ok(RepoProcessResult::NeedsBackfill(Some(commit)));
        }

        let root_bytes = parsed_blocks
            .blocks
            .get(&parsed_blocks.root)
            .ok_or_else(|| IngestError::Generic(miette::miette!("root block missing from CAR")))?;

        let commit_obj = jacquard_repo::commit::Commit::from_cbor(root_bytes)
            .map_err(|e| IngestError::Generic(miette::miette!("invalid commit object: {e}")))?
            .into_static();

        let validated = validation::ValidatedCommit {
            commit,
            parsed_blocks,
            commit_obj,
            chain_break: validation::ChainBreak::default(), // not used by apply_commit
        };

        let res = ops::apply_commit(
            &mut ctx.txn,
            ctx.state,
            repo_state,
            validated,
            &ctx.state.filter.load(),
        )?;
        let repo_state = res.repo_state;
        #[cfg(feature = "indexer_stream")]
        {
            use std::sync::Arc;

            let mut live_events = res.events.live_events;
            for evt in live_events.drain(..) {
                ctx.broadcast_events
                    .push(BroadcastEvent::LiveRecord(Arc::new(evt)));
            }
            if let Some(last_id) = res.events.last_event_id {
                ctx.broadcast_events
                    .push(BroadcastEvent::Persisted(last_id));
            }
        }
        #[cfg(feature = "jetstream")]
        ctx.jetstream_events.extend(res.events.jetstream_events);

        Ok(RepoProcessResult::Ok(repo_state))
    }

    #[cfg_attr(not(feature = "indexer_stream"), allow(unused_variables))]
    fn handle_identity<'s>(
        ctx: &mut WorkerContext,
        repo_state: RepoState<'s>,
        identity: &Identity<'_>,
        changed: bool,
    ) -> Result<RepoProcessResult<'s, 'static>, IngestError> {
        #[cfg(feature = "indexer_stream")]
        {
            let db = &ctx.state.db;
            let did = &identity.did;
            #[cfg(feature = "jetstream")]
            ctx.jetstream_events.push(crate::jetstream::stage_event(
                &mut ctx.txn.batch,
                db,
                StoredJetstreamEvent::Identity {
                    did: TrimmedDid::from(did).into_static(),
                    handle: identity.handle.clone().map(IntoStatic::into_static),
                    seq: identity.seq,
                    time: identity.time.clone(),
                },
                None,
            )?);

            if changed {
                let evt = IdentityEvt {
                    did: did.clone().into_static(),
                    handle: repo_state.handle.clone().map(IntoStatic::into_static),
                };
                ctx.broadcast_events.push(ops::make_identity_event(db, evt));
            }
        }

        Ok(RepoProcessResult::Ok(repo_state))
    }

    fn handle_account<'s, 'c>(
        ctx: &mut WorkerContext,
        repo_state: RepoState<'s>,
        changed: bool,
        account: &'c Account<'c>,
        was_active: bool,
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        let db = &ctx.state.db;
        let did = &account.did;
        let is_inactive = !account.active;
        #[cfg(feature = "indexer_stream")]
        let evt = AccountEvt {
            did: did.clone().into_static(),
            active: account.active,
            status: account.status.as_ref().map(|s| s.to_cowstr().into_static()),
        };
        #[cfg(feature = "jetstream")]
        ctx.jetstream_events.push(crate::jetstream::stage_event(
            &mut ctx.txn.batch,
            db,
            StoredJetstreamEvent::Account {
                did: TrimmedDid::from(did).into_static(),
                active: account.active,
                status: account.status.as_ref().map(|s| s.to_cowstr().into_static()),
                seq: account.seq,
                time: account.time.clone(),
            },
            None,
        )?);

        if is_inactive {
            match &account.status {
                Some(AccountStatus::Deleted) => {
                    debug!("account deleted, wiping data");
                    ctx.lifecycle_transitions
                        .push((did.clone().into_static(), GaugeState::Synced));
                    crate::ops::delete_repo(&mut ctx.txn.batch, db, did, &repo_state)?;
                    return Ok(RepoProcessResult::Deleted);
                }
                _ => {
                    // status update logic is now handled in RelayWorker;
                    // FirehoseWorker just needs to update gauges if status changed.
                    if changed && was_active {
                        ctx.lifecycle_transitions
                            .push((did.clone().into_static(), GaugeState::Resync(None)));
                    }
                }
            }
        } else {
            // if account became active, update gauges
            if !was_active {
                ctx.lifecycle_transitions
                    .push((did.clone().into_static(), GaugeState::Synced));
            }
        }

        #[cfg(feature = "indexer_stream")]
        if changed {
            ctx.broadcast_events.push(ops::make_account_event(db, evt));
        }

        Ok(RepoProcessResult::Ok(repo_state))
    }

    fn drain_resync_buffer<'s>(
        ctx: &mut WorkerContext,
        did: &Did,
        mut repo_state: RepoState<'s>,
    ) -> Result<RepoProcessResult<'s, 'static>, IngestError> {
        let db = &ctx.state.db;
        let prefix = keys::resync_buffer_prefix(did);

        for guard in db.indexer.resync_buffer.prefix(&prefix) {
            let (key, value) = guard.into_inner().into_diagnostic()?;
            let commit: Commit = rmp_serde::from_slice(&value).into_diagnostic()?;

            let parsed_blocks = TokioHandle::current()
                .block_on(jacquard_repo::car::reader::parse_car_bytes(
                    commit.blocks.as_ref(),
                ))
                .map_err(|e| IngestError::Generic(miette::miette!("malformed CAR: {e}")))?;

            // buffered commits have already been source-checked on arrival; skip host check
            let res = Self::handle_commit(ctx, repo_state, &commit, false, parsed_blocks);
            let res = match res {
                Ok(r) => r,
                Err(e) => {
                    if !Self::check_if_retriable_failure(&e) {
                        ctx.txn.batch.remove(&db.indexer.resync_buffer, key);
                    }
                    return Err(e);
                }
            };
            match res {
                RepoProcessResult::Ok(rs) => {
                    ctx.txn.batch.remove(&db.indexer.resync_buffer, key);
                    repo_state = rs;
                }
                RepoProcessResult::NeedsBackfill(_) => {
                    // commit is already in the buffer, leave it there for the next backfill
                    return Ok(RepoProcessResult::NeedsBackfill(None));
                }
                RepoProcessResult::Deleted => {
                    ctx.txn.batch.remove(&db.indexer.resync_buffer, key);
                    return Ok(RepoProcessResult::Deleted);
                }
            }
        }

        Ok(RepoProcessResult::Ok(repo_state))
    }

    fn trigger_backfill<'s>(
        ctx: &mut WorkerContext,
        did: &Did,
        repo_state: RepoState<'s>,
    ) -> Result<RepoState<'s>, IngestError> {
        let db = &ctx.state.db;
        let mut txn = Txn::new(db);
        let repo_key = keys::repo_key(did);
        let meta_key = keys::repo_metadata_key(did);

        let existing_metadata = db
            .repo_metadata
            .get(&meta_key)
            .into_diagnostic()?
            .map(|b| crate::db::deser_repo_meta(&b))
            .transpose()?;
        let had_metadata = existing_metadata.is_some();
        let mut metadata = existing_metadata.unwrap_or(RepoMetadata {
            index_id: 0, // this is set later
            tracked: true,
        });

        let old_pkey = keys::pending_key(metadata.index_id);
        let was_pending = had_metadata
            && db
                .indexer
                .pending
                .get(old_pkey.as_slice())
                .into_diagnostic()?
                .is_some();
        // remove old pending entry and insert new one with fresh index_id
        if had_metadata {
            // only remove if we had one so we dont delete a random entry
            txn.batch.remove(&db.indexer.pending, old_pkey);
        }

        metadata.index_id = rand::random::<u64>();
        txn.batch.insert(
            &db.indexer.pending,
            keys::pending_key(metadata.index_id),
            &repo_key,
        );
        txn.batch
            .insert(&db.repo_metadata, &meta_key, ser_repo_meta(&metadata)?);
        if !was_pending {
            txn.transition_lifecycle(did, GaugeState::Pending)?;
        }
        txn.commit()?;

        if !was_pending {
            ctx.state.notify_backfill();
        }

        Ok(repo_state)
    }
}
