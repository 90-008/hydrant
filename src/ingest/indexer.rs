use super::*;
use crate::db::{self, keys, ser_repo_meta};
use crate::ingest::stream::{Account, Commit, Identity};
use crate::ingest::validation;
use crate::resolver::{NoSigningKeyError, ResolverError};
use crate::state::AppState;
use crate::types::{GaugeState, RepoMetadata, RepoState, RepoStatus};
use crate::{ops, util};

use fjall::OwnedWriteBatch;

use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;

use jacquard_repo::error::CommitError;
use miette::{Diagnostic, IntoDiagnostic, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use thiserror::Error;
use tokio::runtime::Handle as TokioHandle;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
#[cfg(feature = "indexer_stream")]
use {
    crate::types::{AccountEvt, BroadcastEvent, IdentityEvt},
    jacquard_common::cowstr::ToCowStr,
};

#[derive(Debug)]
pub struct IndexerCommitData {
    pub commit: stream::Commit<'static>,
    /// true if the relay detected a gap (missing seq) before this commit
    /// and the indexer should trigger a backfill.
    pub chain_break: bool,
    /// result of parse_car_bytes, already done by relay so indexer does not re-parse.
    pub parsed_blocks: jacquard_repo::car::reader::ParsedCar,
}

#[derive(Debug)]
pub struct IndexerIdentityData {
    pub identity: stream::Identity<'static>,
    /// whether the identity actually changed (handle or key).
    pub changed: bool,
}

#[derive(Debug)]
pub struct IndexerAccountData {
    pub account: stream::Account<'static>,
    /// whether the repo was active prior to this event.
    pub was_active: bool,
    /// whether any state actually changed (active or status).
    pub changed: bool,
}

#[derive(Debug)]
pub enum IndexerEventData {
    Commit(IndexerCommitData),
    Identity(IndexerIdentityData),
    Account(IndexerAccountData),
    Sync(Did<'static>),
}

#[derive(Debug)]
pub struct IndexerEvent {
    pub seq: i64,
    pub firehose: Url,
    pub data: IndexerEventData,
}

/// message sent from `relay_worker` to the indexer (`FirehoseWorker`) after
/// validation and repo-state management are done.
#[derive(Debug)]
pub enum IndexerMessage {
    /// a firehose event that passed relay-side validation.
    Event(Box<IndexerEvent>),
    /// a new repo was discovered and needs backfill.
    NewRepo(Did<'static>),
    /// backfill for this DID has completed; drain the resync buffer.
    BackfillFinished(Did<'static>),
}

pub type IndexerTx = mpsc::Sender<IndexerMessage>;
pub type IndexerRx = mpsc::Receiver<IndexerMessage>;

#[derive(Debug, Diagnostic, Error)]
enum IngestError {
    #[error("{0}")]
    Generic(miette::Report),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Resolver(#[from] ResolverError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Commit(#[from] CommitError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    NoSigningKey(#[from] NoSigningKeyError),
}

impl From<miette::Report> for IngestError {
    fn from(report: miette::Report) -> Self {
        IngestError::Generic(report)
    }
}

#[derive(Debug)]
enum RepoProcessResult<'s, 'c> {
    // message processed successfully, here is the (possibly updated) state
    Ok(RepoState<'s>),
    // repo was deleted as part of processing
    Deleted,
    // needs backfill; carries the triggering commit to buffer (None when already in the buffer)
    NeedsBackfill(Option<&'c Commit<'c>>),
}

pub struct FirehoseWorker {
    state: Arc<AppState>,
    rx: IndexerRx,
    num_shards: usize,
}

struct WorkerContext<'a> {
    state: &'a AppState,
    batch: OwnedWriteBatch,
    added_blocks: &'a mut i64,
    records_delta: &'a mut i64,
    #[cfg(feature = "indexer_stream")]
    broadcast_events: &'a mut Vec<BroadcastEvent>,
}

impl FirehoseWorker {
    pub fn new(state: Arc<AppState>, rx: IndexerRx, num_shards: usize) -> Self {
        Self {
            state,
            rx,
            num_shards,
        }
    }

    // starts the worker threads and the main dispatch loop
    // the dispatch loop reads from the firehose channel and
    // distributes messages to shards based on the hash of the DID
    pub fn run(mut self, handle: TokioHandle) -> Result<()> {
        let mut shards = Vec::with_capacity(self.num_shards);

        for i in 0..self.num_shards {
            // unbounded here so we dont block other shards potentially
            // if one has a small lag or something
            let (tx, rx) = mpsc::unbounded_channel();
            shards.push(tx);

            let state = self.state.clone();
            let handle = handle.clone();
            std::thread::Builder::new()
                .name(format!("ingest-shard-{i}"))
                .spawn(move || {
                    Self::shard(i, rx, state, handle);
                })
                .into_diagnostic()?;
        }

        info!(num = self.num_shards, "started shards");

        while let Some(msg) = self.rx.blocking_recv() {
            let did = match &msg {
                IndexerMessage::Event(e) => match &e.data {
                    IndexerEventData::Commit(m) => &m.commit.repo,
                    IndexerEventData::Identity(m) => &m.identity.did,
                    IndexerEventData::Account(m) => &m.account.did,
                    IndexerEventData::Sync(did) => did,
                },
                IndexerMessage::NewRepo(did) => did,
                IndexerMessage::BackfillFinished(did) => did,
            };

            let shard_idx = (util::hash(did) as usize) % self.num_shards;
            if let Err(e) = shards[shard_idx].send(msg) {
                error!(shard = shard_idx, err = %e, "failed to send message to shard, shard panicked?");
                break;
            }
        }

        Err(miette::miette!(
            "firehose worker dispatcher shutting down, shard died?"
        ))
    }

    #[inline(always)]
    fn shard(
        id: usize,
        mut rx: mpsc::UnboundedReceiver<IndexerMessage>,
        state: Arc<AppState>,
        handle: TokioHandle,
    ) {
        let _guard = handle.enter();
        debug!(shard = id, "shard started");

        #[cfg(feature = "indexer_stream")]
        let mut broadcast_events = Vec::new();

        while let Some(msg) = rx.blocking_recv() {
            let batch = state.db.inner.batch();
            #[cfg(feature = "indexer_stream")]
            broadcast_events.clear();

            let mut added_blocks = 0;
            let mut records_delta = 0;

            let mut ctx = WorkerContext {
                state: &state,
                batch,
                added_blocks: &mut added_blocks,
                records_delta: &mut records_delta,
                #[cfg(feature = "indexer_stream")]
                broadcast_events: &mut broadcast_events,
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
                                            &mut ctx.batch,
                                            &state.db,
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
                                    state.db.update_count("repos", -1);
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

            if let Err(e) = ctx.batch.commit() {
                error!(shard = id, err = %e, "failed to commit batch");
            }

            if added_blocks > 0 {
                state.db.update_count("blocks", added_blocks);
            }
            if records_delta != 0 {
                state.db.update_count("records", records_delta);
            }
            #[cfg(feature = "indexer_stream")]
            for evt in broadcast_events.drain(..) {
                let _ = state.db.event_tx.send(evt);
            }

            // state.db.inner.persist(fjall::PersistMode::Buffer).ok();
        }
    }

    // don't retry commit or sync on key fetch errors
    // since we'll just try again later if we get commit or sync again
    fn check_if_retriable_failure(e: &IngestError) -> bool {
        matches!(
            e,
            IngestError::Generic(_)
                | IngestError::Resolver(ResolverError::Ratelimited)
                | IngestError::Resolver(ResolverError::Transport(_))
        )
    }
}

impl FirehoseWorker {
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
            db.pending
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
            &mut ctx.batch,
            ctx.state,
            repo_state,
            validated,
            &ctx.state.filter.load(),
        )?;
        let repo_state = res.repo_state;
        *ctx.added_blocks += res.blocks_count;
        *ctx.records_delta += res.records_delta;
        #[cfg(feature = "indexer_stream")]
        ctx.broadcast_events
            .push(BroadcastEvent::Persisted(db.next_event_id.load(SeqCst) - 1));

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

        if is_inactive {
            use crate::ingest::stream::AccountStatus;
            match &account.status {
                Some(AccountStatus::Deleted) => {
                    debug!("account deleted, wiping data");
                    crate::ops::delete_repo(&mut ctx.batch, db, did, &repo_state)?;
                    return Ok(RepoProcessResult::Deleted);
                }
                _ => {
                    // status update logic is now handled in RelayWorker;
                    // FirehoseWorker just needs to update gauges if status changed.
                    if changed && was_active {
                        db.update_gauge_diff(&GaugeState::Synced, &GaugeState::Resync(None));
                    }
                }
            }
        } else {
            // if account became active, update gauges
            if !was_active {
                db.update_gauge_diff(&GaugeState::Resync(None), &GaugeState::Synced);
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

        for guard in db.resync_buffer.prefix(&prefix) {
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
                        ctx.batch.remove(&db.resync_buffer, key);
                    }
                    return Err(e);
                }
            };
            match res {
                RepoProcessResult::Ok(rs) => {
                    ctx.batch.remove(&db.resync_buffer, key);
                    repo_state = rs;
                }
                RepoProcessResult::NeedsBackfill(_) => {
                    // commit is already in the buffer, leave it there for the next backfill
                    return Ok(RepoProcessResult::NeedsBackfill(None));
                }
                RepoProcessResult::Deleted => {
                    ctx.batch.remove(&db.resync_buffer, key);
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
        let mut batch = db.inner.batch();
        let repo_key = keys::repo_key(did);
        let meta_key = keys::repo_metadata_key(did);

        let resync_bytes = db.resync.get(&repo_key).into_diagnostic()?;
        let old_gauge = crate::db::Db::repo_gauge_state(&repo_state, resync_bytes.as_deref());

        let existing_metadata = db
            .repo_metadata
            .get(&meta_key)
            .into_diagnostic()?
            .map(|b| crate::db::deser_repo_meta(&b))
            .transpose()?;
        let had_metadata = existing_metadata.is_some();
        let mut metadata = existing_metadata.unwrap_or_else(|| RepoMetadata {
            index_id: 0, // this is set later
            tracked: true,
        });

        let old_pkey = keys::pending_key(metadata.index_id);
        let was_pending = had_metadata && db.pending.get(&old_pkey).into_diagnostic()?.is_some();
        // remove old pending entry and insert new one with fresh index_id
        if had_metadata {
            // only remove if we had one so we dont delete a random entry
            batch.remove(&db.pending, old_pkey);
        }

        metadata.index_id = rand::random::<u64>();
        batch.insert(&db.pending, keys::pending_key(metadata.index_id), &repo_key);
        batch.insert(&db.repo_metadata, &meta_key, ser_repo_meta(&metadata)?);
        batch.commit().into_diagnostic()?;

        if !was_pending {
            db.update_gauge_diff(&old_gauge, &crate::types::GaugeState::Pending);
            ctx.state.notify_backfill();
        }

        Ok(repo_state)
    }
}
