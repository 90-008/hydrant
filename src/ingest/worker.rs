use super::*;
use crate::db::{self, keys};
use crate::filter::FilterMode;
use crate::ingest::stream::{Account, Commit, Identity, SubscribeReposMessage, Sync};
use crate::ingest::validation::{
    CommitValidationError, SyncValidationError, ValidatedCommit, ValidatedSync, ValidationContext,
    ValidationOptions,
};
use crate::ops;
use crate::resolver::{NoSigningKeyError, ResolverError};
use crate::state::AppState;
use crate::types::{AccountEvt, BroadcastEvent, GaugeState, IdentityEvt, RepoState, RepoStatus};

use fjall::OwnedWriteBatch;

use jacquard_common::IntoStatic;
use jacquard_common::cowstr::ToCowStr;
use jacquard_common::types::did::Did;
use jacquard_repo::error::CommitError;
use miette::{Diagnostic, IntoDiagnostic, Result};
use rand::Rng;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

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

// gate returned by check_repo_state, tells the shard loop what to do with the message
enum ProcessGate<'s, 'c> {
    // did not exist in db, newly queued for backfill, drop
    NewRepo,
    // explicitly untracked, backfilling, or in error, drop
    Drop,
    // inactive repo receiving a non-account message, buffer the commit if present, drop otherwise
    Buffer(Option<&'c Commit<'c>>),
    // ready to process with the latest state
    Ready(RepoState<'s>),
}

// result returned by a message handler after the gate has been resolved
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
    rx: BufferRx,
    verify_signatures: bool,
    ephemeral: bool,
    num_shards: usize,
    validation_opts: Arc<ValidationOptions>,
}

struct WorkerContext<'a> {
    verify_signatures: bool,
    ephemeral: bool,
    state: &'a AppState,
    batch: OwnedWriteBatch,
    added_blocks: &'a mut i64,
    records_delta: &'a mut i64,
    broadcast_events: &'a mut Vec<BroadcastEvent>,
    vctx: ValidationContext<'a>,
}

impl FirehoseWorker {
    pub fn new(
        state: Arc<AppState>,
        rx: BufferRx,
        verify_signatures: bool,
        ephemeral: bool,
        num_shards: usize,
        validation_opts: ValidationOptions,
    ) -> Self {
        Self {
            state,
            rx,
            verify_signatures,
            ephemeral,
            num_shards,
            validation_opts: Arc::new(validation_opts),
        }
    }

    // starts the worker threads and the main dispatch loop
    // the dispatch loop reads from the firehose channel and
    // distributes messages to shards based on the hash of the DID
    pub fn run(mut self, handle: Handle) -> Result<()> {
        let mut shards = Vec::with_capacity(self.num_shards);

        for i in 0..self.num_shards {
            // unbounded here so we dont block other shards potentially
            // if one has a small lag or something
            let (tx, rx) = mpsc::unbounded_channel();
            shards.push(tx);

            let state = self.state.clone();
            let verify = self.verify_signatures;
            let ephemeral = self.ephemeral;
            let handle = handle.clone();
            let validation_opts = self.validation_opts.clone();

            std::thread::Builder::new()
                .name(format!("ingest-shard-{i}"))
                .spawn(move || {
                    Self::shard(i, rx, state, verify, ephemeral, handle, validation_opts);
                })
                .into_diagnostic()?;
        }

        info!(num = self.num_shards, "started shards");

        let _g = handle.enter();

        // dispatch loop
        while let Some(msg) = self.rx.blocking_recv() {
            let did = match &msg {
                IngestMessage::Firehose { msg: m, .. } => match m {
                    SubscribeReposMessage::Commit(c) => &c.repo,
                    SubscribeReposMessage::Identity(i) => &i.did,
                    SubscribeReposMessage::Account(a) => &a.did,
                    SubscribeReposMessage::Sync(s) => &s.did,
                    _ => continue,
                },
                IngestMessage::BackfillFinished(did) => did,
            };

            // todo: consider using a different hasher?
            let mut hasher = DefaultHasher::new();
            did.hash(&mut hasher);
            let hash = hasher.finish();
            let shard_idx = (hash as usize) % self.num_shards;

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
        mut rx: mpsc::UnboundedReceiver<IngestMessage>,
        state: Arc<AppState>,
        verify_signatures: bool,
        ephemeral: bool,
        handle: Handle,
        validation_opts: Arc<ValidationOptions>,
    ) {
        let _guard = handle.enter();
        debug!(shard = id, "shard started");

        let mut broadcast_events = Vec::new();

        while let Some(msg) = rx.blocking_recv() {
            let batch = state.db.inner.batch();
            broadcast_events.clear();

            let mut added_blocks = 0;
            let mut records_delta = 0;

            let mut ctx = WorkerContext {
                state: &state,
                batch,
                added_blocks: &mut added_blocks,
                records_delta: &mut records_delta,
                broadcast_events: &mut broadcast_events,
                vctx: ValidationContext {
                    opts: &validation_opts,
                },
                verify_signatures,
                ephemeral,
            };

            match msg {
                IngestMessage::BackfillFinished(did) => {
                    debug!(did = %did, "backfill finished, verifying state and draining buffer");

                    let repo_key = keys::repo_key(&did);
                    if let Ok(Some(state_bytes)) = state.db.repos.get(&repo_key).into_diagnostic() {
                        match crate::db::deser_repo_state(&state_bytes) {
                            Ok(repo_state) => {
                                let repo_state = repo_state.into_static();

                                match Self::drain_resync_buffer(&mut ctx, &did, repo_state) {
                                    Ok(RepoProcessResult::Ok(s)) => {
                                        // TODO: there might be a race condition here where we get a new commit
                                        // while the resync buffer is being drained, we should handle that probably
                                        // but also it should still be fine since we'll sync eventually anyway
                                        let res = ops::update_repo_status(
                                            &mut ctx.batch,
                                            &state.db,
                                            &did,
                                            s,
                                            RepoStatus::Synced,
                                        );
                                        if let Err(e) = res {
                                            // this can only fail if serde retry fails which would be really weird
                                            error!(did = %did, err = %e, "failed to transition to synced");
                                        }
                                    }
                                    // we don't have to handle this since drain_resync_buffer doesn't delete
                                    // the commits from the resync buffer so they will get retried later
                                    Ok(RepoProcessResult::NeedsBackfill(_)) => {}
                                    Ok(RepoProcessResult::Deleted) => {}
                                    Err(e) => {
                                        error!(did = %did, err = %e, "failed to drain resync buffer")
                                    }
                                };
                            }
                            Err(e) => error!(did = %did, err = %e, "failed to deser repo state"),
                        }
                    }
                }
                IngestMessage::Firehose {
                    relay: firehose,
                    is_pds,
                    msg,
                } => {
                    let _span = tracing::info_span!("firehose", relay = %firehose).entered();
                    let (did, seq) = match &msg {
                        SubscribeReposMessage::Commit(c) => (&c.repo, c.seq),
                        SubscribeReposMessage::Identity(i) => (&i.did, i.seq),
                        SubscribeReposMessage::Account(a) => (&a.did, a.seq),
                        SubscribeReposMessage::Sync(s) => (&s.did, s.seq),
                        _ => continue,
                    };

                    let gate = match Self::check_repo_state(&mut ctx, did, &msg) {
                        Ok(g) => g,
                        Err(e) => {
                            if let IngestError::Generic(ref r) = e {
                                db::check_poisoned_report(r);
                            }
                            error!(did = %did, err = %e, "error in check_repo_state");
                            state
                                .firehose_cursors
                                .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                            continue;
                        }
                    };

                    match gate {
                        ProcessGate::NewRepo | ProcessGate::Drop => {}
                        ProcessGate::Buffer(commit) => {
                            if let Some(commit) = commit {
                                if let Err(e) =
                                    ops::persist_to_resync_buffer(&state.db, did, commit)
                                {
                                    error!(
                                        did = %did, err = %e,
                                        "failed to persist commit to resync_buffer"
                                    );
                                }
                            }
                        }
                        ProcessGate::Ready(mut repo_state) => {
                            // first validate the pds host
                            if let Some(host) = firehose.host_str()
                                && is_pds
                            {
                                let authority = match Self::check_host_authority(
                                    &mut ctx,
                                    did,
                                    &mut repo_state,
                                    host,
                                ) {
                                    Ok(a) => a,
                                    Err(e) => {
                                        error!(did = %did, err = %e, "failed to check host authority");
                                        state
                                            .firehose_cursors
                                            .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                                        continue;
                                    }
                                };
                                match authority {
                                    AuthorityOutcome::Authorized => {}
                                    AuthorityOutcome::WasStale => {
                                        // pds migrated: our data may be stale, backfill from the new host
                                        warn!(did = %did, source_host = host, "pds migration detected, triggering backfill");
                                        if let Err(e) =
                                            Self::trigger_backfill(&mut ctx, did, repo_state)
                                        {
                                            error!(did = %did, err = %e, "failed to trigger backfill");
                                        } else if let SubscribeReposMessage::Commit(commit) = &msg {
                                            if let Err(e) = ops::persist_to_resync_buffer(
                                                &state.db, did, commit,
                                            ) {
                                                error!(
                                                    did = %did, err = %e,
                                                    "failed to persist commit to resync_buffer"
                                                );
                                            }
                                        }
                                        state
                                            .firehose_cursors
                                            .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                                        continue;
                                    }
                                    // todo: ideally ban pds
                                    AuthorityOutcome::WrongHost { expected } => {
                                        warn!(did = %did, got = host, expected = %expected, "commit rejected: wrong host");
                                        state
                                            .firehose_cursors
                                            .peek_with(&firehose, |_, c| c.store(seq, SeqCst));
                                        continue;
                                    }
                                }
                            }

                            let pre_status = repo_state.status.clone();

                            // if it was in deactivated/takendown/suspended state, we can mark it
                            // as synced because we are receiving an active=true account event now.
                            // we do this before dispatching so handle_account sees pre_status correctly
                            if matches!(
                                pre_status,
                                RepoStatus::Deactivated
                                    | RepoStatus::Suspended
                                    | RepoStatus::Takendown
                            ) {
                                if let SubscribeReposMessage::Account(acc) = &msg {
                                    if acc.active {
                                        match ops::update_repo_status(
                                            &mut ctx.batch,
                                            &ctx.state.db,
                                            did,
                                            repo_state,
                                            RepoStatus::Synced,
                                        ) {
                                            Ok(rs) => {
                                                repo_state = rs;
                                                ctx.state.db.update_gauge_diff(
                                                    &GaugeState::Resync(None),
                                                    &GaugeState::Synced,
                                                );
                                            }
                                            Err(e) => {
                                                error!(
                                                    did = %did, err = %e,
                                                    "failed to transition inactive repo to synced"
                                                );
                                                state
                                                    .firehose_cursors
                                                    .peek_with(&firehose, |_, c| {
                                                        c.store(seq, SeqCst)
                                                    });
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }

                            match Self::process_message(&mut ctx, &msg, did, repo_state, pre_status)
                            {
                                Ok(RepoProcessResult::Ok(_)) => {}
                                Ok(RepoProcessResult::Deleted) => {
                                    state.db.update_count("repos", -1);
                                }
                                Ok(RepoProcessResult::NeedsBackfill(Some(commit))) => {
                                    if let Err(e) =
                                        ops::persist_to_resync_buffer(&state.db, did, commit)
                                    {
                                        error!(
                                            did = %did, err = %e,
                                            "failed to persist commit to resync_buffer"
                                        );
                                    }
                                }
                                Ok(RepoProcessResult::NeedsBackfill(None)) => {}
                                Err(e) => {
                                    if let IngestError::Generic(ref r) = e {
                                        db::check_poisoned_report(r);
                                    }
                                    error!(did = %did, err = %e, "error processing message");
                                    if Self::check_if_retriable_failure(&e) {
                                        if let SubscribeReposMessage::Commit(commit) = &msg {
                                            if let Err(e) = ops::persist_to_resync_buffer(
                                                &state.db, did, commit,
                                            ) {
                                                error!(
                                                    did = %did, err = %e,
                                                    "failed to persist commit to resync_buffer"
                                                );
                                            }
                                        }
                                    }
                                }
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

    fn process_message<'s, 'c>(
        ctx: &mut WorkerContext,
        msg: &'c SubscribeReposMessage<'static>,
        did: &Did,
        repo_state: RepoState<'s>,
        pre_status: RepoStatus,
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        match msg {
            SubscribeReposMessage::Commit(commit) => {
                trace!(did = %did, "processing commit");
                Self::handle_commit(ctx, did, repo_state, commit)
            }
            SubscribeReposMessage::Sync(sync) => {
                debug!(did = %did, "processing sync");
                Self::handle_sync(ctx, did, repo_state, sync)
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!(did = %did, "processing identity");
                Self::handle_identity(ctx, did, repo_state, identity)
            }
            SubscribeReposMessage::Account(account) => {
                debug!(did = %did, "processing account");
                Self::handle_account(ctx, did, repo_state, pre_status, account)
            }
            _ => {
                warn!(did = %did, "unknown message type in buffer");
                Ok(RepoProcessResult::Ok(repo_state))
            }
        }
    }

    fn handle_commit<'s, 'c>(
        ctx: &mut WorkerContext,
        did: &Did,
        mut repo_state: RepoState<'s>,
        commit: &'c Commit<'c>,
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        repo_state.advance_message_time(commit.time.0.timestamp_millis());

        let Some(validated) = ctx.validate_commit(did, &mut repo_state, commit)? else {
            return Ok(RepoProcessResult::Ok(repo_state));
        };

        if validated.chain_break.is_broken() {
            warn!(
                did = %did,
                broken = ?validated.chain_break,
                "chain break detected, triggering backfill"
            );
            Self::trigger_backfill(ctx, did, repo_state)?;
            // not updating repo state root commit since we are backfilling anyway
            return Ok(RepoProcessResult::NeedsBackfill(Some(commit)));
        }

        let res = ops::apply_commit(
            &mut ctx.batch,
            &ctx.state.db,
            repo_state,
            validated,
            &ctx.state.filter.load(),
            ctx.ephemeral,
        )?;
        let repo_state = res.repo_state;
        *ctx.added_blocks += res.blocks_count;
        *ctx.records_delta += res.records_delta;
        ctx.broadcast_events.push(BroadcastEvent::Persisted(
            ctx.state.db.next_event_id.load(SeqCst) - 1,
        ));

        Ok(RepoProcessResult::Ok(repo_state))
    }

    fn handle_sync<'s, 'c>(
        ctx: &mut WorkerContext,
        did: &Did,
        mut repo_state: RepoState<'s>,
        sync: &'c Sync<'c>,
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        repo_state.advance_message_time(sync.time.0.timestamp_millis());

        let Some(validated) = ctx.validate_sync(did, &mut repo_state, sync)? else {
            return Ok(RepoProcessResult::Ok(repo_state));
        };

        // skip noop syncs (data CID unchanged)
        if let Some(current_commit) = &repo_state.root {
            if current_commit.data == validated.commit_obj.data {
                debug!(did = %did, "skipping noop sync");
                return Ok(RepoProcessResult::Ok(repo_state));
            }

            if validated.commit_obj.rev.as_str() <= current_commit.rev.to_tid().as_str() {
                debug!(did = %did, "skipping replayed sync");
                return Ok(RepoProcessResult::Ok(repo_state));
            }
        }
        // not updating repo state root commit since we are backfilling anyway

        warn!(did = %did, "sync event, triggering backfill");
        let repo_state = Self::trigger_backfill(ctx, did, repo_state)?;
        Ok(RepoProcessResult::Ok(repo_state))
    }

    fn handle_identity<'s>(
        ctx: &mut WorkerContext,
        did: &Did,
        mut repo_state: RepoState<'s>,
        identity: &Identity<'_>,
    ) -> Result<RepoProcessResult<'s, 'static>, IngestError> {
        let event_ms = identity.time.0.timestamp_millis();
        if repo_state.last_message_time.is_some_and(|t| event_ms <= t) {
            debug!(did = %did, "skipping stale/duplicate identity event");
            return Ok(RepoProcessResult::Ok(repo_state));
        }
        repo_state.advance_message_time(event_ms);

        // todo: make this match relay sync behaviour
        let changed = if identity.handle.is_none() {
            // no handle sent is basically "invalidate your caches"
            ctx.state.resolver.invalidate_sync(did);
            let doc = Handle::current().block_on(ctx.state.resolver.resolve_doc(did))?;
            repo_state.update_from_doc(doc)
        } else {
            let old_handle = repo_state.handle.clone();
            repo_state.handle = identity
                .handle
                .clone()
                .map(IntoStatic::into_static)
                .or(repo_state.handle);
            repo_state.handle != old_handle
        };

        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            keys::repo_key(did),
            crate::db::ser_repo_state(&repo_state)?,
        );

        if changed {
            let evt = IdentityEvt {
                did: did.clone().into_static(),
                handle: repo_state.handle.clone().map(IntoStatic::into_static),
            };
            ctx.broadcast_events
                .push(ops::make_identity_event(&ctx.state.db, evt));
        }

        Ok(RepoProcessResult::Ok(repo_state))
    }

    fn handle_account<'s, 'c>(
        ctx: &mut WorkerContext,
        did: &Did,
        mut repo_state: RepoState<'s>,
        pre_status: RepoStatus,
        account: &'c Account<'c>,
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        let event_ms = account.time.0.timestamp_millis();
        if repo_state.last_message_time.is_some_and(|t| event_ms <= t) {
            debug!(did = %did, "skipping stale/duplicate account event");
            return Ok(RepoProcessResult::Ok(repo_state));
        }
        repo_state.advance_message_time(event_ms);

        // get active before we do any mutations
        let was_inactive = matches!(
            pre_status,
            RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended
        );
        let is_inactive = !account.active;
        let evt = AccountEvt {
            did: did.clone().into_static(),
            active: account.active,
            status: account.status.as_ref().map(|s| s.to_cowstr().into_static()),
        };

        ctx.refresh_doc(&mut repo_state, did)?;

        if !account.active {
            use crate::ingest::stream::AccountStatus;
            match &account.status {
                Some(AccountStatus::Deleted) => {
                    debug!(did = %did, "account deleted, wiping data");
                    crate::ops::delete_repo(&mut ctx.batch, &ctx.state.db, did, &repo_state)?;
                    return Ok(RepoProcessResult::Deleted);
                }
                status => {
                    let target_status = inactive_account_repo_status(did, status);

                    if repo_state.status == target_status {
                        debug!(did = %did, ?target_status, "account status unchanged");
                        ctx.batch.insert(
                            &ctx.state.db.repos,
                            keys::repo_key(did),
                            crate::db::ser_repo_state(&repo_state)?,
                        );
                        return Ok(RepoProcessResult::Ok(repo_state));
                    }

                    repo_state = ops::update_repo_status(
                        &mut ctx.batch,
                        &ctx.state.db,
                        did,
                        repo_state,
                        target_status,
                    )?;
                    ctx.state
                        .db
                        .update_gauge_diff(&GaugeState::Synced, &GaugeState::Resync(None));
                }
            }
        } else {
            // active=true: transition to synced is handled in the shard dispatch before calling this
        }

        if was_inactive != is_inactive || repo_state.status != pre_status {
            ctx.broadcast_events
                .push(ops::make_account_event(&ctx.state.db, evt));
        }

        // persist last_message_time for paths that don't go through update_repo_status
        // (active=true and already synced). harmless double-write for the status-changed path
        ctx.batch.insert(
            &ctx.state.db.repos,
            keys::repo_key(did),
            crate::db::ser_repo_state(&repo_state)?,
        );

        Ok(RepoProcessResult::Ok(repo_state))
    }

    // checks the current state of the repo in the database and returns a gate
    // indicating what the shard loop should do with the message.
    // if the repo is new, creates initial state and triggers backfill
    // for synced repos with buffered commits, drains the buffer first
    // so events are applied in order
    fn check_repo_state<'s, 'c>(
        ctx: &mut WorkerContext,
        did: &Did<'_>,
        msg: &'c SubscribeReposMessage<'static>,
    ) -> Result<ProcessGate<'s, 'c>, IngestError> {
        let repo_key = keys::repo_key(&did);
        let Some(state_bytes) = ctx.state.db.repos.get(&repo_key).into_diagnostic()? else {
            let filter = ctx.state.filter.load();

            if filter.mode == FilterMode::Filter && !filter.signals.is_empty() {
                let commit = match msg {
                    SubscribeReposMessage::Commit(c) => c,
                    _ => return Ok(ProcessGate::NewRepo),
                };
                let touches_signal = commit.ops.iter().any(|op| {
                    op.path
                        .split_once('/')
                        .map(|(col, _)| {
                            let m = filter.matches_signal(col);
                            debug!(
                                did = %did, path = %op.path, col = %col, signals = ?filter.signals, matched = m,
                                "signal check"
                            );
                            m
                        })
                        .unwrap_or(false)
                });
                if !touches_signal {
                    trace!(did = %did, "dropping commit, no signal-matching ops");
                    return Ok(ProcessGate::NewRepo);
                }
            }

            debug!(did = %did, "discovered new account from firehose, queueing backfill");

            let repo_state = RepoState::untracked(rand::rng().next_u64());
            let mut batch = ctx.state.db.inner.batch();
            batch.insert(
                &ctx.state.db.repos,
                &repo_key,
                crate::db::ser_repo_state(&repo_state)?,
            );
            batch.insert(
                &ctx.state.db.pending,
                keys::pending_key(repo_state.index_id),
                &repo_key,
            );
            batch.commit().into_diagnostic()?;

            ctx.state.db.update_count("repos", 1);
            ctx.state
                .db
                .update_gauge_diff(&GaugeState::Synced, &GaugeState::Pending);

            ctx.state.notify_backfill();

            return Ok(ProcessGate::NewRepo);
        };

        let repo_state = crate::db::deser_repo_state(&state_bytes)?.into_static();

        if !repo_state.tracked && repo_state.status != RepoStatus::Backfilling {
            trace!(did = %did, "ignoring message, repo is explicitly untracked");
            return Ok(ProcessGate::Drop);
        }

        match &repo_state.status {
            RepoStatus::Synced => {
                // lazy drain: if there are buffered commits, drain them now before
                // applying the live message so events are applied in order
                if ops::has_buffered_commits(&ctx.state.db, did) {
                    return match Self::drain_resync_buffer(ctx, did, repo_state)? {
                        RepoProcessResult::Ok(rs) => Ok(ProcessGate::Ready(rs)),
                        // gap triggered during drain, so drop the live message
                        RepoProcessResult::NeedsBackfill(_) => Ok(ProcessGate::Drop),
                        RepoProcessResult::Deleted => Ok(ProcessGate::Drop),
                    };
                }
                Ok(ProcessGate::Ready(repo_state))
            }
            RepoStatus::Backfilling | RepoStatus::Error(_) => {
                debug!(
                    did = %did, status = ?repo_state.status,
                    "ignoring message, repo is backfilling or in error state"
                );
                Ok(ProcessGate::Drop)
            }
            RepoStatus::Deactivated | RepoStatus::Suspended | RepoStatus::Takendown => {
                // account events always pass through because the
                // shard dispatch handles the active=true transition
                if let SubscribeReposMessage::Account(_) = msg {
                    return Ok(ProcessGate::Ready(repo_state));
                }
                // buffer commits and drop everything else until we get an active=true message
                let commit = match msg {
                    SubscribeReposMessage::Commit(c) => Some(c.as_ref()),
                    _ => None,
                };
                Ok(ProcessGate::Buffer(commit))
            }
        }
    }

    fn drain_resync_buffer<'s>(
        ctx: &mut WorkerContext,
        did: &Did,
        mut repo_state: RepoState<'s>,
    ) -> Result<RepoProcessResult<'s, 'static>, IngestError> {
        let prefix = keys::resync_buffer_prefix(did);

        for guard in ctx.state.db.resync_buffer.prefix(&prefix) {
            let (key, value) = guard.into_inner().into_diagnostic()?;
            let commit: Commit = rmp_serde::from_slice(&value).into_diagnostic()?;

            // buffered commits have already been source-checked on arrival; skip host check
            let res = Self::handle_commit(ctx, did, repo_state, &commit);
            let res = match res {
                Ok(r) => r,
                Err(e) => {
                    if !Self::check_if_retriable_failure(&e) {
                        ctx.batch.remove(&ctx.state.db.resync_buffer, key);
                    }
                    return Err(e);
                }
            };
            match res {
                RepoProcessResult::Ok(rs) => {
                    ctx.batch.remove(&ctx.state.db.resync_buffer, key);
                    repo_state = rs;
                }
                RepoProcessResult::NeedsBackfill(_) => {
                    // commit is already in the buffer, leave it there for the next backfill
                    return Ok(RepoProcessResult::NeedsBackfill(None));
                }
                RepoProcessResult::Deleted => {
                    ctx.batch.remove(&ctx.state.db.resync_buffer, key);
                    return Ok(RepoProcessResult::Deleted);
                }
            }
        }

        Ok(RepoProcessResult::Ok(repo_state))
    }

    // transitions repo to Backfilling, commits the status change immediately (separate from
    // ctx.batch), updates the gauge, and pings the backfill worker. returns the updated state.
    fn trigger_backfill<'s>(
        ctx: &mut WorkerContext,
        did: &Did,
        repo_state: RepoState<'s>,
    ) -> Result<RepoState<'s>, IngestError> {
        let mut batch = ctx.state.db.inner.batch();
        let repo_state = ops::update_repo_status(
            &mut batch,
            &ctx.state.db,
            did,
            repo_state,
            RepoStatus::Backfilling,
        )?;
        batch.commit().into_diagnostic()?;
        ctx.state
            .db
            .update_gauge_diff(&GaugeState::Synced, &GaugeState::Pending);
        ctx.state.notify_backfill();
        Ok(repo_state)
    }

    fn check_host_authority(
        ctx: &mut WorkerContext,
        did: &Did,
        repo_state: &mut RepoState,
        source_host: &str,
    ) -> Result<AuthorityOutcome, IngestError> {
        let outcome =
            super::check_host_authority(&ctx.state.resolver, did, repo_state, source_host)?;
        if !matches!(outcome, AuthorityOutcome::Authorized) {
            ctx.batch.insert(
                &ctx.state.db.repos,
                keys::repo_key(did),
                crate::db::ser_repo_state(repo_state)?,
            );
        }
        Ok(outcome)
    }
}

impl WorkerContext<'_> {
    fn refresh_doc(&mut self, repo_state: &mut RepoState, did: &Did) -> Result<(), IngestError> {
        super::refresh_doc(&self.state.resolver, did, repo_state)?;
        self.batch.insert(
            &self.state.db.repos,
            keys::repo_key(did),
            crate::db::ser_repo_state(repo_state)?,
        );
        Ok(())
    }

    fn fetch_key(&self, did: &Did) -> Result<Option<PublicKey<'static>>> {
        super::fetch_key(&self.state.resolver, self.verify_signatures, did)
    }

    fn validate_commit<'s, 'c>(
        &mut self,
        did: &Did,
        repo_state: &mut RepoState<'s>,
        commit: &'c Commit<'c>,
    ) -> Result<Option<ValidatedCommit<'c>>, IngestError> {
        let key = self.fetch_key(did)?;
        match self.vctx.validate_commit(commit, repo_state, key.as_ref()) {
            Ok(v) => return Ok(Some(v)),
            Err(CommitValidationError::StaleRev) => {
                debug!(did = %did, commit_rev = %commit.rev, "skipping replayed commit");
                return Ok(None);
            }
            Err(CommitValidationError::SigFailure) => {}
            Err(e) => {
                warn!(did = %did, err = %e, "commit rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(repo_state, did)?;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_commit(commit, repo_state, key.as_ref()) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                warn!(did = %did, err = %e, "commit rejected after key refresh");
                Ok(None)
            }
        }
    }

    fn validate_sync<'s>(
        &mut self,
        did: &Did,
        repo_state: &mut RepoState<'s>,
        sync: &Sync<'_>,
    ) -> Result<Option<ValidatedSync>, IngestError> {
        let key = self.fetch_key(did)?;
        match self.vctx.validate_sync(sync, key.as_ref()) {
            Ok(v) => return Ok(Some(v)),
            Err(SyncValidationError::SigFailure) => {}
            Err(e) => {
                warn!(did = %did, err = %e, "sync rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(repo_state, did)?;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_sync(sync, key.as_ref()) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                warn!(did = %did, err = %e, "sync rejected after key refresh");
                Ok(None)
            }
        }
    }
}
