use crate::db::{self, keys};
use crate::filter::FilterMode;
use crate::ingest::stream::{Account, Commit, Identity, SubscribeReposMessage, Sync};
use crate::ingest::{BufferRx, IngestMessage};
use crate::ops;
use crate::resolver::{NoSigningKeyError, ResolverError};
use crate::state::AppState;
use crate::types::{AccountEvt, BroadcastEvent, GaugeState, IdentityEvt, RepoState, RepoStatus};

use fjall::OwnedWriteBatch;

use jacquard_common::IntoStatic;
use jacquard_common::cowstr::ToCowStr;
use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::did::Did;
use jacquard_repo::error::CommitError;
use miette::{Context, Diagnostic, IntoDiagnostic, Result};
use rand::Rng;
use smol_str::ToSmolStr;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use thiserror::Error;
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
}

struct WorkerContext<'a> {
    verify_signatures: bool,
    ephemeral: bool,
    state: &'a AppState,
    batch: OwnedWriteBatch,
    added_blocks: &'a mut i64,
    records_delta: &'a mut i64,
    broadcast_events: &'a mut Vec<BroadcastEvent>,
    handle: &'a tokio::runtime::Handle,
}

impl FirehoseWorker {
    pub fn new(
        state: Arc<AppState>,
        rx: BufferRx,
        verify_signatures: bool,
        ephemeral: bool,
        num_shards: usize,
    ) -> Self {
        Self {
            state,
            rx,
            verify_signatures,
            ephemeral,
            num_shards,
        }
    }

    // starts the worker threads and the main dispatch loop
    // the dispatch loop reads from the firehose channel and
    // distributes messages to shards based on the hash of the DID
    pub fn run(mut self, handle: tokio::runtime::Handle) -> Result<()> {
        let mut shards = Vec::with_capacity(self.num_shards);

        for i in 0..self.num_shards {
            let (tx, rx) = mpsc::unbounded_channel();
            shards.push(tx);

            let state = self.state.clone();
            let verify = self.verify_signatures;
            let ephemeral = self.ephemeral;
            let handle = handle.clone();

            std::thread::Builder::new()
                .name(format!("ingest-shard-{i}"))
                .spawn(move || {
                    Self::shard(i, rx, state, verify, ephemeral, handle);
                })
                .into_diagnostic()?;
        }

        info!("started {} ingest shards", self.num_shards);

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

            let mut hasher = DefaultHasher::new();
            did.hash(&mut hasher);
            let hash = hasher.finish();
            let shard_idx = (hash as usize) % self.num_shards;

            if let Err(e) = shards[shard_idx].send(msg) {
                error!(shard = shard_idx, err = %e, "failed to send message to shard");
                // break if send fails; receiver likely closed
                break;
            }
        }

        error!("firehose worker dispatcher shutting down");

        Ok(())
    }

    #[inline(always)]
    fn shard(
        id: usize,
        mut rx: mpsc::UnboundedReceiver<IngestMessage>,
        state: Arc<AppState>,
        verify_signatures: bool,
        ephemeral: bool,
        handle: tokio::runtime::Handle,
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
                handle: &handle,
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
                IngestMessage::Firehose { relay, msg } => {
                    let _span = tracing::info_span!("firehose", relay = %relay).entered();
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
                                .relay_cursors
                                .peek_with(&relay, |_, c| c.store(seq, SeqCst));
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
                                                    .relay_cursors
                                                    .peek_with(&relay, |_, c| c.store(seq, SeqCst));
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }

                            match Self::process_message(&mut ctx, &msg, did, repo_state, pre_status)
                            {
                                Ok(RepoProcessResult::Ok(_)) => {}
                                Ok(RepoProcessResult::Deleted) => {}
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
                        .relay_cursors
                        .peek_with(&relay, |_, c| c.store(seq, SeqCst));
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

            state.db.inner.persist(fjall::PersistMode::Buffer).ok();
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

        // skip replayed events (already seen revision)
        if matches!(repo_state.rev, Some(ref rev) if commit.rev.as_str() <= rev.to_tid().as_str()) {
            debug!(
                did = %did,
                commit_rev = %commit.rev,
                state_rev = %repo_state.rev.as_ref().map(|r| r.to_tid()).expect("we checked in if"),
                "skipping replayed event"
            );
            return Ok(RepoProcessResult::Ok(repo_state));
        }

        if let (Some(repo), Some(prev_commit)) = (&repo_state.data, &commit.prev_data)
            && repo
                != &prev_commit
                    .0
                    .to_ipld()
                    .into_diagnostic()
                    .wrap_err("invalid cid from relay")?
        {
            warn!(
                did = %did,
                repo = %repo,
                prev_commit = %prev_commit.0,
                "gap detected, triggering backfill"
            );

            let mut batch = ctx.state.db.inner.batch();
            let _repo_state = ops::update_repo_status(
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
            return Ok(RepoProcessResult::NeedsBackfill(Some(commit)));
        }

        let signing_key = Self::fetch_key(ctx, did)?;
        let res = ops::apply_commit(
            &mut ctx.batch,
            &ctx.state.db,
            repo_state,
            commit,
            signing_key.as_ref(),
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

        Self::refresh_doc(ctx, &mut repo_state, did)?;

        match ops::verify_sync_event(sync.blocks.as_ref(), Self::fetch_key(ctx, did)?.as_ref()) {
            Ok((root, rev)) => {
                if let Some(current_data) = &repo_state.data {
                    if current_data == &root.to_ipld().expect("valid cid") {
                        debug!(did = %did, "skipping noop sync");
                        return Ok(RepoProcessResult::Ok(repo_state));
                    }
                }

                if let Some(current_rev) = &repo_state.rev {
                    if rev.as_str() <= current_rev.to_tid().as_str() {
                        debug!(did = %did, "skipping replayed sync");
                        return Ok(RepoProcessResult::Ok(repo_state));
                    }
                }

                warn!(did = %did, "sync event, triggering backfill");
                let mut batch = ctx.state.db.inner.batch();
                repo_state = ops::update_repo_status(
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
                Ok(RepoProcessResult::Ok(repo_state))
            }
            Err(e) => {
                error!(did = %did, err = %e, "failed to process sync event");
                Ok(RepoProcessResult::Ok(repo_state))
            }
        }
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

        let changed = if identity.handle.is_none() {
            // no handle sent is basically "invalidate your caches"
            ctx.state.resolver.invalidate_sync(did);
            let doc = ctx.handle.block_on(ctx.state.resolver.resolve_doc(did))?;
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

        Self::refresh_doc(ctx, &mut repo_state, did)?;

        if !account.active {
            use crate::ingest::stream::AccountStatus;
            match &account.status {
                Some(AccountStatus::Deleted) => {
                    debug!(did = %did, "account deleted, wiping data");
                    crate::ops::delete_repo(&mut ctx.batch, &ctx.state.db, did, &repo_state)?;
                    return Ok(RepoProcessResult::Deleted);
                }
                status => {
                    let target_status = match status {
                        Some(status) => match status {
                            AccountStatus::Deleted => {
                                unreachable!("deleted account status is handled before")
                            }
                            AccountStatus::Takendown => RepoStatus::Takendown,
                            AccountStatus::Suspended => RepoStatus::Suspended,
                            AccountStatus::Deactivated => RepoStatus::Deactivated,
                            AccountStatus::Throttled => RepoStatus::Error("throttled".into()),
                            AccountStatus::Desynchronized => {
                                RepoStatus::Error("desynchronized".into())
                            }
                            AccountStatus::Other(s) => {
                                warn!(
                                    did = %did, status = %s,
                                    "unknown account status, will put in error state"
                                );
                                RepoStatus::Error(s.to_smolstr())
                            }
                        },
                        None => {
                            warn!(did = %did, "account inactive but no status provided");
                            RepoStatus::Error("unknown".into())
                        }
                    };

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

    // refreshes the handle, pds url and signing key of a did
    fn refresh_doc(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        did: &Did,
    ) -> Result<(), IngestError> {
        ctx.state.resolver.invalidate_sync(did);
        let doc = ctx.handle.block_on(ctx.state.resolver.resolve_doc(did))?;
        repo_state.update_from_doc(doc);
        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            keys::repo_key(did),
            crate::db::ser_repo_state(&repo_state)?,
        );
        Ok(())
    }

    fn fetch_key(
        ctx: &WorkerContext,
        did: &Did,
    ) -> Result<Option<PublicKey<'static>>, IngestError> {
        if ctx.verify_signatures {
            let key = ctx
                .handle
                .block_on(ctx.state.resolver.resolve_signing_key(did))?;
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }
}
