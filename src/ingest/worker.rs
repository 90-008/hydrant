use crate::db::{self, keys};
use crate::ingest::{BufferedMessage, IngestMessage};
use crate::ops;
use crate::resolver::{NoSigningKeyError, ResolverError};
use crate::state::AppState;
use crate::types::{AccountEvt, BroadcastEvent, IdentityEvt, RepoState, RepoStatus};
use jacquard::api::com_atproto::sync::subscribe_repos::SubscribeReposMessage;

use fjall::OwnedWriteBatch;

use jacquard::cowstr::ToCowStr;
use jacquard::types::crypto::PublicKey;
use jacquard::types::did::Did;
use jacquard_api::com_atproto::sync::subscribe_repos::Commit;
use jacquard_common::IntoStatic;
use jacquard_repo::error::CommitError;
use miette::{Context, Diagnostic, IntoDiagnostic, Result};
use smol_str::ToSmolStr;
use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
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

#[derive(Debug)]
enum RepoProcessResult<'s, 'c> {
    Deleted,
    Syncing(Option<&'c Commit<'c>>),
    Ok(RepoState<'s>),
}

pub struct FirehoseWorker {
    state: Arc<AppState>,
    rx: mpsc::UnboundedReceiver<BufferedMessage>,
    verify_signatures: bool,
    num_shards: usize,
}

struct WorkerContext<'a> {
    verify_signatures: bool,
    state: &'a AppState,
    repo_cache: &'a mut HashMap<Did<'static>, RepoState<'static>>,
    batch: &'a mut OwnedWriteBatch,
    added_blocks: &'a mut i64,
    records_delta: &'a mut i64,
    broadcast_events: &'a mut Vec<BroadcastEvent>,
    handle: &'a tokio::runtime::Handle,
}

impl FirehoseWorker {
    pub fn new(
        state: Arc<AppState>,
        rx: mpsc::UnboundedReceiver<BufferedMessage>,
        verify_signatures: bool,
        num_shards: usize,
    ) -> Self {
        Self {
            state,
            rx,
            verify_signatures,
            num_shards,
        }
    }

    // starts the worker threads and the main dispatch loop
    // the dispatch loop reads from the firehose channel and distributes messages to shards
    // based on the consistent hash of the DID
    pub fn run(mut self, handle: tokio::runtime::Handle) -> Result<()> {
        let mut shards = Vec::with_capacity(self.num_shards);

        for i in 0..self.num_shards {
            let (tx, rx) = mpsc::unbounded_channel();
            shards.push(tx);

            let state = self.state.clone();
            let verify = self.verify_signatures;
            let handle = handle.clone();

            std::thread::Builder::new()
                .name(format!("ingest-shard-{}", i))
                .spawn(move || {
                    Self::worker_thread(i, rx, state, verify, handle);
                })
                .into_diagnostic()?;
        }

        info!("started {} ingest shards", self.num_shards);

        let _g = handle.enter();

        // dispatch loop
        while let Some(msg) = self.rx.blocking_recv() {
            let did = match &msg {
                IngestMessage::Firehose(m) => match m {
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
                error!("failed to send message to shard {shard_idx}: {e}");
                // break if send fails; receiver likely closed
                break;
            }
        }

        error!("firehose worker dispatcher shutting down");

        Ok(())
    }

    // synchronous worker loop running on a dedicated thread
    // pulls messages from the channel, builds batches, and processes them
    // enters the tokio runtime only when necessary (key resolution)
    fn worker_thread(
        id: usize,
        mut rx: mpsc::UnboundedReceiver<BufferedMessage>,
        state: Arc<AppState>,
        verify_signatures: bool,
        handle: tokio::runtime::Handle,
    ) {
        let _guard = handle.enter();
        debug!("shard {id} started");

        let mut repo_cache = HashMap::new();
        let mut deleted = HashSet::new();
        let mut broadcast_events = Vec::new();

        while let Some(msg) = rx.blocking_recv() {
            let mut batch = state.db.inner.batch();
            repo_cache.clear();
            deleted.clear();
            broadcast_events.clear();

            let mut added_blocks = 0;
            let mut records_delta = 0;

            let mut ctx = WorkerContext {
                state: &state,
                repo_cache: &mut repo_cache,
                batch: &mut batch,
                added_blocks: &mut added_blocks,
                records_delta: &mut records_delta,
                broadcast_events: &mut broadcast_events,
                handle: &handle,
                verify_signatures,
            };

            match msg {
                IngestMessage::BackfillFinished(did) => {
                    debug!("backfill finished for {did}, verifying state and draining buffer");

                    // load repo state to transition status and draining buffer
                    let repo_key = keys::repo_key(&did);
                    if let Ok(Some(state_bytes)) = state.db.repos.get(&repo_key).into_diagnostic() {
                        match crate::db::deser_repo_state(&state_bytes) {
                            Ok(repo_state) => {
                                let repo_state = repo_state.into_static();

                                match Self::drain_resync_buffer(&mut ctx, &did, repo_state) {
                                    Ok(res) => match res {
                                        RepoProcessResult::Ok(s) => {
                                            // TODO: there might be a race condition here where we get a new commit
                                            // while the resync buffer is being drained, we should handle that probably
                                            // but also it should still be fine since we'll sync eventually anyway
                                            match ops::update_repo_status(
                                                &mut batch,
                                                &state.db,
                                                &did,
                                                s,
                                                RepoStatus::Synced,
                                            ) {
                                                Ok(s) => {
                                                    repo_cache.insert(did.clone(), s.into_static());
                                                }
                                                Err(e) => {
                                                    // this can only fail if serde retry fails which would be really weird
                                                    error!(
                                                        "failed to transition {did} to synced: {e}"
                                                    );
                                                }
                                            }
                                        }
                                        RepoProcessResult::Deleted => {
                                            deleted.insert(did.clone());
                                        }
                                        // we don't have to handle this since drain_resync_buffer doesn't delete
                                        // the commits from the resync buffer so they will get retried later
                                        RepoProcessResult::Syncing(_) => {}
                                    },
                                    Err(e) => {
                                        error!("failed to drain resync buffer for {did}: {e}")
                                    }
                                };
                            }
                            Err(e) => error!("failed to deser repo state for {did}: {e}"),
                        }
                    }
                }
                IngestMessage::Firehose(msg) => {
                    let (did, seq) = match &msg {
                        SubscribeReposMessage::Commit(c) => (&c.repo, c.seq),
                        SubscribeReposMessage::Identity(i) => (&i.did, i.seq),
                        SubscribeReposMessage::Account(a) => (&a.did, a.seq),
                        SubscribeReposMessage::Sync(s) => (&s.did, s.seq),
                        _ => continue,
                    };

                    if deleted.contains(did) {
                        continue;
                    }

                    match Self::process_message(&mut ctx, &msg, did) {
                        Ok(RepoProcessResult::Ok(_)) => {}
                        Ok(RepoProcessResult::Deleted) => {
                            deleted.insert(did.clone());
                        }
                        Ok(RepoProcessResult::Syncing(Some(commit))) => {
                            if let Err(e) = ops::persist_to_resync_buffer(&state.db, did, commit) {
                                error!("failed to persist commit to resync_buffer for {did}: {e}");
                            }
                        }
                        Ok(RepoProcessResult::Syncing(None)) => {}
                        Err(e) => {
                            if let IngestError::Generic(e) = &e {
                                db::check_poisoned_report(e);
                            }
                            error!("error processing message for {did}: {e}");
                            if Self::check_if_retriable_failure(&e) {
                                if let SubscribeReposMessage::Commit(commit) = &msg {
                                    if let Err(e) =
                                        ops::persist_to_resync_buffer(&state.db, did, commit)
                                    {
                                        error!(
                                            "failed to persist commit to resync_buffer for {did}: {e}"
                                        );
                                    }
                                }
                            }
                        }
                    }

                    state
                        .cur_firehose
                        .store(seq, std::sync::atomic::Ordering::SeqCst);
                }
            }

            if let Err(e) = batch.commit() {
                error!("failed to commit batch in shard {id}: {e}");
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

    // dont retry commit or sync on key fetch errors
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
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        let check_repo_res = Self::check_repo_state(ctx, did, msg)?;
        let mut repo_state = match check_repo_res {
            RepoProcessResult::Syncing(_) | RepoProcessResult::Deleted => {
                return Ok(check_repo_res);
            }
            RepoProcessResult::Ok(s) => s,
        };

        match msg {
            SubscribeReposMessage::Commit(commit) => {
                trace!("processing buffered commit for {did}");

                return Self::process_commit(ctx, did, repo_state, commit);
            }
            SubscribeReposMessage::Sync(sync) => {
                debug!("processing buffered sync for {did}");

                match ops::verify_sync_event(
                    sync.blocks.as_ref(),
                    Self::fetch_key(ctx, did)?.as_ref(),
                ) {
                    Ok((root, rev)) => {
                        if let Some(current_data) = &repo_state.data {
                            if current_data == &root.to_ipld().expect("valid cid") {
                                debug!("skipping noop sync for {did}");
                                return Ok(RepoProcessResult::Ok(repo_state));
                            }
                        }

                        if let Some(current_rev) = &repo_state.rev {
                            if rev.as_str() <= current_rev.to_tid().as_str() {
                                debug!("skipping replayed sync for {did}");
                                return Ok(RepoProcessResult::Ok(repo_state));
                            }
                        }

                        warn!("sync event for {did}: triggering backfill");
                        let mut batch = ctx.state.db.inner.batch();
                        repo_state = ops::update_repo_status(
                            &mut batch,
                            &ctx.state.db,
                            did,
                            repo_state,
                            RepoStatus::Backfilling,
                        )?;
                        ctx.state.db.update_count("pending", 1);
                        batch.insert(&ctx.state.db.pending, keys::repo_key(did), &[]);
                        batch.commit().into_diagnostic()?;
                        ctx.state.notify_backfill();
                        return Ok(RepoProcessResult::Ok(repo_state));
                    }
                    Err(e) => {
                        error!("failed to process sync event for {did}: {e}");
                    }
                }
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!("processing buffered identity for {did}");
                let handle = identity
                    .handle
                    .as_ref()
                    .map(|h| h.to_cowstr().into_static());

                let evt = IdentityEvt {
                    did: did.clone().into_static(),
                    handle,
                };
                ctx.broadcast_events
                    .push(ops::make_identity_event(&ctx.state.db, evt));
            }
            SubscribeReposMessage::Account(account) => {
                debug!("processing buffered account for {did}");
                let evt = AccountEvt {
                    did: did.clone().into_static(),
                    active: account.active,
                    status: account.status.as_ref().map(|s| s.to_cowstr().into_static()),
                };

                if !account.active {
                    use jacquard::api::com_atproto::sync::subscribe_repos::AccountStatus;
                    match &account.status {
                        Some(AccountStatus::Deleted) => {
                            debug!("account {did} deleted, wiping data");
                            ops::delete_repo(ctx.batch, &ctx.state.db, did)?;
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
                                    AccountStatus::Throttled => {
                                        RepoStatus::Error("throttled".into())
                                    }
                                    AccountStatus::Desynchronized => {
                                        RepoStatus::Error("desynchronized".into())
                                    }
                                    AccountStatus::Other(s) => {
                                        warn!(
                                            "unknown account status for {did}, will put in error state: {s}"
                                        );
                                        RepoStatus::Error(s.to_smolstr())
                                    }
                                },
                                None => {
                                    warn!("account {did} inactive but no status provided");
                                    RepoStatus::Error("unknown".into())
                                }
                            };

                            if repo_state.status == target_status {
                                debug!("account status unchanged for {did}: {target_status:?}");
                                return Ok(RepoProcessResult::Ok(repo_state));
                            }

                            repo_state = ops::update_repo_status(
                                ctx.batch,
                                &ctx.state.db,
                                did,
                                repo_state,
                                target_status,
                            )?;
                            ctx.repo_cache.insert(
                                did.clone().into_static(),
                                repo_state.clone().into_static(),
                            );
                        }
                    }
                } else {
                    // normally we would initiate backfill here
                    // but we don't have to do anything because:
                    // 1. we handle changing repo status to Synced before this (in check repo state)
                    // 2. initiating backfilling is also handled there
                }
                ctx.broadcast_events
                    .push(ops::make_account_event(&ctx.state.db, evt));
            }
            _ => {
                warn!("unknown message type in buffer for {did}");
            }
        }

        Ok(RepoProcessResult::Ok(repo_state))
    }

    fn process_commit<'c, 'ns, 's: 'ns>(
        ctx: &mut WorkerContext,
        did: &Did,
        repo_state: RepoState<'s>,
        commit: &'c Commit<'c>,
    ) -> Result<RepoProcessResult<'ns, 'c>, IngestError> {
        // check for replayed events (already seen revision)
        if matches!(repo_state.rev, Some(ref rev) if commit.rev.as_str() <= rev.to_tid().as_str()) {
            debug!(
                "skipping replayed event for {}: {} <= {}",
                did,
                commit.rev,
                repo_state
                    .rev
                    .as_ref()
                    .map(|r| r.to_tid())
                    .expect("we checked in if")
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
                "gap detected for {}: repo {} != commit prev {}. triggering backfill",
                did, repo, prev_commit.0
            );

            let mut batch = ctx.state.db.inner.batch();
            let repo_state = ops::update_repo_status(
                &mut batch,
                &ctx.state.db,
                did,
                repo_state,
                RepoStatus::Backfilling,
            )?;
            ctx.state.db.update_count("pending", 1);
            batch.insert(&ctx.state.db.pending, keys::repo_key(did), &[]);
            batch.commit().into_diagnostic()?;
            ctx.repo_cache
                .insert(did.clone().into_static(), repo_state.clone().into_static());
            ctx.state.notify_backfill();
            return Ok(RepoProcessResult::Syncing(Some(commit)));
        }

        let res = ops::apply_commit(
            ctx.batch,
            &ctx.state.db,
            repo_state,
            &commit,
            Self::fetch_key(ctx, did)?.as_ref(),
        )?;
        let repo_state = res.repo_state;
        ctx.repo_cache
            .insert(did.clone().into_static(), repo_state.clone().into_static());
        *ctx.added_blocks += res.blocks_count;
        *ctx.records_delta += res.records_delta;
        ctx.broadcast_events.push(BroadcastEvent::Persisted(
            ctx.state
                .db
                .next_event_id
                .load(std::sync::atomic::Ordering::SeqCst)
                - 1,
        ));

        Ok(RepoProcessResult::Ok(repo_state))
    }

    // checks the current state of the repo in the database
    // if the repo is new, creates initial state and triggers backfill
    // handles transitions between states (backfilling -> synced, etc)
    fn check_repo_state<'s, 'c>(
        ctx: &mut WorkerContext,
        did: &Did<'_>,
        msg: &'c SubscribeReposMessage<'static>,
    ) -> Result<RepoProcessResult<'s, 'c>, IngestError> {
        // check if we have this repo
        if let Some(state) = ctx.repo_cache.get(did) {
            return Ok(RepoProcessResult::Ok(state.clone()));
        }

        let repo_key = keys::repo_key(&did);
        let Some(state_bytes) = ctx.state.db.repos.get(&repo_key).into_diagnostic()? else {
            // we don't know this repo, but we are receiving events for it
            // this means we should backfill it before processing its events
            debug!("discovered new account {did} from firehose, queueing backfill");

            let new_state = RepoState::backfilling(did);
            // using a separate batch here since we want to make it known its being backfilled
            // immediately. we could use the batch for the unit of work we are doing but
            // then we wouldn't be able to start backfilling until the unit of work is done

            let mut batch = ctx.state.db.inner.batch();
            batch.insert(
                &ctx.state.db.repos,
                &repo_key,
                crate::db::ser_repo_state(&new_state)?,
            );
            batch.insert(&ctx.state.db.pending, repo_key, &[]);
            ctx.state.db.update_count("repos", 1);
            ctx.state.db.update_count("pending", 1);
            batch.commit().into_diagnostic()?;

            ctx.state.notify_backfill();

            return Ok(RepoProcessResult::Syncing(None));
        };
        let mut repo_state = crate::db::deser_repo_state(&state_bytes)?.into_static();

        // if we are backfilling or it is new, DON'T mark it as synced yet
        // the backfill worker will do that when it finishes
        match &repo_state.status {
            RepoStatus::Synced => {
                // lazy drain: if there are buffered commits, drain them now
                if ops::has_buffered_commits(&ctx.state.db, did) {
                    Self::drain_resync_buffer(ctx, did, repo_state)
                } else {
                    Ok(RepoProcessResult::Ok(repo_state))
                }
            }
            RepoStatus::Backfilling | RepoStatus::Error(_) => {
                debug!(
                    "ignoring active status for {did} as it is {:?}",
                    repo_state.status
                );
                Ok(RepoProcessResult::Syncing(None))
            }
            RepoStatus::Deactivated | RepoStatus::Suspended | RepoStatus::Takendown => {
                // if it was in deactivated/takendown/suspended state, we can mark it as synced
                // because we are receiving live events now
                // UNLESS it is an account status event that keeps it deactivated
                if let SubscribeReposMessage::Account(acc) = msg {
                    if !acc.active {
                        return Ok(RepoProcessResult::Ok(repo_state));
                    }
                }
                repo_state = ops::update_repo_status(
                    ctx.batch,
                    &ctx.state.db,
                    did,
                    repo_state,
                    RepoStatus::Synced,
                )?;
                ctx.repo_cache
                    .insert(did.clone().into_static(), repo_state.clone());
                Ok(RepoProcessResult::Ok(repo_state))
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

            let res = Self::process_commit(ctx, did, repo_state, &commit);
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
                RepoProcessResult::Syncing(_) => {
                    return Ok(RepoProcessResult::Syncing(None));
                }
                RepoProcessResult::Deleted => {
                    ctx.batch.remove(&ctx.state.db.resync_buffer, key);
                    return Ok(RepoProcessResult::Deleted);
                }
            }
        }

        Ok(RepoProcessResult::Ok(repo_state))
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
