use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use fjall::OwnedWriteBatch;

use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::did::Did;
use jacquard_common::{CowStr, IntoStatic};
use miette::{IntoDiagnostic, Result};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{debug, error, info, info_span, trace, warn};
use url::Url;

use crate::db::{self, keys};
use crate::ingest::stream::{
    Account, Commit, Identity, InfoName, SubscribeReposMessage, Sync, encode_frame,
};
use crate::ingest::validation::{
    CommitValidationError, SyncValidationError, ValidatedCommit, ValidatedSync, ValidationContext,
    ValidationOptions,
};
use crate::ingest::{BufferRx, IngestMessage};
use crate::state::AppState;
use crate::types::{RelayBroadcast, RepoState, RepoStatus};

struct WorkerContext<'a> {
    verify_signatures: bool,
    state: &'a AppState,
    vctx: ValidationContext<'a>,
    batch: OwnedWriteBatch,
    pending_broadcasts: Vec<RelayBroadcast>,
}

struct WorkerMessage {
    is_pds: bool,
    firehose: Url,
    msg: SubscribeReposMessage<'static>,
}

pub struct RelayWorker {
    state: Arc<AppState>,
    rx: BufferRx,
    verify_signatures: bool,
    num_shards: usize,
    validation_opts: Arc<ValidationOptions>,
}

impl RelayWorker {
    pub fn new(
        state: Arc<AppState>,
        rx: BufferRx,
        verify_signatures: bool,
        num_shards: usize,
        validation_opts: ValidationOptions,
    ) -> Self {
        Self {
            state,
            rx,
            verify_signatures,
            num_shards,
            validation_opts: Arc::new(validation_opts),
        }
    }

    pub fn run(mut self, handle: Handle) -> Result<()> {
        let mut shards = Vec::with_capacity(self.num_shards);

        for i in 0..self.num_shards {
            let (tx, rx) = mpsc::unbounded_channel();
            shards.push(tx);

            let state = self.state.clone();
            let verify = self.verify_signatures;
            let h = handle.clone();
            let opts = self.validation_opts.clone();

            std::thread::Builder::new()
                .name(format!("relay-shard-{i}"))
                .spawn(move || {
                    Self::shard(i, rx, state, verify, h, opts);
                })
                .into_diagnostic()?;
        }

        info!(num = self.num_shards, "relay worker: started shards");

        let _g = handle.enter();

        while let Some(msg) = self.rx.blocking_recv() {
            let IngestMessage::Firehose {
                relay: firehose,
                is_pds,
                msg,
            } = msg
            else {
                continue;
            };

            // #info only pertains to us, the direct consumer
            if let SubscribeReposMessage::Info(inf) = msg {
                match inf.name {
                    InfoName::OutdatedCursor => {
                        // todo: handle
                    }
                    InfoName::Other(name) => {
                        let message = inf
                            .message
                            .unwrap_or_else(|| CowStr::Borrowed("<no message>"));
                        info!(name = %name, "relay sent info: {message}");
                    }
                }
                continue;
            }

            let shard_idx = {
                let did = match &msg {
                    SubscribeReposMessage::Commit(c) => &c.repo,
                    SubscribeReposMessage::Identity(i) => &i.did,
                    SubscribeReposMessage::Account(a) => &a.did,
                    SubscribeReposMessage::Sync(s) => &s.did,
                    _ => continue,
                };
                let mut hasher = DefaultHasher::new();
                did.hash(&mut hasher);
                let idx = (hasher.finish() as usize) % self.num_shards;
                idx
            };

            if let Err(e) = shards[shard_idx].send(WorkerMessage {
                firehose,
                is_pds,
                msg,
            }) {
                error!(shard = shard_idx, err = %e, "relay worker: failed to send to shard");
                break;
            }
        }

        Err(miette::miette!("relay worker dispatcher shutting down"))
    }

    fn shard(
        id: usize,
        mut rx: mpsc::UnboundedReceiver<WorkerMessage>,
        state: Arc<AppState>,
        verify_signatures: bool,
        handle: Handle,
        validation_opts: Arc<ValidationOptions>,
    ) {
        let _guard = handle.enter();
        let span = info_span!("worker_shard", shard = id, did = tracing::field::Empty);
        let _entered = span.clone().entered();
        debug!("relay shard started");

        let mut ctx = WorkerContext {
            verify_signatures,
            state: &state,
            vctx: ValidationContext {
                opts: &validation_opts,
            },
            batch: state.db.inner.batch(),
            pending_broadcasts: Vec::with_capacity(1),
        };

        while let Some(msg) = rx.blocking_recv() {
            let (did, seq) = match &msg.msg {
                SubscribeReposMessage::Commit(c) => (&c.repo, c.seq),
                SubscribeReposMessage::Identity(i) => (&i.did, i.seq),
                SubscribeReposMessage::Account(a) => (&a.did, a.seq),
                SubscribeReposMessage::Sync(s) => (&s.did, s.seq),
                _ => continue,
            };

            span.record("did", &**did);

            let firehose = msg.firehose.clone();
            if let Err(e) = Self::process_message(&mut ctx, msg) {
                error!(err = %e, "relay shard: error processing message");
            }

            let res = std::mem::replace(&mut ctx.batch, ctx.state.db.inner.batch()).commit();
            if let Err(e) = res {
                error!(shard = id, err = %e, "relay shard: failed to commit batch");
                continue;
            }

            for broadcast in ctx.pending_broadcasts.drain(..) {
                let _ = state.db.relay_broadcast_tx.send(broadcast);
            }

            // advance cursor for this firehose
            ctx.state
                .firehose_cursors
                .peek_with(&firehose, |_, c| c.store(seq, Ordering::SeqCst));
        }
    }

    fn process_message(ctx: &mut WorkerContext, msg: WorkerMessage) -> Result<()> {
        let did = msg
            .msg
            .did()
            .expect("that we checked if we are in valid commit");
        let mut repo_state = ctx.load_repo_state(did)?;

        if let Some(host) = msg.firehose.host_str()
            && msg.is_pds
        {
            let outcome = ctx.check_host_authority(did, &mut repo_state, host)?;
            if let super::AuthorityOutcome::WrongHost { expected } = outcome {
                warn!(got = host, expected = %expected, "message rejected: wrong host");
                return Ok(());
            }
        }

        match msg.msg {
            SubscribeReposMessage::Commit(commit) => {
                trace!("processing commit");
                Self::handle_commit(ctx, &mut repo_state, *commit)
            }
            SubscribeReposMessage::Sync(sync) => {
                debug!("processing sync");
                Self::handle_sync(ctx, &mut repo_state, *sync)
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!("processing identity");
                Self::handle_identity(ctx, &mut repo_state, *identity, msg.is_pds)
            }
            SubscribeReposMessage::Account(account) => {
                debug!("processing account");
                Self::handle_account(ctx, &mut repo_state, *account)
            }
            _ => Ok(()),
        }
    }

    fn handle_commit(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        mut commit: Commit<'static>,
    ) -> Result<()> {
        if repo_state.status != RepoStatus::Synced {
            return Ok(());
        }

        let Some(validated) = ctx.validate_commit(repo_state, &commit)? else {
            return Ok(());
        };
        let ValidatedCommit {
            chain_break,
            commit_obj,
            ..
        } = validated;

        if chain_break.is_broken() {
            warn!(broken = ?chain_break, "out of sync");
            // todo: we need Desynchronized on RepoStatus (and Throttled)
            repo_state.status = RepoStatus::Error("desynchronized".into());
        }

        let repo_key = keys::repo_key(&commit.repo);
        ctx.queue_emit(|seq| {
            commit.seq = seq;
            encode_frame("#commit", &commit)
        })?;

        repo_state.root = Some(commit_obj.into());
        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    fn handle_sync(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        mut sync: Sync<'static>,
    ) -> Result<()> {
        if repo_state.status != RepoStatus::Synced {
            return Ok(());
        }

        let Some(validated) = ctx.validate_sync(repo_state, &sync)? else {
            return Ok(());
        };

        let repo_key = keys::repo_key(&sync.did);
        ctx.queue_emit(|seq| {
            sync.seq = seq;
            encode_frame("#sync", &sync)
        })?;

        repo_state.root = Some(validated.commit_obj.into());
        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    fn handle_identity(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        mut identity: Identity<'static>,
        is_pds: bool,
    ) -> Result<()> {
        let event_ms = identity.time.0.timestamp_millis();
        if repo_state.last_message_time.is_some_and(|t| event_ms <= t) {
            debug!("skipping stale/duplicate identity event");
            return Ok(());
        }
        repo_state.advance_message_time(event_ms);

        // refresh did doc if a pds sent this event
        // or if there is no handle specified
        if is_pds || identity.handle.is_none() {
            ctx.state.resolver.invalidate_sync(&identity.did);
            let doc = Handle::current().block_on(ctx.state.resolver.resolve_doc(&identity.did));
            match doc {
                Ok(doc) => {
                    repo_state.update_from_doc(doc);
                }
                Err(err) => {
                    warn!(err = %err, "couldnt fetch identity");
                }
            }
        }

        // don't pass handle through if it doesnt match ours for pds events
        if is_pds && repo_state.handle != identity.handle {
            identity.handle = None;
        }

        let repo_key = keys::repo_key(&identity.did);
        ctx.queue_emit(|seq| {
            identity.seq = seq;
            encode_frame("#identity", &identity)
        })?;

        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    fn handle_account(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        mut account: Account<'static>,
    ) -> Result<()> {
        let event_ms = account.time.0.timestamp_millis();
        if repo_state.last_message_time.is_some_and(|t| event_ms <= t) {
            debug!("skipping stale/duplicate account event");
            return Ok(());
        }
        repo_state.advance_message_time(event_ms);

        if !account.active {
            use crate::ingest::stream::AccountStatus;
            match &account.status {
                Some(AccountStatus::Deleted) => {
                    // todo: dont remove repo state?
                    // forward the event and remove repo state
                    let repo_key = keys::repo_key(&account.did);
                    ctx.queue_emit(|seq| {
                        account.seq = seq;
                        encode_frame("#account", &account)
                    })?;
                    ctx.batch.remove(&ctx.state.db.repos, repo_key);
                    return Ok(());
                }
                status => {
                    repo_state.status = super::inactive_account_repo_status(&account.did, status);
                }
            }
        } else {
            repo_state.status = RepoStatus::Synced;
        }

        let repo_key = keys::repo_key(&account.did);
        ctx.queue_emit(|seq| {
            account.seq = seq;
            encode_frame("#account", &account)
        })?;

        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }
}

impl WorkerContext<'_> {
    fn check_host_authority(
        &mut self,
        did: &Did,
        repo_state: &mut RepoState,
        source_host: &str,
    ) -> Result<super::AuthorityOutcome> {
        let outcome =
            super::check_host_authority(&self.state.resolver, did, repo_state, source_host)?;
        if !matches!(outcome, super::AuthorityOutcome::Authorized) {
            self.batch.insert(
                &self.state.db.repos,
                keys::repo_key(did),
                db::ser_repo_state(repo_state)?,
            );
        }
        Ok(outcome)
    }

    fn refresh_doc(&mut self, did: &Did, repo_state: &mut RepoState) -> Result<()> {
        super::refresh_doc(&self.state.resolver, did, repo_state)?;
        self.batch.insert(
            &self.state.db.repos,
            keys::repo_key(did),
            db::ser_repo_state(repo_state)?,
        );
        Ok(())
    }

    fn validate_commit<'c>(
        &mut self,
        repo_state: &mut RepoState,
        commit: &'c Commit<'c>,
    ) -> Result<Option<ValidatedCommit<'c>>> {
        let did = &commit.repo;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_commit(commit, repo_state, key.as_ref()) {
            Ok(v) => return Ok(Some(v)),
            Err(CommitValidationError::StaleRev) => {
                trace!("skipping replayed commit");
                return Ok(None);
            }
            Err(CommitValidationError::SigFailure) => {}
            Err(e) => {
                warn!(err = %e, "commit rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(did, repo_state)?;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_commit(commit, repo_state, key.as_ref()) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                warn!(err = %e, "commit rejected after key refresh");
                Ok(None)
            }
        }
    }

    fn validate_sync(
        &mut self,
        repo_state: &mut RepoState,
        sync: &Sync<'_>,
    ) -> Result<Option<ValidatedSync>> {
        let did = &sync.did;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_sync(sync, key.as_ref()) {
            Ok(v) => return Ok(Some(v)),
            Err(SyncValidationError::SigFailure) => {}
            Err(e) => {
                warn!(err = %e, "sync rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(did, repo_state)?;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_sync(sync, key.as_ref()) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                warn!(err = %e, "sync rejected after key refresh");
                Ok(None)
            }
        }
    }

    fn fetch_key(&self, did: &Did) -> Result<Option<PublicKey<'static>>> {
        super::fetch_key(&self.state.resolver, self.verify_signatures, did)
    }

    fn load_repo_state(&self, did: &Did) -> Result<RepoState<'static>> {
        let key = keys::repo_key(did);
        let Some(bytes) = self.state.db.repos.get(&key).into_diagnostic()? else {
            return Ok(RepoState {
                status: RepoStatus::Synced,
                root: None,
                last_updated_at: chrono::Utc::now().timestamp(),
                index_id: 0,
                tracked: true,
                handle: None,
                pds: None,
                signing_key: None,
                last_message_time: None,
            });
        };
        Ok(db::deser_repo_state(&bytes)?.into_static())
    }

    fn queue_emit(&mut self, make_frame: impl FnOnce(i64) -> Result<bytes::Bytes>) -> Result<()> {
        let seq = self.state.db.next_relay_seq.fetch_add(1, Ordering::SeqCst);
        let frame = make_frame(seq as i64)?;
        self.batch.insert(
            &self.state.db.relay_events,
            keys::relay_event_key(seq),
            frame.as_ref(),
        );
        self.pending_broadcasts.push(RelayBroadcast::Persisted(seq));
        Ok(())
    }
}
