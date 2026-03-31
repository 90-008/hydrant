use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "relay")]
use std::sync::atomic::Ordering;

use fjall::OwnedWriteBatch;

use jacquard_api::com_atproto::sync::get_repo_status::{
    GetRepoStatus, GetRepoStatusError, GetRepoStatusOutputStatus,
};
use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::did::Did;
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use jacquard_common::{CowStr, IntoStatic};
use miette::{IntoDiagnostic, Result};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{debug, error, info, info_span, trace, warn};
use url::Url;

use crate::db::{self, keys};
use crate::ingest::stream::AccountStatus;
#[cfg(feature = "relay")]
use crate::ingest::stream::encode_frame;
use crate::ingest::stream::{Account, Commit, Identity, InfoName, SubscribeReposMessage, Sync};
use crate::ingest::validation::{
    CommitValidationError, SyncValidationError, ValidatedCommit, ValidatedSync, ValidationContext,
    ValidationOptions,
};
use crate::ingest::{BufferRx, IngestMessage};
use crate::state::AppState;
#[cfg(feature = "relay")]
use crate::types::RelayBroadcast;
use crate::types::{RepoState, RepoStatus};
use crate::util;
use smol_str::{SmolStr, ToSmolStr};

struct WorkerContext<'a> {
    verify_signatures: bool,
    state: &'a AppState,
    vctx: ValidationContext<'a>,
    batch: OwnedWriteBatch,
    #[cfg(feature = "relay")]
    pending_broadcasts: Vec<RelayBroadcast>,
    #[cfg(feature = "indexer")]
    pending_hook_messages: Vec<crate::ingest::indexer::IndexerMessage>,
    #[cfg(feature = "indexer")]
    hook: crate::ingest::indexer::IndexerTx,
    http: reqwest::Client,
    error_counts: HashMap<u64, u32, nohash_hasher::BuildNoHashHasher<u64>>,
}

struct WorkerMessage {
    is_pds: bool,
    firehose: Url,
    msg: SubscribeReposMessage<'static>,
}

pub struct RelayWorker {
    state: Arc<AppState>,
    rx: BufferRx,
    #[cfg(feature = "indexer")]
    hook: crate::ingest::indexer::IndexerTx,
    verify_signatures: bool,
    num_shards: usize,
    validation_opts: Arc<ValidationOptions>,
    http: reqwest::Client,
}

impl RelayWorker {
    pub fn new(
        state: Arc<AppState>,
        rx: BufferRx,
        #[cfg(feature = "indexer")] hook: crate::ingest::indexer::IndexerTx,
        verify_signatures: bool,
        num_shards: usize,
        validation_opts: ValidationOptions,
    ) -> Self {
        Self {
            state,
            rx,
            #[cfg(feature = "indexer")]
            hook,
            verify_signatures,
            num_shards,
            validation_opts: Arc::new(validation_opts),
            http: reqwest::Client::new(),
        }
    }

    pub fn run(mut self, handle: Handle) -> Result<()> {
        let mut shards = Vec::with_capacity(self.num_shards);

        for i in 0..self.num_shards {
            let (tx, rx) = mpsc::unbounded_channel();
            shards.push(tx);

            let state = self.state.clone();
            #[cfg(feature = "indexer")]
            let hook = self.hook.clone();
            let verify = self.verify_signatures;
            let h = handle.clone();
            let opts = self.validation_opts.clone();
            let http = self.http.clone();

            std::thread::Builder::new()
                .name(format!("relay-shard-{i}"))
                .spawn(move || {
                    Self::shard(
                        i,
                        rx,
                        state,
                        #[cfg(feature = "indexer")]
                        hook,
                        verify,
                        h,
                        opts,
                        http,
                    );
                })
                .into_diagnostic()?;
        }

        info!(num = self.num_shards, "relay worker: started shards");

        let _g = handle.enter();

        while let Some(msg) = self.rx.blocking_recv() {
            let IngestMessage::Firehose { url, is_pds, msg } = msg;

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
                (util::hash(did) as usize) % self.num_shards
            };

            if let Err(e) = shards[shard_idx].send(WorkerMessage {
                firehose: url,
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
        #[cfg(feature = "indexer")] hook: crate::ingest::indexer::IndexerTx,
        verify_signatures: bool,
        handle: Handle,
        validation_opts: Arc<ValidationOptions>,
        http: reqwest::Client,
    ) {
        let _guard = handle.enter();
        let span = info_span!("worker_shard", shard = id);
        let _entered = span.clone().entered();
        debug!("relay shard started");

        let mut ctx = WorkerContext {
            verify_signatures,
            state: &state,
            vctx: ValidationContext {
                opts: &validation_opts,
            },
            batch: state.db.inner.batch(),
            #[cfg(feature = "relay")]
            pending_broadcasts: Vec::with_capacity(2),
            #[cfg(feature = "indexer")]
            pending_hook_messages: Vec::with_capacity(2),
            #[cfg(feature = "indexer")]
            hook,
            http,
            error_counts: Default::default(),
        };

        while let Some(msg) = rx.blocking_recv() {
            let (did, seq) = match &msg.msg {
                SubscribeReposMessage::Commit(c) => (c.repo.clone(), c.seq),
                SubscribeReposMessage::Identity(i) => (i.did.clone(), i.seq),
                SubscribeReposMessage::Account(a) => (a.did.clone(), a.seq),
                SubscribeReposMessage::Sync(s) => (s.did.clone(), s.seq),
                _ => continue,
            };

            let firehose = msg.firehose.clone();
            let _span = info_span!("relay", did = %did, firehose = %firehose, seq = %seq).entered();

            if let Err(e) = Self::process_message(&mut ctx, msg) {
                error!(did = %did, err = %e, "relay shard: error processing message");
            }

            let res = std::mem::replace(&mut ctx.batch, ctx.state.db.inner.batch()).commit();
            if let Err(e) = res {
                error!(shard = id, err = %e, "relay shard: failed to commit batch");
                continue;
            }

            #[cfg(feature = "relay")]
            for broadcast in ctx.pending_broadcasts.drain(..) {
                let _ = state.db.relay_broadcast_tx.send(broadcast);
            }
            #[cfg(feature = "indexer")]
            for msg in ctx.pending_hook_messages.drain(..) {
                let _ = ctx.hook.blocking_send(msg);
            }

            // advance cursor for this firehose only if we are the terminal consumer (relay mode)
            // in events mode, FirehoseWorker will advance the cursor after processing
            #[cfg(feature = "relay")]
            {
                ctx.state
                    .firehose_cursors
                    .peek_with(&firehose, |_, c| c.store(seq, Ordering::SeqCst));
            }
        }
    }

    fn process_message(ctx: &mut WorkerContext, msg: WorkerMessage) -> Result<()> {
        let Some(mut repo_state) = ctx.load_repo_state(&msg)? else {
            return Ok(());
        };
        let did = msg.msg.did().expect("already checked for did");

        if let Some(host) = msg.firehose.host_str()
            && msg.is_pds
        {
            let outcome = ctx.check_host_authority(did, &mut repo_state, host)?;
            if let AuthorityOutcome::WrongHost { expected } = outcome {
                if !ctx.inc_error(host) {
                    warn!(got = host, expected = %expected, "message rejected: wrong host");
                }
                return Ok(());
            }
            ctx.reset_error(host);
        }

        match msg.msg {
            SubscribeReposMessage::Commit(commit) => {
                trace!("processing commit");
                Self::handle_commit(ctx, &mut repo_state, &msg.firehose, *commit)
            }
            SubscribeReposMessage::Sync(sync) => {
                debug!("processing sync");
                Self::handle_sync(ctx, &mut repo_state, &msg.firehose, *sync)
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!("processing identity");
                Self::handle_identity(ctx, &mut repo_state, &msg.firehose, *identity, msg.is_pds)
            }
            SubscribeReposMessage::Account(account) => {
                debug!("processing account");
                Self::handle_account(ctx, &mut repo_state, &msg.firehose, *account, msg.is_pds)
            }
            _ => Ok(()),
        }
    }

    fn handle_commit(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        #[allow(unused_variables)] firehose: &Url,
        #[allow(unused_mut)] mut commit: Commit<'static>,
    ) -> Result<()> {
        if !repo_state.active {
            return Ok(());
        }

        repo_state.advance_message_time(commit.time.0.timestamp_millis());

        let Some(validated) = ctx.validate_commit(repo_state, &commit)? else {
            return Ok(());
        };
        let ValidatedCommit {
            chain_break,
            commit_obj,
            parsed_blocks,
            ..
        } = validated;

        if chain_break.is_broken() {
            // chain breaks are not grounds for blocking when acting as a relay
            debug!(broken = ?chain_break, "chain break, forwarding anyway");
        }

        let repo_key = keys::repo_key(&commit.repo);

        #[cfg(feature = "indexer")]
        {
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: commit.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Commit(
                            crate::ingest::indexer::IndexerCommitData {
                                commit,
                                chain_break: chain_break.is_broken(),
                                parsed_blocks,
                            },
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            ctx.queue_emit(|seq| {
                commit.seq = seq;
                encode_frame("#commit", &commit)
            })?;
        }

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
        #[allow(unused_variables)] firehose: &Url,
        #[allow(unused_mut)] mut sync: Sync<'static>,
    ) -> Result<()> {
        if !repo_state.active {
            return Ok(());
        }

        repo_state.advance_message_time(sync.time.0.timestamp_millis());

        let Some(validated) = ctx.validate_sync(repo_state, &sync)? else {
            return Ok(());
        };

        let repo_key = keys::repo_key(&sync.did);

        #[cfg(feature = "indexer")]
        {
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: sync.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Sync(
                            sync.did.into_static(),
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            ctx.queue_emit(|seq| {
                sync.seq = seq;
                encode_frame("#sync", &sync)
            })?;
        }

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
        #[allow(unused_variables)] firehose: &Url,
        mut identity: Identity<'static>,
        is_pds: bool,
    ) -> Result<()> {
        let event_ms = identity.time.0.timestamp_millis();
        if repo_state.last_message_time.is_some_and(|t| event_ms <= t) {
            debug!("skipping stale/duplicate identity event");
            return Ok(());
        }
        repo_state.advance_message_time(event_ms);

        #[cfg(feature = "indexer")]
        let (was_handle, was_signing_key) = (
            repo_state.handle.clone().map(IntoStatic::into_static),
            repo_state.signing_key.clone().map(IntoStatic::into_static),
        );

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

        #[cfg(feature = "indexer")]
        {
            let changed =
                repo_state.handle != was_handle || repo_state.signing_key != was_signing_key;
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: identity.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Identity(
                            crate::ingest::indexer::IndexerIdentityData { identity, changed },
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            ctx.queue_emit(|seq| {
                identity.seq = seq;
                encode_frame("#identity", &identity)
            })?;
        }

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
        firehose: &Url,
        #[allow(unused_mut)] mut account: Account<'static>,
        is_pds: bool,
    ) -> Result<()> {
        let event_ms = account.time.0.timestamp_millis();
        if repo_state.last_message_time.is_some_and(|t| event_ms <= t) {
            debug!("skipping stale/duplicate account event");
            return Ok(());
        }

        repo_state.advance_message_time(event_ms);

        // always capture was_active for count tracking, not just in indexer mode
        let was_active = repo_state.active;
        #[cfg(feature = "indexer")]
        let was_status = repo_state.status.clone();

        repo_state.active = account.active;
        if !account.active {
            use crate::ingest::stream::AccountStatus;
            match &account.status {
                Some(AccountStatus::Deleted) => {
                    // keep a Deleted tombstone so any stale commits that arrive later
                    // (e.g. from the upstream backfill window) are not forwarded.
                    // per spec: "if any further #commit messages are emitted for the repo,
                    // all downstream services should ignore the event and not pass it through."
                    repo_state.status = RepoStatus::Deleted;
                }
                status => {
                    repo_state.status = ctx.inactive_account_repo_status(&account.did, status);
                }
            }
        } else {
            // active=true: desynchronized/throttled may still carry active=true per spec.
            // anything else (including unknown statuses) is treated as synced.
            use crate::ingest::stream::AccountStatus;
            repo_state.status = match &account.status {
                Some(AccountStatus::Desynchronized) => RepoStatus::Desynchronized,
                Some(AccountStatus::Throttled) => RepoStatus::Throttled,
                _ => RepoStatus::Synced,
            };
        }

        // update per-PDS active account count on transitions
        if is_pds {
            if let Some(host) = firehose.host_str() {
                let count_key = keys::pds_account_count_key(host);
                if !was_active && repo_state.active {
                    ctx.state.db.update_count(&count_key, 1);
                } else if was_active && !repo_state.active {
                    ctx.state.db.update_count(&count_key, -1);
                }
            }
        }

        let repo_key = keys::repo_key(&account.did);

        #[cfg(feature = "indexer")]
        {
            let changed = repo_state.active != was_active || repo_state.status != was_status;
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: account.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Account(
                            crate::ingest::indexer::IndexerAccountData {
                                account,
                                was_active,
                                changed,
                            },
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            ctx.queue_emit(|seq| {
                account.seq = seq;
                encode_frame("#account", &account)
            })?;
        }

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
    /// increments host error counter, returns if host is suppressed or not
    fn inc_error(&mut self, host: &str) -> bool {
        let error_count = self.error_counts.entry(util::hash(&host)).or_default();
        let is_suppressed = *error_count > 50;
        *error_count += 1;
        is_suppressed
    }

    fn reset_error(&mut self, host: &str) {
        if let Some(count) = self.error_counts.get_mut(&util::hash(&host)) {
            *count = 0;
        }
    }

    fn check_host_authority(
        &mut self,
        did: &Did,
        repo_state: &mut RepoState,
        source_host: &str,
    ) -> Result<AuthorityOutcome> {
        let pds_host = |pds: &str| {
            Url::parse(pds)
                .ok()
                .and_then(|u| u.host_str().map(SmolStr::new))
        };

        let expected = repo_state.pds.as_deref().and_then(pds_host);
        if expected.as_deref() == Some(source_host) {
            return Ok(AuthorityOutcome::Authorized);
        }

        // try again once
        self.refresh_doc(did, repo_state)?;
        let Some(expected) = repo_state.pds.as_deref().and_then(pds_host) else {
            miette::bail!("can't get pds host???");
        };

        Ok((expected.as_str() == source_host)
            .then_some(AuthorityOutcome::WasStale)
            .unwrap_or(AuthorityOutcome::WrongHost { expected }))
    }

    fn refresh_doc(&mut self, did: &Did, repo_state: &mut RepoState) -> Result<()> {
        let db = &self.state.db;
        self.state.resolver.invalidate_sync(did);
        let doc = Handle::current()
            .block_on(self.state.resolver.resolve_doc(did))
            .map_err(|e| miette::miette!("{e}"))?;
        repo_state.update_from_doc(doc);
        repo_state.touch();

        self.batch.insert(
            &db.repos,
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
                debug!(err = %e, "commit rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(did, repo_state)?;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_commit(commit, repo_state, key.as_ref()) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                debug!(err = %e, "commit rejected after key refresh");
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
                debug!(err = %e, "sync rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(did, repo_state)?;
        let key = self.fetch_key(did)?;
        match self.vctx.validate_sync(sync, key.as_ref()) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                debug!(err = %e, "sync rejected after key refresh");
                Ok(None)
            }
        }
    }

    fn fetch_key(&self, did: &Did) -> Result<Option<PublicKey<'static>>> {
        if self.verify_signatures {
            let key = Handle::current()
                .block_on(self.state.resolver.resolve_signing_key(did))
                .map_err(|e| miette::miette!("{e}"))?;
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    /// maps an inactive account status to the corresponding `RepoStatus`.
    /// panics on `AccountStatus::Deleted`, caller must handle that
    fn inactive_account_repo_status(
        &self,
        did: &Did,
        status: &Option<AccountStatus<'_>>,
    ) -> RepoStatus {
        match status {
            Some(AccountStatus::Takendown) => RepoStatus::Takendown,
            Some(AccountStatus::Suspended) => RepoStatus::Suspended,
            Some(AccountStatus::Deactivated) => RepoStatus::Deactivated,
            Some(AccountStatus::Throttled) => RepoStatus::Throttled,
            Some(AccountStatus::Desynchronized) => RepoStatus::Desynchronized,
            Some(AccountStatus::Other(s)) => {
                warn!(did = %did, status = %s, "unknown account status");
                RepoStatus::Error(s.to_smolstr())
            }
            Some(AccountStatus::Deleted) => {
                unreachable!("deleted is handled before status mapping")
            }
            None => {
                warn!(did = %did, "account inactive but no status provided");
                RepoStatus::Error("unknown".into())
            }
        }
    }

    async fn check_repo_status(
        &self,
        did: &Did<'_>,
        pds: &Url,
    ) -> Result<Option<RepoState<'static>>> {
        let req = GetRepoStatus::new().did(did.clone().into_static()).build();
        let resp = self
            .http
            .xrpc(crate::util::url_to_fluent_uri(pds))
            .send(&req)
            .await;

        let output = match resp {
            Err(_) => return Ok(None),
            Ok(r) => match r.into_output() {
                Ok(o) => o,
                Err(XrpcError::Xrpc(GetRepoStatusError::RepoNotFound(_))) => {
                    // pds explicitly says it doesn't have this repo
                    // we shouldnt really get here unless the pds is buggy?
                    // or somehow the repo gets gon right after we receive the event
                    let mut repo_state = RepoState::backfilling();
                    repo_state.active = false;
                    repo_state.status = RepoStatus::Error("not_found".into());
                    return Ok(Some(repo_state));
                }
                Err(_) => return Ok(None),
            },
        };

        let mut repo_state = RepoState::backfilling();
        repo_state.active = output.active;
        repo_state.status = match output.status {
            Some(GetRepoStatusOutputStatus::Takendown) => RepoStatus::Takendown,
            Some(GetRepoStatusOutputStatus::Suspended) => RepoStatus::Suspended,
            Some(GetRepoStatusOutputStatus::Deactivated) => RepoStatus::Deactivated,
            Some(GetRepoStatusOutputStatus::Deleted) => RepoStatus::Deleted,
            Some(GetRepoStatusOutputStatus::Desynchronized) => RepoStatus::Desynchronized,
            Some(GetRepoStatusOutputStatus::Throttled) => RepoStatus::Throttled,
            Some(GetRepoStatusOutputStatus::Other(s)) => RepoStatus::Error(s.into()),
            None => output
                .active
                .then_some(RepoStatus::Synced)
                .unwrap_or_else(|| RepoStatus::Error("unknown".into())),
        };

        Ok(Some(repo_state))
    }

    fn load_repo_state(&mut self, msg: &WorkerMessage) -> Result<Option<RepoState<'static>>> {
        let db = &self.state.db;
        let did = msg.msg.did().expect("we checked if valid");
        let repo_key = keys::repo_key(did);
        let metadata_key = keys::repo_metadata_key(did);

        let metadata = db
            .repo_metadata
            .get(&metadata_key)
            .into_diagnostic()?
            .map(|bytes| db::deser_repo_metadata(&bytes))
            .transpose()?;

        if metadata.map_or(false, |m| !m.tracked) {
            trace!(did = %did, "ignoring message, repo is explicitly untracked");
            return Ok(None);
        }

        let repo_state_opt = db
            .repos
            .get(&repo_key)
            .into_diagnostic()?
            .map(|bytes| db::deser_repo_state(bytes.as_ref()).map(|s| s.into_static()))
            .transpose()?;

        if let Some(repo_state) = repo_state_opt {
            return Ok(Some(repo_state));
        }

        #[cfg(feature = "indexer")]
        {
            let filter = self.state.filter.load();
            if filter.mode == crate::filter::FilterMode::Filter && !filter.signals.is_empty() {
                let commit = match &msg.msg {
                    SubscribeReposMessage::Commit(c) => c,
                    _ => return Ok(None),
                };
                let touches_signal = commit.ops.iter().any(|op| {
                    op.path
                        .split_once('/')
                        .map(|(col, _)| {
                            let m = filter.matches_signal(col);
                            debug!(
                                did = %did, path = %op.path, col = %col,
                                signals = ?filter.signals, matched = m,
                                "signal check"
                            );
                            m
                        })
                        .unwrap_or(false)
                });
                if !touches_signal {
                    trace!(did = %did, "dropping commit, no signal-matching ops");
                    return Ok(None);
                }
            }
        }

        debug!(did = %did, "discovered new account from firehose, queueing backfill");

        // resolve doc to initialize repo state
        self.state.resolver.invalidate_sync(did);
        let doc = tokio::runtime::Handle::current()
            .block_on(self.state.resolver.resolve_doc(did))
            .into_diagnostic()?;

        // if it's a PDS, verify it's the authoritative one
        if msg.is_pds {
            let pds_host = doc.pds.host_str().map(|h| h.to_string());
            if pds_host.as_deref() != msg.firehose.host_str() {
                warn!(did = %did, got = ?pds_host, expected = ?msg.firehose.host_str(), "message rejected: wrong host for new account");
                return Ok(None);
            }
        }

        // try to get upstream status
        let mut repo_state = tokio::runtime::Handle::current()
            .block_on(self.check_repo_status(did, &doc.pds))
            .ok()
            .flatten()
            .unwrap_or_else(RepoState::backfilling);

        repo_state.update_from_doc(doc);

        self.batch.insert(
            &db.repos,
            &repo_key,
            crate::db::ser_repo_state(&repo_state)?,
        );

        #[cfg(feature = "indexer")]
        {
            self.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::NewRepo(
                    did.clone().into_static(),
                ));
        }

        db.update_count("repos", 1);

        // track initial active state for per-PDS rate limiting
        if msg.is_pds && repo_state.active {
            if let Some(host) = msg.firehose.host_str() {
                db.update_count(&keys::pds_account_count_key(host), 1);
            }
        }

        Ok(Some(repo_state))
    }

    #[cfg(feature = "relay")]
    fn queue_emit(&mut self, make_frame: impl FnOnce(i64) -> Result<bytes::Bytes>) -> Result<()> {
        let db = &self.state.db;
        let seq = db.next_relay_seq.fetch_add(1, Ordering::SeqCst);
        let frame = make_frame(seq as i64)?;
        self.batch
            .insert(&db.relay_events, keys::relay_event_key(seq), frame.as_ref());
        self.pending_broadcasts.push(RelayBroadcast::Persisted(seq));
        Ok(())
    }
}

/// outcome of a host authority check.
enum AuthorityOutcome {
    /// stored pds matched the source host immediately.
    Authorized,
    /// pds migrated: doc now points to this host, but our stored state was stale.
    WasStale,
    /// host did not match even after doc resolution.
    WrongHost { expected: SmolStr },
}
