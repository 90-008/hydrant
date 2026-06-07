use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "relay")]
use std::sync::atomic::Ordering;
#[cfg(feature = "firehose-diagnostics")]
use std::time::Instant;

use fjall::OwnedWriteBatch;

use jacquard_api::com_atproto::sync::get_repo_status::{
    GetRepoStatus, GetRepoStatusError, GetRepoStatusOutput, GetRepoStatusOutputStatus,
};
use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::did::Did;
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use jacquard_common::{CowStr, IntoStatic};
use miette::{IntoDiagnostic, Result};
use tokio::runtime::Handle;
use tracing::{debug, error, info, info_span, trace, warn};
use url::Url;

use crate::db::keys::pds_account_count_key;
#[cfg(all(feature = "relay", feature = "jetstream"))]
use crate::db::types::TrimmedDid;
use crate::db::{self, CountDeltas, keys};
#[cfg(feature = "firehose-diagnostics")]
use crate::ingest::firehose_stats::{
    HostAuthorityStatsOutcome, RelayMessageKind, RepoStateLoadOutcome, ValidationStatsOutcome,
};
use crate::ingest::stream::AccountStatus;
#[cfg(feature = "relay")]
use crate::ingest::stream::encode_frame;
use crate::ingest::stream::{Account, Commit, Identity, InfoName, SubscribeReposMessage, Sync};
use crate::ingest::validation::{
    CommitValidationError, SyncValidationError, ValidatedCommit, ValidatedSync, ValidationContext,
    ValidationOptions,
};
use crate::ingest::{BufferRx, BufferTx, IngestMessage};
use crate::state::AppState;
#[cfg(feature = "relay")]
use crate::types::RelayBroadcast;
#[cfg(all(feature = "relay", feature = "jetstream"))]
use crate::types::StoredJetstreamEvent;
use crate::types::{RepoState, RepoStatus};
use crate::util;
use smol_str::{SmolStr, ToSmolStr};

fn map_repo_status_probe(output: Option<GetRepoStatusOutput<'_>>) -> Option<RepoState<'static>> {
    let output = output?;

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

    Some(repo_state)
}

#[cfg(feature = "firehose-diagnostics")]
fn relay_message_kind(msg: &SubscribeReposMessage<'_>) -> Option<RelayMessageKind> {
    match msg {
        SubscribeReposMessage::Commit(_) => Some(RelayMessageKind::Commit),
        SubscribeReposMessage::Sync(_) => Some(RelayMessageKind::Sync),
        SubscribeReposMessage::Identity(_) => Some(RelayMessageKind::Identity),
        SubscribeReposMessage::Account(_) => Some(RelayMessageKind::Account),
        SubscribeReposMessage::Info(_) => None,
    }
}

#[cfg(feature = "firehose-diagnostics")]
fn commit_validation_outcome(
    res: &std::result::Result<ValidatedCommit<'_>, CommitValidationError>,
) -> ValidationStatsOutcome {
    match res {
        Ok(_) => ValidationStatsOutcome::Accepted,
        Err(CommitValidationError::StaleRev) => ValidationStatsOutcome::Stale,
        Err(CommitValidationError::SigFailure) => ValidationStatsOutcome::SigFailure,
        Err(_) => ValidationStatsOutcome::Rejected,
    }
}

#[cfg(feature = "firehose-diagnostics")]
fn sync_validation_outcome(
    res: &std::result::Result<ValidatedSync, SyncValidationError>,
) -> ValidationStatsOutcome {
    match res {
        Ok(_) => ValidationStatsOutcome::Accepted,
        Err(SyncValidationError::SigFailure) => ValidationStatsOutcome::SigFailure,
        Err(_) => ValidationStatsOutcome::Rejected,
    }
}

struct WorkerContext<'a> {
    verify_signatures: bool,
    state: &'a AppState,
    vctx: ValidationContext<'a>,
    #[cfg(feature = "firehose-diagnostics")]
    stats: Arc<crate::ingest::firehose_stats::RelayShardStats>,
    batch: OwnedWriteBatch,
    count_deltas: CountDeltas,
    #[cfg(feature = "relay")]
    pending_broadcasts: Vec<RelayBroadcast>,
    #[cfg(all(feature = "relay", feature = "jetstream"))]
    pending_jetstream_events: Vec<(
        crate::types::StoredJetstreamEvent<'static>,
        Option<crate::jetstream::JetstreamEphemeral>,
    )>,
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
    rxs: Vec<BufferRx>,
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
        #[cfg(feature = "indexer")] hook: crate::ingest::indexer::IndexerTx,
        verify_signatures: bool,
        num_shards: usize,
        validation_opts: ValidationOptions,
    ) -> (BufferTx, Self) {
        let (tx, rxs) = BufferTx::channel(num_shards);
        (
            tx,
            Self {
                state,
                rxs,
                #[cfg(feature = "indexer")]
                hook,
                verify_signatures,
                num_shards,
                validation_opts: Arc::new(validation_opts),
                http: reqwest::Client::new(),
            },
        )
    }

    pub fn run(self, handle: Handle) -> Result<()> {
        let (exit_tx, exit_rx) = std::sync::mpsc::channel();

        for (i, rx) in self.rxs.into_iter().enumerate() {
            let state = Arc::clone(&self.state);
            #[cfg(feature = "indexer")]
            let hook = self.hook.clone();
            let verify = self.verify_signatures;
            let h = handle.clone();
            let opts = self.validation_opts.clone();
            let http = self.http.clone();
            let exit_tx = exit_tx.clone();

            std::thread::Builder::new()
                .name(format!("relay-shard-{i}"))
                .spawn(move || {
                    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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
                    }));
                    let _ = exit_tx.send((i, res));
                })
                .into_diagnostic()?;
        }
        drop(exit_tx);

        info!(num = self.num_shards, "relay worker: started shards");

        match exit_rx.recv() {
            Ok((id, Ok(()))) => Err(miette::miette!("relay worker shard {id} shut down")),
            Ok((id, Err(_))) => Err(miette::miette!("relay worker shard {id} panicked")),
            Err(_) => Err(miette::miette!("relay worker shards shut down")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn shard(
        id: usize,
        mut rx: BufferRx,
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
        #[cfg(feature = "firehose-diagnostics")]
        let shard_stats = state.firehose_stats.relay_shard(id);

        let mut ctx = WorkerContext {
            verify_signatures,
            state: &state,
            vctx: ValidationContext {
                opts: &validation_opts,
            },
            #[cfg(feature = "firehose-diagnostics")]
            stats: shard_stats.clone(),
            batch: state.db.inner.batch(),
            count_deltas: CountDeltas::default(),
            #[cfg(feature = "relay")]
            pending_broadcasts: Vec::with_capacity(2),
            #[cfg(all(feature = "relay", feature = "jetstream"))]
            pending_jetstream_events: Vec::with_capacity(2),
            #[cfg(feature = "indexer")]
            pending_hook_messages: Vec::with_capacity(2),
            #[cfg(feature = "indexer")]
            hook,
            http,
            error_counts: Default::default(),
        };

        while let Some(msg) = rx.blocking_recv() {
            #[cfg(feature = "firehose-diagnostics")]
            let message_started = Instant::now();
            let IngestMessage::Firehose { url, is_pds, msg } = msg;
            if let SubscribeReposMessage::Info(inf) = msg {
                #[cfg(feature = "firehose-diagnostics")]
                shard_stats.record_info();
                match inf.name {
                    InfoName::OutdatedCursor => {}
                    InfoName::Other(name) => {
                        let message = inf.message.unwrap_or(CowStr::Borrowed("<no message>"));
                        info!(name = %name, "relay sent info: {message}");
                    }
                }
                continue;
            }

            let msg = WorkerMessage {
                is_pds,
                firehose: url,
                msg,
            };

            ctx.count_deltas = CountDeltas::default();
            let (did, seq) = match &msg.msg {
                SubscribeReposMessage::Commit(c) => (c.repo.clone(), c.seq),
                SubscribeReposMessage::Identity(i) => (i.did.clone(), i.seq),
                SubscribeReposMessage::Account(a) => (a.did.clone(), a.seq),
                SubscribeReposMessage::Sync(s) => (s.did.clone(), s.seq),
                _ => continue,
            };
            #[cfg(feature = "firehose-diagnostics")]
            shard_stats.record_received(seq);

            let firehose = msg.firehose.clone();
            let _span = info_span!("relay", did = %did, firehose = %firehose, seq = %seq).entered();

            #[cfg(feature = "firehose-diagnostics")]
            let process_started = Instant::now();
            if let Err(e) = Self::process_message(&mut ctx, msg) {
                #[cfg(feature = "firehose-diagnostics")]
                shard_stats.record_process_error();
                error!(did = %did, err = %e, "relay shard: error processing message");
            }
            #[cfg(feature = "firehose-diagnostics")]
            let process_message = process_started.elapsed();

            let mut batch = std::mem::replace(&mut ctx.batch, ctx.state.db.inner.batch());
            #[cfg(feature = "firehose-diagnostics")]
            let stage_counts_started = Instant::now();
            let reservation = ctx
                .state
                .db
                .stage_count_deltas(&mut batch, &ctx.count_deltas);
            #[cfg(feature = "firehose-diagnostics")]
            let stage_counts = stage_counts_started.elapsed();

            #[cfg(all(feature = "relay", feature = "jetstream"))]
            let mut jetstream_broadcasts = Vec::new();

            #[cfg(feature = "firehose-diagnostics")]
            let stage_and_commit_started = Instant::now();
            #[cfg(all(feature = "relay", feature = "jetstream"))]
            let res = {
                let _lock = ctx.state.db.jetstream_lock.lock();
                let mut stage_res = Ok(());
                for (event, ephemeral) in ctx.pending_jetstream_events.drain(..) {
                    match crate::jetstream::stage_event(&mut batch, &ctx.state.db, event, ephemeral)
                    {
                        Ok(broadcast) => jetstream_broadcasts.push(broadcast),
                        Err(e) => {
                            stage_res = Err(e);
                            break;
                        }
                    }
                }
                stage_res.and_then(|_| batch.commit().into_diagnostic())
            };

            #[cfg(not(all(feature = "relay", feature = "jetstream")))]
            let res = batch.commit();
            #[cfg(feature = "firehose-diagnostics")]
            let stage_and_commit = stage_and_commit_started.elapsed();

            if let Err(e) = res {
                #[cfg(feature = "firehose-diagnostics")]
                shard_stats.record_commit_error();
                error!(shard = id, err = %e, "relay shard: failed to commit batch");
                drop(reservation);
                continue;
            }
            #[cfg(feature = "firehose-diagnostics")]
            let apply_counts_started = Instant::now();
            ctx.state.db.apply_count_deltas(&ctx.count_deltas);
            drop(reservation);
            #[cfg(feature = "firehose-diagnostics")]
            let apply_counts = apply_counts_started.elapsed();

            #[cfg(feature = "firehose-diagnostics")]
            let broadcast_started = Instant::now();
            #[cfg(feature = "relay")]
            for broadcast in ctx.pending_broadcasts.drain(..) {
                let _ = state.db.relay_broadcast_tx.send(broadcast);
            }
            #[cfg(all(feature = "relay", feature = "jetstream"))]
            for broadcast in jetstream_broadcasts {
                let _ = state.db.jetstream_tx.send(broadcast);
            }
            #[cfg(feature = "indexer")]
            for msg in ctx.pending_hook_messages.drain(..) {
                let _ = ctx.hook.blocking_send(msg);
            }
            #[cfg(feature = "firehose-diagnostics")]
            let broadcast = broadcast_started.elapsed();

            // advance cursor for this firehose only if we are the terminal consumer (relay mode)
            // in events mode, FirehoseWorker will advance the cursor after processing
            #[cfg(feature = "firehose-diagnostics")]
            let cursor_started = Instant::now();
            #[cfg(feature = "relay")]
            {
                ctx.state
                    .firehose_cursors
                    .peek_with(&firehose, |_, c| c.store(seq, Ordering::SeqCst));
            }
            #[cfg(feature = "firehose-diagnostics")]
            shard_stats.record_processed(crate::ingest::firehose_stats::RelayShardTimings {
                process_message,
                stage_counts,
                stage_and_commit,
                apply_counts,
                broadcast,
                cursor: cursor_started.elapsed(),
                total: message_started.elapsed(),
            });
        }
    }

    fn process_message(ctx: &mut WorkerContext, msg: WorkerMessage) -> Result<()> {
        #[cfg(feature = "firehose-diagnostics")]
        if let Some(kind) = relay_message_kind(&msg.msg) {
            ctx.stats.record_message_kind(kind);
        }

        #[cfg(feature = "firehose-diagnostics")]
        let load_started = Instant::now();
        let repo_state_result = ctx.load_repo_state(&msg);
        #[cfg(feature = "firehose-diagnostics")]
        ctx.stats.record_repo_state_load(load_started.elapsed());
        let Some(mut repo_state) = repo_state_result? else {
            return Ok(());
        };
        let did = msg.msg.did().expect("already checked for did");

        if let Some(host) = msg.firehose.host_str()
            && msg.is_pds
        {
            #[cfg(feature = "firehose-diagnostics")]
            let authority_started = Instant::now();
            let outcome_result = ctx.check_host_authority(did, &mut repo_state, host);
            #[cfg(feature = "firehose-diagnostics")]
            ctx.stats.record_host_authority(
                authority_started.elapsed(),
                match &outcome_result {
                    Ok(AuthorityOutcome::Authorized) => HostAuthorityStatsOutcome::Authorized,
                    Ok(AuthorityOutcome::WasStale) => HostAuthorityStatsOutcome::WasStale,
                    Ok(AuthorityOutcome::WrongHost { .. }) => HostAuthorityStatsOutcome::WrongHost,
                    Err(_) => HostAuthorityStatsOutcome::Error,
                },
            );
            let outcome = outcome_result?;
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
                #[cfg(feature = "firehose-diagnostics")]
                let started = Instant::now();
                let result = Self::handle_commit(ctx, &mut repo_state, &msg.firehose, *commit);
                #[cfg(feature = "firehose-diagnostics")]
                ctx.stats
                    .record_handle_message(RelayMessageKind::Commit, started.elapsed());
                result
            }
            SubscribeReposMessage::Sync(sync) => {
                debug!("processing sync");
                #[cfg(feature = "firehose-diagnostics")]
                let started = Instant::now();
                let result = Self::handle_sync(ctx, &mut repo_state, &msg.firehose, *sync);
                #[cfg(feature = "firehose-diagnostics")]
                ctx.stats
                    .record_handle_message(RelayMessageKind::Sync, started.elapsed());
                result
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!("processing identity");
                #[cfg(feature = "firehose-diagnostics")]
                let started = Instant::now();
                let result = Self::handle_identity(
                    ctx,
                    &mut repo_state,
                    &msg.firehose,
                    *identity,
                    msg.is_pds,
                );
                #[cfg(feature = "firehose-diagnostics")]
                ctx.stats
                    .record_handle_message(RelayMessageKind::Identity, started.elapsed());
                result
            }
            SubscribeReposMessage::Account(account) => {
                debug!("processing account");
                #[cfg(feature = "firehose-diagnostics")]
                let started = Instant::now();
                let result =
                    Self::handle_account(ctx, &mut repo_state, &msg.firehose, *account, msg.is_pds);
                #[cfg(feature = "firehose-diagnostics")]
                ctx.stats
                    .record_handle_message(RelayMessageKind::Account, started.elapsed());
                result
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

        #[cfg(not(feature = "indexer"))]
        let _ = parsed_blocks;

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
            #[cfg(feature = "jetstream")]
            let jetstream_ops = commit
                .ops
                .iter()
                .enumerate()
                .filter_map(|(idx, op)| {
                    matches!(op.action.as_str(), "create" | "update" | "delete")
                        .then(|| split_collection(&op.path).map(|col| (idx as u32, col)))
                        .flatten()
                })
                .collect::<Vec<_>>();
            #[cfg(feature = "jetstream")]
            let jetstream_did = TrimmedDid::from(&commit.repo).into_static();

            let _relay_seq = ctx.queue_emit(|seq| {
                commit.seq = seq;
                encode_frame("#commit", &commit)
            })?;
            #[cfg(feature = "jetstream")]
            {
                let parsed_car = tokio::runtime::Handle::current()
                    .block_on(jacquard_repo::car::reader::parse_car_bytes(
                        commit.blocks.as_ref(),
                    ))
                    .ok();
                for (op_index, collection) in jetstream_ops {
                    let ephemeral = parsed_car.as_ref().and_then(|car| {
                        crate::jetstream::build_ephemeral_from_relay(
                            &commit,
                            op_index,
                            collection.as_str(),
                            true,
                            car,
                        )
                    });
                    ctx.pending_jetstream_events.push((
                        StoredJetstreamEvent::RelayCommit {
                            did: jetstream_did.clone(),
                            collection,
                            relay_seq: _relay_seq,
                            op_index,
                        },
                        ephemeral,
                    ));
                }
            }
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
        if !repo_state.should_process_identity_time(event_ms) {
            debug!("skipping stale/duplicate identity event");
            return Ok(());
        }
        repo_state.advance_identity_time(event_ms);
        let was_active = repo_state.active;
        let was_pds_host = Self::pds_host(repo_state.pds.as_deref());

        #[cfg(feature = "indexer")]
        let (was_handle, was_signing_key) = (
            repo_state.handle.clone().map(IntoStatic::into_static),
            repo_state.signing_key.clone().map(IntoStatic::into_static),
        );

        // refresh did doc if a pds sent this event
        // or if there is no handle specified
        if is_pds || identity.handle.is_none() {
            ctx.state.resolver.invalidate_sync(&identity.did);
            #[cfg(feature = "firehose-diagnostics")]
            let resolve_started = Instant::now();
            let doc = Handle::current().block_on(ctx.state.resolver.resolve_doc(&identity.did));
            #[cfg(feature = "firehose-diagnostics")]
            ctx.stats.record_resolve_doc(resolve_started.elapsed());
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

        Self::update_pds_account_count(
            ctx,
            was_active,
            was_pds_host.as_deref(),
            repo_state.active,
            Self::pds_host(repo_state.pds.as_deref()).as_deref(),
        );

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
            let _relay_seq = ctx.queue_emit(|seq| {
                identity.seq = seq;
                encode_frame("#identity", &identity)
            })?;
            #[cfg(feature = "jetstream")]
            ctx.pending_jetstream_events.push((
                StoredJetstreamEvent::RelayIdentity {
                    did: TrimmedDid::from(&identity.did).into_static(),
                    relay_seq: _relay_seq,
                },
                None,
            ));
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
        #[allow(unused_variables)] firehose: &Url,
        #[allow(unused_mut)] mut account: Account<'static>,
        _is_pds: bool,
    ) -> Result<()> {
        let event_ms = account.time.0.timestamp_millis();
        if !repo_state.should_process_account_time(event_ms) {
            debug!("skipping stale/duplicate account event");
            return Ok(());
        }

        repo_state.advance_account_time(event_ms);

        // always capture was_active for count tracking, not just in indexer mode
        let was_active = repo_state.active;
        let was_pds_host = Self::pds_host(repo_state.pds.as_deref());
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

        Self::update_pds_account_count(
            ctx,
            was_active,
            was_pds_host.as_deref(),
            repo_state.active,
            Self::pds_host(repo_state.pds.as_deref()).as_deref(),
        );

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
            let _relay_seq = ctx.queue_emit(|seq| {
                account.seq = seq;
                encode_frame("#account", &account)
            })?;
            #[cfg(feature = "jetstream")]
            ctx.pending_jetstream_events.push((
                StoredJetstreamEvent::RelayAccount {
                    did: TrimmedDid::from(&account.did).into_static(),
                    relay_seq: _relay_seq,
                },
                None,
            ));
        }

        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    fn pds_host(pds: Option<&str>) -> Option<SmolStr> {
        pds.and_then(|pds| Url::parse(pds).ok())
            .and_then(|url| url.host_str().map(SmolStr::new))
    }

    fn update_pds_account_count(
        ctx: &mut WorkerContext,
        old_active: bool,
        old_host: Option<&str>,
        new_active: bool,
        new_host: Option<&str>,
    ) {
        if old_active && old_host == new_host && new_active {
            return;
        }

        let mut update_host = |host: &str, delta| {
            let count_key = pds_account_count_key(host);
            ctx.count_deltas.add(&count_key, delta);
            let count = ctx.count_deltas.projected_count(&ctx.state.db, &count_key);
            ctx.state
                .apply_host_limit_status(&mut ctx.batch, host, count);
        };

        if old_active && let Some(host) = old_host {
            update_host(host, -1);
        }

        if new_active && let Some(host) = new_host {
            update_host(host, 1);
        }
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
        #[cfg(feature = "firehose-diagnostics")]
        let refresh_started = Instant::now();
        let result = (|| {
            let db = &self.state.db;
            self.state.resolver.invalidate_sync(did);
            #[cfg(feature = "firehose-diagnostics")]
            let resolve_started = Instant::now();
            let doc = Handle::current()
                .block_on(self.state.resolver.resolve_doc(did))
                .map_err(|e| miette::miette!("{e}"));
            #[cfg(feature = "firehose-diagnostics")]
            self.stats.record_resolve_doc(resolve_started.elapsed());
            let doc = doc?;
            repo_state.update_from_doc(doc);
            repo_state.touch();

            self.batch.insert(
                &db.repos,
                keys::repo_key(did),
                db::ser_repo_state(repo_state)?,
            );
            Ok(())
        })();
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_refresh_doc(refresh_started.elapsed());
        result
    }

    fn validate_commit<'c>(
        &mut self,
        repo_state: &mut RepoState,
        commit: &'c Commit<'c>,
    ) -> Result<Option<ValidatedCommit<'c>>> {
        let did = &commit.repo;
        let key = self.fetch_key(did)?;
        #[cfg(feature = "firehose-diagnostics")]
        let validate_started = Instant::now();
        let validation = self.vctx.validate_commit(commit, repo_state, key.as_ref());
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_validate_commit(
            validate_started.elapsed(),
            commit_validation_outcome(&validation),
        );
        match validation {
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
        #[cfg(feature = "firehose-diagnostics")]
        let validate_started = Instant::now();
        let validation = self.vctx.validate_commit(commit, repo_state, key.as_ref());
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_validate_commit(
            validate_started.elapsed(),
            commit_validation_outcome(&validation),
        );
        match validation {
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
        #[cfg(feature = "firehose-diagnostics")]
        let validate_started = Instant::now();
        let validation = self.vctx.validate_sync(sync, key.as_ref());
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_validate_sync(
            validate_started.elapsed(),
            sync_validation_outcome(&validation),
        );
        match validation {
            Ok(v) => return Ok(Some(v)),
            Err(SyncValidationError::SigFailure) => {}
            Err(e) => {
                debug!(err = %e, "sync rejected");
                return Ok(None);
            }
        }

        self.refresh_doc(did, repo_state)?;
        let key = self.fetch_key(did)?;
        #[cfg(feature = "firehose-diagnostics")]
        let validate_started = Instant::now();
        let validation = self.vctx.validate_sync(sync, key.as_ref());
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_validate_sync(
            validate_started.elapsed(),
            sync_validation_outcome(&validation),
        );
        match validation {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                debug!(err = %e, "sync rejected after key refresh");
                Ok(None)
            }
        }
    }

    fn fetch_key(&self, did: &Did) -> Result<Option<PublicKey<'static>>> {
        #[cfg(feature = "firehose-diagnostics")]
        let started = Instant::now();
        let result = if self.verify_signatures {
            Handle::current()
                .block_on(self.state.resolver.resolve_signing_key(did))
                .map(Some)
                .map_err(|e| miette::miette!("{e}"))
        } else {
            Ok(None)
        };
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_fetch_key(started.elapsed());
        result
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
                    // treat probe-time 404s like any other transient probe failure.
                    // we already have a live event from the authoritative host, so
                    // inserting an inactive placeholder here can wedge the repo until
                    // a later account event arrives.
                    return Ok(map_repo_status_probe(None));
                }
                Err(_) => return Ok(None),
            },
        };

        Ok(map_repo_status_probe(Some(output)))
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
            .map(|bytes| db::deser_repo_meta(&bytes))
            .transpose()?;

        if metadata.is_some_and(|m| !m.tracked) {
            trace!(did = %did, "ignoring message, repo is explicitly untracked");
            #[cfg(feature = "firehose-diagnostics")]
            self.stats
                .record_repo_state_outcome(RepoStateLoadOutcome::Drop);
            return Ok(None);
        }

        let repo_state_opt = db
            .repos
            .get(&repo_key)
            .into_diagnostic()?
            .map(|bytes| db::deser_repo_state(bytes.as_ref()).map(|s| s.into_static()))
            .transpose()?;

        if let Some(repo_state) = repo_state_opt {
            #[cfg(feature = "firehose-diagnostics")]
            self.stats
                .record_repo_state_outcome(RepoStateLoadOutcome::Hit);
            return Ok(Some(repo_state));
        }

        #[cfg(feature = "indexer")]
        {
            let filter = self.state.filter.load();
            if filter.mode == crate::filter::FilterMode::Filter && !filter.signals.is_empty() {
                let commit = match &msg.msg {
                    SubscribeReposMessage::Commit(c) => c,
                    _ => {
                        #[cfg(feature = "firehose-diagnostics")]
                        self.stats
                            .record_repo_state_outcome(RepoStateLoadOutcome::Drop);
                        return Ok(None);
                    }
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
                    #[cfg(feature = "firehose-diagnostics")]
                    self.stats
                        .record_repo_state_outcome(RepoStateLoadOutcome::Drop);
                    return Ok(None);
                }
            }
        }

        debug!(did = %did, "discovered new account from firehose, queueing backfill");
        #[cfg(feature = "firehose-diagnostics")]
        let new_account_started = Instant::now();

        // resolve doc to initialize repo state
        self.state.resolver.invalidate_sync(did);
        #[cfg(feature = "firehose-diagnostics")]
        let resolve_started = Instant::now();
        let doc = tokio::runtime::Handle::current()
            .block_on(self.state.resolver.resolve_doc(did))
            .into_diagnostic();
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_resolve_doc(resolve_started.elapsed());
        let doc = doc?;

        // if it's a PDS, verify it's the authoritative one
        if msg.is_pds {
            let pds_host = doc.pds.host_str().map(|h| h.to_string());
            if pds_host.as_deref() != msg.firehose.host_str() {
                warn!(did = %did, got = ?pds_host, expected = ?msg.firehose.host_str(), "message rejected: wrong host for new account");
                #[cfg(feature = "firehose-diagnostics")]
                self.stats
                    .record_repo_state_outcome(RepoStateLoadOutcome::Drop);
                return Ok(None);
            }

            if let Some(host) = msg.firehose.host_str() {
                let count = self.state.db.get_count_sync(&pds_account_count_key(host));
                if self.state.is_over_account_limit(host, count) {
                    warn!(did = %did, host, count, "account limit reached for host, dropping new account");
                    #[cfg(feature = "firehose-diagnostics")]
                    self.stats
                        .record_repo_state_outcome(RepoStateLoadOutcome::Drop);
                    return Ok(None);
                }
            }
        }

        // try to get upstream status
        #[cfg(feature = "firehose-diagnostics")]
        let probe_started = Instant::now();
        let repo_state =
            tokio::runtime::Handle::current().block_on(self.check_repo_status(did, &doc.pds));
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_repo_status_probe(probe_started.elapsed());
        let mut repo_state = repo_state
            .ok()
            .flatten()
            .unwrap_or_else(RepoState::backfilling);

        repo_state.update_from_doc(doc);
        RelayWorker::update_pds_account_count(
            self,
            false,
            None,
            repo_state.active,
            RelayWorker::pds_host(repo_state.pds.as_deref()).as_deref(),
        );

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

        self.count_deltas.add("repos", 1);

        #[cfg(feature = "firehose-diagnostics")]
        {
            self.stats
                .record_repo_state_outcome(RepoStateLoadOutcome::Miss);
            self.stats.record_new_account(new_account_started.elapsed());
        }

        Ok(Some(repo_state))
    }

    #[cfg(feature = "relay")]
    fn queue_emit(&mut self, make_frame: impl FnOnce(i64) -> Result<bytes::Bytes>) -> Result<u64> {
        #[cfg(feature = "firehose-diagnostics")]
        let started = Instant::now();
        let result = (|| {
            let db = &self.state.db;
            let seq = db.next_relay_seq.fetch_add(1, Ordering::SeqCst);
            let frame = make_frame(seq as i64)?;
            self.batch
                .insert(&db.relay_events, keys::relay_event_key(seq), frame.as_ref());
            self.pending_broadcasts
                .push(RelayBroadcast::Ephemeral(seq, frame));
            self.pending_broadcasts.push(RelayBroadcast::Persisted(seq));
            Ok(seq)
        })();
        #[cfg(feature = "firehose-diagnostics")]
        self.stats.record_queue_emit(started.elapsed());
        result
    }
}

#[cfg(all(feature = "relay", feature = "jetstream"))]
fn split_collection(path: &str) -> Option<CowStr<'static>> {
    path.split_once('/')
        .map(|(collection, _)| CowStr::Owned(collection.to_smolstr()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_repo_status_probe_falls_back_to_live_discovery() {
        assert!(map_repo_status_probe(None).is_none());
    }

    #[test]
    fn active_repo_status_probe_maps_to_synced_repo_state() {
        let repo_state = map_repo_status_probe(Some(GetRepoStatusOutput {
            did: Did::new("did:plc:testrepo").expect("valid did"),
            active: true,
            status: None,
            rev: None,
            extra_data: None,
        }))
        .expect("probe should map");

        assert!(repo_state.active);
        assert_eq!(repo_state.status, RepoStatus::Synced);
    }

    #[test]
    fn throttled_repo_status_probe_preserves_host_status() {
        let repo_state = map_repo_status_probe(Some(GetRepoStatusOutput {
            did: Did::new("did:plc:testrepo").expect("valid did"),
            active: true,
            status: Some(GetRepoStatusOutputStatus::Throttled),
            rev: None,
            extra_data: None,
        }))
        .expect("probe should map");

        assert!(repo_state.active);
        assert_eq!(repo_state.status, RepoStatus::Throttled);
    }
}
