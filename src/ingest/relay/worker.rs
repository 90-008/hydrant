use std::sync::Arc;

use crate::ingest::firehose_stats::StatsInstant;

use miette::{IntoDiagnostic, Result};
use tokio::runtime::Handle;
use tracing::{debug, error, info, info_span, trace, warn};
use url::Url;

use crate::db::CountDeltas;
use crate::ingest::stream::{InfoName, SubscribeReposMessage};
use crate::ingest::validation::ValidationOptions;
use crate::ingest::{BufferRx, BufferTx, IngestMessage};
use crate::state::AppState;

use super::sink::{EventSink, SinkSeed};
use super::{AuthorityOutcome, WorkerContext, relay_message_kind};

pub struct WorkerMessage {
    pub(crate) is_pds: bool,
    pub(crate) firehose: Url,
    pub(crate) msg: SubscribeReposMessage<'static>,
}

pub struct RelayWorker {
    pub(crate) state: Arc<AppState>,
    pub(crate) rxs: Vec<BufferRx>,
    pub(crate) seed: SinkSeed,
    pub(crate) verify_signatures: bool,
    pub(crate) num_shards: usize,
    pub(crate) validation_opts: Arc<ValidationOptions>,
    pub(crate) http: reqwest::Client,
}

impl RelayWorker {
    pub fn new(
        state: Arc<AppState>,
        seed: SinkSeed,
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
                seed,
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
            let seed = self.seed.clone();
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
                            seed,
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
        seed: SinkSeed,
        verify_signatures: bool,
        handle: Handle,
        validation_opts: Arc<ValidationOptions>,
        http: reqwest::Client,
    ) {
        let _guard = handle.enter();
        let span = info_span!("worker_shard", shard = id);
        let _entered = span.clone().entered();
        debug!("relay shard started");
        let shard_stats = state.firehose_stats.relay_shard(id);

        let mut ctx = WorkerContext {
            verify_signatures,
            state: &state,
            vctx: crate::ingest::validation::ValidationContext {
                opts: &validation_opts,
            },
            stats: shard_stats.clone(),
            batch: state.db.inner.batch(),
            count_deltas: CountDeltas::default(),
            sink: EventSink::new(&seed, shard_stats.clone()),
            http,
            error_counts: Default::default(),
            wrong_host_authority: Default::default(),
        };

        while let Some(msg) = rx.blocking_recv() {
            let message_started = StatsInstant::now();
            let IngestMessage::Firehose { url, is_pds, msg } = msg;
            if let SubscribeReposMessage::Info(inf) = msg {
                shard_stats.record_info();
                match inf.name {
                    InfoName::OutdatedCursor => {}
                    InfoName::Other(name) => {
                        let message = inf
                            .message
                            .unwrap_or(jacquard_common::CowStr::Borrowed("<no message>"));
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
            shard_stats.record_received(seq);

            let firehose = msg.firehose.clone();
            let _span = info_span!("relay", did = %did, firehose = %firehose, seq = %seq).entered();

            let process_started = StatsInstant::now();
            if let Err(e) = Self::process_message(&mut ctx, msg) {
                shard_stats.record_process_error();
                error!(did = %did, err = %e, "relay shard: error processing message");
            }
            let process_message = process_started.elapsed();

            let mut batch = std::mem::replace(&mut ctx.batch, ctx.state.db.inner.batch());
            let stage_counts_started = StatsInstant::now();
            let reservation = ctx
                .state
                .db
                .stage_count_deltas(&mut batch, &ctx.count_deltas);
            let stage_counts = stage_counts_started.elapsed();

            let stage_and_commit_started = StatsInstant::now();
            let staged = match ctx.sink.commit_batch(&state, batch) {
                Ok(staged) => staged,
                Err(e) => {
                    shard_stats.record_commit_error();
                    error!(shard = id, err = %e, "relay shard: failed to commit batch");
                    drop(reservation);
                    continue;
                }
            };
            let stage_and_commit = stage_and_commit_started.elapsed();
            let apply_counts_started = StatsInstant::now();
            ctx.state.db.apply_count_deltas(&ctx.count_deltas);
            drop(reservation);
            let apply_counts = apply_counts_started.elapsed();

            let broadcast_started = StatsInstant::now();
            ctx.sink.flush(&state, staged);
            let broadcast = broadcast_started.elapsed();

            // advance cursor for this firehose only if we are the terminal consumer (relay mode)
            // in events mode, FirehoseWorker will advance the cursor after processing
            let cursor_started = StatsInstant::now();
            ctx.sink.advance_cursor(&state, &firehose, seq);
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
        if let Some(kind) = relay_message_kind(&msg.msg) {
            ctx.stats.record_message_kind(kind);
        }

        let load_started = StatsInstant::now();
        let repo_state_result = ctx.load_repo_state(&msg);
        ctx.stats.record_repo_state_load(load_started.elapsed());
        let Some(mut repo_state) = repo_state_result? else {
            return Ok(());
        };
        let did = msg.msg.did().expect("already checked for did");

        if let Some(host) = msg.firehose.host_str()
            && msg.is_pds
        {
            let authority_started = StatsInstant::now();
            let outcome_result = ctx.check_host_authority(did, &mut repo_state, host);
            ctx.stats.record_host_authority(
                authority_started.elapsed(),
                match &outcome_result {
                    Ok(AuthorityOutcome::Authorized) => {
                        crate::ingest::firehose_stats::HostAuthorityStatsOutcome::Authorized
                    }
                    Ok(AuthorityOutcome::WasStale) => {
                        crate::ingest::firehose_stats::HostAuthorityStatsOutcome::WasStale
                    }
                    Ok(AuthorityOutcome::WrongHost { .. }) => {
                        crate::ingest::firehose_stats::HostAuthorityStatsOutcome::WrongHost
                    }
                    Err(_) => crate::ingest::firehose_stats::HostAuthorityStatsOutcome::Error,
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
                let started = StatsInstant::now();
                let result = Self::handle_commit(ctx, &mut repo_state, &msg.firehose, *commit);
                ctx.stats.record_handle_message(
                    crate::ingest::firehose_stats::RelayMessageKind::Commit,
                    started.elapsed(),
                );
                result
            }
            SubscribeReposMessage::Sync(sync) => {
                debug!("processing sync");
                let started = StatsInstant::now();
                let result = Self::handle_sync(ctx, &mut repo_state, &msg.firehose, *sync);
                ctx.stats.record_handle_message(
                    crate::ingest::firehose_stats::RelayMessageKind::Sync,
                    started.elapsed(),
                );
                result
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!("processing identity");
                let started = StatsInstant::now();
                let result = Self::handle_identity(
                    ctx,
                    &mut repo_state,
                    &msg.firehose,
                    *identity,
                    msg.is_pds,
                );
                ctx.stats.record_handle_message(
                    crate::ingest::firehose_stats::RelayMessageKind::Identity,
                    started.elapsed(),
                );
                result
            }
            SubscribeReposMessage::Account(account) => {
                debug!("processing account");
                let started = StatsInstant::now();
                let result =
                    Self::handle_account(ctx, &mut repo_state, &msg.firehose, *account, msg.is_pds);
                ctx.stats.record_handle_message(
                    crate::ingest::firehose_stats::RelayMessageKind::Account,
                    started.elapsed(),
                );
                result
            }
            _ => Ok(()),
        }
    }
}
