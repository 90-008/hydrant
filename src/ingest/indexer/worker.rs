use miette::{Diagnostic, IntoDiagnostic, Result};
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Handle as TokioHandle;
use tracing::info;

use super::message::{IndexerRx, IndexerTx};
use crate::ingest::mailbox::ShardedSender;
use crate::ingest::stream::Commit;
use crate::resolver::{NoSigningKeyError, ResolverError};
use crate::state::AppState;
use crate::types::RepoState;
use jacquard_repo::error::CommitError;

#[derive(Debug, Diagnostic, Error)]
pub(crate) enum IngestError {
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
#[allow(clippy::large_enum_variant)]
pub(crate) enum RepoProcessResult<'s, 'c> {
    // message processed successfully, here is the (possibly updated) state
    Ok(RepoState<'s>),
    // repo was deleted as part of processing
    Deleted,
    // needs backfill; carries the triggering commit to buffer (None when already in the buffer)
    NeedsBackfill(Option<&'c Commit<'c>>),
}

pub struct FirehoseWorker {
    pub(crate) state: Arc<AppState>,
    pub(crate) rxs: Vec<IndexerRx>,
}

impl FirehoseWorker {
    pub fn new(state: Arc<AppState>, num_shards: usize) -> (IndexerTx, Self) {
        let (inner, rxs) = ShardedSender::channel(num_shards);
        (IndexerTx { inner }, Self { state, rxs })
    }

    pub fn run(self, handle: TokioHandle) -> Result<()> {
        let num_shards = self.rxs.len();
        let (exit_tx, exit_rx) = std::sync::mpsc::channel();

        for (i, rx) in self.rxs.into_iter().enumerate() {
            let state = Arc::clone(&self.state);
            let handle = handle.clone();
            let exit_tx = exit_tx.clone();
            std::thread::Builder::new()
                .name(format!("ingest-shard-{i}"))
                .spawn(move || {
                    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        Self::shard(i, rx, state, handle);
                    }));
                    let _ = exit_tx.send((i, res));
                })
                .into_diagnostic()?;
        }
        drop(exit_tx);

        info!(num = num_shards, "started shards");

        match exit_rx.recv() {
            Ok((id, Ok(()))) => Err(miette::miette!("firehose worker shard {id} shut down")),
            Ok((id, Err(_))) => Err(miette::miette!("firehose worker shard {id} panicked")),
            Err(_) => Err(miette::miette!("firehose worker shards shut down")),
        }
    }
}
