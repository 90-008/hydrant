use crate::db::{self, deser_repo_state};
use crate::filter::{FilterHandle, FilterMode};
use crate::ingest::stream::{FirehoseStream, SubscribeReposMessage, decode_frame};
use crate::ingest::{BufferTx, IngestMessage};
use crate::state::AppState;
use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{Span, debug, error, info, trace};
use url::Url;

pub struct FirehoseIngestor {
    state: Arc<AppState>,
    buffer_tx: BufferTx,
    relay_host: Url,
    filter: FilterHandle,
    _verify_signatures: bool,
}

impl FirehoseIngestor {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: BufferTx,
        relay_host: Url,
        filter: FilterHandle,
        verify_signatures: bool,
    ) -> Self {
        Self {
            state,
            buffer_tx,
            relay_host,
            filter,
            _verify_signatures: verify_signatures,
        }
    }

    #[tracing::instrument(skip(self), fields(relay = %self.relay_host))]
    pub async fn run(self) -> Result<()> {
        loop {
            let start_cursor = db::get_firehose_cursor(&self.state.db, &self.relay_host).await?;

            match start_cursor {
                Some(c) => info!(cursor = %c, "resuming from cursor"),
                None => info!("no cursor found, live tailing"),
            }

            let mut stream =
                match FirehoseStream::connect(self.relay_host.clone(), start_cursor).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(err = %e, "failed to connect to firehose, retrying in 5s");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

            info!("firehose connected");

            while let Some(bytes_res) = stream.next().await {
                let bytes = match bytes_res {
                    Ok(b) => b,
                    Err(e) => {
                        error!(err = %e, "firehose stream error");
                        break;
                    }
                };
                match decode_frame(&bytes) {
                    Ok(msg) => self.handle_message(msg).await,
                    Err(e) => {
                        error!(err = %e, "firehose stream error");
                        break;
                    }
                }
            }

            error!("firehose disconnected, reconnecting in 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn handle_message(&self, msg: SubscribeReposMessage<'_>) {
        let did = match &msg {
            SubscribeReposMessage::Commit(commit) => &commit.repo,
            SubscribeReposMessage::Identity(identity) => &identity.did,
            SubscribeReposMessage::Account(account) => &account.did,
            SubscribeReposMessage::Sync(sync) => &sync.did,
            _ => return,
        };

        let process = self
            .should_process(did)
            .await
            .inspect_err(|e| error!(did = %did, err = %e, "failed to check if we should process"))
            .unwrap_or(false);
        if !process {
            trace!(did = %did, "skipping: not in filter");
            return;
        }
        trace!(did = %did, "forwarding message to ingest buffer");

        if let Err(e) = self.buffer_tx.send(IngestMessage::Firehose {
            relay: self.relay_host.clone(),
            msg: msg.into_static(),
        }) {
            error!(err = %e, "failed to send message to buffer processor");
        }
    }

    async fn should_process(&self, did: &Did<'_>) -> Result<bool> {
        let filter = self.filter.load();
        let state = self.state.clone();
        let did = did.clone().into_static();
        let span = Span::current();

        tokio::task::spawn_blocking(move || {
            let _entered = span.entered();
            let _entered = tracing::info_span!("should_process", repo = %did).entered();

            let excl_key = crate::db::filter::exclude_key(did.as_str())?;
            if state.db.filter.contains_key(&excl_key).into_diagnostic()? {
                return Ok(false);
            }

            match filter.mode {
                FilterMode::Full => Ok(true),
                FilterMode::Filter => {
                    let repo_key = crate::db::keys::repo_key(&did);
                    if let Some(bytes) = state.db.repos.get(&repo_key).into_diagnostic()? {
                        let repo_state = deser_repo_state(&bytes)?;

                        if repo_state.tracked {
                            trace!(did = %did, "tracked repo, processing");
                            return Ok(true);
                        } else {
                            debug!(did = %did, "known but explicitly untracked, skipping");
                            return Ok(false);
                        }
                    }

                    if !filter.signals.is_empty() {
                        trace!(did = %did, "unknown — passing to worker for signal check");
                        Ok(true)
                    } else {
                        trace!(did = %did, "unknown and no signals configured, skipping");
                        Ok(false)
                    }
                }
            }
        })
        .await
        .into_diagnostic()
        .flatten()
    }
}
