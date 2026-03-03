use crate::db;
use crate::filter::{FilterHandle, FilterMode};
use crate::ingest::stream::{FirehoseStream, SubscribeReposMessage, decode_frame};
use crate::ingest::{BufferTx, IngestMessage};
use crate::state::AppState;
use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{debug, error, info, trace};
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

    pub async fn run(mut self) -> Result<()> {
        loop {
            let current_cursor = self.state.cur_firehose.load(Ordering::SeqCst);
            let start_cursor = if current_cursor > 0 {
                Some(current_cursor)
            } else {
                db::get_firehose_cursor(&self.state.db).await?
            };
            match start_cursor {
                Some(c) => info!(cursor = %c, "resuming from cursor"),
                None => info!("no cursor found, live tailing"),
            }

            if let Some(c) = start_cursor {
                self.state.cur_firehose.store(c, Ordering::SeqCst);
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

    async fn handle_message(&mut self, msg: SubscribeReposMessage<'_>) {
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

        if let Err(e) = self
            .buffer_tx
            .send(IngestMessage::Firehose(msg.into_static()))
        {
            error!(err = %e, "failed to send message to buffer processor");
        }
    }

    async fn should_process(&self, did: &Did<'_>) -> Result<bool> {
        let filter = self.filter.load();

        let excl_key = crate::db::filter::exclude_key(did.as_str())?;
        if self
            .state
            .db
            .filter
            .contains_key(&excl_key)
            .into_diagnostic()?
        {
            return Ok(false);
        }

        match filter.mode {
            FilterMode::Full => Ok(true),
            FilterMode::Filter => {
                let repo_key = crate::db::keys::repo_key(did);
                if let Some(state_bytes) = self.state.db.repos.get(&repo_key).into_diagnostic()? {
                    let repo_state: crate::types::RepoState =
                        rmp_serde::from_slice(&state_bytes).into_diagnostic()?;

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
    }
}
