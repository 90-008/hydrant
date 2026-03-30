use crate::filter::{FilterHandle, FilterMode};
use crate::ingest::stream::{FirehoseError, FirehoseStream, SubscribeReposMessage, decode_frame};
use crate::ingest::{BufferTx, IngestMessage};
use crate::state::AppState;
use crate::util::WatchEnabledExt;
use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{Span, debug, error, info, trace, warn};
use url::Url;

pub struct FirehoseIngestor {
    state: Arc<AppState>,
    buffer_tx: BufferTx,
    relay_host: Url,
    is_pds: bool,
    filter: FilterHandle,
    enabled: watch::Receiver<bool>,
    _verify_signatures: bool,
}

impl FirehoseIngestor {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: BufferTx,
        relay_host: Url,
        is_pds: bool,
        filter: FilterHandle,
        enabled: watch::Receiver<bool>,
        verify_signatures: bool,
    ) -> Self {
        Self {
            state,
            buffer_tx,
            relay_host,
            is_pds,
            filter,
            enabled,
            _verify_signatures: verify_signatures,
        }
    }

    #[tracing::instrument(skip(self), fields(relay = %self.relay_host))]
    pub async fn run(mut self) -> Result<()> {
        loop {
            self.enabled.wait_enabled("firehose").await;

            let start_cursor = self
                .state
                .firehose_cursors
                .peek_with(&self.relay_host, |_, c| {
                    let val = c.load(Ordering::SeqCst);
                    (val > 0).then_some(val)
                })
                .flatten();

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

            let disconnected_by_error = loop {
                tokio::select! {
                    msg = stream.next() => {
                        let Some(bytes_res) = msg else { break true; };
                        let bytes = match bytes_res {
                            Ok(b) => b,
                            Err(e) => {
                                error!(err = %e, "firehose stream error");
                                break true;
                            }
                        };
                        match decode_frame(&bytes) {
                            Ok(msg) => self.handle_message(msg).await,
                            Err(e) => {
                                match e {
                                    // dont disconnect on unknown op or type
                                    FirehoseError::UnknownOp(op) => {
                                        warn!(op = %op, "unknown frame op");
                                        continue;
                                    },
                                    FirehoseError::UnknownType(t) => {
                                        warn!(ty = %t, "unknown frame type");
                                        continue;
                                    },
                                    // everything else is a hard error
                                    FirehoseError::RelayError { error, message } => {
                                        let message = message.unwrap_or_else(|| "<no message>".to_owned());
                                        error!(err = %error, "relay sent error: {message}");
                                    },
                                    e => error!(err = %e, "firehose stream error"),
                                }
                                break true;
                            }
                        }
                    }
                    _ = self.enabled.changed() => {
                        if !*self.enabled.borrow() {
                            info!("firehose disabled, disconnecting");
                            break false;
                        }
                    }
                }
            };

            if disconnected_by_error {
                error!("firehose disconnected, reconnecting in 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
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

        if let Err(e) = self
            .buffer_tx
            .send(IngestMessage::Firehose {
                url: self.relay_host.clone(),
                is_pds: self.is_pds,
                msg: msg.into_static(),
            })
            .await
        {
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
                    let metadata_key = crate::db::keys::repo_metadata_key(&did);
                    if let Some(bytes) = state
                        .db
                        .repo_metadata
                        .get(&metadata_key)
                        .into_diagnostic()?
                    {
                        let metadata = crate::db::deser_repo_metadata(bytes.as_ref())?;

                        if metadata.tracked {
                            trace!(did = %did, "tracked repo, processing");
                            return Ok(true);
                        } else {
                            debug!(did = %did, "known but explicitly untracked, skipping");
                            return Ok(false);
                        }
                    }

                    if !filter.signals.is_empty() {
                        trace!(did = %did, "unknown, passing to worker for signal check");
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
