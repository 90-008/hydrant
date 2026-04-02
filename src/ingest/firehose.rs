use crate::filter::{FilterHandle, FilterMode};
use crate::ingest::stream::{FirehoseError, FirehoseStream, SubscribeReposMessage, decode_frame};
use crate::ingest::{BufferTx, IngestMessage};
use crate::state::AppState;
use crate::util::throttle::ThrottleHandle;
use crate::util::{
    WatchEnabledExt, is_status_their_fault, is_timeout, is_tls_cert_error, is_tls_error_their_fault,
};
use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::watch;
use tokio_websockets::Error as WsError;
use tokio_websockets::upgrade::Error as WsUpgradeError;
use tracing::{Span, debug, error, info, trace, warn};
use url::Url;

fn is_throttle_worthy(e: &WsError) -> bool {
    use std::error::Error;

    if is_timeout(e) {
        return true;
    }

    match e {
        WsError::Rustls(e) if is_tls_error_their_fault(e) => return true,
        WsError::Io(io_err) if is_tls_cert_error(io_err) => return true,
        WsError::CannotResolveHost => return true,
        WsError::Upgrade(WsUpgradeError::DidNotSwitchProtocols(status))
            if is_status_their_fault(*status) =>
        {
            return true;
        }
        WsError::Protocol(_) | WsError::PayloadTooLong { .. } => return true,
        _ => {}
    }

    let mut src = e.source();
    while let Some(s) = src {
        if let Some(io_err) = s.downcast_ref::<std::io::Error>() {
            if is_tls_cert_error(io_err) {
                return true;
            }
        }
        src = s.source();
    }

    false
}

pub struct FirehoseIngestor {
    state: Arc<AppState>,
    buffer_tx: BufferTx,
    relay_host: Url,
    is_pds: bool,
    filter: FilterHandle,
    enabled: watch::Receiver<bool>,
    _verify_signatures: bool,
    throttle: ThrottleHandle,
}

impl FirehoseIngestor {
    pub async fn new(
        state: Arc<AppState>,
        buffer_tx: BufferTx,
        relay_host: Url,
        is_pds: bool,
        filter: FilterHandle,
        enabled: watch::Receiver<bool>,
        verify_signatures: bool,
    ) -> Self {
        let throttle = state.throttler.get_handle(&relay_host).await;
        Self {
            state,
            buffer_tx,
            relay_host,
            is_pds,
            filter,
            enabled,
            _verify_signatures: verify_signatures,
            throttle,
        }
    }

    #[tracing::instrument(skip(self), fields(relay = %self.relay_host))]
    pub async fn run(mut self) -> Result<()> {
        let host = self.relay_host.host_str().unwrap_or("");
        let count_key = crate::db::keys::pds_account_count_key(&host);

        // this is not for connection throttling (thats handled by ThrottleHandle)
        // its for stream errors (cbor decode etc)
        let mut backoff = Duration::from_secs(0);
        const MAX_BACKOFF: Duration = Duration::from_secs(60 * 60); // 1 hour

        loop {
            if self.state.pds_meta.load().is_banned(host) {
                break Ok(());
            }

            self.enabled.wait_enabled("firehose").await;

            tokio::time::sleep(backoff).await;

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

            let mut stream = match FirehoseStream::connect(self.relay_host.clone(), start_cursor)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    // todo: figure out how to pass timeout to tungesteite i guess
                    // if let FirehoseError::WebSocket(err) = &e
                    //     && is_timeout(&err)
                    // {
                    //     if !self.throttle.record_timeout() {
                    //         continue;
                    //     }
                    // }
                    let do_throttle = matches!(&e, FirehoseError::WebSocket(e) if is_throttle_worthy(e))
                        || matches!(&e, FirehoseError::EmptyFrame);
                    let timeout = if do_throttle {
                        self.throttle.record_failure();
                        let until = self.throttle.throttled_until();
                        Duration::from_secs((until - chrono::Utc::now().timestamp()) as u64)
                    } else {
                        Duration::from_secs(10)
                    };
                    let fmt = humantime::format_duration(timeout);
                    error!(err = %e, in = %fmt, "failed to connect to firehose, retrying later");
                    tokio::time::sleep(timeout).await;
                    continue;
                }
            };

            self.throttle.record_success();
            info!("firehose connected");

            let res = loop {
                tokio::select! {
                    msg = stream.next() => {
                        let Some(bytes_res) = msg else { break Err(FirehoseError::EmptyFrame); };
                        let bytes = match bytes_res {
                            Ok(b) => b,
                            Err(e) => break Err(e),
                        };
                        match decode_frame(&bytes) {
                            Ok(msg) => {
                                if self.is_pds {
                                    let tier = {
                                        let meta = self.state.pds_meta.load();
                                        let banned = meta.is_banned(host);
                                        if banned {
                                            break Ok(());
                                        }
                                        meta.tier_for(host, &self.state.rate_tiers)
                                    };
                                    let accounts = self.state.db.get_count(&count_key).await;
                                    tokio::select! {
                                        _ = self.throttle.wait_for_allow(accounts, &tier) => {}
                                        _ = self.enabled.changed() => {
                                            if !*self.enabled.borrow() {
                                                info!("firehose disabled, disconnecting");
                                                break Ok(());
                                            }
                                        }
                                    }
                                }
                                self.handle_message(msg).await
                            },
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
                                    e => break Err(e),
                                }
                            }
                        }
                        backoff = Duration::from_secs(0);
                    }
                    _ = self.enabled.changed() => {
                        if !*self.enabled.borrow() {
                            info!("firehose disabled, disconnecting");
                            break Ok(());
                        }
                    }
                }
            };

            if let Err(e) = res {
                if let FirehoseError::RelayError { error, message } = e {
                    let message = message.map_or(Cow::Borrowed("<no message>"), Cow::Owned);
                    error!(err = %error, "relay sent error: {message}");
                } else if backoff.as_secs() < 60 {
                    // stop logging errors after a minute of retries
                    // as to not spam logs, unlikely for error to change atp
                    error!(err = %e, "firehose stream error");
                }
                if backoff.is_zero() {
                    backoff = Duration::from_secs(5);
                }
                let fmt = humantime::format_duration(backoff);
                error!(in = %fmt, "firehose disconnected, reconnecting");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
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
                        let metadata = crate::db::deser_repo_meta(bytes.as_ref())?;

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
