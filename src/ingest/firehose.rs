use crate::filter::{FilterHandle, FilterMode};
use crate::ingest::stream::{FirehoseError, FirehoseStream, SubscribeReposMessage, decode_frame};
use crate::ingest::{BufferTx, IngestMessage};
use crate::pds_meta::HostStatus;
use crate::state::AppState;
use crate::util::WatchEnabledExt;
use crate::util::throttle::ThrottleHandle;
use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};
use rand::RngExt;
use rand::rngs::SmallRng;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{Span, debug, error, info, trace, warn};
use url::Url;

// these match ref relay
const MAX_FAILURES: usize = 15;
const MAX_BACKOFF: Duration = Duration::from_secs(60);

trait AddJitter: rand::Rng {
    fn add_jitter(&mut self, timeout: Duration) -> Duration {
        let timeout_secs = timeout.as_secs_f32();
        let amt = timeout_secs * self.random_range(-0.12..0.12);
        Duration::from_secs_f32(timeout_secs + amt)
    }
}
impl<R: rand::Rng> AddJitter for R {}

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

    #[tracing::instrument(skip(self), fields(host = %self.relay_host))]
    pub async fn run(mut self) -> Result<()> {
        let host = self.relay_host.host_str().unwrap_or("");
        let count_key = crate::db::keys::pds_account_count_key(&host);

        let mut rng: SmallRng = rand::make_rng();

        loop {
            if self.state.pds_meta.load().is_banned(host) {
                break Ok(());
            }
            self.enabled.wait_enabled("firehose").await;

            // get cursor
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
                    let Some(secs) = self.on_failure().await else {
                        break Ok(());
                    };
                    let timeout = rng.add_jitter(Duration::from_secs(secs).min(MAX_BACKOFF));
                    let fmt = humantime::format_duration(timeout);
                    error!(err = %e, in = %fmt, "failed to connect to firehose, retrying later");
                    tokio::time::sleep(timeout).await;
                    continue;
                }
            };

            info!("firehose connected");
            let mut marked_active = false;
            let active_sleep_secs = if cfg!(debug_assertions) { 1 } else { 60 };
            let mut active_sleep =
                std::pin::pin!(tokio::time::sleep(Duration::from_secs(active_sleep_secs)));

            let res = loop {
                tokio::select! {
                    msg = stream.next() => {
                        let bytes = match msg {
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
                                        let override_name = meta.hosts.get(host).and_then(|h| h.tier.as_ref());
                                        self.state.tier_policy.resolve(host, override_name)
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
                                self.handle_message(msg).await;
                            },
                            Err(e) => match e {
                                // don't disconnect on unknown op or type
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
                            },
                        }
                    }
                    _ = &mut active_sleep, if !marked_active => {
                        marked_active = true;
                        // only reset failure state once the stream has been healthy for a bit
                        // so we dont get in a "connects successfully, sends garbage" situation
                        self.throttle.record_success();
                        if self.is_pds {
                            let (current_status, tier) = {
                                let meta = self.state.pds_meta.load();
                                let override_name = meta.hosts.get(host).and_then(|h| h.tier.as_ref());
                                (meta.status(host), self.state.tier_policy.resolve(host, override_name))
                            };
                            if current_status == HostStatus::Banned {
                                break Ok(());
                            }
                            let count = self.state.db.get_count_sync(&count_key);
                            let new_status = tier.account_limit.is_some_and(|l| count >= l)
                                .then_some(HostStatus::Throttled)
                                .unwrap_or(HostStatus::Active);
                            debug!(
                                host,
                                ?current_status,
                                account_limit = ?tier.account_limit,
                                count,
                                ?new_status,
                                "active_sleep: computed status transition"
                            );

                            if current_status != new_status {
                                if let Err(e) = self.set_host_status(new_status) {
                                    error!(err = %e, "failed to update host status");
                                }
                            }
                        }
                    }
                    _ = self.enabled.changed() => {
                        if !*self.enabled.borrow() {
                            info!("firehose disabled, disconnecting");
                            break Ok(());
                        }
                    }
                }
            };

            match res {
                Ok(()) => {}
                Err(FirehoseError::StreamClosed { code: 1001, reason }) => {
                    debug!(reason = %reason, "host gone away");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(FirehoseError::FutureCursor) => {
                    if self.is_pds
                        && let Err(e) = self.set_host_status(HostStatus::Idle)
                    {
                        error!(err = %e, "failed to update host status to idle");
                    }
                    debug!("outdated cursor, backing off");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
                Err(FirehoseError::RelayError { error, message }) => {
                    let message = message
                        .as_deref()
                        .map_or(Cow::Borrowed("<no message>"), Cow::Borrowed);
                    error!(err = %error, "relay sent error: {message}");
                    let Some(secs) = self.on_failure().await else {
                        break Ok(());
                    };
                    let timeout = rng.add_jitter(Duration::from_secs(secs).min(MAX_BACKOFF));
                    let fmt = humantime::format_duration(timeout);
                    error!(in = %fmt, "firehose disconnected, reconnecting later");
                    tokio::time::sleep(timeout).await;
                }
                Err(e) => {
                    let Some(secs) = self.on_failure().await else {
                        break Ok(());
                    };
                    let timeout = rng.add_jitter(Duration::from_secs(secs).min(MAX_BACKOFF));
                    let fmt = humantime::format_duration(timeout);
                    error!(err = %e, in = %fmt, "firehose stream error, reconnecting later");
                    tokio::time::sleep(timeout).await;
                }
            }
        }
    }

    /// record a failure and return the backoff duration in seconds,
    /// or `None` if the failure threshold was reached and the subscriber should stop
    async fn on_failure(&self) -> Option<u64> {
        let secs = self.throttle.record_failure().unwrap_or_else(|| {
            let until = self.throttle.throttled_until();
            0.max(until - chrono::Utc::now().timestamp()) as u64
        });
        let failures = self.throttle.consecutive_failures();
        if failures >= MAX_FAILURES {
            warn!(failures, "too many consecutive failures, giving up on host");
            if self.is_pds {
                if let Err(e) = self.set_host_status(HostStatus::Offline) {
                    error!(err = %e, "failed to update host status to offline");
                }
            }
            return None;
        }
        Some(secs)
    }

    fn set_host_status(&self, status: HostStatus) -> Result<()> {
        let Some(host) = self.relay_host.host_str() else {
            return Ok(());
        };

        debug!(host = %host, status = ?status, "updating host status");

        let mut batch = self.state.db.inner.batch();
        crate::db::pds_meta::set_status(&mut batch, &self.state.db.filter, host, status)?;
        batch.commit().into_diagnostic()?;

        crate::pds_meta::PdsMeta::update_host(&self.state.pds_meta, host, |h| h.status = status);

        Ok(())
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
