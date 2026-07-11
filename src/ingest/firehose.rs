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
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{Span, debug, error, info, trace, warn};
use url::Url;

// these match ref relay
const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
struct FirehoseFailure {
    kind: &'static str,
    detail: String,
}

impl FirehoseFailure {
    fn new(kind: &'static str, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
}

fn classify_firehose_error(err: &FirehoseError) -> FirehoseFailure {
    match err {
        FirehoseError::WebSocket(err) => classify_websocket_error(err),
        FirehoseError::UnknownScheme(scheme) => {
            FirehoseFailure::new("config", format!("unsupported URL scheme `{scheme}`"))
        }
        FirehoseError::InvalidUri(err) => {
            FirehoseFailure::new("config", format!("invalid websocket URI: {err}"))
        }
        FirehoseError::Decode(err) => {
            FirehoseFailure::new("decode", format!("failed to decode firehose frame: {err}"))
        }
        FirehoseError::EmptyFrame => FirehoseFailure::new("protocol", "received empty frame"),
        FirehoseError::RelayError { error, message } => FirehoseFailure::new(
            "relay_error",
            message
                .as_deref()
                .map(|message| format!("{error}: {message}"))
                .unwrap_or_else(|| error.clone()),
        ),
        FirehoseError::UnknownOp(op) => {
            FirehoseFailure::new("protocol", format!("unknown frame op {op}"))
        }
        FirehoseError::MissingType => FirehoseFailure::new("protocol", "missing frame type header"),
        FirehoseError::UnknownType(ty) => {
            FirehoseFailure::new("protocol", format!("unknown frame type `{ty}`"))
        }
        FirehoseError::Cbor(err) => {
            FirehoseFailure::new("decode", format!("cbor decode error: {err}"))
        }
        FirehoseError::StreamClosed { code, reason } => {
            FirehoseFailure::new("stream_closed", format!("close code {code}: {reason}"))
        }
        FirehoseError::TcpDropped => FirehoseFailure::new("tcp_dropped", "tcp layer dropped"),
        FirehoseError::FutureCursor => FirehoseFailure::new("cursor", "future cursor"),
    }
}

fn classify_websocket_error(err: &tokio_websockets::Error) -> FirehoseFailure {
    match err {
        tokio_websockets::Error::CannotResolveHost => {
            FirehoseFailure::new("dns", "host could not be resolved")
        }
        tokio_websockets::Error::Io(err) => {
            let kind = match err.kind() {
                ErrorKind::ConnectionRefused => "tcp_refused",
                ErrorKind::ConnectionReset => "tcp_reset",
                ErrorKind::ConnectionAborted => "tcp_aborted",
                ErrorKind::TimedOut => "tcp_timeout",
                ErrorKind::UnexpectedEof => "tcp_eof",
                ErrorKind::AddrInUse => "tcp_addr_in_use",
                ErrorKind::AddrNotAvailable => "tcp_addr_not_available",
                ErrorKind::PermissionDenied => "tcp_permission",
                _ => "io",
            };
            FirehoseFailure::new(kind, format!("{err}"))
        }
        tokio_websockets::Error::InvalidDNSName(_) => {
            FirehoseFailure::new("tls_invalid_dns_name", format!("{err}"))
        }
        tokio_websockets::Error::Rustls(_) => FirehoseFailure::new("tls", format!("{err}")),
        tokio_websockets::Error::Upgrade(upgrade) => {
            let kind = match upgrade {
                tokio_websockets::upgrade::Error::DidNotSwitchProtocols(_) => "http_upgrade",
                _ => "websocket_upgrade",
            };
            FirehoseFailure::new(kind, format!("{upgrade}"))
        }
        tokio_websockets::Error::UnsupportedScheme => {
            FirehoseFailure::new("config", "unsupported websocket URL scheme")
        }
        tokio_websockets::Error::Protocol(err) => {
            FirehoseFailure::new("websocket_protocol", format!("{err}"))
        }
        tokio_websockets::Error::PayloadTooLong { len, max_len } => FirehoseFailure::new(
            "websocket_payload",
            format!("payload length {len} > {max_len}"),
        ),
        _ => FirehoseFailure::new("websocket", format!("{err}")),
    }
}

fn message_stats(msg: &SubscribeReposMessage<'_>) -> (&'static str, Option<i64>) {
    match msg {
        SubscribeReposMessage::Commit(commit) => ("commit", Some(commit.seq)),
        SubscribeReposMessage::Sync(sync) => ("sync", Some(sync.seq)),
        SubscribeReposMessage::Identity(identity) => ("identity", Some(identity.seq)),
        SubscribeReposMessage::Account(account) => ("account", Some(account.seq)),
        SubscribeReposMessage::Info(_) => ("info", None),
    }
}

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
    max_failures: usize,
    stats: Arc<crate::ingest::firehose_stats::FirehoseSourceStats>,
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
        max_failures: usize,
    ) -> Self {
        let throttle = state.throttler.get_handle(&relay_host).await;
        let stats = state.firehose_stats.handle(&relay_host).await;
        Self {
            state,
            buffer_tx,
            relay_host,
            is_pds,
            filter,
            enabled,
            _verify_signatures: verify_signatures,
            throttle,
            max_failures,
            stats,
        }
    }

    #[tracing::instrument(skip(self), fields(host = %self.relay_host))]
    pub async fn run(mut self) -> Result<()> {
        let host = self.relay_host.host_str().unwrap_or("");
        let count_key = crate::db::keys::pds_account_count_key(host);

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
            self.stats.record_connect_attempt(start_cursor);

            let host_status = self.is_pds.then(|| {
                let meta = self.state.pds_meta.load();
                meta.status(host).as_str()
            });
            debug!(
                is_pds = self.is_pds,
                cursor = ?start_cursor,
                host_status = ?host_status,
                consecutive_failures = self.throttle.consecutive_failures(),
                throttled_until = self.throttle.throttled_until(),
                "connecting to firehose"
            );
            let connect_started = Instant::now();
            let mut stream =
                match FirehoseStream::connect(self.relay_host.clone(), start_cursor).await {
                    Ok(s) => s,
                    Err(e) => {
                        let failure = classify_firehose_error(&e);
                        self.stats.record_connect_error(failure.kind);
                        let secs = match self.on_failure(&failure).await {
                            Some(secs) => secs,
                            None => {
                                error!(
                                    err = %e,
                                    failure_kind = failure.kind,
                                    failure = %failure.detail,
                                    failures = self.throttle.consecutive_failures(),
                                    max_failures = self.max_failures,
                                    "failed to connect to firehose, giving up"
                                );
                                break Ok(());
                            }
                        };
                        let timeout = rng.add_jitter(Duration::from_secs(secs).min(MAX_BACKOFF));
                        let fmt = humantime::format_duration(timeout);
                        error!(
                            err = %e,
                            failure_kind = failure.kind,
                            failure = %failure.detail,
                            failures = self.throttle.consecutive_failures(),
                            max_failures = self.max_failures,
                            in = %fmt,
                            "failed to connect to firehose, retrying later"
                        );
                        tokio::time::sleep(timeout).await;
                        continue;
                    }
                };

            info!(
                elapsed_ms = connect_started.elapsed().as_millis(),
                "firehose connected"
            );
            self.stats.record_connected(connect_started.elapsed());
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
                        self.stats.record_frame(bytes.len());
                        match decode_frame(&bytes) {
                            Ok(msg) => {
                                let (kind, seq) = message_stats(&msg);
                                self.stats.record_decoded(kind, seq);
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
                                    let throttle_started = crate::ingest::firehose_stats::StatsInstant::now();
                                    let mut disabled = false;
                                    loop {
                                        tokio::select! {
                                            _ = self.throttle.wait_for_allow(accounts, &tier) => {
                                                self.stats.record_throttle_wait(throttle_started.elapsed());
                                                break;
                                            }
                                            res = self.enabled.changed() => {
                                                if res.is_err() || !*self.enabled.borrow() {
                                                    disabled = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if disabled {
                                        info!("firehose disabled, disconnecting");
                                        break Ok(());
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

                            if current_status != new_status
                                && let Err(e) = self.set_host_status(new_status)
                            {
                                error!(err = %e, "failed to update host status");
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
                // 1001 "going away" means the server shut down cleanly (restart, deploy, etc.).
                // this is not a real failure so we intentionally skip on_failure / backoff and
                // reconnect quickly. a malicious source could abuse this but they could equally
                // just drop the TCP connection, which would hit the normal backoff path anyway.
                Err(FirehoseError::StreamClosed { code: 1001, reason }) => {
                    self.stats.record_stream_error("stream_closed");
                    debug!(reason = %reason, "host gone away");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(FirehoseError::FutureCursor) => {
                    self.stats.record_stream_error("future_cursor");
                    if self.is_pds
                        && let Err(e) = self.set_host_status(HostStatus::Idle)
                    {
                        error!(err = %e, "failed to update host status to idle");
                    }
                    if let Err(e) = self.clear_stale_cursor() {
                        error!(err = %e, "failed to clear outdated cursor");
                    }
                    warn!("outdated cursor, cleared stored cursor and retrying from live tail");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(FirehoseError::RelayError { error, message }) => {
                    let message = message
                        .as_deref()
                        .map_or(Cow::Borrowed("<no message>"), Cow::Borrowed);
                    let failure =
                        FirehoseFailure::new("relay_error", format!("{error}: {message}"));
                    self.stats.record_stream_error(failure.kind);
                    error!(err = %error, "relay sent error: {message}");
                    let secs = match self.on_failure(&failure).await {
                        Some(secs) => secs,
                        None => {
                            error!(
                                failure_kind = failure.kind,
                                failure = %failure.detail,
                                failures = self.throttle.consecutive_failures(),
                                max_failures = self.max_failures,
                                "firehose disconnected, giving up"
                            );
                            break Ok(());
                        }
                    };
                    let timeout = rng.add_jitter(Duration::from_secs(secs).min(MAX_BACKOFF));
                    let fmt = humantime::format_duration(timeout);
                    error!(
                        failure_kind = failure.kind,
                        failure = %failure.detail,
                        failures = self.throttle.consecutive_failures(),
                        max_failures = self.max_failures,
                        in = %fmt,
                        "firehose disconnected, reconnecting later"
                    );
                    tokio::time::sleep(timeout).await;
                }
                Err(e) => {
                    let failure = classify_firehose_error(&e);
                    self.stats.record_stream_error(failure.kind);
                    let secs = match self.on_failure(&failure).await {
                        Some(secs) => secs,
                        None => {
                            error!(
                                err = %e,
                                failure_kind = failure.kind,
                                failure = %failure.detail,
                                failures = self.throttle.consecutive_failures(),
                                max_failures = self.max_failures,
                                "firehose stream error, giving up"
                            );
                            break Ok(());
                        }
                    };
                    let timeout = rng.add_jitter(Duration::from_secs(secs).min(MAX_BACKOFF));
                    let fmt = humantime::format_duration(timeout);
                    error!(
                        err = %e,
                        failure_kind = failure.kind,
                        failure = %failure.detail,
                        failures = self.throttle.consecutive_failures(),
                        max_failures = self.max_failures,
                        in = %fmt,
                        "firehose stream error, reconnecting later"
                    );
                    tokio::time::sleep(timeout).await;
                }
            }
        }
    }

    /// record a failure and return the backoff duration in seconds,
    /// or `None` if the failure threshold was reached and the subscriber should stop
    async fn on_failure(&self, failure: &FirehoseFailure) -> Option<u64> {
        let secs = self
            .throttle
            .record_failure_detail(failure.kind, failure.detail.clone())
            .unwrap_or_else(|| {
                let until = self.throttle.throttled_until();
                0.max(until - chrono::Utc::now().timestamp()) as u64
            });
        let failures = self.throttle.consecutive_failures();
        if failures >= self.max_failures {
            warn!(
                failures,
                max_failures = self.max_failures,
                failure_kind = failure.kind,
                failure = %failure.detail,
                "too many consecutive failures, giving up on host"
            );
            if self.is_pds
                && let Err(e) = self.set_host_status(HostStatus::Offline)
            {
                error!(err = %e, "failed to update host status to offline");
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

    fn clear_stale_cursor(&self) -> Result<()> {
        let key = crate::db::keys::firehose_cursor_key_from_url(&self.relay_host);
        self.state.db.cursors.remove(key).into_diagnostic()?;
        self.state
            .firehose_cursors
            .peek_with(&self.relay_host, |_, cursor| {
                cursor.store(0, Ordering::SeqCst)
            });
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

        let should_process_started = crate::ingest::firehose_stats::StatsInstant::now();
        let process = self
            .should_process(did)
            .await
            .inspect_err(|e| error!(did = %did, err = %e, "failed to check if we should process"))
            .unwrap_or(false);
        self.stats
            .record_should_process(should_process_started.elapsed());
        if !process {
            self.stats.record_skipped();
            trace!(did = %did, "skipping: not in filter");
            return;
        }
        trace!(did = %did, "forwarding message to ingest buffer");

        let send_started = crate::ingest::firehose_stats::StatsInstant::now();
        let res = self
            .buffer_tx
            .send(IngestMessage::Firehose {
                url: self.relay_host.clone(),
                is_pds: self.is_pds,
                msg: msg.into_static(),
            })
            .await;
        {
            let elapsed = send_started.elapsed();
            if res.is_ok() {
                self.stats.record_forwarded(elapsed);
            } else {
                self.stats.record_forward_error(elapsed);
            }
        }
        if let Err(e) = res {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::sync::atomic::AtomicI64;

    #[tokio::test]
    async fn clear_stale_cursor_resets_db_and_memory() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = Config {
            database_path: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let state = Arc::new(AppState::new(&cfg)?);
        let relay_host = Url::parse("wss://example.com/").into_diagnostic()?;

        crate::db::set_firehose_cursor(&state.db, &relay_host, 1234)?;
        let _ = state
            .firehose_cursors
            .insert_async(relay_host.clone(), AtomicI64::new(1234))
            .await;

        let ingestor = FirehoseIngestor::new(
            state.clone(),
            BufferTx::channel(1).0,
            relay_host.clone(),
            true,
            state.filter.clone(),
            state.firehose_enabled.subscribe(),
            false,
            1,
        )
        .await;

        ingestor.clear_stale_cursor()?;

        assert!(
            crate::db::get_firehose_cursor(&state.db, &relay_host)
                .await?
                .is_none()
        );
        let in_memory = state
            .firehose_cursors
            .peek_with(&relay_host, |_, cursor| cursor.load(Ordering::SeqCst))
            .unwrap_or(-1);
        assert_eq!(in_memory, 0);

        Ok(())
    }
}
