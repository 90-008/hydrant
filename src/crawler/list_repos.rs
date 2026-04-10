use crate::db::keys::crawler_cursor_key;
use crate::db::{Db, keys};
use crate::state::AppState;
use crate::util::throttle::{OrFailure, ThrottleHandle, Throttler};
use crate::util::{
    ErrorForStatus, RetryOutcome, RetryWithBackoff, WatchEnabledExt, is_io_error_their_fault,
    is_status_their_fault, is_tls_cert_error, parse_retry_after,
};
use chrono::{DateTime, TimeDelta, Utc};
use fjall::OwnedWriteBatch;
use futures::FutureExt;
use jacquard_api::com_atproto::repo::describe_repo::DescribeRepoOutput;
use jacquard_api::com_atproto::sync::list_repos::ListReposOutput;
use jacquard_common::{IntoStatic, types::string::Did};
use miette::{Context, IntoDiagnostic, Result};
use rand::RngExt;
use rand::rngs::SmallRng;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::ops::{Add, Mul, Sub};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{Instrument, debug, error, info, trace, warn};
use url::Url;

use super::worker::{CrawlerBatch, CursorUpdate};
use super::{CrawlerStats, InFlight, InFlightGuard, base_url};

const MAX_RETRY_BATCH: usize = 1000;
const BLOCKING_TASK_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const MAX_RETRY_ATTEMPTS: u32 = 5;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RetryState {
    pub(super) after: DateTime<Utc>,
    pub(super) duration: TimeDelta,
    pub(super) attempts: u32,
    #[serde(serialize_with = "crate::util::ser_status_code")]
    #[serde(deserialize_with = "crate::util::deser_status_code")]
    pub(super) status: Option<StatusCode>,
}

impl RetryState {
    pub(super) fn new(secs: i64) -> Self {
        let duration = TimeDelta::seconds(secs);
        Self {
            duration,
            after: Utc::now().add(duration),
            attempts: 0,
            status: None,
        }
    }

    /// returns the next retry state or `None` if the attempt count would reach the cap.
    pub(super) fn next_attempt(self) -> Option<Self> {
        let attempts = self.attempts + 1;
        if attempts >= MAX_RETRY_ATTEMPTS {
            return None;
        }
        let duration = self.duration * 2;
        Some(Self {
            after: Utc::now().add(duration),
            duration,
            attempts,
            status: None,
        })
    }

    pub(super) fn with_status(mut self, code: StatusCode) -> Self {
        self.status = Some(code);
        self
    }
}

trait ToRetryState {
    fn to_retry_state(&self) -> RetryState;
}

impl ToRetryState for ThrottleHandle {
    fn to_retry_state(&self) -> RetryState {
        let after = chrono::DateTime::from_timestamp_secs(self.throttled_until()).unwrap();
        RetryState {
            duration: after.sub(Utc::now()),
            after,
            attempts: 0,
            status: None,
        }
    }
}

pub(super) enum CrawlCheckResult {
    Signal,
    NoSignal,
    Retry(RetryState),
}

impl From<RetryState> for CrawlCheckResult {
    fn from(value: RetryState) -> Self {
        Self::Retry(value)
    }
}

fn is_throttle_worthy(e: &reqwest::Error) -> bool {
    use std::error::Error;

    if e.is_timeout() {
        return true;
    }

    let mut src = e.source();
    while let Some(s) = src {
        if let Some(e) = s.downcast_ref::<std::io::Error>() {
            if is_io_error_their_fault(&e) || is_tls_cert_error(e) {
                return true;
            }
        }
        src = s.source();
    }

    e.status().map_or(false, |s| {
        // lets not consider internal server errors for throttling
        // a server might be having a hard time on one request, but not on the rest
        s.as_u16() != 500 && is_status_their_fault(s.as_u16())
    })
}

/// shared describeRepo signal-checking logic used by both relay and retry producers.
#[derive(Clone)]
pub(crate) struct SignalChecker {
    pub(crate) http: reqwest::Client,
    pub(crate) state: Arc<AppState>,
    pub(crate) throttler: Throttler,
}

impl SignalChecker {
    fn check_repo_signals(
        &self,
        filter: Arc<crate::filter::FilterConfig>,
        did: Did<'static>,
    ) -> impl Future<Output = (Did<'static>, CrawlCheckResult)> + Send + 'static {
        let resolver = self.state.resolver.clone();
        let http = self.http.clone();
        let throttler = self.throttler.clone();
        async move {
            const MAX_RETRIES: u32 = 5;

            let pds_url = (|| resolver.resolve_identity_info(&did))
                .retry(MAX_RETRIES, |e, attempt| {
                    matches!(e, crate::resolver::ResolverError::Ratelimited)
                        .then(|| Duration::from_secs(1 << attempt.min(5)))
                })
                .await;

            let pds_url = match pds_url {
                Ok((url, _)) => url,
                Err(RetryOutcome::Ratelimited) => {
                    error!(
                        retries = MAX_RETRIES,
                        "rate limited resolving identity, giving up"
                    );
                    return (did, RetryState::new(60).into());
                }
                Err(RetryOutcome::Failed(e)) => {
                    error!(err = %e, "failed to resolve identity");
                    return (did, RetryState::new(60).into());
                }
            };

            let throttle = throttler.get_handle(&pds_url).await;
            if throttle.is_throttled() {
                trace!(host = pds_url.host_str(), "skipping throttled pds");
                return (did, throttle.to_retry_state().into());
            }

            let _permit = throttle.acquire().unit_error().or_failure(&throttle, || ());
            let Ok(_permit) = _permit.await else {
                trace!(
                    host = pds_url.host_str(),
                    "pds failed while waiting for permit"
                );
                return (did, throttle.to_retry_state().into());
            };

            enum RequestError {
                Reqwest(reqwest::Error),
                RateLimited(Option<u64>),
                Throttled,
            }

            let mut describe_url = pds_url.join("/xrpc/com.atproto.repo.describeRepo").unwrap();
            describe_url.query_pairs_mut().append_pair("repo", &did);

            let resp = async {
                let resp = http
                    .get(describe_url)
                    .timeout(throttle.timeout())
                    .send()
                    .await
                    .map_err(RequestError::Reqwest)?;
                if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                    return Err(RequestError::RateLimited(parse_retry_after(&resp)));
                }
                resp.error_for_status().map_err(RequestError::Reqwest)
            }
            .or_failure(&throttle, || RequestError::Throttled)
            .await;

            let resp = match resp {
                Ok(r) => {
                    throttle.record_success();
                    r
                }
                Err(RequestError::RateLimited(secs)) => {
                    throttle.record_ratelimit(secs);
                    return (
                        did,
                        throttle
                            .to_retry_state()
                            .with_status(StatusCode::TOO_MANY_REQUESTS)
                            .into(),
                    );
                }
                Err(RequestError::Throttled) => {
                    return (did, throttle.to_retry_state().into());
                }
                Err(RequestError::Reqwest(e)) => {
                    if e.is_timeout() && !throttle.record_timeout() {
                        let mut retry_state = RetryState::new(60);
                        retry_state.status = e.status();
                        return (did, retry_state.into());
                    }
                    if is_throttle_worthy(&e) {
                        if let Some(secs) = throttle.record_failure() {
                            warn!(url = %pds_url, secs, "throttling pds due to hard failure");
                        }
                        let mut retry_state = throttle.to_retry_state();
                        retry_state.status = e.status();
                        return (did, retry_state.into());
                    }
                    match e.status() {
                        Some(StatusCode::NOT_FOUND | StatusCode::GONE) => {
                            trace!("repo not found");
                            return (did, CrawlCheckResult::NoSignal);
                        }
                        Some(s) if s.is_client_error() => {
                            error!(status = %s, "repo unavailable");
                            return (did, CrawlCheckResult::NoSignal);
                        }
                        _ => {
                            error!(err = %e, "repo errored");
                            let mut retry_state = RetryState::new(60 * 15);
                            retry_state.status = e.status();
                            return (did, retry_state.into());
                        }
                    }
                }
            };

            let bytes = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    error!(err = %e, "failed to read describeRepo response");
                    return (did, RetryState::new(60 * 5).into());
                }
            };

            let out = match serde_json::from_slice::<DescribeRepoOutput>(&bytes) {
                Ok(out) => out,
                Err(e) => {
                    error!(err = %e, "failed to parse describeRepo response");
                    return (did, RetryState::new(60 * 10).into());
                }
            };

            let found_signal = out
                .collections
                .iter()
                .any(|col| filter.matches_signal(col.as_str()));
            if !found_signal {
                trace!("no signal-matching collections found");
            }

            (
                did,
                found_signal
                    .then_some(CrawlCheckResult::Signal)
                    .unwrap_or(CrawlCheckResult::NoSignal),
            )
        }
    }

    /// checks signals for each DID in `in_flight`, writes retry/no-signal state changes
    /// into `batch`, and returns a vec containing only the guards for confirmed DIDs.
    ///
    /// guards for non-confirmed DIDs are dropped here, releasing their in-flight slots.
    /// confirmed DIDs' retry entries are NOT removed here. the worker removes them
    /// atomically with the pending insert.
    pub(super) async fn check_signals_batch(
        &self,
        in_flight: Vec<InFlightGuard>,
        filter: &Arc<crate::filter::FilterConfig>,
        batch: &mut OwnedWriteBatch,
        existing: &HashMap<Did<'static>, RetryState>,
    ) -> Result<Vec<InFlightGuard>> {
        let db = &self.state.db;
        let mut valid: Vec<Did<'static>> = Vec::new();
        let mut set = tokio::task::JoinSet::new();

        for guard in &in_flight {
            let filter = filter.clone();
            let did = guard.did.clone();
            let span = tracing::info_span!("signals", did = %did);
            set.spawn(self.check_repo_signals(filter, did).instrument(span));
        }

        while let Some(res) = tokio::time::timeout(Duration::from_secs(60), set.join_next())
            .await
            .into_diagnostic()
            .map_err(|_| {
                error!("signal check task timed out after 60s");
                miette::miette!("signal check task timed out")
            })?
        {
            let (did, result) = match res {
                Ok(inner) => inner,
                Err(e) => {
                    error!(err = ?e, "signal check panicked");
                    continue;
                }
            };

            match result {
                CrawlCheckResult::Signal => {
                    valid.push(did);
                }
                CrawlCheckResult::NoSignal => {
                    batch.remove(&db.crawler, keys::crawler_retry_key(&did));
                }
                CrawlCheckResult::Retry(state) => {
                    let prev_attempts = existing.get(&did).map(|s| s.attempts).unwrap_or(0);
                    let carried = RetryState {
                        attempts: prev_attempts,
                        ..state
                    };
                    let next = match carried.next_attempt() {
                        Some(next) => next,
                        None => RetryState {
                            attempts: MAX_RETRY_ATTEMPTS,
                            ..state
                        },
                    };
                    batch.insert(
                        &db.crawler,
                        keys::crawler_retry_key(&did),
                        rmp_serde::to_vec(&next).into_diagnostic()?,
                    );
                }
            }
        }

        let valid_set: std::collections::HashSet<&Did<'static>> = valid.iter().collect();
        Ok(in_flight
            .into_iter()
            .filter(|g| valid_set.contains(g.as_did()))
            .collect())
    }
}

pub(crate) struct ListReposProducer {
    pub(crate) url: Url,
    pub(crate) checker: SignalChecker,
    pub(crate) in_flight: InFlight,
    pub(crate) tx: mpsc::Sender<CrawlerBatch>,
    pub(crate) enabled: watch::Receiver<bool>,
    pub(crate) stats: CrawlerStats,
}

impl ListReposProducer {
    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            if let Err(e) = self.crawl().await {
                error!(err = ?e, relay = %self.url, "fatal relay crawl error, restarting in 30s");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }

    async fn get_cursor(&self) -> Result<Option<SmolStr>> {
        let key = crawler_cursor_key(self.url.as_str());
        let cursor_bytes = Db::get(self.checker.state.db.cursors.clone(), &key).await?;
        Ok(cursor_bytes
            .as_deref()
            .and_then(|b| rmp_serde::from_slice::<SmolStr>(b).ok()))
    }

    async fn crawl(&mut self) -> Result<()> {
        let db = &self.checker.state.db;
        let base = base_url(&self.url)?;

        let mut cursor = self.get_cursor().await?;
        match &cursor {
            Some(c) => info!(cursor = %c, "resuming"),
            None => info!("starting from scratch"),
        }

        loop {
            self.enabled.wait_enabled("crawler").await;

            let mut url = base
                .join("/xrpc/com.atproto.sync.listRepos")
                .into_diagnostic()?;
            url.query_pairs_mut().append_pair("limit", "1000");
            if let Some(c) = &cursor {
                url.query_pairs_mut().append_pair("cursor", c.as_str());
            }

            let fetch_result = (|| self.checker.http.get(url.clone()).send().error_for_status())
                .retry(5, |e: &reqwest::Error, attempt| {
                    matches!(e.status(), Some(StatusCode::TOO_MANY_REQUESTS))
                        .then(|| Duration::from_secs(1 << attempt.min(5)))
                })
                .await;

            let res = match fetch_result {
                Ok(r) => r,
                Err(RetryOutcome::Ratelimited) => {
                    warn!("rate limited by relay after retries");
                    continue;
                }
                Err(RetryOutcome::Failed(e)) => {
                    error!(err = %e, "crawler failed to fetch listRepos");
                    continue;
                }
            };

            let bytes = match res.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    error!(err = %e, "cant read listRepos response");
                    continue;
                }
            };

            let filter = self.checker.state.filter.load_full();

            struct ParseResult {
                unknown_dids: Vec<Did<'static>>,
                cursor: Option<SmolStr>,
                count: usize,
            }

            let parse_result = {
                let repos = db.repos.clone();
                let filter_ks = db.filter.clone();
                let crawler_ks = db.crawler.clone();

                tokio::time::timeout(
                    BLOCKING_TASK_TIMEOUT,
                    tokio::task::spawn_blocking(move || -> miette::Result<Option<ParseResult>> {
                        let output = match serde_json::from_slice::<ListReposOutput>(&bytes) {
                            Ok(out) => out.into_static(),
                            Err(e) => {
                                error!(err = %e, "failed to parse listRepos response");
                                return Ok(None);
                            }
                        };
                        if output.repos.is_empty() {
                            return Ok(None);
                        }
                        let count = output.repos.len();
                        let next_cursor = output.cursor.map(|c| c.as_str().into());
                        let mut unknown = Vec::new();
                        for repo in output.repos {
                            let excl_key = crate::db::filter::exclude_key(repo.did.as_str())?;
                            if filter_ks.contains_key(&excl_key).into_diagnostic()? {
                                continue;
                            }
                            let retry_key = keys::crawler_retry_key(&repo.did);
                            if crawler_ks.contains_key(&retry_key).into_diagnostic()? {
                                continue;
                            }
                            let did_key = keys::repo_key(&repo.did);
                            if !repos.contains_key(&did_key).into_diagnostic()? {
                                unknown.push(repo.did.into_static());
                            }
                        }
                        Ok(Some(ParseResult {
                            unknown_dids: unknown,
                            cursor: next_cursor,
                            count,
                        }))
                    }),
                )
                .await
            }
            .into_diagnostic()?
            .map_err(|_| {
                error!(
                    "spawn_blocking task for parsing listRepos timed out after {}s",
                    BLOCKING_TASK_TIMEOUT.as_secs()
                );
                miette::miette!("spawn_blocking task for parsing listRepos timed out")
            })?;

            let ParseResult {
                unknown_dids,
                cursor: next_cursor,
                count,
            } = match parse_result {
                Ok(Some(res)) => res,
                Ok(None) => {
                    info!("enumeration pass complete, sleeping 1h");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(3600)) => {
                            info!("resuming after 1h sleep");
                        }
                        _ = self.enabled.changed() => {
                            if !*self.enabled.borrow() { return Ok(()); }
                        }
                    }
                    continue;
                }
                Err(e) => return Err(e).wrap_err("error while crawling"),
            };

            debug!(count, "fetched repos");
            self.stats.record_crawled(count);

            let pass_complete;
            if let Some(new_cursor) = next_cursor {
                cursor = Some(new_cursor.as_str().into());
                pass_complete = false;
            } else {
                info!("reached end of list.");
                pass_complete = true;
            }

            let cursor_update = cursor
                .as_ref()
                .map(|c| -> Result<CursorUpdate> {
                    Ok(CursorUpdate {
                        key: crawler_cursor_key(self.url.as_str()),
                        value: rmp_serde::to_vec(c.as_str())
                            .into_diagnostic()
                            .wrap_err("cant serialize cursor")?,
                    })
                })
                .transpose()?;

            let in_flight = self.in_flight.acquire(unknown_dids).await;
            let confirmed = if filter.check_signals() && !in_flight.is_empty() {
                let mut retry_batch = db.inner.batch();
                let confirmed = self
                    .checker
                    .check_signals_batch(in_flight, &filter, &mut retry_batch, &HashMap::new())
                    .await?;
                tokio::time::timeout(
                    BLOCKING_TASK_TIMEOUT,
                    tokio::task::spawn_blocking(move || retry_batch.commit().into_diagnostic()),
                )
                .await
                .into_diagnostic()?
                .map_err(|_| miette::miette!("retry state commit timed out"))?
                .inspect_err(|e| error!(err = ?e, "retry state commit failed"))
                .ok();
                confirmed
            } else {
                in_flight
            };

            self.tx
                .send(CrawlerBatch {
                    guards: confirmed,
                    cursor_update,
                })
                .await
                .ok();

            if pass_complete {
                info!("enumeration complete, sleeping 1h before next pass");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(3600)) => {
                        info!("resuming after 1h sleep");
                    }
                    _ = self.enabled.changed() => {
                        if !*self.enabled.borrow() { return Ok(()); }
                    }
                }
            }
        }
    }
}

// -- RelayProducer: cursor display for the stats ticker ------------------

/// formats the current cursor for a relay as a display string, used by the stats ticker.
pub(super) async fn cursor_display(state: &AppState, relay_host: &Url) -> SmolStr {
    let key = crawler_cursor_key(relay_host.as_str());
    let cursor_bytes = match Db::get(state.db.cursors.clone(), &key).await {
        Ok(b) => b,
        Err(e) => return e.to_smolstr(),
    };
    match cursor_bytes
        .as_deref()
        .map(rmp_serde::from_slice::<SmolStr>)
    {
        None | Some(Err(_)) => "none".to_smolstr(),
        Some(Ok(c)) => c,
    }
}

// -- RetryProducer --------------------------------------------------------

/// re-checks signal for repos whose `describeRepo` previously failed or timed out.
pub(crate) struct RetryProducer {
    pub(crate) checker: SignalChecker,
    pub(crate) in_flight: InFlight,
    pub(crate) tx: mpsc::Sender<CrawlerBatch>,
}

impl RetryProducer {
    pub(crate) async fn run(self) {
        loop {
            match self.process_queue().await {
                Ok(Some(dur)) => tokio::time::sleep(dur.max(Duration::from_secs(1))).await,
                Ok(None) => tokio::time::sleep(Duration::from_secs(60)).await,
                Err(e) => {
                    error!(err = %e, "retry loop failed");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        }
    }

    async fn process_queue(&self) -> Result<Option<Duration>> {
        let state = self.checker.state.clone();

        struct ScanResult {
            ready: Vec<Did<'static>>,
            existing: HashMap<Did<'static>, RetryState>,
            next_wake: Option<Duration>,
            had_more: bool,
        }

        let ScanResult {
            ready,
            existing,
            next_wake,
            had_more,
        } = tokio::task::spawn_blocking(move || -> Result<ScanResult> {
            let now = chrono::Utc::now();
            let mut rng: SmallRng = rand::make_rng();
            let mut ready: Vec<Did> = Vec::new();
            let mut existing: HashMap<Did<'static>, RetryState> = HashMap::new();
            let mut next_wake: Option<Duration> = None;
            let mut had_more = false;

            for guard in state.db.crawler.prefix(keys::CRAWLER_RETRY_PREFIX) {
                let (key, val) = guard.into_inner().into_diagnostic()?;
                let state: RetryState = rmp_serde::from_slice(&val).into_diagnostic()?;
                let did = keys::crawler_retry_parse_key(&key)?.to_did();

                if state.attempts >= MAX_RETRY_ATTEMPTS {
                    continue;
                }

                let backoff = TimeDelta::seconds(
                    state
                        .duration
                        .as_seconds_f64()
                        .mul(rng.random_range(0.01..0.07)) as i64,
                );
                if state.after + backoff > now {
                    let wake = (state.after - now).to_std().unwrap_or(Duration::ZERO);
                    next_wake = Some(next_wake.map(|w| w.min(wake)).unwrap_or(wake));
                    continue;
                }

                if ready.len() >= MAX_RETRY_BATCH {
                    had_more = true;
                    break;
                }

                ready.push(did.clone());
                existing.insert(did, state);
            }

            Ok(ScanResult {
                ready,
                existing,
                next_wake,
                had_more,
            })
        })
        .await
        .into_diagnostic()??;

        if ready.is_empty() {
            return Ok(next_wake);
        }

        debug!(count = ready.len(), "retrying pending repos");

        let filter = self.checker.state.filter.load();
        let in_flight = self.in_flight.acquire(ready).await;
        let mut retry_batch = self.checker.state.db.inner.batch();
        let confirmed = self
            .checker
            .check_signals_batch(in_flight, &filter, &mut retry_batch, &existing)
            .await?;

        tokio::task::spawn_blocking(move || retry_batch.commit().into_diagnostic())
            .await
            .into_diagnostic()?
            .inspect_err(|e| error!(err = ?e, "retry state commit failed"))
            .ok();

        if !confirmed.is_empty() {
            info!(count = confirmed.len(), "recovered from retry queue");
            self.tx
                .send(CrawlerBatch {
                    guards: confirmed,
                    cursor_update: None,
                })
                .await
                .ok();
        }

        Ok(had_more.then_some(Duration::ZERO).or(next_wake))
    }
}
