use crate::db::{Db, keys, ser_repo_state};
use crate::state::AppState;
use crate::types::RepoState;
use futures::FutureExt;
use jacquard_api::com_atproto::repo::list_records::ListRecordsOutput;
use jacquard_api::com_atproto::sync::list_repos::ListReposOutput;
use jacquard_common::{IntoStatic, types::string::Did};
use miette::{Context, IntoDiagnostic, Result};
use rand::Rng;
use rand::RngExt;
use rand::rngs::SmallRng;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::future::Future;
use std::ops::Mul;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{Instrument, debug, error, info, trace, warn};
use url::Url;

enum CrawlCheckResult {
    Signal,
    NoSignal,
    Retry { retry_after: i64, status: u16 },
}

/// outcome of [`RetryWithBackoff::retry`] when the operation does not succeed.
enum RetryOutcome<E> {
    /// ratelimited after exhausting all retries
    Ratelimited,
    /// non-ratelimit failure, carrying the last error
    Failed(E),
}

/// extension trait that adds `.retry()` to async `FnMut` closures.
///
/// `on_ratelimit` receives the error and current attempt number.
/// returning `Some(duration)` signals a transient failure and provides the backoff;
/// returning `None` signals a terminal failure.
trait RetryWithBackoff<T, E, Fut>: FnMut() -> Fut
where
    Fut: Future<Output = Result<T, E>>,
{
    async fn retry(
        &mut self,
        max_retries: u32,
        on_ratelimit: impl Fn(&E, u32) -> Option<Duration>,
    ) -> Result<T, RetryOutcome<E>> {
        let mut attempt = 0u32;
        loop {
            match self().await {
                Ok(val) => return Ok(val),
                Err(e) => match on_ratelimit(&e, attempt) {
                    Some(_) if attempt >= max_retries => return Err(RetryOutcome::Ratelimited),
                    Some(backoff) => {
                        let jitter = Duration::from_millis(rand::rng().random_range(0..2000));
                        tokio::time::sleep(backoff + jitter).await;
                        attempt += 1;
                    }
                    None => return Err(RetryOutcome::Failed(e)),
                },
            }
        }
    }
}

impl<T, E, F, Fut> RetryWithBackoff<T, E, Fut> for F
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
}

/// extension trait that adds `.error_for_status()` to futures returning a reqwest `Response`.
trait ErrorForStatus: Future<Output = Result<reqwest::Response, reqwest::Error>> {
    fn error_for_status(self) -> impl Future<Output = Result<reqwest::Response, reqwest::Error>>
    where
        Self: Sized,
    {
        futures::FutureExt::map(self, |r| r.and_then(|r| r.error_for_status()))
    }
}

impl<F: Future<Output = Result<reqwest::Response, reqwest::Error>>> ErrorForStatus for F {}

/// extracts a retry delay in seconds from rate limit response headers.
///
/// checks in priority order:
/// - `retry-after: <seconds>` (relative)
/// - `ratelimit-reset: <unix timestamp>` (absolute) (ref pds sends this)
fn parse_retry_after(resp: &reqwest::Response) -> Option<u64> {
    let headers = resp.headers();

    let retry_after = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let rate_limit_reset = headers
        .get("ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .map(|ts| {
            let now = chrono::Utc::now().timestamp();
            (ts - now).max(1) as u64
        });

    retry_after.or(rate_limit_reset)
}

// cloudflare-specific status codes
const CONNECTION_TIMEOUT: StatusCode = unsafe {
    match StatusCode::from_u16(522) {
        Ok(s) => s,
        _ => std::hint::unreachable_unchecked(),
    }
};
const SITE_FROZEN: StatusCode = unsafe {
    match StatusCode::from_u16(530) {
        Ok(s) => s,
        _ => std::hint::unreachable_unchecked(),
    }
};

fn is_throttle_worthy(e: &reqwest::Error) -> bool {
    use std::error::Error;

    if e.is_timeout() {
        return true;
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

    e.status().map_or(false, |s| {
        matches!(
            s,
            StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
                | CONNECTION_TIMEOUT
                | SITE_FROZEN
        )
    })
}

fn is_tls_cert_error(io_err: &std::io::Error) -> bool {
    let Some(inner) = io_err.get_ref() else {
        return false;
    };
    if let Some(rustls_err) = inner.downcast_ref::<rustls::Error>() {
        return matches!(rustls_err, rustls::Error::InvalidCertificate(_));
    }
    if let Some(nested_io) = inner.downcast_ref::<std::io::Error>() {
        return is_tls_cert_error(nested_io);
    }
    false
}

async fn check_repo_signals(
    http: Arc<reqwest::Client>,
    resolver: crate::resolver::Resolver,
    filter: Arc<crate::filter::FilterConfig>,
    did: Did<'static>,
    throttler: Arc<Throttler>,
) -> (Did<'static>, CrawlCheckResult) {
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
            // no pds handle to read retry_after from; use a short default
            let retry_after = chrono::Utc::now().timestamp() + 60;
            return (
                did,
                CrawlCheckResult::Retry {
                    retry_after,
                    status: 429,
                },
            );
        }
        Err(RetryOutcome::Failed(e)) => {
            error!(err = %e, "failed to resolve identity");
            let retry_after = chrono::Utc::now().timestamp() + 60;
            return (
                did,
                CrawlCheckResult::Retry {
                    retry_after,
                    status: 0,
                },
            );
        }
    };

    let throttle = throttler.get_handle(&pds_url).await;
    if throttle.is_throttled() {
        trace!(host = pds_url.host_str(), "skipping throttled pds");
        return (
            did,
            CrawlCheckResult::Retry {
                retry_after: throttle.throttled_until(),
                status: 0,
            },
        );
    }

    let _permit = throttle.acquire().unit_error().or_failure(&throttle, || ());
    let Ok(_permit) = _permit.await else {
        trace!(
            host = pds_url.host_str(),
            "pds failed while waiting for permit"
        );
        return (
            did,
            CrawlCheckResult::Retry {
                retry_after: throttle.throttled_until(),
                status: 0,
            },
        );
    };

    enum RequestError {
        Reqwest(reqwest::Error),
        RateLimited(Option<u64>),
        /// hard failure notification from another task on this PDS
        Throttled,
    }

    let mut found_signal = false;
    for signal in filter.signals.iter() {
        let res = async {
            let mut list_records_url = pds_url.join("/xrpc/com.atproto.repo.listRecords").unwrap();
            list_records_url
                .query_pairs_mut()
                .append_pair("repo", &did)
                .append_pair("collection", signal)
                .append_pair("limit", "1");

            let resp = async {
                let resp = http
                    .get(list_records_url.clone())
                    .send()
                    .await
                    .map_err(RequestError::Reqwest)?;

                // dont retry ratelimits since we will just put it in a queue to be tried again later
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
                    return CrawlCheckResult::Retry {
                        retry_after: throttle.throttled_until(),
                        status: 429,
                    };
                }
                Err(RequestError::Throttled) => {
                    return CrawlCheckResult::Retry {
                        retry_after: throttle.throttled_until(),
                        status: 0,
                    };
                }
                Err(RequestError::Reqwest(e)) => {
                    if is_throttle_worthy(&e) {
                        if let Some(mins) = throttle.record_failure() {
                            warn!(url = %pds_url, mins, "throttling pds due to hard failure");
                        }
                        return CrawlCheckResult::Retry {
                            retry_after: throttle.throttled_until(),
                            status: e.status().map_or(0, |s| s.as_u16()),
                        };
                    }

                    match e.status() {
                        Some(StatusCode::NOT_FOUND | StatusCode::GONE) => {
                            trace!("repo not found");
                            return CrawlCheckResult::NoSignal;
                        }
                        Some(s) if s.is_client_error() => {
                            error!(status = %s, "repo unavailable");
                            return CrawlCheckResult::NoSignal;
                        }
                        _ => {
                            error!(err = %e, "repo errored");
                            return CrawlCheckResult::Retry {
                                retry_after: chrono::Utc::now().timestamp() + 60,
                                status: e.status().map_or(0, |s| s.as_u16()),
                            };
                        }
                    }
                }
            };

            let bytes = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    error!(err = %e, "failed to read listRecords response");
                    return CrawlCheckResult::Retry {
                        retry_after: chrono::Utc::now().timestamp() + 60,
                        status: 0,
                    };
                }
            };

            match serde_json::from_slice::<ListRecordsOutput>(&bytes) {
                Ok(out) if !out.records.is_empty() => return CrawlCheckResult::Signal,
                Ok(_) => {}
                Err(e) => {
                    error!(err = %e, "failed to parse listRecords response");
                    return CrawlCheckResult::Retry {
                        retry_after: chrono::Utc::now().timestamp() + 60,
                        status: 0,
                    };
                }
            }

            CrawlCheckResult::NoSignal
        }
        .instrument(tracing::info_span!("check", signal = %signal))
        .await;

        match res {
            CrawlCheckResult::Signal => {
                found_signal = true;
                break;
            }
            CrawlCheckResult::NoSignal => continue,
            other => return (did, other),
        }
    }

    if !found_signal {
        trace!("no signal-matching records found");
    }

    (
        did,
        found_signal
            .then_some(CrawlCheckResult::Signal)
            .unwrap_or(CrawlCheckResult::NoSignal),
    )
}

#[derive(Debug, Serialize, Deserialize)]
enum Cursor {
    Done,
    Next(Option<SmolStr>),
}

pub mod throttle;
use throttle::{OrFailure, Throttler};

pub struct Crawler {
    state: Arc<AppState>,
    relay_host: Url,
    http: Arc<reqwest::Client>,
    max_pending: usize,
    resume_pending: usize,
    count: Arc<AtomicUsize>,
    crawled_count: Arc<AtomicUsize>,
    throttled: Arc<AtomicBool>,
    pds_throttler: Arc<Throttler>,
}

impl Crawler {
    pub fn new(
        state: Arc<AppState>,
        relay_host: Url,
        max_pending: usize,
        resume_pending: usize,
    ) -> Self {
        let http = Arc::new(
            reqwest::Client::builder()
                .user_agent(concat!(
                    env!("CARGO_PKG_NAME"),
                    "/",
                    env!("CARGO_PKG_VERSION")
                ))
                .gzip(true)
                .build()
                .expect("that reqwest will build"),
        );

        Self {
            state,
            relay_host,
            http,
            max_pending,
            resume_pending,
            count: Arc::new(AtomicUsize::new(0)),
            crawled_count: Arc::new(AtomicUsize::new(0)),
            throttled: Arc::new(AtomicBool::new(false)),
            pds_throttler: Arc::new(Throttler::new()),
        }
    }

    pub async fn run(self) -> Result<()> {
        let crawler = Arc::new(self);

        // stats ticker
        tokio::spawn({
            use std::time::Instant;
            let count = crawler.count.clone();
            let crawled_count = crawler.crawled_count.clone();
            let throttled = crawler.throttled.clone();
            let pds_throttler = crawler.pds_throttler.clone();
            let mut last_time = Instant::now();
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            async move {
                loop {
                    interval.tick().await;
                    let delta_processed = count.swap(0, Ordering::Relaxed);
                    let delta_crawled = crawled_count.swap(0, Ordering::Relaxed);
                    let is_throttled = throttled.load(Ordering::Relaxed);

                    pds_throttler.evict_clean().await;

                    if delta_processed == 0 && delta_crawled == 0 {
                        if is_throttled {
                            info!("throttled: pending queue full");
                        } else {
                            debug!("no repos crawled or processed in 60s");
                        }
                        continue;
                    }

                    let elapsed = last_time.elapsed().as_secs_f64();
                    info!(
                        processed = delta_processed,
                        crawled = delta_crawled,
                        elapsed,
                        "progress"
                    );
                    last_time = Instant::now();
                }
            }
        });

        // retry thread
        std::thread::spawn({
            let crawler = crawler.clone();
            let handle = tokio::runtime::Handle::current();
            move || {
                use std::thread::sleep;

                let _g = handle.enter();

                loop {
                    match crawler.process_retry_queue() {
                        Ok(Some(next_ts)) => {
                            let secs = (next_ts - chrono::Utc::now().timestamp()).max(1) as u64;
                            sleep(Duration::from_secs(secs));
                        }
                        Ok(None) => {
                            sleep(Duration::from_secs(60));
                        }
                        Err(e) => {
                            error!(err = %e, "retry loop failed");
                            sleep(Duration::from_secs(60));
                        }
                    }
                }
            }
        });

        loop {
            if let Err(e) = Self::crawl(crawler.clone()).await {
                error!(err = ?e, "fatal error, restarting in 30s");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }

    async fn crawl(crawler: Arc<Self>) -> Result<()> {
        let mut relay_url = crawler.relay_host.clone();
        match relay_url.scheme() {
            "wss" => relay_url
                .set_scheme("https")
                .map_err(|_| miette::miette!("invalid url: {relay_url}"))?,
            "ws" => relay_url
                .set_scheme("http")
                .map_err(|_| miette::miette!("invalid url: {relay_url}"))?,
            _ => {}
        }

        let mut rng: SmallRng = rand::make_rng();
        let db = &crawler.state.db;

        let cursor_key = b"crawler_cursor";
        let cursor_bytes = Db::get(db.cursors.clone(), cursor_key).await?;
        let mut cursor: Cursor = cursor_bytes
            .as_deref()
            .map(rmp_serde::from_slice)
            .transpose()
            .into_diagnostic()
            .wrap_err("can't parse cursor")?
            .unwrap_or(Cursor::Next(None));
        let mut was_throttled = false;

        loop {
            // throttle check
            loop {
                let pending = crawler.state.db.get_count("pending").await;
                if pending > crawler.max_pending as u64 {
                    if !was_throttled {
                        debug!(
                            pending,
                            max = crawler.max_pending,
                            "throttling: above max pending"
                        );
                        was_throttled = true;
                        crawler.throttled.store(true, Ordering::Relaxed);
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else if pending > crawler.resume_pending as u64 {
                    if !was_throttled {
                        debug!(
                            pending,
                            resume = crawler.resume_pending,
                            "throttling: entering cooldown"
                        );
                        was_throttled = true;
                        crawler.throttled.store(true, Ordering::Relaxed);
                    }

                    loop {
                        let current_pending = crawler.state.db.get_count("pending").await;
                        if current_pending <= crawler.resume_pending as u64 {
                            break;
                        }
                        debug!(
                            pending = current_pending,
                            resume = crawler.resume_pending,
                            "cooldown, waiting"
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    break;
                } else {
                    if was_throttled {
                        info!("throttling released");
                        was_throttled = false;
                        crawler.throttled.store(false, Ordering::Relaxed);
                    }
                    break;
                }
            }

            let mut list_repos_url = relay_url
                .join("/xrpc/com.atproto.sync.listRepos")
                .into_diagnostic()?;
            list_repos_url
                .query_pairs_mut()
                .append_pair("limit", "1000");
            if let Cursor::Next(Some(c)) = &cursor {
                list_repos_url
                    .query_pairs_mut()
                    .append_pair("cursor", c.as_str());
            }

            let fetch_result = (|| {
                crawler
                    .http
                    .get(list_repos_url.clone())
                    .send()
                    .error_for_status()
            })
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

            let output = match serde_json::from_slice::<ListReposOutput>(&bytes) {
                Ok(out) => out.into_static(),
                Err(e) => {
                    error!(err = %e, "failed to parse listRepos response");
                    continue;
                }
            };

            if output.repos.is_empty() {
                info!("finished enumeration (or empty page)");
                tokio::time::sleep(Duration::from_secs(3600)).await;
                continue;
            }

            debug!(count = output.repos.len(), "fetched repos");
            crawler
                .crawled_count
                .fetch_add(output.repos.len(), Ordering::Relaxed);

            let mut batch = db.inner.batch();
            let mut to_queue = Vec::new();
            let filter = crawler.state.filter.load();

            let mut unknown_dids = Vec::new();
            for repo in output.repos {
                let did_key = keys::repo_key(&repo.did);

                let excl_key = crate::db::filter::exclude_key(repo.did.as_str())?;
                if db.filter.contains_key(&excl_key).into_diagnostic()? {
                    continue;
                }

                if !Db::contains_key(db.repos.clone(), &did_key).await? {
                    unknown_dids.push(repo.did.into_static());
                }
            }

            let valid_dids = if filter.check_signals() && !unknown_dids.is_empty() {
                crawler
                    .check_signals_batch(&unknown_dids, &filter, &mut batch)
                    .await?
            } else {
                unknown_dids
            };

            for did in &valid_dids {
                let did_key = keys::repo_key(did);
                trace!(did = %did, "found new repo");

                let state = RepoState::untracked(rng.next_u64());
                batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
                batch.insert(&db.pending, keys::pending_key(state.index_id), &did_key);
                to_queue.push(did.clone());
            }

            if let Some(new_cursor) = output.cursor {
                cursor = Cursor::Next(Some(new_cursor.as_str().into()));
            } else {
                info!("reached end of list.");
                cursor = Cursor::Done;
            }
            batch.insert(
                &db.cursors,
                cursor_key,
                rmp_serde::to_vec(&cursor)
                    .into_diagnostic()
                    .wrap_err("cant serialize cursor")?,
            );

            tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                .await
                .into_diagnostic()??;

            crawler.account_new_repos(to_queue.len()).await;

            if matches!(cursor, Cursor::Done) {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    }

    /// scan the retry queue for entries whose `retry_after` timestamp has passed,
    /// retry them, and return the earliest still-pending timestamp (if any) so the
    /// caller knows when to wake up next.
    fn process_retry_queue(&self) -> Result<Option<i64>> {
        let db = &self.state.db;
        let now = chrono::Utc::now().timestamp();

        let mut ready: Vec<Did> = Vec::new();
        let mut next_retry: Option<i64> = None;

        let mut rng: SmallRng = rand::make_rng();

        let mut batch = db.inner.batch();
        for guard in db.crawler.prefix(keys::CRAWLER_RETRY_PREFIX) {
            let (key, val) = guard.into_inner().into_diagnostic()?;
            let (retry_after, _) = keys::crawler_retry_parse_value(&val)?;
            let did = keys::crawler_retry_parse_key(&key)?.to_did();

            // we check an extra backoff of 1 - 7% just to make it less likely for
            // many requests to coincide with each other
            let backoff =
                ((retry_after - now).max(0) as f64).mul(rng.random_range(0.01..0.07)) as i64;
            if retry_after + backoff > now {
                next_retry = Some(
                    next_retry
                        .map(|earliest| earliest.min(retry_after))
                        .unwrap_or(retry_after),
                );
                continue;
            }

            ready.push(did);
        }

        if ready.is_empty() {
            return Ok(next_retry);
        }

        info!(count = ready.len(), "retrying pending repos");

        let handle = tokio::runtime::Handle::current();
        let filter = self.state.filter.load();
        let valid_dids = handle.block_on(self.check_signals_batch(&ready, &filter, &mut batch))?;

        let mut rng: SmallRng = rand::make_rng();
        for did in &valid_dids {
            let did_key = keys::repo_key(did);

            if db.repos.contains_key(&did_key).into_diagnostic()? {
                continue;
            }

            let state = RepoState::untracked(rng.next_u64());
            batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
            batch.insert(&db.pending, keys::pending_key(state.index_id), &did_key);
        }

        batch.commit().into_diagnostic()?;

        if !valid_dids.is_empty() {
            info!(count = valid_dids.len(), "recovered from retry queue");
            handle.block_on(self.account_new_repos(valid_dids.len()));
        }

        Ok(next_retry)
    }

    async fn check_signals_batch(
        &self,
        dids: &[Did<'static>],
        filter: &Arc<crate::filter::FilterConfig>,
        batch: &mut fjall::OwnedWriteBatch,
    ) -> Result<Vec<Did<'static>>> {
        let db = &self.state.db;
        let mut valid = Vec::new();
        let mut set = tokio::task::JoinSet::new();

        for did in dids {
            let did = did.clone();
            let http = self.http.clone();
            let resolver = self.state.resolver.clone();
            let filter = filter.clone();
            let throttler = self.pds_throttler.clone();
            let span = tracing::info_span!("signals", did = %did);
            set.spawn(check_repo_signals(http, resolver, filter, did, throttler).instrument(span));
        }

        while let Some(res) = set.join_next().await {
            let (did, result) = res.into_diagnostic()?;
            match result {
                CrawlCheckResult::Signal => {
                    valid.push(did);
                }
                CrawlCheckResult::NoSignal => {}
                CrawlCheckResult::Retry {
                    retry_after,
                    status,
                } => {
                    batch.insert(
                        &db.crawler,
                        keys::crawler_retry_key(&did),
                        keys::crawler_retry_value(retry_after, status),
                    );
                }
            }
        }

        Ok(valid)
    }

    async fn account_new_repos(&self, count: usize) {
        if count == 0 {
            return;
        }

        self.count.fetch_add(count, Ordering::Relaxed);
        self.state
            .db
            .update_count_async("repos", count as i64)
            .await;
        self.state
            .db
            .update_count_async("pending", count as i64)
            .await;
        self.state.notify_backfill();
    }
}
