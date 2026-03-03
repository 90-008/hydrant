use crate::db::types::TrimmedDid;
use crate::db::{Db, keys, ser_repo_state};
use crate::state::AppState;
use crate::types::RepoState;
use jacquard_api::com_atproto::repo::list_records::ListRecordsOutput;
use jacquard_api::com_atproto::sync::list_repos::ListReposOutput;
use jacquard_common::{IntoStatic, types::string::Did};
use miette::{IntoDiagnostic, Result};
use rand::Rng;
use rand::RngExt;
use rand::rngs::SmallRng;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::Jitter;
use reqwest_retry::{
    RetryTransientMiddleware, Retryable, RetryableStrategy, default_on_request_failure,
    default_on_request_success, policies::ExponentialBackoff,
};
use smol_str::{SmolStr, ToSmolStr};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};
use url::Url;

enum CrawlCheckResult {
    Signal,
    NoSignal,
    Ratelimited,
    Failed,
}

struct NoTlsRetry;

impl RetryableStrategy for NoTlsRetry {
    fn handle(
        &self,
        res: &Result<reqwest::Response, reqwest_middleware::Error>,
    ) -> Option<Retryable> {
        match res {
            Ok(success) => default_on_request_success(success),
            Err(error) => {
                if let reqwest_middleware::Error::Reqwest(e) = error {
                    if e.is_timeout() {
                        return Some(Retryable::Fatal);
                    }
                    let mut src = e.source();
                    while let Some(s) = src {
                        if let Some(io_err) = s.downcast_ref::<std::io::Error>() {
                            if is_tls_cert_error(io_err) {
                                return Some(Retryable::Fatal);
                            }
                        }
                        src = s.source();
                    }
                }
                let retryable = default_on_request_failure(error);
                if retryable == Some(Retryable::Transient) {
                    if let reqwest_middleware::Error::Reqwest(e) = error {
                        let url = e.url().map(|u| u.as_str()).unwrap_or("unknown url");
                        let status = e
                            .status()
                            .map(|s| s.to_smolstr())
                            .unwrap_or_else(|| "unknown status".into());
                        warn!("retrying request {url}: {status}");
                    }
                }
                retryable
            }
        }
    }
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

pub struct Crawler {
    state: Arc<AppState>,
    relay_host: Url,
    http: Arc<ClientWithMiddleware>,
    max_pending: usize,
    resume_pending: usize,
    count: Arc<AtomicUsize>,
}

impl Crawler {
    pub fn new(
        state: Arc<AppState>,
        relay_host: Url,
        max_pending: usize,
        resume_pending: usize,
    ) -> Self {
        let retry_policy = ExponentialBackoff::builder()
            .jitter(Jitter::Bounded)
            .build_with_max_retries(5);
        let reqwest_client = reqwest::Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .gzip(true)
            .build()
            .expect("that reqwest will build");

        let http = ClientBuilder::new(reqwest_client)
            .with(RetryTransientMiddleware::new_with_policy_and_strategy(
                retry_policy,
                NoTlsRetry,
            ))
            .build();
        let http = Arc::new(http);

        Self {
            state,
            relay_host,
            http,
            max_pending,
            resume_pending,
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn run(self) -> Result<()> {
        info!("crawler started");

        tokio::spawn({
            let count = self.count.clone();
            let mut last_time = std::time::Instant::now();
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            async move {
                loop {
                    interval.tick().await;
                    let delta = count.swap(0, Ordering::Relaxed);
                    if delta == 0 {
                        continue;
                    }
                    let elapsed = last_time.elapsed().as_secs_f64();
                    let rate = if elapsed > 0.0 {
                        delta as f64 / elapsed
                    } else {
                        0.0
                    };
                    info!("crawler: {rate:.2} repos/s ({delta} repos in {elapsed:.1}s)");
                    last_time = std::time::Instant::now();
                }
            }
        });

        let mut api_url = self.relay_host.clone();
        if api_url.scheme() == "wss" {
            api_url
                .set_scheme("https")
                .map_err(|_| miette::miette!("invalid url: {api_url}"))?;
        } else if api_url.scheme() == "ws" {
            api_url
                .set_scheme("http")
                .map_err(|_| miette::miette!("invalid url: {api_url}"))?;
        }

        let mut rng: SmallRng = rand::make_rng();

        let db = &self.state.db;

        // 1. load cursor
        let cursor_key = b"crawler_cursor";
        let mut cursor: Option<SmolStr> = Db::get(db.cursors.clone(), cursor_key.to_vec())
            .await?
            .map(|bytes| {
                let s = String::from_utf8_lossy(&bytes);
                info!("resuming crawler from cursor: {s}");
                s.into()
            });
        let mut was_throttled = false;

        loop {
            // check throttling
            loop {
                let pending = self.state.db.get_count("pending").await;
                if pending > self.max_pending as u64 {
                    if !was_throttled {
                        debug!(
                            "crawler throttling: pending repos {} > max {}, sleeping...",
                            pending, self.max_pending
                        );
                        was_throttled = true;
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                } else if pending > self.resume_pending as u64 {
                    if !was_throttled {
                        debug!(
                            "crawler throttling: pending repos {} > max {}, entering cooldown...",
                            pending, self.max_pending
                        );
                        was_throttled = true;
                    }

                    while self.state.db.get_count("pending").await > self.resume_pending as u64 {
                        debug!(
                            "crawler cooldown: pending repos {} > resume {}, sleeping...",
                            self.state.db.get_count("pending").await,
                            self.resume_pending
                        );
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                    break;
                } else {
                    if was_throttled {
                        info!("crawler resuming: throttling released");
                        was_throttled = false;
                    }
                    break;
                }
            }

            // 2. fetch listrepos
            let mut list_repos_url = api_url
                .join("/xrpc/com.atproto.sync.listRepos")
                .into_diagnostic()?;
            list_repos_url
                .query_pairs_mut()
                .append_pair("limit", "1000");
            if let Some(c) = &cursor {
                list_repos_url
                    .query_pairs_mut()
                    .append_pair("cursor", c.as_str());
            }

            let res_result = self.http.get(list_repos_url.clone()).send().await;
            let bytes = match res_result {
                Ok(res) => match res.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        error!(
                            "crawler failed to parse list repos response: {e}. retrying in 30s..."
                        );
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }
                },
                Err(e) => {
                    error!("crawler failed to list repos: {e}. retrying in 30s...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            };
            let output = serde_json::from_slice::<ListReposOutput>(&bytes)
                .into_diagnostic()?
                .into_static();

            if output.repos.is_empty() {
                info!("crawler finished enumeration (or empty page). sleeping for 1 hour.");
                tokio::time::sleep(Duration::from_secs(3600)).await;
                continue;
            }

            debug!("crawler fetched {} repos...", output.repos.len());

            let mut batch = db.inner.batch();
            let mut to_queue = Vec::new();
            let filter = self.state.filter.load();
            // we can check whether or not to backfill repos faster if we only have to check
            // certain known signals, since we can just listRecords for those signals
            // if we have glob signals we cant do this since we dont know what signals to check
            let check_signals = filter.mode == crate::filter::FilterMode::Filter
                && !filter.signals.is_empty()
                && !filter.has_glob_signals();

            // 3. process repos
            let mut unknown_dids = Vec::new();
            for repo in output.repos {
                let did_key = keys::repo_key(&repo.did);

                let excl_key = crate::db::filter::exclude_key(repo.did.as_str())?;
                if db.filter.contains_key(&excl_key).into_diagnostic()? {
                    continue;
                }

                // check if known
                if !Db::contains_key(db.repos.clone(), &did_key).await? {
                    unknown_dids.push(repo.did.into_static());
                }
            }

            let valid_dids = if check_signals && !unknown_dids.is_empty() {
                self.check_signals_batch(&unknown_dids, &filter, &mut batch)
                    .await?
            } else {
                unknown_dids
            };

            for did in &valid_dids {
                let did_key = keys::repo_key(did);
                trace!("crawler found new repo: {did}");

                let state = RepoState::backfilling_untracked(rng.next_u64());
                batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
                batch.insert(&db.pending, keys::pending_key(state.index_id), &did_key);
                to_queue.push(did.clone());
            }

            // 4. update cursor
            if let Some(new_cursor) = output.cursor {
                cursor = Some(new_cursor.as_str().into());

                batch.insert(
                    &db.cursors,
                    cursor_key.to_vec(),
                    new_cursor.as_bytes().to_vec(),
                );
            } else {
                // end of pagination
                info!("crawler reached end of list.");
                cursor = None;
            }

            tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                .await
                .into_diagnostic()??;

            self.account_new_repos(to_queue.len()).await;

            if cursor.is_none() {
                // 6. retry previously failed repos before sleeping
                self.retry_failed_repos(&mut rng).await?;

                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    }

    async fn check_signals_batch(
        &self,
        dids: &[Did<'static>],
        filter: &crate::filter::FilterConfig,
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
            set.spawn(async move {
                const MAX_RETRIES: u32 = 5;
                let mut rng: SmallRng = rand::make_rng();

                let pds_url = {
                    let mut attempt = 0u32;
                    loop {
                        match resolver.resolve_identity_info(&did).await {
                            Ok((url, _)) => break url,
                            Err(crate::resolver::ResolverError::Ratelimited)
                                if attempt < MAX_RETRIES =>
                            {
                                let base = Duration::from_secs(1 << attempt);
                                let jitter = Duration::from_millis(rng.random_range(0..2000));
                                let try_in = base + jitter;
                                debug!(
                                    "crawler: rate limited resolving {did}, retry {}/{MAX_RETRIES} in {}s",
                                    attempt + 1,
                                    try_in.as_secs_f64()
                                );
                                tokio::time::sleep(try_in).await;
                                attempt += 1;
                            }
                            Err(crate::resolver::ResolverError::Ratelimited) => {
                                error!(
                                    "crawler: rate limited resolving {did} after {MAX_RETRIES} retries"
                                );
                                return (did, CrawlCheckResult::Ratelimited);
                            }
                            Err(e) => {
                                error!("crawler: failed to resolve {did}: {e}");
                                return (did, CrawlCheckResult::Failed);
                            }
                        }
                    }
                };

                let mut found_signal = false;
                for signal in filter.signals.iter() {
                    let mut list_records_url =
                        pds_url.join("/xrpc/com.atproto.repo.listRecords").unwrap();
                    list_records_url
                        .query_pairs_mut()
                        .append_pair("repo", &did)
                        .append_pair("collection", signal)
                        .append_pair("limit", "1");

                    let res = http
                        .get(list_records_url)
                        .send()
                        .await
                        .into_diagnostic()
                        .map(|res| res.error_for_status().into_diagnostic())
                        .flatten();
                    match res {
                        Ok(res) => {
                            let Ok(bytes) = res.bytes().await else {
                                error!(
                                    "failed to read bytes from listRecords response for repo {did}, signal {signal}"
                                );
                                return (did, CrawlCheckResult::Failed);
                            };
                            match serde_json::from_slice::<ListRecordsOutput>(&bytes) {
                                Ok(out) => {
                                    if !out.records.is_empty() {
                                        found_signal = true;
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "failed to parse listRecords response for repo {did}, signal {signal}: {e}"
                                    );
                                    return (did, CrawlCheckResult::Failed);
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "failed to listRecords for repo {did}, signal {signal}: {e}"
                            );
                            return (did, CrawlCheckResult::Failed);
                        }
                    }
                }

                if found_signal {
                    (did, CrawlCheckResult::Signal)
                } else {
                    trace!("crawler skipped repo {did}: no records match signals");
                    (did, CrawlCheckResult::NoSignal)
                }
            });
        }

        while let Some(res) = set.join_next().await {
            let (did, result) = res.into_diagnostic()?;
            match result {
                CrawlCheckResult::Signal => {
                    batch.remove(&db.crawler, keys::crawler_failed_key(&did));
                    valid.push(did);
                }
                CrawlCheckResult::NoSignal => {
                    batch.remove(&db.crawler, keys::crawler_failed_key(&did));
                }
                CrawlCheckResult::Ratelimited | CrawlCheckResult::Failed => {
                    batch.insert(&db.crawler, keys::crawler_failed_key(&did), []);
                }
            }
        }

        Ok(valid)
    }

    async fn retry_failed_repos(&self, rng: &mut SmallRng) -> Result<()> {
        let db = &self.state.db;
        let filter = self.state.filter.load();

        let check_signals = filter.mode == crate::filter::FilterMode::Filter
            && !filter.signals.is_empty()
            && !filter.has_glob_signals();

        if !check_signals {
            return Ok(());
        }

        let mut failed_dids = Vec::new();
        for guard in db.crawler.prefix(keys::CRAWLER_FAILED_PREFIX) {
            let (key, _) = guard.into_inner().into_diagnostic()?;
            let did_bytes = &key[keys::CRAWLER_FAILED_PREFIX.len()..];
            let trimmed = TrimmedDid::try_from(did_bytes)?;
            failed_dids.push(trimmed.to_did());
        }

        if failed_dids.is_empty() {
            return Ok(());
        }

        info!(
            "crawler: retrying {} previously failed repos",
            failed_dids.len()
        );

        let mut batch = db.inner.batch();
        let valid_dids = self
            .check_signals_batch(&failed_dids, &filter, &mut batch)
            .await?;

        for did in &valid_dids {
            let did_key = keys::repo_key(did);

            if Db::contains_key(db.repos.clone(), &did_key).await? {
                continue;
            }

            let state = RepoState::backfilling_untracked(rng.next_u64());
            batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
            batch.insert(&db.pending, keys::pending_key(state.index_id), &did_key);
        }

        tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
            .await
            .into_diagnostic()??;

        if !valid_dids.is_empty() {
            info!(
                "crawler: recovered {} repos from failed retry",
                valid_dids.len()
            );
            self.account_new_repos(valid_dids.len()).await;
        }

        Ok(())
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
