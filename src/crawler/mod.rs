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
use reqwest::StatusCode;
use smol_str::SmolStr;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{Instrument, debug, error, info, trace, warn};
use url::Url;

enum CrawlCheckResult {
    Signal,
    NoSignal,
    Ratelimited,
    Failed(Option<u16>),
}

/// outcome of [`retry_with_backoff`] when the operation does not succeed.
enum RetryOutcome<E> {
    /// ratelimited after exhausting all retries
    Ratelimited,
    /// non-ratelimit failure, carrying the last error
    Failed(E),
}

/// retries an async operation with exponential backoff when ratelimited.
///
/// `op` is called on each attempt and returns `Result<T, E>`.
/// `is_ratelimited` classifies an error as a ratelimit (triggering a retry)
/// versus a fatal failure (returning immediately).
async fn retry_with_backoff<T, E, F, Fut>(
    rng: &mut SmallRng,
    max_retries: u32,
    mut op: F,
    is_ratelimited: impl Fn(&E) -> bool,
) -> Result<T, RetryOutcome<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(val) => return Ok(val),
            Err(e) if is_ratelimited(&e) => {
                if attempt < max_retries {
                    let base = Duration::from_secs(1 << attempt);
                    let jitter = Duration::from_millis(rng.random_range(0..2000));
                    tokio::time::sleep(base + jitter).await;
                    attempt += 1;
                } else {
                    return Err(RetryOutcome::Ratelimited);
                }
            }
            Err(e) => return Err(RetryOutcome::Failed(e)),
        }
    }
}

async fn check_repo_signals(
    http: Arc<reqwest::Client>,
    resolver: crate::resolver::Resolver,
    filter: Arc<crate::filter::FilterConfig>,
    did: Did<'static>,
) -> (Did<'static>, CrawlCheckResult) {
    const MAX_RETRIES: u32 = 5;
    let mut rng: SmallRng = rand::make_rng();

    let pds_url = retry_with_backoff(
        &mut rng,
        MAX_RETRIES,
        || resolver.resolve_identity_info(&did),
        |e| matches!(e, crate::resolver::ResolverError::Ratelimited),
    );
    let pds_url = match pds_url.await {
        Ok((url, _)) => url,
        Err(RetryOutcome::Ratelimited) => {
            error!(
                retries = MAX_RETRIES,
                "rate limited resolving identity, giving up"
            );
            return (did, CrawlCheckResult::Ratelimited);
        }
        Err(RetryOutcome::Failed(e)) => {
            error!(err = %e, "failed to resolve identity");
            return (did, CrawlCheckResult::Failed(None));
        }
    };

    let mut found_signal = false;
    for signal in filter.signals.iter() {
        let res = async {
            let mut list_records_url = pds_url.join("/xrpc/com.atproto.repo.listRecords").unwrap();
            list_records_url
                .query_pairs_mut()
                .append_pair("repo", &did)
                .append_pair("collection", signal)
                .append_pair("limit", "1");

            let res = retry_with_backoff(
                &mut rng,
                MAX_RETRIES,
                || async {
                    http.get(list_records_url.clone())
                        .send()
                        .await?
                        .error_for_status()
                },
                |e: &reqwest::Error| e.status() == Some(StatusCode::TOO_MANY_REQUESTS),
            );
            let res = match res.await {
                Ok(r) => r,
                Err(RetryOutcome::Ratelimited) => {
                    warn!(
                        retries = MAX_RETRIES,
                        "rate limited on listRecords, giving up"
                    );
                    return CrawlCheckResult::Ratelimited;
                }
                Err(RetryOutcome::Failed(e)) => {
                    match e.status() {
                        Some(StatusCode::NOT_FOUND | StatusCode::GONE) => {
                            trace!("repo not found");
                        }
                        Some(s) if s.is_client_error() => {
                            error!(status = %s, "repo unavailable");
                        }
                        _ => {
                            error!(err = %e, "listRecords failed");
                            return CrawlCheckResult::Failed(e.status().map(|s| s.as_u16()));
                        }
                    }
                    return CrawlCheckResult::NoSignal;
                }
            };

            let bytes = match res.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    error!(err = %e, "failed to read listRecords response");
                    return CrawlCheckResult::Failed(None);
                }
            };

            match serde_json::from_slice::<ListRecordsOutput>(&bytes) {
                Ok(out) => {
                    if !out.records.is_empty() {
                        return CrawlCheckResult::Signal;
                    }
                }
                Err(e) => {
                    error!(err = %e, "failed to parse listRecords response");
                    return CrawlCheckResult::Failed(None);
                }
            }

            CrawlCheckResult::NoSignal
        }
        .instrument(tracing::info_span!("signal_check", signal = %signal))
        .await;

        match res {
            CrawlCheckResult::Signal => {
                found_signal = true;
                break;
            }
            CrawlCheckResult::NoSignal => {
                continue;
            }
            other => {
                return (did, other);
            }
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

pub struct Crawler {
    state: Arc<AppState>,
    relay_host: Url,
    http: Arc<reqwest::Client>,
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
        }
    }

    pub async fn run(self) -> Result<()> {
        tokio::spawn({
            use std::time::Instant;
            let count = self.count.clone();
            let mut last_time = Instant::now();
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            async move {
                loop {
                    interval.tick().await;
                    let delta = count.swap(0, Ordering::Relaxed);
                    if delta == 0 {
                        debug!("no repos processed in 60s");
                        continue;
                    }
                    let elapsed = last_time.elapsed().as_secs_f64();
                    let rate = (elapsed > 0.0)
                        .then(|| delta as f64 / elapsed)
                        .unwrap_or(0.0);
                    info!(rate, delta, elapsed, "crawler progress");
                    last_time = Instant::now();
                }
            }
        });

        let mut relay_url = self.relay_host.clone();
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

        let db = &self.state.db;

        // 1. load cursor
        let cursor_key = b"crawler_cursor";
        let mut cursor: Option<SmolStr> = Db::get(db.cursors.clone(), cursor_key.to_vec())
            .await?
            .map(|bytes| {
                let s = String::from_utf8_lossy(&bytes);
                info!(cursor = %s, "resuming");
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
                            pending,
                            max = self.max_pending,
                            "throttling: above max pending"
                        );
                        was_throttled = true;
                    }
                } else if pending > self.resume_pending as u64 {
                    if !was_throttled {
                        debug!(
                            pending,
                            resume = self.resume_pending,
                            "throttling: entering cooldown"
                        );
                        was_throttled = true;
                    }

                    loop {
                        let current_pending = self.state.db.get_count("pending").await;
                        if current_pending <= self.resume_pending as u64 {
                            break;
                        }
                        debug!(
                            pending = current_pending,
                            resume = self.resume_pending,
                            "cooldown, waiting"
                        );
                    }
                    break;
                } else {
                    if was_throttled {
                        info!("throttling released");
                        was_throttled = false;
                    }
                    break;
                }
            }

            // 2. fetch listrepos
            let mut list_repos_url = relay_url
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
                Ok(res) => {
                    match res.status() {
                        StatusCode::TOO_MANY_REQUESTS => {
                            warn!("rate limited by relay");
                            continue;
                        }
                        s if !s.is_success() => {
                            error!(status = %s, "cant crawl");
                            continue;
                        }
                        _ => {}
                    }
                    match res.bytes().await {
                        Ok(b) => b,
                        Err(e) => {
                            error!(err = %e, "cant read listRepos");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!(err = %e, "crawler failed to list repos");
                    continue;
                }
            };
            let output = serde_json::from_slice::<ListReposOutput>(&bytes)
                .into_diagnostic()?
                .into_static();

            if output.repos.is_empty() {
                info!("finished enumeration (or empty page)");
                tokio::time::sleep(Duration::from_secs(3600)).await;
                continue;
            }

            debug!(count = output.repos.len(), "fetched repos");

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
                trace!(did = %did, "found new repo");

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
                info!("reached end of list.");
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
            let span = tracing::info_span!("check_signals", did = %did);
            set.spawn(check_repo_signals(http, resolver, filter, did).instrument(span));
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
                CrawlCheckResult::Ratelimited => {
                    batch.insert(
                        &db.crawler,
                        keys::crawler_failed_key(&did),
                        429u16.to_be_bytes().as_ref(),
                    );
                }
                CrawlCheckResult::Failed(status) => {
                    let code = status.unwrap_or(0);
                    batch.insert(
                        &db.crawler,
                        keys::crawler_failed_key(&did),
                        code.to_be_bytes().as_ref(),
                    );
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
            let key = guard.key().into_diagnostic()?;
            let did_bytes = &key[keys::CRAWLER_FAILED_PREFIX.len()..];
            let trimmed = TrimmedDid::try_from(did_bytes)?;
            failed_dids.push(trimmed.to_did());
        }

        if failed_dids.is_empty() {
            return Ok(());
        }

        info!("retrying {} previously failed repos", failed_dids.len());

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
            info!("recovered {} repos from failed retry", valid_dids.len());
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
