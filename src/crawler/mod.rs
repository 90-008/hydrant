use crate::state::AppState;
use futures::future::join_all;
use jacquard_common::types::string::Did;
use miette::Result;
use scc::HashSet;
use smol_str::ToSmolStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tracing::info;
use url::Url;

mod by_collection;
mod relay;
pub mod throttle;
mod worker;

use throttle::Throttler;

pub(crate) use by_collection::ByCollectionProducer;
pub(crate) use relay::{RelayProducer, RetryProducer, SignalChecker};
pub(crate) use worker::{CrawlerBatch, CrawlerWorker};

// -- InFlight ------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct InFlight(Arc<HashSet<Did<'static>>>);

impl InFlight {
    pub(crate) fn new() -> Self {
        Self(Arc::new(HashSet::new()))
    }

    pub(crate) async fn acquire(&self, dids: Vec<Did<'static>>) -> Vec<InFlightGuard> {
        let mut guards = Vec::with_capacity(dids.len());
        for did in dids {
            if self.0.insert_async(did.clone()).await.is_err() {
                continue;
            }
            guards.push(InFlightGuard {
                set: self.clone(),
                did,
            });
        }
        guards
    }
}

pub(super) struct InFlightGuard {
    set: InFlight,
    pub(super) did: Did<'static>,
}

impl std::ops::Deref for InFlightGuard {
    type Target = Did<'static>;
    fn deref(&self) -> &Did<'static> {
        &self.did
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.set.0.remove_sync(&self.did);
    }
}

#[derive(Clone)]
pub(crate) struct CrawlerStats(Arc<StatsInner>);

struct StatsInner {
    /// repos committed to the db this interval
    count: AtomicUsize,
    /// repos seen from relay/by-collection this interval
    crawled_count: AtomicUsize,
    throttled: AtomicBool,
    pds_throttler: Throttler,
    state: Arc<AppState>,
    relays: Vec<Url>,
}

impl CrawlerStats {
    pub(crate) fn new(state: Arc<AppState>, relays: Vec<Url>, pds_throttler: Throttler) -> Self {
        Self(Arc::new(StatsInner {
            count: AtomicUsize::new(0),
            crawled_count: AtomicUsize::new(0),
            throttled: AtomicBool::new(false),
            pds_throttler,
            state,
            relays,
        }))
    }

    pub(crate) fn record_processed(&self, n: usize) {
        self.0.count.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn record_crawled(&self, n: usize) {
        self.0.crawled_count.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn set_throttled(&self, v: bool) {
        self.0.throttled.store(v, Ordering::Relaxed);
    }

    pub(crate) async fn task(self) {
        use std::time::Instant;
        let mut last_time = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let delta_processed = self.0.count.swap(0, Ordering::Relaxed);
            let delta_crawled = self.0.crawled_count.swap(0, Ordering::Relaxed);
            let is_throttled = self.0.throttled.load(Ordering::Relaxed);

            self.0.pds_throttler.evict_clean().await;

            if delta_processed == 0 && delta_crawled == 0 {
                if is_throttled {
                    info!("throttled: pending queue full");
                } else {
                    info!("idle: no repos crawled or processed in 60s");
                }
                continue;
            }

            let elapsed = last_time.elapsed().as_secs_f64();

            let cursor_futures: Vec<_> = self
                .0
                .relays
                .iter()
                .map(|relay_host| {
                    let domain = relay_host.host_str().unwrap_or("unknown").to_owned();
                    let relay_host = relay_host.clone();
                    let state = self.0.state.clone();
                    async move {
                        let cursor = relay::cursor_display(&state, &relay_host).await;
                        format!("{domain}={cursor}").into()
                    }
                })
                .collect();

            let cursors: Vec<smol_str::SmolStr> =
                join_all(cursor_futures).await.into_iter().collect();
            let cursors_display = if cursors.is_empty() {
                "none".to_smolstr()
            } else {
                cursors.join(", ").into()
            };

            info!(
                cursors = %cursors_display,
                processed = delta_processed,
                crawled = delta_crawled,
                elapsed,
                "progress"
            );
            last_time = Instant::now();
        }
    }
}

/// normalizes a relay URL to http/https for XRPC requests.
pub(super) fn base_url(url: &Url) -> Result<Url> {
    let mut url = url.clone();
    match url.scheme() {
        "wss" => url
            .set_scheme("https")
            .map_err(|_| miette::miette!("invalid url: {url}"))?,
        "ws" => url
            .set_scheme("http")
            .map_err(|_| miette::miette!("invalid url: {url}"))?,
        _ => {}
    }
    Ok(url)
}
