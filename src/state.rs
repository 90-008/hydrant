use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use arc_swap::ArcSwap;
use miette::Result;
use smol_str::SmolStr;
use tokio::sync::{Notify, watch};
use url::Url;

use crate::{
    config::{Config, RateTier},
    db::Db,
    filter::{FilterHandle, new_handle},
    resolver::Resolver,
    util::throttle::Throttler,
};

/// pds hostname -> tier name. updated atomically via ArcSwap.
pub(crate) type PdsTierHandle = Arc<ArcSwap<HashMap<String, SmolStr>>>;

pub struct AppState {
    pub db: Db,
    pub resolver: Resolver,
    pub(crate) filter: FilterHandle,
    pub(crate) pds_tiers: PdsTierHandle,
    pub(crate) rate_tiers: HashMap<String, RateTier>,
    pub firehose_cursors: scc::HashIndex<Url, AtomicI64>,
    pub backfill_notify: Notify,
    pub crawler_enabled: watch::Sender<bool>,
    pub firehose_enabled: watch::Sender<bool>,
    pub backfill_enabled: watch::Sender<bool>,
    pub ephemeral_ttl: Duration,
    pub throttler: Throttler,
}

impl AppState {
    pub fn new(config: &Config) -> Result<Self> {
        let db = Db::open(config)?;
        let resolver = Resolver::new(config.plc_urls.clone(), config.identity_cache_size);
        let filter_config = crate::db::filter::load(&db.filter)?;

        let crawler_default = match config.enable_crawler {
            Some(b) => b,
            // default: enabled if full-network mode, or if crawler sources are configured
            None => {
                filter_config.mode == crate::filter::FilterMode::Full
                    || !config.crawler_sources.is_empty()
            }
        };

        let filter = new_handle(filter_config);

        // load persisted per-PDS tier assignments from the filter keyspace.
        // trusted_hosts from config are merged in as defaults (not persisted here; they seed
        // only if the host has no existing assignment in the DB).
        let mut tier_map: HashMap<String, SmolStr> = crate::db::pds_tiers::load(&db.filter)
            .unwrap_or_default()
            .into_iter()
            .map(|(host, tier)| (host.to_string(), tier))
            .collect();
        for host in &config.trusted_hosts {
            tier_map
                .entry(host.clone())
                .or_insert_with(|| SmolStr::new("trusted"));
        }
        let pds_tiers = Arc::new(ArcSwap::new(Arc::new(tier_map)));

        let relay_cursors = scc::HashIndex::new();

        let (crawler_enabled, _) = watch::channel(crawler_default);
        let (firehose_enabled, _) = watch::channel(config.enable_firehose);
        let (backfill_enabled, _) = watch::channel(true);

        Ok(Self {
            db,
            resolver,
            filter,
            pds_tiers,
            rate_tiers: config.rate_tiers.clone(),
            firehose_cursors: relay_cursors,
            backfill_notify: Notify::new(),
            crawler_enabled,
            firehose_enabled,
            backfill_enabled,
            ephemeral_ttl: config.ephemeral_ttl.clone(),
            throttler: Throttler::new(),
        })
    }

    pub fn notify_backfill(&self) {
        self.backfill_notify.notify_one();
    }

    /// returns the rate tier for the given PDS hostname.
    /// falls back to the "default" tier if no assignment exists or the assigned tier is unknown.
    pub fn pds_tier_for(&self, host: &str) -> RateTier {
        let default = self
            .rate_tiers
            .get("default")
            .copied()
            .unwrap_or_else(RateTier::default_tier);
        let snapshot = self.pds_tiers.load();
        snapshot
            .get(host)
            .and_then(|name| self.rate_tiers.get(name.as_str()).copied())
            .unwrap_or(default)
    }

    /// pauses the crawler, firehose, and backfill worker, runs `f`, then restores their prior state.
    /// the restore always happens, even if `f` returns an error.
    pub async fn with_ingestion_paused<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let crawler_was = *self.crawler_enabled.borrow();
        let firehose_was = *self.firehose_enabled.borrow();
        let backfill_was = *self.backfill_enabled.borrow();
        self.crawler_enabled.send_replace(false);
        self.firehose_enabled.send_replace(false);
        self.backfill_enabled.send_replace(false);
        let result = f().await;
        self.crawler_enabled.send_replace(crawler_was);
        self.firehose_enabled.send_replace(firehose_was);
        self.backfill_enabled.send_replace(backfill_was);
        result
    }
}
