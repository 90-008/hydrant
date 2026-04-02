use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use miette::Result;
use smol_str::SmolStr;
#[cfg(feature = "indexer")]
use tokio::sync::Notify;
use tokio::sync::watch;
use url::Url;

use crate::{
    config::{Config, RateTier},
    db::Db,
    filter::{FilterHandle, new_handle as new_filter_handle},
    pds_meta::{PdsMeta, PdsMetaHandle, new_handle as new_pds_handle},
    resolver::Resolver,
    util::throttle::Throttler,
};

pub struct AppState {
    pub db: Db,
    pub resolver: Resolver,
    pub(crate) filter: FilterHandle,
    pub(crate) pds_meta: PdsMetaHandle,
    pub(crate) rate_tiers: HashMap<String, RateTier>,
    pub firehose_cursors: scc::HashIndex<Url, AtomicI64>,
    #[cfg(feature = "indexer")]
    pub backfill_notify: Notify,
    #[cfg(feature = "indexer")]
    pub crawler_enabled: watch::Sender<bool>,
    pub firehose_enabled: watch::Sender<bool>,
    #[cfg(feature = "indexer")]
    pub backfill_enabled: watch::Sender<bool>,
    pub ephemeral_ttl: Duration,
    pub throttler: Throttler,
}

impl AppState {
    pub fn new(config: &Config) -> Result<Self> {
        let db = Db::open(config)?;
        let resolver = Resolver::new(config.plc_urls.clone(), config.identity_cache_size);
        let filter_config = crate::db::filter::load(&db.filter)?;

        #[cfg(feature = "indexer")]
        let crawler_default = match config.enable_crawler {
            Some(b) => b,
            // default: enabled if full-network mode, or if crawler sources are configured
            None => {
                filter_config.mode == crate::filter::FilterMode::Full
                    || !config.crawler_sources.is_empty()
            }
        };

        let filter = new_filter_handle(filter_config);

        // load persisted per-PDS tier assignments from the filter keyspace.
        // trusted_hosts from config are merged in as defaults (not persisted here; they seed
        // only if the host has no existing assignment in the DB).
        let mut tiers: HashMap<String, SmolStr> = crate::db::pds_meta::load_tiers(&db.filter)
            .unwrap_or_default()
            .into_iter()
            .map(|(host, tier)| (host.to_string(), tier))
            .collect();
        for host in &config.trusted_hosts {
            tiers
                .entry(host.clone())
                .or_insert_with(|| SmolStr::new("trusted"));
        }

        let banned: HashSet<String> = crate::db::pds_meta::load_banned(&db.filter)
            .unwrap_or_default()
            .into_iter()
            .map(|host| host.to_string())
            .collect();

        let pds_meta = new_pds_handle(PdsMeta { tiers, banned });

        let relay_cursors = scc::HashIndex::new();

        #[cfg(feature = "indexer")]
        let (crawler_enabled, _) = watch::channel(crawler_default);
        let (firehose_enabled, _) = watch::channel(config.enable_firehose);
        #[cfg(feature = "indexer")]
        let (backfill_enabled, _) = watch::channel(true);

        Ok(Self {
            db,
            resolver,
            filter,
            pds_meta,
            rate_tiers: config.rate_tiers.clone(),
            firehose_cursors: relay_cursors,
            #[cfg(feature = "indexer")]
            backfill_notify: Notify::new(),
            #[cfg(feature = "indexer")]
            crawler_enabled,
            firehose_enabled,
            #[cfg(feature = "indexer")]
            backfill_enabled,
            ephemeral_ttl: config.ephemeral_ttl.clone(),
            throttler: Throttler::new(),
        })
    }

    #[cfg(feature = "indexer")]
    pub fn notify_backfill(&self) {
        self.backfill_notify.notify_one();
    }

    /// pauses the crawler, firehose, and backfill worker, runs `f`, then restores their prior state.
    /// the restore always happens, even if `f` returns an error.
    pub async fn with_ingestion_paused<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        #[cfg(feature = "indexer")]
        let crawler_was = *self.crawler_enabled.borrow();
        let firehose_was = *self.firehose_enabled.borrow();
        #[cfg(feature = "indexer")]
        let backfill_was = *self.backfill_enabled.borrow();

        #[cfg(feature = "indexer")]
        self.crawler_enabled.send_replace(false);
        self.firehose_enabled.send_replace(false);
        #[cfg(feature = "indexer")]
        self.backfill_enabled.send_replace(false);

        let result = f().await;

        #[cfg(feature = "indexer")]
        self.crawler_enabled.send_replace(crawler_was);
        self.firehose_enabled.send_replace(firehose_was);
        #[cfg(feature = "indexer")]
        self.backfill_enabled.send_replace(backfill_was);

        result
    }
}
