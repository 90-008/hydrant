use std::collections::HashMap;
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
    config::Config,
    db::Db,
    filter::{FilterHandle, new_handle as new_filter_handle},
    pds_meta::{PdsMeta, PdsMetaHandle, TierPolicy, new_handle as new_pds_handle},
    resolver::Resolver,
    util::throttle::Throttler,
};

pub struct AppState {
    pub db: Db,
    pub resolver: Resolver,
    pub(crate) filter: FilterHandle,
    pub(crate) pds_meta: PdsMetaHandle,
    pub(crate) tier_policy: TierPolicy,
    pub firehose_cursors: scc::HashIndex<Url, AtomicI64>,
    #[cfg(feature = "indexer")]
    pub backfill_notify: Notify,
    #[cfg(feature = "indexer")]
    pub crawler_enabled: watch::Sender<bool>,
    pub firehose_enabled: watch::Sender<bool>,
    #[cfg(feature = "indexer")]
    pub backfill_enabled: watch::Sender<bool>,
    pub ephemeral: bool,
    pub ephemeral_ttl: Duration,
    pub only_index_links: bool,
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

        let tiers: HashMap<String, SmolStr> = crate::db::pds_meta::load_tiers(&db.filter)
            .unwrap_or_default()
            .into_iter()
            .map(|(host, tier)| (host.to_string(), tier))
            .collect();

        let statuses: HashMap<String, crate::pds_meta::HostStatus> =
            crate::db::pds_meta::load_statuses(&db.filter)
                .unwrap_or_default()
                .into_iter()
                .map(|(host, stat)| (host.to_string(), stat))
                .collect();

        let mut hosts = HashMap::new();
        for (host, tier) in tiers {
            hosts
                .entry(host)
                .or_insert_with(crate::pds_meta::HostDesc::default)
                .tier = Some(tier);
        }
        for (host, stat) in statuses {
            hosts
                .entry(host)
                .or_insert_with(crate::pds_meta::HostDesc::default)
                .status = stat;
        }

        let pds_meta = new_pds_handle(PdsMeta { hosts });

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
            tier_policy: config.tier_policy.clone(),
            firehose_cursors: relay_cursors,
            #[cfg(feature = "indexer")]
            backfill_notify: Notify::new(),
            #[cfg(feature = "indexer")]
            crawler_enabled,
            firehose_enabled,
            #[cfg(feature = "indexer")]
            backfill_enabled,
            ephemeral: config.ephemeral,
            ephemeral_ttl: config.ephemeral_ttl.clone(),
            only_index_links: config.only_index_links,
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

    /// applies an account limit status transition for `host`, writing to `batch` and updating
    /// in-memory state. call this after any event that changes the active account count for a PDS.
    pub(crate) fn apply_host_limit_status(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        host: &str,
        count: u64,
    ) {
        use crate::db::pds_meta as db_pds;
        use crate::pds_meta::PdsMeta;
        use tracing::{debug, error};

        let (current_status, limit) = {
            let meta = self.pds_meta.load();
            let override_name = meta.hosts.get(host).and_then(|h| h.tier.as_ref());
            let tier = self.tier_policy.resolve(host, override_name);
            (meta.status(host), tier.account_limit)
        };

        debug!(%host, ?current_status, ?limit, count, "apply_host_limit_status");

        let Some(new_status) = current_status.check_limit_transition(count, limit) else {
            return;
        };

        debug!(%host, count, ?limit, ?new_status, "account count crossed limit, shifting status");

        if let Err(e) = db_pds::set_status(batch, &self.db.filter, host, new_status) {
            error!(%host, err = %e, "failed to write host status");
            return;
        }

        PdsMeta::update_host(&self.pds_meta, host, |h| h.status = new_status);
    }

    /// checks whether `host` is at or over its account limit at the given count.
    /// does not modify any state.
    pub(crate) fn is_over_account_limit(&self, host: &str, count: u64) -> bool {
        let meta = self.pds_meta.load();
        let override_name = meta.hosts.get(host).and_then(|h| h.tier.as_ref());
        self.tier_policy
            .resolve(host, override_name)
            .account_limit
            .is_some_and(|l| count >= l)
    }
}
