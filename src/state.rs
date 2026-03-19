use std::collections::HashMap;
use std::sync::atomic::AtomicI64;

use miette::Result;
use tokio::sync::{Notify, watch};
use url::Url;

use crate::{
    config::Config,
    db::Db,
    filter::{FilterHandle, new_handle},
    resolver::Resolver,
};

pub struct AppState {
    pub db: Db,
    pub resolver: Resolver,
    pub filter: FilterHandle,
    pub relay_cursors: HashMap<Url, AtomicI64>,
    pub backfill_notify: Notify,
    /// Controls whether the crawler is running. Receivers are held by crawler tasks.
    pub crawler_enabled: watch::Sender<bool>,
    /// Controls whether firehose ingestion is running. Receivers are held by ingestor tasks.
    pub firehose_enabled: watch::Sender<bool>,
}

impl AppState {
    pub fn new(config: &Config) -> Result<Self> {
        let db = Db::open(config)?;
        let resolver = Resolver::new(config.plc_urls.clone(), config.identity_cache_size);
        let filter_config = crate::db::filter::load(&db.filter)?;

        let crawler_default = match config.enable_crawler {
            Some(b) => b,
            // default: enabled in full-network mode, disabled in filter mode
            None => filter_config.mode == crate::filter::FilterMode::Full,
        };

        let filter = new_handle(filter_config);

        let relay_cursors = config
            .relays
            .iter()
            .map(|url| (url.clone(), AtomicI64::new(0)))
            .collect();

        let (crawler_enabled, _) = watch::channel(crawler_default);
        let (firehose_enabled, _) = watch::channel(config.enable_firehose);

        Ok(Self {
            db,
            resolver,
            filter,
            relay_cursors,
            backfill_notify: Notify::new(),
            crawler_enabled,
            firehose_enabled,
        })
    }

    pub fn notify_backfill(&self) {
        self.backfill_notify.notify_one();
    }
}
