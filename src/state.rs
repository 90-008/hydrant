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
    /// Controls whether the backfill worker picks up new tasks. Receiver is held by the backfill worker.
    pub backfill_enabled: watch::Sender<bool>,
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
        let (backfill_enabled, _) = watch::channel(true);

        Ok(Self {
            db,
            resolver,
            filter,
            relay_cursors,
            backfill_notify: Notify::new(),
            crawler_enabled,
            firehose_enabled,
            backfill_enabled,
        })
    }

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
