use std::sync::atomic::AtomicI64;

use miette::Result;
use tokio::sync::Notify;

use crate::{config::Config, db::Db, resolver::Resolver};

pub struct AppState {
    pub db: Db,
    pub resolver: Resolver,
    pub cur_firehose: AtomicI64,
    pub backfill_notify: Notify,
}

impl AppState {
    pub fn new(config: &Config) -> Result<Self> {
        let db = Db::open(config)?;
        let resolver = Resolver::new(config.plc_urls.clone(), config.identity_cache_size);

        Ok(Self {
            db,
            resolver,
            cur_firehose: AtomicI64::new(0),
            backfill_notify: Notify::new(),
        })
    }

    pub fn notify_backfill(&self) {
        self.backfill_notify.notify_one();
    }
}
