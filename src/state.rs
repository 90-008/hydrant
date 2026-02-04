use std::sync::atomic::AtomicI64;

use jacquard_common::types::string::Did;
use tokio::sync::mpsc;

use miette::Result;

use crate::{config::Config, db::Db, resolver::Resolver};

pub type BackfillTx = mpsc::UnboundedSender<Did<'static>>;
pub type BackfillRx = mpsc::UnboundedReceiver<Did<'static>>;

pub struct AppState {
    pub db: Db,
    pub backfill_tx: BackfillTx,
    pub resolver: Resolver,
    pub cur_firehose: AtomicI64,
    pub blocked_dids: scc::HashSet<Did<'static>>,
}

impl AppState {
    pub fn new(config: &Config) -> Result<(Self, BackfillRx)> {
        let db = Db::open(
            &config.database_path,
            config.cache_size,
            config.disable_lz4_compression,
        )?;
        let resolver = Resolver::new(config.plc_url.clone());
        let (backfill_tx, backfill_rx) = mpsc::unbounded_channel();

        Ok((
            Self {
                db,
                backfill_tx,
                resolver,
                cur_firehose: AtomicI64::new(0),
                blocked_dids: scc::HashSet::new(),
            },
            backfill_rx,
        ))
    }
}
