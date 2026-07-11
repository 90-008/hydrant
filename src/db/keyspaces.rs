//! per-mode keyspace groups. this is the single place where mode-specific
//! database state is declared: each feature contributes one group struct,
//! opened and initialized here, and `Db` embeds each group behind one cfg.
//!
//! keyspace names, tuning, and debug rendering live in `db::schema`; the
//! by-name and enumeration tables are generated in `db::registry`.

use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions};
use miette::{IntoDiagnostic, Result};
use std::cell::RefCell;
use std::sync::Arc;

use crate::config::Config;

#[cfg(any(
    feature = "indexer",
    feature = "indexer_stream",
    feature = "jetstream",
    feature = "relay"
))]
use super::schema::{self, Ks};

#[cfg(any(feature = "indexer_stream", feature = "jetstream", feature = "relay"))]
use miette::Context;
#[cfg(any(feature = "indexer_stream", feature = "jetstream", feature = "relay"))]
use std::sync::atomic::AtomicU64;

/// everything a keyspace needs to open itself.
///
/// nameable so `Schema::options` can reference it in its signature, but all
/// fields and methods are `pub(super)`: it cannot be constructed or used
/// outside `db`.
pub struct OpenCx<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) cfg: &'a Config,
    pub(super) compression: &'a dyn Fn(&str, i32) -> CompressionType,
    /// names opened so far, checked against the registry at the end of open.
    pub(super) opened: RefCell<Vec<&'static str>>,
}

impl OpenCx<'_> {
    pub(super) fn open_ks(&self, name: &str, opts: KeyspaceCreateOptions) -> Result<Keyspace> {
        self.db.keyspace(name, move || opts).into_diagnostic()
    }

    pub(super) fn record_opened(&self, name: &'static str) {
        self.opened.borrow_mut().push(name);
    }
}

#[cfg(feature = "indexer")]
pub struct IndexerDb {
    /// maps `{DID}|{COL}|{RKey}` -> record CID
    pub records: Ks<schema::Records>,
    /// content-addressable storage of raw DAG-CBOR blocks
    pub blocks: Ks<schema::Blocks>,
    /// backfill queue of `{ID}` -> empty
    pub pending: Ks<schema::Pending>,
    /// per-repo resync/retry state
    pub resync: Ks<schema::Resync>,
    /// live events buffered during backfill
    pub resync_buffer: Ks<schema::ResyncBuffer>,
    /// serializes lifecycle count rebuilds
    pub(crate) lifecycle_count_lock: Arc<std::sync::Mutex<()>>,
}

#[cfg(feature = "indexer")]
impl IndexerDb {
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        Ok(Self {
            records: Ks::open(cx)?,
            blocks: Ks::open(cx)?,
            pending: Ks::open(cx)?,
            resync: Ks::open(cx)?,
            resync_buffer: Ks::open(cx)?,
            lifecycle_count_lock: Arc::new(std::sync::Mutex::new(())),
        })
    }
}

#[cfg(feature = "indexer_stream")]
pub struct StreamDb {
    /// maps `{ID}` (u64 BE) -> `StoredEvent`, the source for the json stream api
    pub events: Ks<schema::Events>,
    pub(crate) event_tx: tokio::sync::broadcast::Sender<crate::types::BroadcastEvent>,
    pub next_event_id: Arc<AtomicU64>,
}

#[cfg(feature = "indexer_stream")]
impl StreamDb {
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        let (event_tx, _) = tokio::sync::broadcast::channel(512);

        Ok(Self {
            events: Ks::open(cx)?,
            event_tx,
            next_event_id: Arc::new(AtomicU64::new(0)),
        })
    }

    /// resume event ids after the last stored event.
    pub(super) fn init(&self) -> Result<()> {
        let mut last_id = 0;
        if let Some(guard) = self.events.iter().next_back() {
            let k = guard.key().into_diagnostic()?;
            last_id = u64::from_be_bytes(
                k.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("expected to be id (8 bytes)")?,
            );
        }
        // relaxed is fine since we are just initializing the db
        self.next_event_id
            .store(last_id + 1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(feature = "jetstream")]
pub(crate) struct JetstreamDb {
    /// maps `{time_us}|{ID}` (16 bytes) -> jetstream event data
    pub(crate) events: Ks<schema::JetstreamEvents>,
    pub(crate) tx: tokio::sync::broadcast::Sender<crate::types::JetstreamBroadcast>,
    pub(crate) next_id: Arc<AtomicU64>,
    pub(crate) last_time_us: Arc<std::sync::atomic::AtomicI64>,
    /// serializes jetstream staging with batch commit in relay mode
    #[cfg(feature = "relay")]
    pub(crate) lock: Arc<parking_lot::Mutex<()>>,
}

#[cfg(feature = "jetstream")]
impl JetstreamDb {
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        let (tx, _) = tokio::sync::broadcast::channel(512);

        Ok(Self {
            events: Ks::open(cx)?,
            tx,
            next_id: Arc::new(AtomicU64::new(0)),
            last_time_us: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            #[cfg(feature = "relay")]
            lock: Arc::new(parking_lot::Mutex::new(())),
        })
    }

    /// resume jetstream ids and time watermark after the last stored event.
    pub(super) fn init(&self) -> Result<()> {
        let mut last_id = 0;
        let mut last_time_us = 0;
        if let Some(guard) = self.events.iter().next_back() {
            let k = guard.key().into_diagnostic()?;
            let (time_us, id) = super::keys::parse_jetstream_event_key(&k)?;
            last_id = id;
            last_time_us = time_us;
        }
        self.next_id
            .store(last_id + 1, std::sync::atomic::Ordering::Relaxed);
        self.last_time_us
            .store(last_time_us as i64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(feature = "relay")]
pub(crate) struct RelayDb {
    /// maps `{SEQ}` (u64 BE) -> re-encoded relay frame
    pub(crate) events: Ks<schema::RelayEvents>,
    pub(crate) next_seq: Arc<AtomicU64>,
    pub(crate) broadcast_tx: tokio::sync::broadcast::Sender<crate::types::RelayBroadcast>,
}

#[cfg(feature = "relay")]
impl RelayDb {
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(512);

        Ok(Self {
            events: Ks::open(cx)?,
            next_seq: Arc::new(AtomicU64::new(0)),
            broadcast_tx,
        })
    }

    /// resume relay sequence numbers after the last stored frame.
    pub(super) fn init(&self) -> Result<()> {
        let mut last_relay_seq = 0u64;
        if let Some(guard) = self.events.iter().next_back() {
            let k = guard.key().into_diagnostic()?;
            last_relay_seq = u64::from_be_bytes(
                k.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("relay_events: invalid key length")?,
            );
        }
        self.next_seq
            .store(last_relay_seq + 1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
