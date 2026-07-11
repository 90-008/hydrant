//! per-mode keyspace groups. this is the single place where mode-specific
//! database state is declared: each feature contributes one group struct,
//! opened and initialized here, and `Db` embeds each group behind one cfg.

use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions};
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;

use crate::config::Config;

#[cfg(any(feature = "indexer_stream", feature = "jetstream", feature = "relay"))]
use miette::Context;
#[cfg(any(feature = "indexer_stream", feature = "jetstream", feature = "relay"))]
use std::sync::atomic::AtomicU64;

#[cfg_attr(
    not(any(
        feature = "indexer",
        feature = "indexer_stream",
        feature = "jetstream",
        feature = "relay"
    )),
    allow(dead_code)
)]
const fn kb(v: u32) -> u32 {
    v * 1024
}
pub(super) const fn mb(v: u64) -> u64 {
    v * 1024 * 1024
}

/// everything a mode group needs to open its keyspaces.
#[cfg_attr(
    not(any(
        feature = "indexer",
        feature = "indexer_stream",
        feature = "jetstream",
        feature = "relay"
    )),
    allow(dead_code)
)]
pub(super) struct OpenCx<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) cfg: &'a Config,
    pub(super) compression: &'a dyn Fn(&str, i32) -> CompressionType,
}

impl OpenCx<'_> {
    pub(super) fn open_ks(&self, name: &str, opts: KeyspaceCreateOptions) -> Result<Keyspace> {
        self.db.keyspace(name, move || opts).into_diagnostic()
    }
}

impl super::Db {
    /// look up any open keyspace by its on-disk name. the mode groups each
    /// contribute their keyspaces, so this stays in sync with the table above.
    pub fn keyspace_by_name(&self, name: &str) -> Option<Keyspace> {
        match name {
            "repos" => return Some(self.repos.clone()),
            "repo_metadata" => return Some(self.repo_metadata.clone()),
            "counts" => return Some(self.counts.clone()),
            "cursors" => return Some(self.cursors.clone()),
            "filter" => return Some(self.filter.clone()),
            "crawler" => return Some(self.crawler.clone()),
            #[cfg(feature = "backlinks")]
            "backlinks" => return Some(self.backlinks.clone()),
            _ => {}
        }
        #[cfg(feature = "indexer")]
        match name {
            "records" => return Some(self.indexer.records.clone()),
            "blocks" => return Some(self.indexer.blocks.clone()),
            "pending" => return Some(self.indexer.pending.clone()),
            "resync" => return Some(self.indexer.resync.clone()),
            "resync_buffer" => return Some(self.indexer.resync_buffer.clone()),
            _ => {}
        }
        #[cfg(feature = "indexer_stream")]
        if name == "events" {
            return Some(self.stream.events.clone());
        }
        #[cfg(feature = "jetstream")]
        if name == "jetstream_events" {
            return Some(self.jetstream.events.clone());
        }
        #[cfg(feature = "relay")]
        if name == "relay_events" {
            return Some(self.relay.events.clone());
        }
        None
    }
}

#[cfg(feature = "indexer")]
pub struct IndexerDb {
    /// maps `{DID}|{COL}|{RKey}` -> record CID
    pub records: Keyspace,
    /// content-addressable storage of raw DAG-CBOR blocks
    pub blocks: Keyspace,
    /// backfill queue of `{ID}` -> empty
    pub pending: Keyspace,
    /// per-repo resync/retry state
    pub resync: Keyspace,
    /// live events buffered during backfill
    pub resync_buffer: Keyspace,
    /// serializes lifecycle count rebuilds
    pub(crate) lifecycle_count_lock: Arc<std::sync::Mutex<()>>,
}

#[cfg(feature = "indexer")]
impl IndexerDb {
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        use fjall::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};

        let opts = KeyspaceCreateOptions::default;
        let pending = cx.open_ks(
            "pending",
            opts()
                // iterated over as a queue, no point reads are used so bloom filters are disabled
                .expect_point_read_hits(true)
                .max_memtable_size(mb(8))
                // its just index of id (int) -> did, and dids arent compressable (especially with the ids being random)
                .data_block_size_policy(BlockSizePolicy::all(kb(8)))
                // and we'll transition from pending to synced anyway, no point trying to compress
                .data_block_compression_policy(CompressionPolicy::disabled())
                // ids are sequential and share prefix so we can use large interval to save space
                .data_block_restart_interval_policy(RestartIntervalPolicy::all(64)),
        )?;
        let resync = cx.open_ks(
            "resync",
            opts()
                // we only point read in backfill when we check for existing resync state
                // ...and also in repos api. so we can disable bloom filters
                .expect_point_read_hits(true)
                .max_memtable_size(mb(8))
                // did -> error state, so its gonna be basically random, cant compress well
                .data_block_size_policy(BlockSizePolicy::all(kb(4)))
                // and we arent going to have many of these anyway, no point trying
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::all(4)),
        )?;
        // this is used in non-ephemeral mode
        let blocks = cx.open_ks(
            "blocks",
            opts()
                // point reads are used a lot by stream, we know the blocks exist though
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cx.cfg.db_blocks_memtable_size_mb))
                // 16 - 128 kb, as the newer blocks will be in the first level (or memtable)
                // and any consumers will probably be streaming the newer events...
                // and blocks are pretty big-ish like around 5kb...
                // replaying will hit later levels so it will be slower but thats honestly
                // an acceptable tradeoff to save more space...
                // todo: we can probably decrease these when we get zstd dict compression?
                .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(64), kb(128)]))
                // lets not compress first level so the reads for new blocks are faster
                // since we will be streaming them to consumers
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    (cx.compression)("blocks", 3),
                    (cx.compression)("blocks", 3),
                    (cx.compression)("blocks", 5),
                ]))
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([8, 16, 32])),
        )?;
        let records = cx.open_ks(
            "records",
            opts()
                // point reads might miss when using getRecord
                // but we assume thats not going to happen often...
                // since this keyspace is big, turning off bloom filters will help a lot with memory/disk space,
                // but leaves point reads vulnerable to disk I/O misses under public query load
                .expect_point_read_hits(!cx.cfg.db_records_bloom_filters)
                .max_memtable_size(mb(cx.cfg.db_records_memtable_size_mb))
                // its just did|col|rkey -> cid, very small (84 bytes for bsky post)
                .data_block_size_policy(BlockSizePolicy::new([kb(8), kb(16)]))
                // cids arent compressable, most rkeys are TIDs so they will get compressed
                // by prefix truncation anyway
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([16, 32])),
        )?;
        let resync_buffer = cx.open_ks(
            "resync_buffer",
            opts()
                // iterated during backfill, no point reads
                .expect_point_read_hits(true)
                .max_memtable_size(mb(16))
                .data_block_size_policy(BlockSizePolicy::all(kb(32)))
                // dont have to compress here since resync buffer will be emptied at some point anyway
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::all(16)),
        )?;

        Ok(Self {
            records,
            blocks,
            pending,
            resync,
            resync_buffer,
            lifecycle_count_lock: Arc::new(std::sync::Mutex::new(())),
        })
    }

    pub(super) fn keyspaces(&self) -> impl Iterator<Item = Keyspace> {
        [
            self.records.clone(),
            self.blocks.clone(),
            self.pending.clone(),
            self.resync.clone(),
            self.resync_buffer.clone(),
        ]
        .into_iter()
    }
}

#[cfg(feature = "indexer_stream")]
pub struct StreamDb {
    /// maps `{ID}` (u64 BE) -> `StoredEvent`, the source for the json stream api
    pub events: Keyspace,
    pub(crate) event_tx: tokio::sync::broadcast::Sender<crate::types::BroadcastEvent>,
    pub next_event_id: Arc<AtomicU64>,
}

#[cfg(feature = "indexer_stream")]
impl StreamDb {
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        use fjall::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};

        let events = cx.open_ks(
            "events",
            KeyspaceCreateOptions::default()
                // only iterators are used here, no point reads
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cx.cfg.db_events_memtable_size_mb))
                // the compression here wont be quite as good since events are quite random
                // eg. by many different repos and different records etc.
                // since its sequential we should still go with bigger block size though
                // backfills will be sequential though...
                .data_block_size_policy(
                    cx.cfg
                        .ephemeral
                        .then(|| BlockSizePolicy::new([kb(64), kb(128), kb(256)]))
                        .unwrap_or_else(|| BlockSizePolicy::new([kb(16), kb(64)])),
                )
                // we are streaming the new events to consumers so we dont want to compress them
                .data_block_compression_policy(
                    cx.cfg
                        .ephemeral
                        .then(|| {
                            CompressionPolicy::new([
                                CompressionType::None,
                                (cx.compression)("events", 3),
                            ])
                        })
                        .unwrap_or_else(|| {
                            CompressionPolicy::new([
                                CompressionType::None,
                                (cx.compression)("events", 3),
                                (cx.compression)("events", 3),
                                (cx.compression)("events", 5),
                            ])
                        }),
                )
                // ids are int, we can prefix truncate a lot
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128])),
        )?;

        let (event_tx, _) = tokio::sync::broadcast::channel(512);

        Ok(Self {
            events,
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

    pub(super) fn keyspaces(&self) -> impl Iterator<Item = Keyspace> {
        [self.events.clone()].into_iter()
    }
}

#[cfg(feature = "jetstream")]
pub(crate) struct JetstreamDb {
    /// maps `{time_us}|{ID}` (16 bytes) -> jetstream event data
    pub(crate) events: Keyspace,
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
        use fjall::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};

        let events = cx.open_ks(
            "jetstream_events",
            KeyspaceCreateOptions::default()
                // time-ordered append-only stream metadata, only iterated for replay.
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cx.cfg.db_events_memtable_size_mb))
                .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(64), kb(128)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    (cx.compression)("jetstream_events", 3),
                    (cx.compression)("jetstream_events", 5),
                ]))
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128])),
        )?;

        let (tx, _) = tokio::sync::broadcast::channel(512);

        Ok(Self {
            events,
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

    pub(super) fn keyspaces(&self) -> impl Iterator<Item = Keyspace> {
        [self.events.clone()].into_iter()
    }
}

#[cfg(feature = "relay")]
pub(crate) struct RelayDb {
    /// maps `{SEQ}` (u64 BE) -> re-encoded relay frame
    pub(crate) events: Keyspace,
    pub(crate) next_seq: Arc<AtomicU64>,
    pub(crate) broadcast_tx: tokio::sync::broadcast::Sender<crate::types::RelayBroadcast>,
}

#[cfg(feature = "relay")]
impl RelayDb {
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        use fjall::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};

        let events = cx.open_ks(
            "relay_events",
            KeyspaceCreateOptions::default()
                // only iterated for cursor replay
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cx.cfg.db_events_memtable_size_mb))
                .data_block_size_policy(BlockSizePolicy::new([kb(64), kb(128), kb(256)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    (cx.compression)("events", 3),
                    (cx.compression)("events", 3),
                    (cx.compression)("events", 5),
                ]))
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128])),
        )?;

        let (broadcast_tx, _) = tokio::sync::broadcast::channel(512);

        Ok(Self {
            events,
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

    pub(super) fn keyspaces(&self) -> impl Iterator<Item = Keyspace> {
        [self.events.clone()].into_iter()
    }
}
