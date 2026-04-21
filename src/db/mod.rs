use crate::config::Compression;
use crate::db::compaction::CountsGcFilterFactory;
use crate::types::{RepoMetadata, RepoState};

#[cfg(feature = "indexer_stream")]
use crate::types::BroadcastEvent;
#[cfg(feature = "relay")]
use crate::types::RelayBroadcast;

use fjall::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Slice,
};
use lsm_tree::compaction::Factory;
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::SmolStr;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use url::Url;

pub mod compaction;
pub mod ephemeral;
pub mod filter;
pub mod keys;
pub mod migration;
pub mod pds_meta;
pub mod types;

use tracing::error;

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
use tokio::sync::broadcast;

fn default_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
}

pub struct Db {
    pub inner: Arc<Database>,
    pub path: std::path::PathBuf,
    pub repos: Keyspace,
    pub repo_metadata: Keyspace,
    pub cursors: Keyspace,
    pub counts: Keyspace,
    pub filter: Keyspace,
    pub crawler: Keyspace,
    #[cfg(feature = "indexer")]
    pub records: Keyspace,
    #[cfg(feature = "indexer")]
    pub blocks: Keyspace,
    #[cfg(feature = "indexer")]
    pub pending: Keyspace,
    #[cfg(feature = "indexer")]
    pub resync: Keyspace,
    #[cfg(feature = "indexer")]
    pub resync_buffer: Keyspace,
    #[cfg(feature = "indexer_stream")]
    pub events: Keyspace,
    #[cfg(feature = "backlinks")]
    pub backlinks: Keyspace,
    #[cfg(feature = "indexer_stream")]
    pub(crate) event_tx: broadcast::Sender<BroadcastEvent>,
    #[cfg(feature = "indexer_stream")]
    pub next_event_id: Arc<AtomicU64>,
    #[cfg(feature = "relay")]
    pub(crate) relay_events: Keyspace,
    #[cfg(feature = "relay")]
    pub(crate) next_relay_seq: Arc<AtomicU64>,
    #[cfg(feature = "relay")]
    pub(crate) relay_broadcast_tx: broadcast::Sender<RelayBroadcast>,
    pub counts_map: HashMap<SmolStr, u64>,
    next_count_delta_id: Arc<AtomicU64>,
    count_delta_checkpoint_watermark: Arc<AtomicU64>,
    count_delta_gc_watermark: Arc<AtomicU64>,
    count_delta_in_flight: Arc<Mutex<BTreeSet<u64>>>,
}

#[derive(Debug, Clone, Default)]
pub struct CountDeltas {
    deltas: BTreeMap<SmolStr, i64>,
}

impl CountDeltas {
    pub(crate) fn add(&mut self, key: &str, delta: i64) {
        if delta == 0 {
            return;
        }

        let entry = self.deltas.entry(SmolStr::new(key)).or_insert(0);
        *entry += delta;
        if *entry == 0 {
            self.deltas.remove(key);
        }
    }

    #[cfg(feature = "indexer")]
    pub(crate) fn add_gauge_diff(
        &mut self,
        old: &crate::types::GaugeState,
        new: &crate::types::GaugeState,
    ) {
        use crate::types::GaugeState;

        if old == new {
            return;
        }

        match (old, new) {
            (GaugeState::Pending, GaugeState::Pending) => {}
            (GaugeState::Pending, _) => self.add("pending", -1),
            (_, GaugeState::Pending) => self.add("pending", 1),
            _ => {}
        }

        match (old.is_resync(), new.is_resync()) {
            (true, false) => self.add("resync", -1),
            (false, true) => self.add("resync", 1),
            _ => {}
        }

        if let GaugeState::Resync(Some(kind)) = old {
            self.add(
                match kind {
                    crate::types::ResyncErrorKind::Ratelimited => "error_ratelimited",
                    crate::types::ResyncErrorKind::Transport => "error_transport",
                    crate::types::ResyncErrorKind::Generic => "error_generic",
                },
                -1,
            );
        }

        if let GaugeState::Resync(Some(kind)) = new {
            self.add(
                match kind {
                    crate::types::ResyncErrorKind::Ratelimited => "error_ratelimited",
                    crate::types::ResyncErrorKind::Transport => "error_transport",
                    crate::types::ResyncErrorKind::Generic => "error_generic",
                },
                1,
            );
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.deltas.len()
    }

    pub(crate) fn get(&self, key: &str) -> i64 {
        self.deltas.get(key).copied().unwrap_or(0)
    }

    pub(crate) fn projected_count(&self, db: &Db, key: &str) -> u64 {
        apply_count_delta(db.get_count_sync(key), self.get(key))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SmolStr, &i64)> {
        self.deltas.iter()
    }
}

pub(crate) struct CountDeltaReservation {
    in_flight: Arc<Mutex<BTreeSet<u64>>>,
    start_id: u64,
}

impl Drop for CountDeltaReservation {
    fn drop(&mut self) {
        let Ok(mut in_flight) = self.in_flight.lock() else {
            error!(
                start_id = self.start_id,
                "count delta reservations poisoned"
            );
            return;
        };
        in_flight.remove(&self.start_id);
    }
}

const fn kb(v: u32) -> u32 {
    v * 1024
}
const fn mb(v: u64) -> u64 {
    v * 1024 * 1024
}

fn apply_count_delta(current: u64, delta: i64) -> u64 {
    if delta >= 0 {
        current.saturating_add(delta as u64)
    } else {
        current.saturating_sub(delta.unsigned_abs())
    }
}

impl Db {
    pub fn open(cfg: &crate::config::Config) -> Result<Self> {
        let count_delta_gc_watermark = Arc::new(AtomicU64::new(0));
        let db = Database::builder(&cfg.database_path)
            .cache_size(cfg.cache_size * 2_u64.pow(20) / 2)
            .manual_journal_persist(true)
            .journal_compression(match cfg.journal_compression {
                Compression::Lz4 => CompressionType::Lz4,
                Compression::Zstd => CompressionType::Zstd { level: 3 },
                Compression::None => CompressionType::None,
            })
            .worker_threads(cfg.db_worker_threads)
            .max_journaling_size(mb(cfg.db_max_journaling_size_mb))
            .with_compaction_filter_factories({
                let ephemeral = cfg.ephemeral;
                let count_delta_gc_watermark = count_delta_gc_watermark.clone();
                let f = move |ks: &str| match ks {
                    "counts" => Some(Arc::new(CountsGcFilterFactory {
                        drop_collection_counts: ephemeral,
                        delta_gc_watermark: count_delta_gc_watermark.clone(),
                    }) as Arc<dyn Factory>),
                    _ => None,
                };
                Arc::new(f)
            })
            .open()
            .into_diagnostic()?;
        let db = Arc::new(db);

        let opts = default_opts;
        let open_ks = |name: &str, opts: KeyspaceCreateOptions| {
            db.keyspace(name, move || opts).into_diagnostic()
        };

        let load_dict = |name: &str| -> Option<Arc<[u8]>> {
            let path = cfg.database_path.join(format!("dict_{name}.bin"));
            if path.exists() {
                if let Ok(bytes) = std::fs::read(&path) {
                    tracing::debug!(
                        "loaded zstd dictionary for keyspace {name} ({} bytes)",
                        bytes.len()
                    );
                    return Some(bytes.into());
                }
            }
            None
        };
        let dicts = ["repos", "blocks", "events", "backlinks"].into_iter().fold(
            std::collections::HashMap::new(),
            |mut acc, name| {
                let Some(dict) = load_dict(name) else {
                    return acc;
                };
                acc.insert(name, dict);
                acc
            },
        );
        let get_compression = |name: &str, level: i32| match cfg.data_compression {
            Compression::Lz4 => CompressionType::Lz4,
            Compression::Zstd => dicts
                .get(name)
                .map(|dict| CompressionType::ZstdDict {
                    level,
                    dict: dict.clone(),
                })
                .unwrap_or_else(|| CompressionType::Zstd { level }),
            Compression::None => CompressionType::None,
        };

        let repos = open_ks(
            "repos",
            opts()
                // crawler checks if a repo doesn't exist
                .expect_point_read_hits(false)
                .max_memtable_size(mb(cfg.db_repos_memtable_size_mb))
                // did -> repo state, not gonna be compressable well because dids are random
                // and repo state doesnt have repeats really..
                // these block sizes work fine since we insert into repos constantly anyway
                // whenever we update anything related to a repo, so the repos that arent
                // being updated will be compacted away!
                .data_block_size_policy(BlockSizePolicy::new([kb(4), kb(4), kb(16), kb(64)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    CompressionType::None,
                    get_compression("repos", 3),
                    get_compression("repos", 5),
                ]))
                // did plc are random so the interval wont rlly matter
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([2, 4])),
        )?;
        let repo_metadata = open_ks(
            "repo_metadata",
            opts()
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_repos_memtable_size_mb / 2))
                // its did -> random u64 id + bool, not much to compress, very small
                .data_block_size_policy(BlockSizePolicy::new([kb(2), kb(4), kb(8)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    CompressionType::None,
                    get_compression("repos", 3),
                ]))
                // did plc are random so the interval wont rlly matter
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([2, 4])),
        )?;
        #[cfg(feature = "indexer")]
        let pending = open_ks(
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
        #[cfg(feature = "indexer")]
        let resync = open_ks(
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
        #[cfg(feature = "indexer")]
        let blocks = open_ks(
            "blocks",
            opts()
                // point reads are used a lot by stream, we know the blocks exist though
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_blocks_memtable_size_mb))
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
                    get_compression("blocks", 3),
                    get_compression("blocks", 3),
                    get_compression("blocks", 5),
                ]))
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([8, 16, 32])),
        )?;
        #[cfg(feature = "indexer")]
        let records = open_ks(
            "records",
            opts()
                // point reads might miss when using getRecord
                // but we assume thats not going to happen often... (todo: should be a config option maybe?)
                // and since this keyspace is big, turning off bloom filters will help a lot
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_records_memtable_size_mb))
                // its just did|col|rkey -> cid, very small (84 bytes for bsky post)
                .data_block_size_policy(BlockSizePolicy::new([kb(8), kb(16)]))
                // cids arent compressable, most rkeys are TIDs so they will get compressed
                // by prefix truncation anyway
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([16, 32])),
        )?;
        let cursors = open_ks(
            "cursors",
            opts()
                // cursor point reads hit almost 100% of the time
                .expect_point_read_hits(true)
                .max_memtable_size(mb(4))
                // its just cursors...
                .data_block_size_policy(BlockSizePolicy::all(kb(1)))
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::all(1)),
        )?;
        #[cfg(feature = "indexer")]
        let resync_buffer = open_ks(
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
        #[cfg(feature = "indexer_stream")]
        let events = open_ks(
            "events",
            opts()
                // only iterators are used here, no point reads
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_events_memtable_size_mb))
                // the compression here wont be quite as good since events are quite random
                // eg. by many different repos and different records etc.
                // since its sequential we should still go with bigger block size though
                // backfills will be sequential though...
                .data_block_size_policy(
                    cfg.ephemeral
                        .then(|| BlockSizePolicy::new([kb(64), kb(128), kb(256)]))
                        .unwrap_or_else(|| BlockSizePolicy::new([kb(16), kb(64)])),
                )
                // we are streaming the new events to consumers so we dont want to compress them
                .data_block_compression_policy(
                    cfg.ephemeral
                        .then(|| {
                            CompressionPolicy::new([
                                CompressionType::None,
                                get_compression("events", 3),
                            ])
                        })
                        .unwrap_or_else(|| {
                            CompressionPolicy::new([
                                CompressionType::None,
                                get_compression("events", 3),
                                get_compression("events", 3),
                                get_compression("events", 5),
                            ])
                        }),
                )
                // ids are int, we can prefix truncate a lot
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128])),
        )?;
        let counts = open_ks(
            "counts",
            opts()
                // count increments hit because counters are mostly pre-initialized
                .expect_point_read_hits(true)
                .max_memtable_size(mb(16))
                // the data is very small
                // this is at worst did|col -> u64, so its tiny (40 bytes)
                .data_block_size_policy(BlockSizePolicy::all(kb(2)))
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::all(7)),
        )?;

        // filter handles high-volume point reads (repo excludes) so it needs the bloom filter
        let filter = open_ks(
            "filter",
            opts()
                .max_memtable_size(mb(16))
                // dids arent compressable so this is fine, and we have nothing as the value
                .data_block_size_policy(BlockSizePolicy::all(kb(1)))
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::all(2)),
        )?;

        let crawler = open_ks(
            "crawler",
            opts()
                // only iterators are used here
                .expect_point_read_hits(true)
                .max_memtable_size(mb(8))
                // did -> failed state, not very compressable
                .data_block_size_policy(BlockSizePolicy::all(kb(2)))
                .data_block_compression_policy(CompressionPolicy::disabled())
                .data_block_restart_interval_policy(RestartIntervalPolicy::all(2)),
        )?;

        #[cfg(feature = "relay")]
        let relay_events = open_ks(
            "relay_events",
            opts()
                // only iterated for cursor replay
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_events_memtable_size_mb))
                .data_block_size_policy(BlockSizePolicy::new([kb(64), kb(128), kb(256)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    get_compression("events", 3),
                    get_compression("events", 3),
                    get_compression("events", 5),
                ]))
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128])),
        )?;

        #[cfg(feature = "backlinks")]
        let backlinks = open_ks(
            "backlinks",
            opts()
                // lets assume we hit backlinks, getBacklinks will use iterator anyway
                // so we can disable bloom filter okay
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_records_memtable_size_mb))
                // same as records basically
                .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(32)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    get_compression("backlinks", 3),
                ]))
                .data_block_restart_interval_policy(RestartIntervalPolicy::new([16, 32])),
        )?;

        // when adding new keyspaces, make sure to add them to the /stats endpoint
        // and also update any relevant /debug/* endpoints

        #[cfg(feature = "indexer_stream")]
        let (event_tx, _) = broadcast::channel(10000);

        #[cfg(feature = "relay")]
        let (relay_broadcast_tx, _) = broadcast::channel(10000);

        let this = Self {
            inner: db,
            path: cfg.database_path.clone(),
            repos,
            repo_metadata,
            #[cfg(feature = "indexer")]
            records,
            #[cfg(feature = "indexer")]
            blocks,
            cursors,
            #[cfg(feature = "indexer")]
            pending,
            #[cfg(feature = "indexer")]
            resync,
            #[cfg(feature = "indexer")]
            resync_buffer,
            #[cfg(feature = "indexer_stream")]
            events,
            counts,
            filter,
            crawler,
            #[cfg(feature = "backlinks")]
            backlinks,
            #[cfg(feature = "indexer_stream")]
            event_tx,
            counts_map: HashMap::new(),
            #[cfg(feature = "indexer_stream")]
            next_event_id: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "relay")]
            relay_events,
            #[cfg(feature = "relay")]
            next_relay_seq: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "relay")]
            relay_broadcast_tx,
            next_count_delta_id: Arc::new(AtomicU64::new(0)),
            count_delta_checkpoint_watermark: Arc::new(AtomicU64::new(0)),
            count_delta_gc_watermark,
            count_delta_in_flight: Arc::new(Mutex::new(BTreeSet::new())),
        };

        migration::run(&this)?;

        #[cfg(feature = "relay")]
        {
            let mut last_relay_seq = 0u64;
            if let Some(guard) = this.relay_events.iter().next_back() {
                let k = guard.key().into_diagnostic()?;
                last_relay_seq = u64::from_be_bytes(
                    k.as_ref()
                        .try_into()
                        .into_diagnostic()
                        .wrap_err("relay_events: invalid key length")?,
                );
            }
            this.next_relay_seq
                .store(last_relay_seq + 1, std::sync::atomic::Ordering::Relaxed);
        }

        #[cfg(feature = "indexer_stream")]
        {
            let mut last_id = 0;
            if let Some(guard) = this.events.iter().next_back() {
                let k = guard.key().into_diagnostic()?;
                last_id = u64::from_be_bytes(
                    k.as_ref()
                        .try_into()
                        .into_diagnostic()
                        .wrap_err("expected to be id (8 bytes)")?,
                );
            }
            // relaxed is fine since we are just initializing the db
            this.next_event_id
                .store(last_id + 1, std::sync::atomic::Ordering::Relaxed);
        }

        // load counts into memory
        for guard in this.counts.prefix(keys::COUNT_KS_PREFIX) {
            let (k, v) = guard.into_inner().into_diagnostic()?;
            let name = std::str::from_utf8(&k[keys::COUNT_KS_PREFIX.len()..])
                .into_diagnostic()
                .wrap_err("expected valid utf8 for ks count key")?;
            let _ = this
                .counts_map
                .insert_sync(SmolStr::new(name), read_u64_counter(&v)?);
        }

        let durable_watermark = load_count_delta_watermark(&this)?;
        replay_count_deltas(&this, durable_watermark)?;
        this.count_delta_checkpoint_watermark
            .store(durable_watermark, Ordering::Relaxed);
        this.count_delta_gc_watermark
            .store(durable_watermark, Ordering::Relaxed);

        let next_count_delta_id = this
            .counts
            .prefix(keys::COUNT_DELTA_PREFIX)
            .next_back()
            .map(|guard| -> Result<u64> {
                let key = guard.key().into_diagnostic()?;
                let (id, _) = keys::parse_count_delta_key(&key)?;
                Ok(id + 1)
            })
            .transpose()?
            .unwrap_or(durable_watermark.saturating_add(1));
        this.next_count_delta_id
            .store(next_count_delta_id, Ordering::Relaxed);

        Ok(this)
    }

    pub fn train_dict(&self, ks_name: &str) -> Result<()> {
        let ks = match ks_name {
            #[cfg(feature = "indexer")]
            "blocks" => &self.blocks,
            #[cfg(feature = "indexer_stream")]
            "events" => &self.events,
            "repos" => &self.repos,
            #[cfg(feature = "backlinks")]
            "backlinks" => &self.backlinks,
            _ => miette::bail!("unknown keyspace for training: {ks_name}"),
        };

        let dict_size = match ks_name {
            "blocks" => kb(128),
            "events" => kb(64),
            "repos" => kb(64),
            "backlinks" => kb(64),
            _ => kb(32),
        };

        let samples: Vec<Vec<u8>> = if ks_name == "blocks" {
            #[cfg(not(feature = "indexer"))]
            miette::bail!("indexer feature required for blocks keyspace training");

            #[cfg(feature = "indexer")]
            {
                // sample up to 200 data blocks per collection, discovered lazily in the predicate
                let per_collection_limit = 200usize;
                let collection_counts: RefCell<std::collections::HashMap<Vec<u8>, usize>> =
                    RefCell::new(std::collections::HashMap::new());

                let new = ks
                    .sample_data_blocks(5000, |first, _last| {
                        let Some(sep_idx) = first.iter().position(|&b| b == keys::SEP) else {
                            return false;
                        };
                        let mut counts = collection_counts.borrow_mut();
                        let count = counts.entry(first[..sep_idx].to_vec()).or_insert(0);
                        if *count >= per_collection_limit {
                            return false;
                        }
                        *count += 1;
                        true
                    })
                    .into_diagnostic()?;

                new.into_iter().map(|s| s.to_vec()).collect()
            }
        } else {
            let mut seen_keys = HashSet::new();
            let captured_keys = RefCell::new(Vec::new());
            let new = ks
                .sample_data_blocks(1000, |first, last| {
                    let passes = !seen_keys.contains(&(first.to_vec(), last.to_vec()));
                    if passes {
                        captured_keys
                            .borrow_mut()
                            .push((first.to_vec(), last.to_vec()));
                    }
                    passes
                })
                .into_diagnostic()?;

            new.into_iter()
                .zip(captured_keys.into_inner())
                .filter_map(|(s, keys)| seen_keys.insert(keys).then_some(s.to_vec()))
                .collect()
        };

        if samples.is_empty() {
            miette::bail!("no samples found for keyspace {ks_name}, skipping training");
        }

        tracing::info!(
            "training zstd dictionary for keyspace {ks_name} ({} samples, {dict_size} bytes limit)...",
            samples.len(),
        );
        let dict_bytes =
            lsm_tree::train_zstd_dict(&samples, dict_size as usize).into_diagnostic()?;
        let path = self.path.join(format!("dict_{ks_name}.bin"));
        std::fs::write(&path, &dict_bytes).into_diagnostic()?;
        tracing::info!(
            "saved zstd dictionary for keyspace {ks_name} ({} bytes) to {}. please restart the indexer to use the new dictionary.",
            dict_bytes.len(),
            path.display()
        );

        Ok(())
    }

    pub fn persist(&self) -> Result<()> {
        #[cfg(not(feature = "__persist_sync_all"))]
        const MODE: PersistMode = PersistMode::Buffer;
        #[cfg(feature = "__persist_sync_all")]
        const MODE: PersistMode = PersistMode::SyncAll;
        self.inner.persist(MODE).into_diagnostic()?;
        Ok(())
    }

    pub async fn compact(&self) -> Result<()> {
        let compact = |ks: Keyspace| async move {
            tokio::task::spawn_blocking(move || ks.major_compact().into_diagnostic())
                .await
                .into_diagnostic()?
        };

        let mut tasks = vec![
            compact(self.repos.clone()),
            compact(self.cursors.clone()),
            compact(self.repo_metadata.clone()),
            compact(self.counts.clone()),
            compact(self.filter.clone()),
            compact(self.crawler.clone()),
        ];

        #[cfg(feature = "indexer")]
        {
            tasks.push(compact(self.records.clone()));
            tasks.push(compact(self.blocks.clone()));
            tasks.push(compact(self.pending.clone()));
            tasks.push(compact(self.resync.clone()));
            tasks.push(compact(self.resync_buffer.clone()));
        }
        #[cfg(feature = "indexer_stream")]
        tasks.push(compact(self.events.clone()));

        #[cfg(feature = "relay")]
        tasks.push(compact(self.relay_events.clone()));

        #[cfg(feature = "backlinks")]
        tasks.push(compact(self.backlinks.clone()));

        futures::future::try_join_all(tasks).await?;

        Ok(())
    }

    pub async fn get(ks: Keyspace, key: impl Into<Slice>) -> Result<Option<Slice>> {
        let key = key.into();
        tokio::task::spawn_blocking(move || ks.get(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    #[allow(dead_code)]
    pub async fn insert(
        ks: Keyspace,
        key: impl Into<Slice>,
        value: impl Into<Slice>,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();
        tokio::task::spawn_blocking(move || ks.insert(key, value).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    #[allow(dead_code)]
    pub async fn remove(ks: Keyspace, key: impl Into<Slice>) -> Result<()> {
        let key = key.into();
        tokio::task::spawn_blocking(move || ks.remove(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    #[allow(dead_code)]
    pub async fn contains_key(ks: Keyspace, key: impl Into<Slice>) -> Result<bool> {
        let key = key.into();
        tokio::task::spawn_blocking(move || ks.contains_key(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    pub(crate) fn stage_count_deltas(
        &self,
        batch: &mut OwnedWriteBatch,
        deltas: &CountDeltas,
    ) -> Option<CountDeltaReservation> {
        if deltas.is_empty() {
            return None;
        }

        let start_id = self
            .next_count_delta_id
            .fetch_add(deltas.len() as u64, Ordering::SeqCst);
        self.count_delta_in_flight
            .lock()
            .expect("count delta reservations poisoned")
            .insert(start_id);

        for (offset, (key, delta)) in deltas.iter().enumerate() {
            batch.insert(
                &self.counts,
                keys::count_delta_key(start_id + offset as u64, key),
                delta.to_be_bytes(),
            );
        }

        Some(CountDeltaReservation {
            in_flight: self.count_delta_in_flight.clone(),
            start_id,
        })
    }

    pub(crate) fn apply_count_deltas(&self, deltas: &CountDeltas) {
        for (key, delta) in deltas.iter() {
            self.update_count(key, *delta);
        }
    }

    pub fn checkpoint_count_deltas(&self) -> Result<Option<u64>> {
        let start = self
            .count_delta_checkpoint_watermark
            .load(Ordering::SeqCst)
            .saturating_add(1);

        let mut end = self
            .next_count_delta_id
            .load(Ordering::SeqCst)
            .saturating_sub(1);

        let lowest_in_flight = self
            .count_delta_in_flight
            .lock()
            .expect("count delta reservations poisoned")
            .first()
            .copied();
        if let Some(lowest_in_flight) = lowest_in_flight {
            end = end.min(lowest_in_flight.saturating_sub(1));
        }

        if end < start {
            return Ok(None);
        }

        let aggregated = load_count_delta_range(self, start, end)?;
        let mut batch = self.inner.batch();

        for (name, delta) in aggregated {
            let current = get_persisted_ks_count(self, &name)?;
            set_ks_count(&mut batch, self, &name, apply_count_delta(current, delta));
        }

        set_count_delta_watermark(&mut batch, self, end);
        batch.commit().into_diagnostic()?;
        self.count_delta_checkpoint_watermark
            .store(end, Ordering::SeqCst);

        Ok(Some(end))
    }

    pub fn mark_count_checkpoint_persisted(&self, watermark: u64) {
        self.count_delta_gc_watermark
            .store(watermark, Ordering::SeqCst);
    }

    pub fn update_count(&self, key: &str, delta: i64) -> u64 {
        let mut entry = self.counts_map.entry_sync(SmolStr::new(key)).or_insert(0);
        if delta < 0 && *entry < delta.unsigned_abs() {
            error!(
                key,
                current = *entry,
                decrement = delta.unsigned_abs(),
                "count underflow !!! this is a bug"
            );
            *entry = 0;
        } else {
            *entry = apply_count_delta(*entry, delta);
        }
        *entry
    }

    pub async fn update_count_async(&self, key: &str, delta: i64) {
        let mut entry = self
            .counts_map
            .entry_async(SmolStr::new(key))
            .await
            .or_insert(0);
        if delta < 0 && *entry < delta.unsigned_abs() {
            error!(
                key,
                current = *entry,
                decrement = delta.unsigned_abs(),
                "count underflow !!! this is a bug"
            );
            *entry = 0;
        } else {
            *entry = apply_count_delta(*entry, delta);
        }
    }

    pub async fn get_count(&self, key: &str) -> u64 {
        self.counts_map
            .read_async(key, |_, v| *v)
            .await
            .unwrap_or(0)
    }

    pub fn get_count_sync(&self, key: &str) -> u64 {
        self.counts_map.read_sync(key, |_, v| *v).unwrap_or(0)
    }
}

#[cfg(feature = "indexer")]
mod indexer;

#[cfg(feature = "indexer")]
pub use indexer::*;

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct FirehoseSourceMeta {
    #[serde(default)]
    pub(crate) is_pds: bool,
}

pub fn set_firehose_cursor(db: &Db, relay: &Url, cursor: i64) -> Result<()> {
    db.cursors
        .insert(
            keys::firehose_cursor_key_from_url(relay),
            cursor.to_be_bytes(),
        )
        .into_diagnostic()
}

pub async fn get_firehose_cursor(db: &Db, relay: &Url) -> Result<Option<i64>> {
    let key = keys::firehose_cursor_key_from_url(relay);
    Db::get(db.cursors.clone(), key)
        .await?
        .map(|v: Slice| {
            Ok(i64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("cursor is not 8 bytes")?,
            ))
        })
        .transpose()
}

pub fn ser_repo_meta(state: &RepoMetadata) -> Result<Vec<u8>> {
    rmp_serde::to_vec(&state).into_diagnostic()
}

pub fn deser_repo_meta(bytes: &[u8]) -> Result<RepoMetadata> {
    rmp_serde::from_slice(bytes).into_diagnostic()
}

pub fn ser_repo_state(state: &RepoState) -> Result<Vec<u8>> {
    rmp_serde::to_vec(&state).into_diagnostic()
}

pub fn deser_repo_state<'b>(bytes: &'b [u8]) -> Result<RepoState<'b>> {
    rmp_serde::from_slice(bytes).into_diagnostic()
}

pub fn check_poisoned(e: &fjall::Error) {
    if matches!(e, fjall::Error::Poisoned) {
        error!("!!! DATABASE POISONED !!! exiting");
        std::process::exit(10);
    }
}

pub fn check_poisoned_report(e: &miette::Report) {
    let Some(err) = e.downcast_ref::<fjall::Error>() else {
        return;
    };
    self::check_poisoned(err);
}

pub fn set_ks_count(batch: &mut OwnedWriteBatch, db: &Db, name: &str, count: u64) {
    let key = keys::count_keyspace_key(name);
    batch.insert(&db.counts, key, count.to_be_bytes());
}

pub fn set_count_delta_watermark(batch: &mut OwnedWriteBatch, db: &Db, watermark: u64) {
    batch.insert(
        &db.counts,
        keys::COUNT_DELTA_WATERMARK_KEY,
        watermark.to_be_bytes(),
    );
}

pub fn load_count_delta_watermark(db: &Db) -> Result<u64> {
    db.counts
        .get(keys::COUNT_DELTA_WATERMARK_KEY)
        .into_diagnostic()?
        .map(|value| read_u64_counter(&value))
        .transpose()
        .map(|watermark| watermark.unwrap_or(0))
}

fn read_u64_counter(value: &[u8]) -> Result<u64> {
    value
        .try_into()
        .into_diagnostic()
        .wrap_err("counter value must be 8 bytes")
        .map(u64::from_be_bytes)
}

fn read_i64_counter_delta(value: &[u8]) -> Result<i64> {
    value
        .try_into()
        .into_diagnostic()
        .wrap_err("counter delta must be 8 bytes")
        .map(i64::from_be_bytes)
}

fn replay_count_deltas(db: &Db, watermark: u64) -> Result<()> {
    let start = watermark.saturating_add(1);
    for (name, delta) in load_count_delta_range(db, start, u64::MAX)? {
        db.update_count(&name, delta);
    }
    Ok(())
}

fn load_count_delta_range(db: &Db, start: u64, end: u64) -> Result<BTreeMap<SmolStr, i64>> {
    let mut aggregated = BTreeMap::new();
    for guard in db.counts.range(keys::count_delta_start_key(start)..) {
        let (key, value) = guard.into_inner().into_diagnostic()?;
        if !key.starts_with(keys::COUNT_DELTA_PREFIX) {
            break;
        }

        let (id, name) = keys::parse_count_delta_key(&key)?;
        if id > end {
            break;
        }

        *aggregated.entry(SmolStr::new(name)).or_insert(0) += read_i64_counter_delta(&value)?;
    }
    Ok(aggregated)
}

fn get_persisted_ks_count(db: &Db, name: &str) -> Result<u64> {
    db.counts
        .get(keys::count_keyspace_key(name))
        .into_diagnostic()?
        .map(|value| read_u64_counter(&value))
        .transpose()
        .map(|count| count.unwrap_or(0))
}

/// load the persisted (day, count) pair for the daily PDS add counter, if present.
/// returns `None` if no entry exists or the stored data is malformed.
#[cfg(feature = "relay")]
pub fn load_pds_daily_adds(db: &Db) -> Result<Option<(u64, u64)>> {
    let Some(val) = db.cursors.get(keys::PDS_DAILY_ADDS_KEY).into_diagnostic()? else {
        return Ok(None);
    };
    if val.len() < 16 {
        miette::bail!("malformed pds daily limit value");
    }
    let day = u64::from_be_bytes(val[..8].try_into().into_diagnostic()?);
    let count = u64::from_be_bytes(val[8..].try_into().into_diagnostic()?);
    Ok(Some((day, count)))
}

/// persist the daily PDS add counter (day, count) to the cursors keyspace.
/// value layout: [day: u64 BE][count: u64 BE] = 16 bytes.
///
/// takes the `cursors` keyspace directly so the caller can clone it into a
/// `spawn_blocking` closure without needing an owned `Db`.
#[cfg(feature = "relay")]
pub fn save_pds_daily_adds(db: &Db, day: u64, count: u64) -> Result<()> {
    let mut value = [0u8; 16];
    value[..8].copy_from_slice(&day.to_be_bytes());
    value[8..].copy_from_slice(&count.to_be_bytes());
    db.cursors
        .insert(keys::PDS_DAILY_ADDS_KEY, value)
        .into_diagnostic()
}

pub fn load_persisted_firehose_sources(
    db: &crate::db::Db,
) -> Result<Vec<crate::config::FirehoseSource>> {
    use crate::db::keys::FIREHOSE_SOURCE_PREFIX;

    let mut sources = Vec::new();
    for entry in db.crawler.prefix(FIREHOSE_SOURCE_PREFIX) {
        let (key, val) = entry.into_inner().into_diagnostic()?;
        let url_bytes = &key[FIREHOSE_SOURCE_PREFIX.len()..];
        let url_str = std::str::from_utf8(url_bytes).into_diagnostic()?;
        let url = Url::parse(url_str).into_diagnostic()?;
        let meta: FirehoseSourceMeta = rmp_serde::from_slice(&val)
            .map_err(|e| miette::miette!("failed to deserialize firehose source meta: {e}"))?;
        sources.push(crate::config::FirehoseSource {
            url,
            is_pds: meta.is_pds,
        });
    }
    Ok(sources)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(path: &std::path::Path) -> crate::config::Config {
        crate::config::Config {
            database_path: path.to_path_buf(),
            ..Default::default()
        }
    }

    #[test]
    fn count_deltas_replay_and_checkpoint_across_restart() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());

        {
            let db = Db::open(&cfg)?;
            let mut batch = db.inner.batch();
            set_ks_count(&mut batch, &db, "repos", 10);
            batch.commit().into_diagnostic()?;

            let mut batch = db.inner.batch();
            let mut deltas = CountDeltas::default();
            deltas.add("repos", 2);
            deltas.add("pending", 1);
            let reservation = db
                .stage_count_deltas(&mut batch, &deltas)
                .expect("count deltas should reserve ids");
            batch.commit().into_diagnostic()?;
            db.apply_count_deltas(&deltas);
            drop(reservation);
            db.persist()?;
        }

        {
            let db = Db::open(&cfg)?;
            assert_eq!(db.get_count_sync("repos"), 12);
            assert_eq!(db.get_count_sync("pending"), 1);
            let checkpointed_watermark = db
                .checkpoint_count_deltas()?
                .expect("checkpoint should fold pending deltas");
            db.persist()?;
            db.mark_count_checkpoint_persisted(checkpointed_watermark);
            assert_eq!(load_count_delta_watermark(&db)?, checkpointed_watermark);
        }

        {
            let db = Db::open(&cfg)?;
            assert_eq!(db.get_count_sync("repos"), 12);
            assert_eq!(db.get_count_sync("pending"), 1);
            assert!(load_count_delta_watermark(&db)? >= 1);
        }

        Ok(())
    }

    #[test]
    fn checkpoint_skips_inflight_deltas() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());
        let db = Db::open(&cfg)?;

        let mut batch = db.inner.batch();
        let mut deltas = CountDeltas::default();
        deltas.add("repos", 1);
        let reservation = db
            .stage_count_deltas(&mut batch, &deltas)
            .expect("count deltas should reserve ids");

        assert!(db.checkpoint_count_deltas()?.is_none());

        batch.commit().into_diagnostic()?;
        db.apply_count_deltas(&deltas);
        drop(reservation);

        assert!(db.checkpoint_count_deltas()?.is_some());

        Ok(())
    }

    #[test]
    fn checkpointed_count_deltas_are_gcable() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());
        let db = Db::open(&cfg)?;

        let mut batch = db.inner.batch();
        let mut deltas = CountDeltas::default();
        deltas.add("repos", 1);
        let reservation = db
            .stage_count_deltas(&mut batch, &deltas)
            .expect("count deltas should reserve ids");
        batch.commit().into_diagnostic()?;
        db.apply_count_deltas(&deltas);
        drop(reservation);

        let watermark = db
            .checkpoint_count_deltas()?
            .expect("checkpoint should fold pending deltas");
        db.persist()?;
        db.mark_count_checkpoint_persisted(watermark);

        db.counts.rotate_memtable_and_wait().into_diagnostic()?;
        db.counts.major_compact().into_diagnostic()?;

        assert_eq!(db.counts.prefix(keys::COUNT_DELTA_PREFIX).count(), 0);

        Ok(())
    }
}
