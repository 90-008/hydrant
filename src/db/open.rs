use fjall::{
    CompressionType, Database, KeyspaceCreateOptions,
    config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy},
};
use lsm_tree::compaction::Factory;
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::SmolStr;
use std::collections::BTreeSet;
#[cfg(feature = "jetstream")]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

use crate::config::{Compression, Config};

use super::compaction::CountsGcFilterFactory;
use super::counts::{load_count_delta_watermark, read_u64_counter, replay_count_deltas};
use super::keys;
use super::{Db, migration};

const fn kb(v: u32) -> u32 {
    v * 1024
}
const fn mb(v: u64) -> u64 {
    v * 1024 * 1024
}
fn default_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
}

impl Db {
    pub fn open(cfg: &Config) -> Result<Self> {
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
            if path.exists()
                && let Ok(bytes) = std::fs::read(&path)
            {
                tracing::debug!(
                    "loaded zstd dictionary for keyspace {name} ({} bytes)",
                    bytes.len()
                );
                return Some(bytes.into());
            }
            None
        };
        let dicts = ["repos", "blocks", "events", "jetstream_events", "backlinks"]
            .into_iter()
            .fold(std::collections::HashMap::new(), |mut acc, name| {
                let Some(dict) = load_dict(name) else {
                    return acc;
                };
                acc.insert(name, dict);
                acc
            });
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

        #[cfg(feature = "jetstream")]
        let jetstream_events = open_ks(
            "jetstream_events",
            opts()
                // time-ordered append-only stream metadata, only iterated for replay.
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_events_memtable_size_mb))
                .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(64), kb(128)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    get_compression("jetstream_events", 3),
                    get_compression("jetstream_events", 5),
                ]))
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
        let (event_tx, _) = broadcast::channel(512);

        #[cfg(feature = "relay")]
        let (relay_broadcast_tx, _) = broadcast::channel(512);

        #[cfg(feature = "jetstream")]
        let (jetstream_tx, _) = broadcast::channel(512);

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
            #[cfg(feature = "jetstream")]
            jetstream_events,
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
            #[cfg(feature = "jetstream")]
            jetstream_tx,
            #[cfg(feature = "jetstream")]
            next_jetstream_id: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "jetstream")]
            last_jetstream_time_us: Arc::new(AtomicI64::new(0)),
            #[cfg(all(feature = "jetstream", feature = "relay"))]
            jetstream_lock: Arc::new(parking_lot::Mutex::new(())),
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
            #[cfg(feature = "indexer")]
            lifecycle_count_lock: Arc::new(Mutex::new(())),
            compaction_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

        #[cfg(feature = "jetstream")]
        {
            let mut last_id = 0;
            let mut last_time_us = 0;
            if let Some(guard) = this.jetstream_events.iter().next_back() {
                let k = guard.key().into_diagnostic()?;
                let (time_us, id) = keys::parse_jetstream_event_key(&k)?;
                last_id = id;
                last_time_us = time_us;
            }
            this.next_jetstream_id
                .store(last_id + 1, std::sync::atomic::Ordering::Relaxed);
            this.last_jetstream_time_us
                .store(last_time_us as i64, std::sync::atomic::Ordering::Relaxed);
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

        // always stay strictly above the durable watermark so that after a migration
        // deletes delta keys, new deltas are not assigned ids that checkpoint/replay
        // would silently skip (finding 5f309024bc588191aa1a79eb449e3630).
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
            .unwrap_or(0)
            .max(durable_watermark.saturating_add(1));
        this.next_count_delta_id
            .store(next_count_delta_id, Ordering::Relaxed);

        Ok(this)
    }
}
