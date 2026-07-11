use fjall::{
    CompressionType, Database, KeyspaceCreateOptions,
    config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy},
};
use lsm_tree::compaction::Factory;
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::SmolStr;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::{Compression, Config};

use super::compaction::CountsGcFilterFactory;
use super::counts::{load_count_delta_watermark, read_u64_counter, replay_count_deltas};
use super::keys;
use super::keyspaces::{OpenCx, mb};
use super::{Db, migration};

const fn kb(v: u32) -> u32 {
    v * 1024
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
        let cx = OpenCx {
            db: &db,
            cfg,
            compression: &get_compression,
        };
        let open_ks = |name: &str, opts: KeyspaceCreateOptions| cx.open_ks(name, opts);

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
        let indexer = super::keyspaces::IndexerDb::open(&cx)?;
        #[cfg(feature = "indexer_stream")]
        let stream = super::keyspaces::StreamDb::open(&cx)?;
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

        #[cfg(feature = "jetstream")]
        let jetstream = super::keyspaces::JetstreamDb::open(&cx)?;
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
        let relay = super::keyspaces::RelayDb::open(&cx)?;

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



        let this = Self {
            inner: db,
            path: cfg.database_path.clone(),
            repos,
            repo_metadata,
            cursors,
            counts,
            filter,
            crawler,
            #[cfg(feature = "indexer")]
            indexer,
            #[cfg(feature = "indexer_stream")]
            stream,
            #[cfg(feature = "jetstream")]
            jetstream,
            #[cfg(feature = "relay")]
            relay,
            #[cfg(feature = "backlinks")]
            backlinks,
            counts_map: HashMap::new(),
            next_count_delta_id: Arc::new(AtomicU64::new(0)),
            count_delta_checkpoint_watermark: Arc::new(AtomicU64::new(0)),
            count_delta_gc_watermark,
            count_delta_in_flight: Arc::new(Mutex::new(BTreeSet::new())),
            compaction_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        migration::run(&this)?;

        #[cfg(feature = "relay")]
        this.relay.init()?;
        #[cfg(feature = "indexer_stream")]
        this.stream.init()?;
        #[cfg(feature = "jetstream")]
        this.jetstream.init()?;

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
