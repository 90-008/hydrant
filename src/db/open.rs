use fjall::{CompressionType, Database};
use lsm_tree::compaction::Factory;
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::SmolStr;
use std::collections::{BTreeSet, HashMap as StdHashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::{Compression, Config};

use super::compaction::CountsGcFilterFactory;
use super::counts::{load_count_delta_watermark, read_u64_counter, replay_count_deltas};
use super::keys;
use super::keyspaces::OpenCx;
use super::schema::{Ks, mb};
use super::{Db, migration, registry, schema};

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
        let dicts = registry::trainable()
            .into_iter()
            .fold(StdHashMap::new(), |mut acc, (name, _)| {
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
            opened: std::cell::RefCell::new(Vec::new()),
        };

        let repos = Ks::<schema::Repos>::open(&cx)?;
        let repo_metadata = Ks::<schema::RepoMetadata>::open(&cx)?;
        let cursors = Ks::<schema::Cursors>::open(&cx)?;
        let counts = Ks::<schema::Counts>::open(&cx)?;
        let filter = Ks::<schema::Filter>::open(&cx)?;
        let crawler = Ks::<schema::Crawler>::open(&cx)?;
        #[cfg(feature = "backlinks")]
        let backlinks = Ks::<schema::Backlinks>::open(&cx)?;
        #[cfg(feature = "indexer")]
        let indexer = super::keyspaces::IndexerDb::open(&cx)?;
        #[cfg(feature = "indexer_stream")]
        let stream = super::keyspaces::StreamDb::open(&cx)?;
        #[cfg(feature = "jetstream")]
        let jetstream = super::keyspaces::JetstreamDb::open(&cx)?;
        #[cfg(feature = "relay")]
        let relay = super::keyspaces::RelayDb::open(&cx)?;

        // every opened keyspace must have a registry row and vice versa, so the
        // by-name, /stats, /debug, and training tables cannot silently drift.
        let opened: BTreeSet<&'static str> = cx.opened.borrow().iter().copied().collect();
        let registered: BTreeSet<&'static str> = registry::names().into_iter().collect();
        if opened != registered {
            let missing: Vec<_> = registered.difference(&opened).collect();
            let unregistered: Vec<_> = opened.difference(&registered).collect();
            miette::bail!(
                "keyspace registry drift: registered but not opened: {missing:?}, opened but not registered: {unregistered:?}"
            );
        }

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
