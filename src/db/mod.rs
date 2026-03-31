use crate::config::Compression;
use crate::db::compaction::DropPrefixFilterFactory;
#[cfg(feature = "indexer")]
use crate::types::BroadcastEvent;
#[cfg(feature = "relay")]
use crate::types::RelayBroadcast;
use crate::types::{RepoMetadata, RepoState};

use fjall::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Slice,
};
use jacquard_common::IntoStatic;
use jacquard_common::types::string::Did;
use lsm_tree::compaction::Factory;
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::SmolStr;

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use url::Url;

pub mod compaction;
pub mod ephemeral;
pub mod filter;
pub mod keys;
pub mod migration;
pub mod pds_tiers;
pub mod types;

use tokio::sync::broadcast;
use tracing::error;

fn default_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
}

pub struct Db {
    pub inner: Arc<Database>,
    pub path: std::path::PathBuf,
    pub repos: Keyspace,
    pub records: Keyspace,
    pub blocks: Keyspace,
    pub cursors: Keyspace,
    pub pending: Keyspace,
    pub resync: Keyspace,
    pub resync_buffer: Keyspace,
    pub repo_metadata: Keyspace,
    pub events: Keyspace,
    pub counts: Keyspace,
    pub filter: Keyspace,
    pub crawler: Keyspace,
    #[cfg(feature = "backlinks")]
    pub backlinks: Keyspace,
    #[cfg(feature = "indexer")]
    pub(crate) event_tx: broadcast::Sender<BroadcastEvent>,
    #[cfg(feature = "indexer")]
    pub next_event_id: Arc<AtomicU64>,
    #[cfg(feature = "relay")]
    pub(crate) relay_events: Keyspace,
    #[cfg(feature = "relay")]
    pub(crate) next_relay_seq: Arc<AtomicU64>,
    #[cfg(feature = "relay")]
    pub(crate) relay_broadcast_tx: broadcast::Sender<RelayBroadcast>,
    pub counts_map: HashMap<SmolStr, u64>,
}

macro_rules! update_gauge_diff_impl {
    ($self:ident, $old:ident, $new:ident, $update_method:ident $(, $await:tt)?) => {{
        use crate::types::GaugeState;

        if $old == $new {
            return;
        }

        // pending
        match ($old, $new) {
            (GaugeState::Pending, GaugeState::Pending) => {}
            (GaugeState::Pending, _) => $self.$update_method("pending", -1) $(.$await)?,
            (_, GaugeState::Pending) => $self.$update_method("pending", 1) $(.$await)?,
            _ => {}
        }

        // resync
        let old_resync = $old.is_resync();
        let new_resync = $new.is_resync();
        match (old_resync, new_resync) {
            (true, false) => $self.$update_method("resync", -1) $(.$await)?,
            (false, true) => $self.$update_method("resync", 1) $(.$await)?,
            _ => {}
        }

        // error kinds
        if let GaugeState::Resync(Some(kind)) = $old {
            let key = match kind {
                crate::types::ResyncErrorKind::Ratelimited => "error_ratelimited",
                crate::types::ResyncErrorKind::Transport => "error_transport",
                crate::types::ResyncErrorKind::Generic => "error_generic",
            };
            $self.$update_method(key, -1) $(.$await)?;
        }

        if let GaugeState::Resync(Some(kind)) = $new {
            let key = match kind {
                crate::types::ResyncErrorKind::Ratelimited => "error_ratelimited",
                crate::types::ResyncErrorKind::Transport => "error_transport",
                crate::types::ResyncErrorKind::Generic => "error_generic",
            };
            $self.$update_method(key, 1) $(.$await)?;
        }
    }};
}

const fn kb(v: u32) -> u32 {
    v * 1024
}
const fn mb(v: u64) -> u64 {
    v * 1024 * 1024
}

impl Db {
    pub fn open(cfg: &crate::config::Config) -> Result<Self> {
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
                let f = move |ks: &str| match ks {
                    "counts" => ephemeral.then(|| -> Arc<dyn Factory> {
                        Arc::new(DropPrefixFilterFactory {
                            prefix: keys::COUNT_COLLECTION_PREFIX,
                        })
                    }),
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
                .max_memtable_size(mb(16))
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

        #[cfg(feature = "indexer")]
        let (event_tx, _) = broadcast::channel(10000);

        #[cfg(feature = "relay")]
        let (relay_broadcast_tx, _) = broadcast::channel(10000);

        let this = Self {
            inner: db,
            path: cfg.database_path.clone(),
            repos,
            records,
            blocks,
            cursors,
            pending,
            resync,
            resync_buffer,
            repo_metadata,
            events,
            counts,
            filter,
            crawler,
            #[cfg(feature = "backlinks")]
            backlinks,
            #[cfg(feature = "indexer")]
            event_tx,
            counts_map: HashMap::new(),
            #[cfg(feature = "indexer")]
            next_event_id: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "relay")]
            relay_events,
            #[cfg(feature = "relay")]
            next_relay_seq: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "relay")]
            relay_broadcast_tx,
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

        #[cfg(feature = "indexer")]
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
            let _ = this.counts_map.insert_sync(
                SmolStr::new(name),
                u64::from_be_bytes(v.as_ref().try_into().unwrap()),
            );
        }

        Ok(this)
    }

    pub fn train_dict(&self, ks_name: &str) -> Result<()> {
        let ks = match ks_name {
            "blocks" => &self.blocks,
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
        tokio::try_join!(
            compact(self.repos.clone()),
            compact(self.records.clone()),
            compact(self.blocks.clone()),
            compact(self.cursors.clone()),
            compact(self.pending.clone()),
            compact(self.resync.clone()),
            compact(self.resync_buffer.clone()),
            compact(self.repo_metadata.clone()),
            compact(self.events.clone()),
            compact(self.counts.clone()),
            compact(self.filter.clone()),
            compact(self.crawler.clone()),
        )?;
        #[cfg(feature = "relay")]
        compact(self.relay_events.clone()).await?;
        #[cfg(feature = "backlinks")]
        compact(self.backlinks.clone()).await?;
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

    pub async fn contains_key(ks: Keyspace, key: impl Into<Slice>) -> Result<bool> {
        let key = key.into();
        tokio::task::spawn_blocking(move || ks.contains_key(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    pub fn update_count(&self, key: &str, delta: i64) {
        let mut entry = self.counts_map.entry_sync(SmolStr::new(key)).or_insert(0);
        if delta >= 0 {
            *entry = entry.saturating_add(delta as u64);
        } else {
            let decrement = delta.unsigned_abs();
            if *entry < decrement {
                error!(
                    key,
                    current = *entry,
                    decrement,
                    "count underflow !!! this is a bug"
                );
                *entry = 0;
            } else {
                *entry -= decrement;
            }
        }
    }

    pub async fn update_count_async(&self, key: &str, delta: i64) {
        let mut entry = self
            .counts_map
            .entry_async(SmolStr::new(key))
            .await
            .or_insert(0);
        if delta >= 0 {
            *entry = entry.saturating_add(delta as u64);
        } else {
            let decrement = delta.unsigned_abs();
            if *entry < decrement {
                error!(
                    key,
                    current = *entry,
                    decrement,
                    "count underflow !!! this is a bug"
                );
                *entry = 0;
            } else {
                *entry -= decrement;
            }
        }
    }

    pub async fn get_count(&self, key: &str) -> u64 {
        self.counts_map
            .read_async(key, |_, v| *v)
            .await
            .unwrap_or(0)
    }

    pub(crate) fn update_gauge_diff(
        &self,
        old: &crate::types::GaugeState,
        new: &crate::types::GaugeState,
    ) {
        update_gauge_diff_impl!(self, old, new, update_count);
    }

    pub(crate) async fn update_gauge_diff_async(
        &self,
        old: &crate::types::GaugeState,
        new: &crate::types::GaugeState,
    ) {
        update_gauge_diff_impl!(self, old, new, update_count_async, await);
    }

    pub(crate) fn update_repo_state<F, T>(
        batch: &mut OwnedWriteBatch,
        repos: &Keyspace,
        did: &Did<'_>,
        f: F,
    ) -> Result<Option<(RepoState<'static>, T)>>
    where
        F: FnOnce(&mut RepoState, (&[u8], &mut fjall::OwnedWriteBatch)) -> Result<(bool, T)>,
    {
        let key = keys::repo_key(did);
        if let Some(bytes) = repos.get(&key).into_diagnostic()? {
            let mut state: RepoState = deser_repo_state(bytes.as_ref())?.into_static();
            let (changed, result) = f(&mut state, (key.as_slice(), batch))?;
            if changed {
                batch.insert(repos, key, ser_repo_state(&state)?);
            }
            Ok(Some((state, result)))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn update_repo_state_async<F, T>(
        &self,
        did: &Did<'_>,
        f: F,
    ) -> Result<Option<(RepoState<'static>, T)>>
    where
        F: FnOnce(&mut RepoState, (&[u8], &mut fjall::OwnedWriteBatch)) -> Result<(bool, T)>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let mut batch = self.inner.batch();
        let repos = self.repos.clone();
        let did = did.clone().into_static();

        tokio::task::spawn_blocking(move || {
            let Some((state, t)) = Self::update_repo_state(&mut batch, &repos, &did, f)? else {
                return Ok(None);
            };
            batch.commit().into_diagnostic()?;
            Ok(Some((state, t)))
        })
        .await
        .into_diagnostic()?
    }

    pub(crate) fn repo_gauge_state(
        repo_state: &RepoState,
        resync_bytes: Option<&[u8]>,
    ) -> crate::types::GaugeState {
        match repo_state.status {
            crate::types::RepoStatus::Synced => crate::types::GaugeState::Synced,
            crate::types::RepoStatus::Error(_)
            | crate::types::RepoStatus::Deactivated
            | crate::types::RepoStatus::Takendown
            | crate::types::RepoStatus::Suspended
            | crate::types::RepoStatus::Deleted
            | crate::types::RepoStatus::Desynchronized
            | crate::types::RepoStatus::Throttled => {
                if let Some(resync_bytes) = resync_bytes {
                    if let Ok(crate::types::ResyncState::Error { kind, .. }) =
                        rmp_serde::from_slice::<crate::types::ResyncState>(resync_bytes)
                    {
                        crate::types::GaugeState::Resync(Some(kind))
                    } else {
                        crate::types::GaugeState::Resync(None)
                    }
                } else {
                    crate::types::GaugeState::Resync(None)
                }
            }
        }
    }
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
        .map(|v| {
            Ok(i64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("cursor is not 8 bytes")?,
            ))
        })
        .transpose()
}

pub fn ser_repo_metadata(state: &RepoMetadata) -> Result<Vec<u8>> {
    rmp_serde::to_vec(&state).into_diagnostic()
}

pub fn deser_repo_metadata(bytes: &[u8]) -> Result<RepoMetadata> {
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

pub fn persist_counts(db: &Db) -> Result<()> {
    let mut batch = db.inner.batch();
    db.counts_map.iter_sync(|k, v| {
        set_ks_count(&mut batch, db, k, *v);
        true
    });
    batch.commit().into_diagnostic()
}

pub fn set_record_count(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    did: &Did<'_>,
    collection: &str,
    count: u64,
) {
    let key = keys::count_collection_key(did, collection);
    batch.insert(&db.counts, key, count.to_be_bytes());
}

pub fn update_record_count(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    did: &Did<'_>,
    collection: &str,
    delta: i64,
) -> Result<()> {
    let key = keys::count_collection_key(did, collection);
    let count = db
        .counts
        .get(&key)
        .into_diagnostic()?
        .map(|v| -> Result<_> {
            Ok(u64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("expected to be count (8 bytes)")?,
            ))
        })
        .transpose()?
        .unwrap_or(0);
    let new_count = if delta >= 0 {
        count.saturating_add(delta as u64)
    } else {
        count.saturating_sub(delta.unsigned_abs())
    };
    batch.insert(&db.counts, key, new_count.to_be_bytes());
    Ok(())
}

pub fn get_record_count(db: &Db, did: &Did<'_>, collection: &str) -> Result<u64> {
    let key = keys::count_collection_key(did, collection);
    let count = db
        .counts
        .get(&key)
        .into_diagnostic()?
        .map(|v| -> Result<_> {
            Ok(u64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("expected to be count (8 bytes)")?,
            ))
        })
        .transpose()?;
    Ok(count.unwrap_or(0))
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct FirehoseSourceMeta {
    #[serde(default)]
    pub(crate) is_pds: bool,
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

pub fn load_persisted_crawler_sources(
    db: &crate::db::Db,
) -> Result<Vec<crate::config::CrawlerSource>> {
    use crate::db::keys::CRAWLER_SOURCE_PREFIX;

    let mut sources = Vec::new();
    for entry in db.crawler.prefix(CRAWLER_SOURCE_PREFIX) {
        let (key, val) = entry.into_inner().into_diagnostic()?;
        let url_bytes = &key[CRAWLER_SOURCE_PREFIX.len()..];
        let url_str = std::str::from_utf8(url_bytes).into_diagnostic()?;
        let url = Url::parse(url_str).into_diagnostic()?;
        let mode: crate::config::CrawlerMode = rmp_serde::from_slice(&val).into_diagnostic()?;
        sources.push(crate::config::CrawlerSource { url, mode });
    }
    Ok(sources)
}
