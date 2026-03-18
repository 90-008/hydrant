use crate::config::Compression;
use crate::db::compaction::DropPrefixFilterFactory;
use crate::types::{BroadcastEvent, RepoState};

use fjall::config::{BlockSizePolicy, CompressionPolicy};
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Slice,
};
use jacquard_common::IntoStatic;
use jacquard_common::types::string::Did;
use lsm_tree::compaction::Factory;
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::SmolStr;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use url::Url;

pub mod compaction;
pub mod ephemeral;
pub mod filter;
pub mod keys;
pub mod types;

use tokio::sync::broadcast;
use tracing::error;

fn default_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
}

#[derive(Clone)]
pub struct Db {
    pub inner: Arc<Database>,
    pub repos: Keyspace,
    pub records: Keyspace,
    pub blocks: Keyspace,
    pub cursors: Keyspace,
    pub pending: Keyspace,
    pub resync: Keyspace,
    pub resync_buffer: Keyspace,
    pub events: Keyspace,
    pub counts: Keyspace,
    pub filter: Keyspace,
    pub crawler: Keyspace,
    // only meaningful in ephemeral mode; empty and unused in non-ephemeral
    pub block_refcounts: scc::HashMap<Slice, u32>,
    pub event_tx: broadcast::Sender<BroadcastEvent>,
    pub next_event_id: Arc<AtomicU64>,
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

impl Db {
    pub fn open(cfg: &crate::config::Config) -> Result<Self> {
        const fn kb(v: u32) -> u32 {
            v * 1024
        }
        const fn mb(v: u64) -> u64 {
            v * 1024 * 1024
        }

        let db = Database::builder(&cfg.database_path)
            .cache_size(cfg.cache_size * 2_u64.pow(20) / 2)
            .manual_journal_persist(true)
            .journal_compression(match cfg.journal_compression {
                Compression::Lz4 => CompressionType::Lz4,
                Compression::None => CompressionType::None,
            })
            .worker_threads(cfg.db_worker_threads)
            .max_journaling_size(mb(cfg.db_max_journaling_size_mb))
            .with_compaction_filter_factories({
                let ephemeral = cfg.ephemeral;
                let f = move |ks: &str| {
                    tracing::info!("with_compaction_filter_factories queried for keyspace: {ks}",);
                    match ks {
                        "counts" => ephemeral.then(|| -> Arc<dyn Factory> {
                            Arc::new(DropPrefixFilterFactory {
                                prefix: keys::COUNT_COLLECTION_PREFIX,
                            })
                        }),
                        _ => None,
                    }
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

        let compression: CompressionType = match cfg.data_compression {
            Compression::Lz4 => CompressionType::Lz4,
            Compression::None => CompressionType::None,
        };

        let repos = open_ks(
            "repos",
            opts()
                // crawler checks if a repo doesn't exist
                .expect_point_read_hits(false)
                .max_memtable_size(mb(cfg.db_repos_memtable_size_mb))
                // did -> repo state, not gonna be compressable well because dids are random
                // and repo state doesnt have repeats really
                .data_block_size_policy(BlockSizePolicy::new([kb(4), kb(4), kb(8), kb(64)]))
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    CompressionType::None,
                    compression,
                ])),
        )?;
        let pending = open_ks(
            "pending",
            opts()
                // iterated over as a queue, no point reads are used so bloom filters are disabled
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_pending_memtable_size_mb))
                // its just index of id (int) -> did, and dids arent compressable (especially with the ids being random)
                .data_block_size_policy(BlockSizePolicy::all(kb(1)))
                // and we'll transition from pending to synced anyway, no point trying to compress
                .data_block_compression_policy(CompressionPolicy::disabled()),
        )?;
        let resync = open_ks(
            "resync",
            opts()
                // we only point read in backfill when we check for existing resync state
                // ...and also in repos api. so we can disable bloom filters
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_pending_memtable_size_mb))
                // did -> error state, so its gonna be basically random, cant compress well
                .data_block_size_policy(BlockSizePolicy::all(kb(4)))
                // and we arent going to have many of these anyway, no point trying
                .data_block_compression_policy(CompressionPolicy::disabled()),
        )?;
        let blocks = open_ks(
            "blocks",
            opts()
                // point reads are used a lot by stream, we know the blocks exist though
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_blocks_memtable_size_mb))
                // 16 - 128 kb is probably fine, as the newer blocks will be in the first levels
                // and any consumers will probably be streaming the newer events...
                // and blocks are pretty big-ish like around 5kb usually so this helps i think
                .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(64), kb(128)]))
                // lets not compress first level so the reads for new blocks are faster
                // since we will be streaming them to consumers
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    compression,
                ])),
        )?;
        let records = open_ks(
            "records",
            opts()
                // point reads might miss when using getRecord
                // but we assume thats not going to happen often... (todo: should be a config option maybe?)
                // and since this keyspace is big, turning off bloom filters will help a lot
                .expect_point_read_hits(true)
                .max_memtable_size(mb(cfg.db_records_memtable_size_mb))
                // its just did|col|rkey -> cid, very small
                .data_block_size_policy(BlockSizePolicy::new([kb(8), kb(16)]))
                // cids arent compressable, most rkeys are TIDs so they will get compressed
                // by prefix truncation anyway
                .data_block_compression_policy(CompressionPolicy::disabled()),
        )?;
        let cursors = open_ks(
            "cursors",
            opts()
                // cursor point reads hit almost 100% of the time
                .expect_point_read_hits(true)
                .max_memtable_size(mb(4))
                // its just cursors...
                .data_block_size_policy(BlockSizePolicy::all(kb(1)))
                .data_block_compression_policy(CompressionPolicy::disabled()),
        )?;
        let resync_buffer = open_ks(
            "resync_buffer",
            opts()
                // iterated during backfill, no point reads
                .expect_point_read_hits(true)
                .max_memtable_size(mb(16))
                .data_block_size_policy(BlockSizePolicy::all(kb(32)))
                // dont have to compress here since resync buffer will be emptied at some point anyway
                .data_block_compression_policy(CompressionPolicy::disabled()),
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
                .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(64)]))
                // we are streaming the new events to consumers so we dont want to compress them
                .data_block_compression_policy(CompressionPolicy::new([
                    CompressionType::None,
                    compression,
                ])),
        )?;
        let counts = open_ks(
            "counts",
            opts()
                // count increments hit because counters are mostly pre-initialized
                .expect_point_read_hits(true)
                .max_memtable_size(mb(16))
                // the data is very small
                // this is at worst did|col -> u64, so its tiny
                .data_block_size_policy(BlockSizePolicy::all(kb(2)))
                .data_block_compression_policy(CompressionPolicy::disabled()),
        )?;

        // filter handles high-volume point reads (repo excludes) so it needs the bloom filter
        let filter = open_ks(
            "filter",
            opts()
                .max_memtable_size(mb(16))
                // dids arent compressable so this is fine, and we have nothing as the value
                .data_block_size_policy(BlockSizePolicy::all(kb(1)))
                .data_block_compression_policy(CompressionPolicy::disabled()),
        )?;

        let crawler = open_ks(
            "crawler",
            opts()
                // only iterators are used here
                .expect_point_read_hits(true)
                .max_memtable_size(mb(16))
                // did -> failed state, not very compressable
                .data_block_size_policy(BlockSizePolicy::all(kb(2)))
                .data_block_compression_policy(CompressionPolicy::disabled()),
        )?;

        // when adding new keyspaces, make sure to add them to the /stats endpoint
        // and also update any relevant /debug/* endpoints

        let mut last_id = 0;
        if let Some(guard) = events.iter().next_back() {
            let k = guard.key().into_diagnostic()?;
            last_id = u64::from_be_bytes(
                k.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("expected to be id (8 bytes)")?,
            );
        }

        // load counts into memory
        let counts_map = HashMap::new();
        for guard in counts.prefix(keys::COUNT_KS_PREFIX) {
            let (k, v) = guard.into_inner().into_diagnostic()?;
            let name = std::str::from_utf8(&k[keys::COUNT_KS_PREFIX.len()..])
                .into_diagnostic()
                .wrap_err("expected valid utf8 for ks count key")?;
            let _ = counts_map.insert_sync(
                SmolStr::new(name),
                u64::from_be_bytes(v.as_ref().try_into().unwrap()),
            );
        }
        // ensure critical counts are initialized
        for ks_name in ["repos", "pending", "resync"] {
            let _ = counts_map
                .entry_sync(SmolStr::new(ks_name))
                .or_insert_with(|| {
                    let ks = match ks_name {
                        "repos" => &repos,
                        "pending" => &pending,
                        "resync" => &resync,
                        _ => unreachable!(),
                    };
                    ks.iter().count() as u64
                });
        }

        let (event_tx, _) = broadcast::channel(10000);

        Ok(Self {
            inner: db,
            repos,
            records,
            blocks,
            cursors,
            pending,
            resync,
            resync_buffer,
            events,
            counts,
            filter,
            crawler,
            block_refcounts: scc::HashMap::default(),
            event_tx,
            counts_map,
            next_event_id: Arc::new(AtomicU64::new(last_id + 1)),
        })
    }

    pub fn persist(&self) -> Result<()> {
        #[cfg(not(feature = "sync_all"))]
        const MODE: PersistMode = PersistMode::Buffer;
        #[cfg(feature = "sync_all")]
        const MODE: PersistMode = PersistMode::SyncAll;
        self.inner.persist(MODE).into_diagnostic()?;
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        self.repos.major_compact().into_diagnostic()?;
        self.records.major_compact().into_diagnostic()?;
        self.blocks.major_compact().into_diagnostic()?;
        self.cursors.major_compact().into_diagnostic()?;
        self.pending.major_compact().into_diagnostic()?;
        self.resync.major_compact().into_diagnostic()?;
        self.resync_buffer.major_compact().into_diagnostic()?;
        self.events.major_compact().into_diagnostic()?;
        self.counts.major_compact().into_diagnostic()?;
        self.filter.major_compact().into_diagnostic()?;
        self.crawler.major_compact().into_diagnostic()?;
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
            *entry = entry.saturating_sub(delta.unsigned_abs());
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
            *entry = entry.saturating_sub(delta.unsigned_abs());
        }
    }

    pub async fn get_count(&self, key: &str) -> u64 {
        self.counts_map
            .read_async(key, |_, v| *v)
            .await
            .unwrap_or(0)
    }

    pub fn update_gauge_diff(
        &self,
        old: &crate::types::GaugeState,
        new: &crate::types::GaugeState,
    ) {
        update_gauge_diff_impl!(self, old, new, update_count);
    }

    pub async fn update_gauge_diff_async(
        &self,
        old: &crate::types::GaugeState,
        new: &crate::types::GaugeState,
    ) {
        update_gauge_diff_impl!(self, old, new, update_count_async, await);
    }

    pub fn update_repo_state<F, T>(
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

    pub async fn update_repo_state_async<F, T>(
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

    pub fn repo_gauge_state(
        repo_state: &RepoState,
        resync_bytes: Option<&[u8]>,
    ) -> crate::types::GaugeState {
        match repo_state.status {
            crate::types::RepoStatus::Synced => crate::types::GaugeState::Synced,
            crate::types::RepoStatus::Backfilling => crate::types::GaugeState::Pending,
            crate::types::RepoStatus::Error(_)
            | crate::types::RepoStatus::Deactivated
            | crate::types::RepoStatus::Takendown
            | crate::types::RepoStatus::Suspended => {
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

    pub async fn repo_gauge_state_async(
        &self,
        repo_state: &RepoState<'_>,
        did_key: &[u8],
    ) -> crate::types::GaugeState {
        let repo_state = repo_state.clone().into_static();
        let did_key = did_key.to_vec();

        let db_resync = self.resync.clone();

        tokio::task::spawn_blocking(move || {
            let resync_bytes_opt = db_resync.get(&did_key).ok().flatten();
            Self::repo_gauge_state(&repo_state, resync_bytes_opt.as_deref())
        })
        .await
        .unwrap_or(crate::types::GaugeState::Resync(None))
    }
}

pub fn set_firehose_cursor(db: &Db, relay: &Url, cursor: i64) -> Result<()> {
    db.cursors
        .insert(keys::firehose_cursor_key(relay), cursor.to_be_bytes())
        .into_diagnostic()
}

pub async fn get_firehose_cursor(db: &Db, relay: &Url) -> Result<Option<i64>> {
    let per_relay_key = keys::firehose_cursor_key(relay);
    if let Some(v) = Db::get(db.cursors.clone(), per_relay_key).await? {
        return Ok(Some(i64::from_be_bytes(
            v.as_ref()
                .try_into()
                .into_diagnostic()
                .wrap_err("cursor is not 8 bytes")?,
        )));
    }

    Db::get(db.cursors.clone(), keys::CURSOR_KEY)
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
