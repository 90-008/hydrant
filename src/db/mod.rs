use crate::types::{BroadcastEvent, RepoState};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Slice};
use jacquard::IntoStatic;
use jacquard_common::types::string::Did;
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::{SmolStr, format_smolstr};

use std::sync::Arc;

pub mod keys;
pub mod types;

use std::sync::atomic::AtomicU64;
use tokio::sync::broadcast;
use tracing::error;

pub const RECORDS_PARTITION_PREFIX: &str = "r:";

fn default_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
}

pub struct Db {
    pub inner: Arc<Database>,
    pub repos: Keyspace,
    pub record_partitions: HashMap<String, Keyspace>,
    pub blocks: Keyspace,
    pub cursors: Keyspace,
    pub pending: Keyspace,
    pub resync: Keyspace,
    pub resync_buffer: Keyspace,
    pub events: Keyspace,
    pub counts: Keyspace,
    pub event_tx: broadcast::Sender<BroadcastEvent>,
    pub next_event_id: Arc<AtomicU64>,
    pub counts_map: HashMap<SmolStr, u64>,
    pub record_partition_overrides: Vec<(glob::Pattern, u64)>,
    pub record_partition_default_size: u64,
}

impl Db {
    pub fn open(cfg: &crate::config::Config) -> Result<Self> {
        let db = Database::builder(&cfg.database_path)
            .cache_size(cfg.cache_size * 2_u64.pow(20) / 2)
            .manual_journal_persist(true)
            .journal_compression(
                cfg.disable_lz4_compression
                    .then_some(fjall::CompressionType::None)
                    .unwrap_or(fjall::CompressionType::Lz4),
            )
            .worker_threads(cfg.db_worker_threads)
            .max_journaling_size(cfg.db_max_journaling_size_mb * 1024 * 1024)
            .open()
            .into_diagnostic()?;
        let db = Arc::new(db);

        let opts = default_opts;
        let open_ks = |name: &str, opts: KeyspaceCreateOptions| {
            db.keyspace(name, move || opts).into_diagnostic()
        };

        let repos = open_ks(
            "repos",
            opts()
                .expect_point_read_hits(true)
                .max_memtable_size(cfg.db_repos_memtable_size_mb * 1024 * 1024),
        )?;
        let blocks = open_ks(
            "blocks",
            opts()
                // point reads are used a lot by stream
                .expect_point_read_hits(true)
                .max_memtable_size(cfg.db_blocks_memtable_size_mb * 1024 * 1024),
        )?;
        let cursors = open_ks("cursors", opts().expect_point_read_hits(true))?;
        let pending = open_ks(
            "pending",
            opts().max_memtable_size(cfg.db_pending_memtable_size_mb * 1024 * 1024),
        )?;
        let resync = open_ks("resync", opts())?;
        let resync_buffer = open_ks("resync_buffer", opts())?;
        let events = open_ks(
            "events",
            opts().max_memtable_size(cfg.db_events_memtable_size_mb * 1024 * 1024),
        )?;
        let counts = open_ks("counts", opts().expect_point_read_hits(true))?;

        let record_partitions = HashMap::new();
        {
            let names = db.list_keyspace_names();
            for name in names {
                let name_str: &str = name.as_ref();
                if let Some(collection) = name_str.strip_prefix(RECORDS_PARTITION_PREFIX) {
                    let opts = Self::get_record_partition_opts(cfg, collection);
                    let ks = db.keyspace(name_str, move || opts).into_diagnostic()?;
                    let _ = record_partitions.insert_sync(collection.to_string(), ks);
                }
            }
        }

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

        let (event_tx, _) = broadcast::channel(10000);

        Ok(Self {
            inner: db,
            repos,
            record_partitions,
            blocks,
            cursors,
            pending,
            resync,
            resync_buffer,
            events,
            counts,
            event_tx,
            counts_map,
            next_event_id: Arc::new(AtomicU64::new(last_id + 1)),
            record_partition_overrides: cfg.db_records_partition_overrides.clone(),
            record_partition_default_size: cfg.db_records_default_memtable_size_mb,
        })
    }

    fn get_record_partition_opts(
        cfg: &crate::config::Config,
        collection: &str,
    ) -> KeyspaceCreateOptions {
        let size = cfg
            .db_records_partition_overrides
            .iter()
            .find(|(p, _)| p.matches(collection))
            .map(|(_, s)| *s)
            .unwrap_or(cfg.db_records_default_memtable_size_mb);

        default_opts().max_memtable_size(size * 1024 * 1024)
    }

    pub fn record_partition(&self, collection: &str) -> Result<Keyspace> {
        use scc::hash_map::Entry;
        match self.record_partitions.entry_sync(collection.to_string()) {
            Entry::Occupied(o) => Ok(o.get().clone()),
            Entry::Vacant(v) => {
                let name = format_smolstr!("{}{}", RECORDS_PARTITION_PREFIX, collection);
                let size = self
                    .record_partition_overrides
                    .iter()
                    .find(|(p, _)| p.matches(collection))
                    .map(|(_, s)| *s)
                    .unwrap_or(self.record_partition_default_size);

                let ks = self
                    .inner
                    .keyspace(&name, move || {
                        default_opts().max_memtable_size(size * 1024 * 1024)
                    })
                    .into_diagnostic()?;
                Ok(v.insert_entry(ks).get().clone())
            }
        }
    }

    pub fn persist(&self) -> Result<()> {
        self.inner.persist(PersistMode::SyncAll).into_diagnostic()?;
        Ok(())
    }

    pub async fn get(ks: Keyspace, key: impl AsRef<[u8]>) -> Result<Option<Slice>> {
        let key = key.as_ref().to_vec();
        tokio::task::spawn_blocking(move || ks.get(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    #[allow(dead_code)]
    pub async fn insert(
        ks: Keyspace,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        tokio::task::spawn_blocking(move || ks.insert(key, value).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    #[allow(dead_code)]
    pub async fn remove(ks: Keyspace, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec();
        tokio::task::spawn_blocking(move || ks.remove(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    pub async fn contains_key(ks: Keyspace, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref().to_vec();
        tokio::task::spawn_blocking(move || ks.contains_key(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

    pub fn update_count(&self, key: &str, delta: i64) {
        let mut entry = self.counts_map.entry_sync(SmolStr::new(key)).or_insert(0);
        *entry = (*entry as i64).saturating_add(delta) as u64;
    }

    pub async fn update_count_async(&self, key: &str, delta: i64) {
        let mut entry = self
            .counts_map
            .entry_async(SmolStr::new(key))
            .await
            .or_insert(0);
        *entry = (*entry as i64).saturating_add(delta) as u64;
    }

    pub async fn get_count(&self, key: &str) -> u64 {
        self.counts_map
            .read_async(key, |_, v| *v)
            .await
            .unwrap_or(0)
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
}

pub fn set_firehose_cursor(db: &Db, cursor: i64) -> Result<()> {
    db.cursors
        .insert(keys::CURSOR_KEY, cursor.to_be_bytes())
        .into_diagnostic()
}

pub async fn get_firehose_cursor(db: &Db) -> Result<Option<i64>> {
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
    let new_count = (count as i64).saturating_add(delta) as u64;
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
