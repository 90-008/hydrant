use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Slice};
use futures::FutureExt;
use miette::{IntoDiagnostic, Result};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

pub mod keys;

use std::sync::atomic::AtomicU64;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Db {
    pub inner: Arc<Database>,
    pub repos: Keyspace,
    pub records: Keyspace,
    pub blocks: Keyspace,
    pub cursors: Keyspace,
    pub buffer: Keyspace,
    pub pending: Keyspace,
    pub errors: Keyspace,
    pub events: Keyspace,
    pub counts: Keyspace,
    pub event_tx: broadcast::Sender<u64>,
    pub next_event_id: Arc<AtomicU64>,
}

impl Db {
    pub fn open(
        path: impl AsRef<Path>,
        cache_size: u64,
        disable_lz4_compression: bool,
    ) -> Result<Self> {
        let db = Database::builder(path.as_ref())
            .cache_size(cache_size * 2_u64.pow(20) / 2)
            .manual_journal_persist(true)
            .journal_compression(
                disable_lz4_compression
                    .then_some(fjall::CompressionType::None)
                    .unwrap_or(fjall::CompressionType::Lz4),
            )
            .open()
            .into_diagnostic()?;
        let db = Arc::new(db);

        let opts = || KeyspaceCreateOptions::default();

        let repos = db.keyspace("repos", opts).into_diagnostic()?;
        let records = db
            .keyspace("records", || opts().max_memtable_size(32 * 2_u64.pow(20)))
            .into_diagnostic()?;

        let block_opts = || {
            opts()
                // point reads are used a lot by stream
                .expect_point_read_hits(true)
                .max_memtable_size(32 * 2_u64.pow(20))
        };
        let blocks = db.keyspace("blocks", block_opts).into_diagnostic()?;
        let cursors = db.keyspace("cursors", opts).into_diagnostic()?;
        let buffer = db.keyspace("buffer", opts).into_diagnostic()?;
        let pending = db.keyspace("pending", opts).into_diagnostic()?;
        let errors = db.keyspace("errors", opts).into_diagnostic()?;
        let events = db.keyspace("events", opts).into_diagnostic()?;
        let counts = db.keyspace("counts", opts).into_diagnostic()?;

        let mut last_id = 0;
        let mut iter = events.iter();
        if let Some(guard) = iter.next_back() {
            let res: Result<fjall::KvPair, _> = guard.into_inner().map_err(|e| miette::miette!(e));
            if let Ok(kv) = res {
                let k = &kv.0;
                let mut buf = [0u8; 8];
                if k.len() == 8 {
                    buf.copy_from_slice(k);
                    last_id = u64::from_be_bytes(buf);
                }
            }
        }

        let (event_tx, _) = broadcast::channel(10000);

        Ok(Self {
            inner: db,
            repos,
            records,
            blocks,
            cursors,
            buffer,
            pending,
            errors,
            events,
            counts,
            event_tx,
            next_event_id: Arc::new(AtomicU64::new(last_id + 1)),
        })
    }

    pub fn persist(&self) -> Result<()> {
        self.inner
            .persist(PersistMode::SyncData)
            .into_diagnostic()?;
        Ok(())
    }

    pub async fn get(ks: Keyspace, key: impl AsRef<[u8]>) -> Result<Option<Slice>> {
        let key = key.as_ref().to_vec();
        tokio::task::spawn_blocking(move || ks.get(key).into_diagnostic())
            .await
            .into_diagnostic()?
    }

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

    pub fn increment_count(
        &self,
        key: impl AsRef<[u8]>,
        delta: i64,
    ) -> impl Future<Output = Result<i64>> + Send + 'static {
        let key = key.as_ref().to_vec();
        let counts = self.counts.clone();
        tokio::task::spawn_blocking(move || {
            let current = counts
                .get(&key)
                .into_diagnostic()?
                .map(|v| {
                    let mut buf = [0u8; 8];
                    if v.len() == 8 {
                        buf.copy_from_slice(&v);
                        i64::from_be_bytes(buf)
                    } else {
                        0
                    }
                })
                .unwrap_or(0);
            let new_val = current.saturating_add(delta);
            counts
                .insert(key, new_val.to_be_bytes())
                .into_diagnostic()?;
            Ok(new_val)
        })
        .map(|res| res.into_diagnostic().flatten())
    }

    pub async fn get_count(&self, key: impl AsRef<[u8]>) -> Result<i64> {
        let key = key.as_ref().to_vec();
        let counts = self.counts.clone();
        tokio::task::spawn_blocking(move || {
            Ok(counts
                .get(&key)
                .into_diagnostic()?
                .map(|v| {
                    let mut buf = [0u8; 8];
                    if v.len() == 8 {
                        buf.copy_from_slice(&v);
                        i64::from_be_bytes(buf)
                    } else {
                        0
                    }
                })
                .unwrap_or(0))
        })
        .await
        .into_diagnostic()
        .flatten()
    }
}
