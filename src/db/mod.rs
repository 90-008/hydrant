use crate::types::{BroadcastEvent, RepoState};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Slice};
use futures::FutureExt;
use jacquard::IntoStatic;
use jacquard_common::types::string::Did;
use miette::{Context, IntoDiagnostic, Result};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

pub mod keys;

use std::sync::atomic::AtomicU64;
use tokio::sync::broadcast;
use tracing::error;

pub struct Db {
    pub inner: Arc<Database>,
    pub repos: Keyspace,
    pub records: Keyspace,
    pub blocks: Keyspace,
    pub cursors: Keyspace,
    pub buffer: Keyspace,
    pub pending: Keyspace,
    pub resync: Keyspace,
    pub events: Keyspace,
    pub counts: Keyspace,
    pub event_tx: broadcast::Sender<BroadcastEvent>,
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

        let opts = KeyspaceCreateOptions::default;
        let open_ks = |name: &str, opts: KeyspaceCreateOptions| {
            db.keyspace(name, move || opts).into_diagnostic()
        };

        let repos = open_ks("repos", opts().expect_point_read_hits(true))?;
        let records = open_ks("records", opts().max_memtable_size(32 * 2_u64.pow(20)))?;
        let blocks = open_ks(
            "blocks",
            opts()
                // point reads are used a lot by stream
                .expect_point_read_hits(true)
                .max_memtable_size(32 * 2_u64.pow(20)),
        )?;
        let cursors = open_ks("cursors", opts().expect_point_read_hits(true))?;
        let buffer = open_ks("buffer", opts())?;
        let pending = open_ks("pending", opts())?;
        let resync = open_ks("resync", opts())?;
        let events = open_ks("events", opts())?;
        let counts = open_ks("counts", opts().expect_point_read_hits(true))?;

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

        let (event_tx, _) = broadcast::channel(10000);

        Ok(Self {
            inner: db,
            repos,
            records,
            blocks,
            cursors,
            buffer,
            pending,
            resync,
            events,
            counts,
            event_tx,
            next_event_id: Arc::new(AtomicU64::new(last_id + 1)),
        })
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

    pub fn update_repo_state<F, T>(
        mut batch: OwnedWriteBatch,
        repos: &Keyspace,
        did: &Did<'_>,
        f: F,
    ) -> Result<(Option<(RepoState, T)>, fjall::OwnedWriteBatch)>
    where
        F: FnOnce(&mut RepoState, (&[u8], &mut fjall::OwnedWriteBatch)) -> Result<(bool, T)>,
    {
        let key = keys::repo_key(did);
        if let Some(bytes) = repos.get(key).into_diagnostic()? {
            let mut state: RepoState = deser_repo_state(bytes)?;
            let (changed, result) = f(&mut state, (key, &mut batch))?;
            if changed {
                batch.insert(repos, key, ser_repo_state(&state)?);
            }
            Ok((Some((state, result)), batch))
        } else {
            Ok((None, batch))
        }
    }

    pub async fn update_repo_state_async<F, T>(
        &self,
        did: &Did<'_>,
        f: F,
    ) -> Result<Option<(RepoState, T)>>
    where
        F: FnOnce(&mut RepoState, (&[u8], &mut fjall::OwnedWriteBatch)) -> Result<(bool, T)>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let batch = self.inner.batch();
        let repos = self.repos.clone();
        let did = did.clone().into_static();

        tokio::task::spawn_blocking(move || {
            let (Some((state, t)), batch) = Self::update_repo_state(batch, &repos, &did, f)? else {
                return Ok(None);
            };
            batch.commit().into_diagnostic()?;
            Ok(Some((state, t)))
        })
        .await
        .into_diagnostic()?
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
        Self::check_poisoned(err);
    }
}

pub fn ser_repo_state(state: &RepoState) -> Result<Vec<u8>> {
    rmp_serde::to_vec(&state).into_diagnostic()
}

pub fn deser_repo_state(bytes: impl AsRef<[u8]>) -> Result<RepoState> {
    rmp_serde::from_slice(bytes.as_ref()).into_diagnostic()
}
