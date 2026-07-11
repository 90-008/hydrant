use crate::types::{RepoMetadata, RepoState};

#[cfg(feature = "indexer_stream")]
use crate::types::BroadcastEvent;
#[cfg(feature = "jetstream")]
use crate::types::JetstreamBroadcast;
#[cfg(feature = "relay")]
use crate::types::RelayBroadcast;

use fjall::{Database, Keyspace, PersistMode, Slice};
use miette::{Context, IntoDiagnostic, Result};
use scc::HashMap;
use smol_str::SmolStr;

use std::collections::BTreeSet;
#[cfg(feature = "jetstream")]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use url::Url;

pub mod compaction;
pub mod counts;
#[cfg(feature = "indexer")]
pub(crate) use counts::CountDeltaReservation;
pub use counts::{CountDeltas, load_count_delta_watermark, set_ks_count};
pub mod ephemeral;
pub mod filter;
pub mod keys;
#[cfg(feature = "indexer")]
mod lifecycle_counts;
pub mod migration;
pub mod pds_meta;
pub mod types;

mod open;
mod train;

#[cfg(feature = "indexer")]
pub(crate) use lifecycle_counts::LifecycleCountBatch;

use tracing::error;

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
use tokio::sync::broadcast;

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
    #[cfg(feature = "jetstream")]
    pub(crate) jetstream_events: Keyspace,
    #[cfg(feature = "backlinks")]
    pub backlinks: Keyspace,
    #[cfg(feature = "indexer_stream")]
    pub(crate) event_tx: broadcast::Sender<BroadcastEvent>,
    #[cfg(feature = "indexer_stream")]
    pub next_event_id: Arc<AtomicU64>,
    #[cfg(feature = "jetstream")]
    pub(crate) jetstream_tx: broadcast::Sender<JetstreamBroadcast>,
    #[cfg(feature = "jetstream")]
    pub(crate) next_jetstream_id: Arc<AtomicU64>,
    #[cfg(feature = "jetstream")]
    pub(crate) last_jetstream_time_us: Arc<AtomicI64>,
    #[cfg(all(feature = "jetstream", feature = "relay"))]
    pub(crate) jetstream_lock: Arc<parking_lot::Mutex<()>>,
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
    #[cfg(feature = "indexer")]
    lifecycle_count_lock: Arc<Mutex<()>>,
    pub(crate) compaction_running: Arc<std::sync::atomic::AtomicBool>,
}

impl Db {
    pub fn persist(&self) -> Result<()> {
        #[cfg(not(feature = "__persist_sync_all"))]
        const MODE: PersistMode = PersistMode::Buffer;
        #[cfg(feature = "__persist_sync_all")]
        const MODE: PersistMode = PersistMode::SyncAll;
        self.inner.persist(MODE).into_diagnostic()?;
        Ok(())
    }

    pub async fn compact(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self
            .compaction_running
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(miette::miette!("compaction already in progress"));
        }
        struct Guard(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _guard = Guard(self.compaction_running.clone());

        let compact = |ks: Keyspace| async move {
            tokio::task::spawn_blocking(move || ks.major_compact().into_diagnostic())
                .await
                .into_diagnostic()?
        };

        #[cfg_attr(not(any(feature = "indexer", feature = "indexer_stream", feature = "jetstream", feature = "relay", feature = "backlinks")), allow(unused_mut))]
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

        #[cfg(feature = "jetstream")]
        tasks.push(compact(self.jetstream_events.clone()));

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

#[cfg(feature = "indexer")]
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
    use crate::config::Config;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn test_db_compact_concurrency_guard() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let mut cfg = Config::default();
        cfg.database_path = tmp.path().to_path_buf();

        let db = Db::open(&cfg)?;

        // Manually mark compaction as running
        db.compaction_running.store(true, Ordering::SeqCst);

        // Attempting to compact should now fail
        let res = db.compact().await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("already in progress"));

        // Release the lock
        db.compaction_running.store(false, Ordering::SeqCst);

        // Compacting should now succeed
        let res = db.compact().await;
        assert!(res.is_ok());

        Ok(())
    }
}
