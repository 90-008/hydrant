use std::sync::Arc;
use std::sync::atomic::Ordering;

use miette::{Context, IntoDiagnostic, Result};
use tokio::sync::watch;
use tracing::{error, info};
use url::Url;

use crate::db::{self, keys};
use crate::ingest::{BufferTx, firehose::FirehoseIngestor};
use crate::state::AppState;

pub(super) struct FirehoseIngestorHandle {
    abort: tokio::task::AbortHandle,
    pub(super) is_pds: bool,
}

impl Drop for FirehoseIngestorHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(super) struct FirehoseShared {
    pub(super) buffer_tx: BufferTx,
    pub(super) verify_signatures: bool,
}

/// a snapshot of a single firehose relay's runtime state.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FirehoseSourceInfo {
    pub url: Url,
    /// true if added via the API and persisted to the database; false for `RELAY_HOSTS` sources.
    pub persisted: bool,
    /// true when this is a direct PDS connection; enables host authority enforcement.
    pub is_pds: bool,
}

pub(super) async fn spawn_firehose_ingestor(
    relay_url: &Url,
    is_pds: bool,
    state: &Arc<AppState>,
    shared: &FirehoseShared,
    enabled: watch::Receiver<bool>,
) -> Result<FirehoseIngestorHandle> {
    use std::sync::atomic::AtomicI64;

    let start = db::get_firehose_cursor(&state.db, relay_url).await?;
    // insert into relay_cursors if not already present; existing in-memory cursor takes precedence
    let _ = state
        .firehose_cursors
        .insert_async(relay_url.clone(), AtomicI64::new(start.unwrap_or(0)))
        .await;

    info!(relay = %relay_url, is_pds, cursor = ?start, "starting firehose ingestor");

    let ingestor = FirehoseIngestor::new(
        state.clone(),
        shared.buffer_tx.clone(),
        relay_url.clone(),
        is_pds,
        state.filter.clone(),
        enabled,
        shared.verify_signatures,
    );

    let relay_for_log = relay_url.clone();
    let abort = tokio::spawn(async move {
        if let Err(e) = ingestor.run().await {
            error!(relay = %relay_for_log, err = %e, "firehose ingestor exited with error");
        }
    })
    .abort_handle();

    Ok(FirehoseIngestorHandle { abort, is_pds })
}

/// runtime control over the firehose ingestor component.
#[derive(Clone)]
pub struct FirehoseHandle {
    pub(super) state: Arc<AppState>,
    /// set once by [`Hydrant::run`]; `None` means run() has not been called yet.
    pub(super) shared: Arc<std::sync::OnceLock<FirehoseShared>>,
    /// per-relay running tasks, keyed by url.
    pub(super) tasks: Arc<scc::HashMap<Url, FirehoseIngestorHandle>>,
    /// set of urls persisted in the database (dynamically added sources).
    pub(super) persisted: Arc<scc::HashSet<Url>>,
}

impl FirehoseHandle {
    /// enable the firehose. no-op if already enabled.
    pub fn enable(&self) {
        self.state.firehose_enabled.send_replace(true);
    }
    /// disable the firehose. the current message finishes processing before the connection closes.
    pub fn disable(&self) {
        self.state.firehose_enabled.send_replace(false);
    }
    /// returns the current enabled state of the firehose.
    pub fn is_enabled(&self) -> bool {
        *self.state.firehose_enabled.borrow()
    }

    /// reset the stored cursor for the given relay URL.
    ///
    /// clears the `firehose_cursor|{host}|{scheme}` entry from the cursors keyspace and zeroes
    /// the in-memory cursor. the next connection will tail live events from the current head.
    pub async fn reset_cursor(&self, url: &str) -> Result<()> {
        let relay_url = Url::parse(url)
            .into_diagnostic()
            .wrap_err_with(|| format!("invalid relay url: {url:?}"))?;
        let key = keys::firehose_cursor_key_from_url(&relay_url);
        let db = self.state.db.clone();
        tokio::task::spawn_blocking(move || db.cursors.remove(key).into_diagnostic())
            .await
            .into_diagnostic()??;

        self.state.firehose_cursors.peek_with(&relay_url, |_, c| {
            c.store(0, Ordering::SeqCst);
        });
        Ok(())
    }

    /// return info on all currently active firehose sources.
    pub async fn list_sources(&self) -> Vec<FirehoseSourceInfo> {
        let mut sources = Vec::new();
        self.tasks
            .iter_async(|url, handle| {
                sources.push(FirehoseSourceInfo {
                    url: url.clone(),
                    persisted: self.persisted.contains_sync(url),
                    is_pds: handle.is_pds,
                });
                true
            })
            .await;
        sources
    }

    /// add a new firehose relay at runtime.
    ///
    /// the URL is persisted to the database and will be re-spawned on restart. if a relay with
    /// the same URL already exists it is replaced: the running task is stopped and a new one
    /// is started. any cursor state for that URL is preserved.
    ///
    /// returns an error if called before [`Hydrant::run`].
    pub async fn add_source(&self, url: Url, is_pds: bool) -> Result<()> {
        let Some(shared) = self.shared.get() else {
            miette::bail!("firehose not yet started: call Hydrant::run() first");
        };

        let db = self.state.db.clone();
        let key = keys::firehose_source_key(url.as_str());
        let value = rmp_serde::to_vec(&crate::db::FirehoseSourceMeta { is_pds })
            .map_err(|e| miette::miette!("failed to serialize firehose source meta: {e}"))?;
        tokio::task::spawn_blocking(move || db.crawler.insert(key, value).into_diagnostic())
            .await
            .into_diagnostic()??;

        let enabled_rx = self.state.firehose_enabled.subscribe();
        let handle = spawn_firehose_ingestor(&url, is_pds, &self.state, shared, enabled_rx).await?;

        let _ = self.persisted.insert_async(url.clone()).await;
        match self.tasks.entry_async(url).await {
            scc::hash_map::Entry::Vacant(e) => {
                e.insert_entry(handle);
            }
            scc::hash_map::Entry::Occupied(mut e) => {
                *e.get_mut() = handle;
            }
        }
        Ok(())
    }

    /// remove a firehose relay at runtime by URL.
    ///
    /// aborts the running ingestor task. if the source was added via the API it is removed from
    /// the database and will not reappear on restart. `RELAY_HOSTS` sources are only stopped for
    /// the current session; they reappear on the next restart.
    ///
    /// returns `true` if the relay was found and removed, `false` if it was not running.
    /// returns an error if called before [`Hydrant::run`].
    pub async fn remove_source(&self, url: &Url) -> Result<bool> {
        if self.shared.get().is_none() {
            miette::bail!("firehose not yet started: call Hydrant::run() first");
        }

        if self.tasks.remove_async(url).await.is_none() {
            return Ok(false);
        }

        // remove from relay_cursors (persist thread will stop tracking it)
        self.state.firehose_cursors.remove_async(url).await;

        if self.persisted.remove_async(url).await.is_some() {
            let db = self.state.db.clone();
            let key = keys::firehose_source_key(url.as_str());
            tokio::task::spawn_blocking(move || db.crawler.remove(key).into_diagnostic())
                .await
                .into_diagnostic()??;
        }

        Ok(true)
    }
}
