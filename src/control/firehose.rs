use std::sync::Arc;

use miette::{IntoDiagnostic, Result};
use tokio::sync::watch;
use tracing::{error, info};
use url::Url;

use crate::config::FirehoseSource;
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
    pub(super) fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            shared: Arc::new(std::sync::OnceLock::new()),
            tasks: Arc::new(scc::HashMap::new()),
            persisted: Arc::new(scc::HashSet::new()),
        }
    }

    pub(super) async fn spawn_firehose_ingestor(
        &self,
        source: &FirehoseSource,
        shared: &FirehoseShared,
    ) -> Result<()> {
        use std::sync::atomic::AtomicI64;
        let state = &self.state;

        let start = db::get_firehose_cursor(&state.db, &source.url).await?;
        // insert into relay_cursors if not already present; existing in-memory cursor takes precedence
        let _ = state
            .firehose_cursors
            .insert_async(source.url.clone(), AtomicI64::new(start.unwrap_or(0)))
            .await;

        info!(relay = %source.url, source.is_pds, cursor = ?start, "starting firehose ingestor");

        let enabled = state.firehose_enabled.subscribe();
        let ingestor = FirehoseIngestor::new(
            state.clone(),
            shared.buffer_tx.clone(),
            source.url.clone(),
            source.is_pds,
            state.filter.clone(),
            enabled,
            shared.verify_signatures,
        )
        .await;

        let abort = tokio::spawn({
            let relay_url = source.url.clone();
            let tasks = self.tasks.clone();
            async move {
                if let Err(e) = ingestor.run().await {
                    error!(relay = %relay_url, err = %e, "firehose ingestor exited with error");
                } else {
                    // remove from tasks since we shutdown
                    tasks.remove_async(&relay_url).await;
                    info!(relay = %relay_url, "firehose shut down!");
                }
            }
        })
        .abort_handle();

        let handle = FirehoseIngestorHandle {
            abort,
            is_pds: source.is_pds,
        };
        self.tasks.upsert_async(source.url.clone(), handle).await;

        Ok(())
    }

    /// enable firehose ingestion, no-op if already enabled.
    pub fn enable(&self) {
        self.state.firehose_enabled.send_replace(true);
    }
    /// disable firehose ingestion, in-flight messages complete before pausing.
    pub fn disable(&self) {
        self.state.firehose_enabled.send_replace(false);
    }
    /// returns the current enabled state of firehose ingestion.
    pub fn is_enabled(&self) -> bool {
        *self.state.firehose_enabled.borrow()
    }

    /// list all currently active firehose sources.
    pub async fn list_sources(&self) -> Vec<FirehoseSourceInfo> {
        let mut out = Vec::new();
        self.tasks
            .any_async(|url, handle| {
                out.push(FirehoseSourceInfo {
                    url: url.clone(),
                    persisted: self.persisted.contains_sync(url),
                    is_pds: handle.is_pds,
                });
                false
            })
            .await;
        out
    }

    /// add a new firehose source at runtime, persisting it to the database.
    ///
    /// if a source with the same URL already exists, it is replaced: the
    /// running task is stopped and a new one is started with the new `is_pds`
    /// setting. existing cursor state for the URL is preserved.
    pub async fn add_source(&self, url: Url, is_pds: bool) -> Result<()> {
        let shared = self
            .shared
            .get()
            .ok_or_else(|| miette::miette!("firehose worker not started"))?;

        // persist to db first
        let key = keys::firehose_source_key(url.as_str());
        tokio::task::spawn_blocking({
            let state = self.state.clone();
            move || {
                let mut batch = state.db.inner.batch();
                let value = rmp_serde::to_vec(&db::FirehoseSourceMeta { is_pds }).map_err(|e| {
                    miette::miette!("failed to serialize firehose source meta: {e}")
                })?;
                batch.insert(&state.db.crawler, key, &value);
                batch.commit().into_diagnostic()?;
                state.db.persist()
            }
        })
        .await
        .into_diagnostic()??;

        let _ = self.persisted.insert_async(url.clone()).await;

        self.spawn_firehose_ingestor(&FirehoseSource { url, is_pds }, shared)
            .await?;

        Ok(())
    }

    /// remove a firehose source at runtime.
    ///
    /// returns `true` if the source was found and removed, `false` otherwise.
    /// if the source was added via the API, it is removed from the database;
    /// if it came from the static config, only the running task is stopped.
    pub async fn remove_source(&self, url: &Url) -> Result<bool> {
        if self.persisted.contains_async(url).await {
            let url_str = url.to_string();
            tokio::task::spawn_blocking({
                let state = self.state.clone();
                move || {
                    state
                        .db
                        .crawler
                        .remove(keys::firehose_source_key(&url_str))
                        .into_diagnostic()?;
                    state.db.persist()
                }
            })
            .await
            .into_diagnostic()??;
            self.persisted.remove_async(url).await;
        }

        Ok(self.tasks.remove_async(url).await.is_some())
    }

    /// reset the stored firehose cursor for a given URL.
    pub async fn reset_cursor(&self, url: &str) -> Result<()> {
        let url = Url::parse(url).into_diagnostic()?;
        let key = keys::firehose_cursor_key_from_url(&url);
        tokio::task::spawn_blocking({
            let state = self.state.clone();
            move || {
                state.db.cursors.remove(key).into_diagnostic()?;
                state.db.persist()
            }
        })
        .await
        .into_diagnostic()??;

        self.state.firehose_cursors.remove_async(&url).await;

        Ok(())
    }
}
