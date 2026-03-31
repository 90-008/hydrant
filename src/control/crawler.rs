use std::sync::Arc;

use miette::{IntoDiagnostic, Result};
use tokio::sync::{mpsc, watch};
use tracing::{error, info};
use url::Url;

use crate::db::keys;
use crate::state::AppState;

pub(super) struct ProducerHandle {
    mode: crate::config::CrawlerMode,
    abort: tokio::task::AbortHandle,
}

impl Drop for ProducerHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(super) struct CrawlerShared {
    pub(super) http: reqwest::Client,
    pub(super) checker: crate::crawler::SignalChecker,
    pub(super) in_flight: crate::crawler::InFlight,
    pub(super) tx: mpsc::Sender<crate::crawler::CrawlerBatch>,
    pub(super) stats: crate::crawler::CrawlerStats,
}

/// a snapshot of a single crawler source's runtime state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CrawlerSourceInfo {
    pub url: Url,
    pub mode: crate::config::CrawlerMode,
    /// whether this source is persisted in the database (i.e. it was dynamically added
    /// and will survive restarts). config-sourced entries have `persisted: false`.
    pub persisted: bool,
}

pub(super) fn spawn_crawler_producer(
    source: &crate::config::CrawlerSource,
    http: &reqwest::Client,
    state: &Arc<AppState>,
    checker: &crate::crawler::SignalChecker,
    in_flight: &crate::crawler::InFlight,
    tx: &mpsc::Sender<crate::crawler::CrawlerBatch>,
    stats: &crate::crawler::CrawlerStats,
    enabled: watch::Receiver<bool>,
) -> ProducerHandle {
    use crate::config::CrawlerMode;
    use crate::crawler::{ByCollectionProducer, ListReposProducer};
    use std::time::Duration;
    use tracing::Instrument;

    let abort = match source.mode {
        CrawlerMode::ListRepos => {
            info!(relay = %source.url, enabled = *state.crawler_enabled.borrow(), "starting relay crawler");
            let span = tracing::info_span!("crawl", url = %source.url);
            tokio::spawn(
                ListReposProducer {
                    url: source.url.clone(),
                    checker: checker.clone(),
                    in_flight: in_flight.clone(),
                    tx: tx.clone(),
                    enabled,
                    stats: stats.clone(),
                }
                .run()
                .instrument(span),
            )
            .abort_handle()
        }
        CrawlerMode::ByCollection => {
            info!(
                host = source.url.host_str(),
                enabled = *state.crawler_enabled.borrow(),
                "starting by-collection crawler"
            );
            let span = tracing::info_span!("by_collection", host = source.url.host_str());
            let http = http.clone();
            let state = state.clone();
            let in_flight = in_flight.clone();
            let tx = tx.clone();
            let stats = stats.clone();
            let url = source.url.clone();
            tokio::spawn(
                async move {
                    loop {
                        let producer = ByCollectionProducer {
                            index_url: url.clone(),
                            http: http.clone(),
                            state: state.clone(),
                            in_flight: in_flight.clone(),
                            tx: tx.clone(),
                            enabled: enabled.clone(),
                            stats: stats.clone(),
                        };
                        if let Err(e) = producer.run().await {
                            error!(err = ?e, "by-collection crawler fatal error, restarting in 30s");
                            tokio::time::sleep(Duration::from_secs(30)).await;
                        }
                    }
                }
                .instrument(span),
            )
            .abort_handle()
        }
    };
    ProducerHandle {
        mode: source.mode,
        abort,
    }
}

/// runtime control over the crawler component.
///
/// the crawler walks `com.atproto.sync.listRepos` on each configured relay to discover
/// repositories that have never emitted a firehose event. in `filter` mode it also
/// checks each discovered repo against the configured signal collections before
/// enqueuing it for backfill.
///
/// disabling the crawler does not affect in-progress repo checks. each one completes
/// its current PDS request before pausing.
#[derive(Clone)]
pub struct CrawlerHandle {
    pub(super) state: Arc<AppState>,
    /// set once by [`Hydrant::run`]; `None` means run() has not been called yet.
    pub(super) shared: Arc<std::sync::OnceLock<CrawlerShared>>,
    /// per-source running tasks, keyed by url.
    pub(super) tasks: Arc<scc::HashMap<Url, ProducerHandle>>,
    /// set of urls persisted in the database (dynamically added sources).
    pub(super) persisted: Arc<scc::HashSet<Url>>,
}

impl CrawlerHandle {
    /// enable the crawler (enables all configured producers). no-op if already enabled.
    pub fn enable(&self) {
        self.state.crawler_enabled.send_replace(true);
    }
    /// disable the crawler (disables all configured producers).
    /// in-progress repo checks finish before the crawler pauses.
    pub fn disable(&self) {
        self.state.crawler_enabled.send_replace(false);
    }
    /// returns the current enabled state of the crawler.
    pub fn is_enabled(&self) -> bool {
        *self.state.crawler_enabled.borrow()
    }

    /// delete all cursor entries associated with the given URL.
    pub async fn reset_cursor(&self, url: &str) -> Result<()> {
        let state = self.state.clone();
        let point_keys = [keys::crawler_cursor_key(url)];
        let by_collection_prefix = keys::by_collection_cursor_prefix(url);
        tokio::task::spawn_blocking(move || {
            let mut batch = state.db.inner.batch();
            for k in point_keys {
                batch.remove(&state.db.cursors, k);
            }
            for entry in state.db.cursors.prefix(&by_collection_prefix) {
                let k = entry.key().into_diagnostic()?;
                batch.remove(&state.db.cursors, k);
            }
            batch.commit().into_diagnostic()?;
            state.db.persist()
        })
        .await
        .into_diagnostic()??;
        Ok(())
    }

    /// return info on all currently active crawler sources.
    ///
    /// returns an empty list if called before [`Hydrant::run`].
    pub async fn list_sources(&self) -> Vec<CrawlerSourceInfo> {
        let mut sources = Vec::new();
        self.tasks
            .iter_async(|url, h| {
                sources.push(CrawlerSourceInfo {
                    url: url.clone(),
                    mode: h.mode,
                    persisted: self.persisted.contains_sync(url),
                });
                true
            })
            .await;
        sources
    }

    /// add a new crawler source at runtime.
    ///
    /// the source is persisted to the database and will be re-spawned on restart.
    /// if a source with the same URL already exists, it is replaced (the old task is
    /// aborted and a new one is started with the new mode).
    ///
    /// returns an error if called before [`Hydrant::run`].
    pub async fn add_source(&self, source: crate::config::CrawlerSource) -> Result<()> {
        let Some(shared) = self.shared.get() else {
            miette::bail!("crawler not yet started: call Hydrant::run() first");
        };

        let state = self.state.clone();
        let key = keys::crawler_source_key(source.url.as_str());
        let val = rmp_serde::to_vec(&source.mode).into_diagnostic()?;
        tokio::task::spawn_blocking(move || {
            state.db.crawler.insert(key, val).into_diagnostic()?;
            state.db.persist()
        })
        .await
        .into_diagnostic()??;

        let enabled_rx = self.state.crawler_enabled.subscribe();
        let handle = spawn_crawler_producer(
            &source,
            &shared.http,
            &self.state,
            &shared.checker,
            &shared.in_flight,
            &shared.tx,
            &shared.stats,
            enabled_rx,
        );

        let _ = self.persisted.insert_async(source.url.clone()).await;
        match self.tasks.entry_async(source.url).await {
            scc::hash_map::Entry::Vacant(e) => {
                e.insert_entry(handle);
            }
            scc::hash_map::Entry::Occupied(mut e) => {
                *e.get_mut() = handle;
            }
        }
        Ok(())
    }

    /// remove a crawler source at runtime by URL.
    ///
    /// aborts the running producer task and removes the source from the database if it
    /// was dynamically added. config-sourced entries are aborted but not persisted, so
    /// they will reappear on restart.
    ///
    /// returns `true` if a source with the given URL was found and removed.
    /// returns an error if called before [`Hydrant::run`].
    pub async fn remove_source(&self, url: &Url) -> Result<bool> {
        if self.shared.get().is_none() {
            miette::bail!("crawler not yet started: call Hydrant::run() first");
        }

        // dropping the ProducerHandle aborts the task via Drop
        if self.tasks.remove_async(url).await.is_none() {
            return Ok(false);
        }

        // remove from DB if it was a persisted source
        if self.persisted.remove_async(url).await.is_some() {
            let state = self.state.clone();
            let key = keys::crawler_source_key(url.as_str());
            tokio::task::spawn_blocking(move || {
                state.db.crawler.remove(key).into_diagnostic()?;
                state.db.persist()
            })
            .await
            .into_diagnostic()??;
        }

        Ok(true)
    }
}
