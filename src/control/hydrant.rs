use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use futures::FutureExt;
use miette::{IntoDiagnostic, Result};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::{Config, SignatureVerification};
use crate::db::{self, filter as db_filter, load_persisted_firehose_sources};
use crate::filter::FilterMode;
use crate::state::AppState;

use super::{FilterControl, pds::PdsControl, ReposControl, DbControl};
use super::firehose::{FirehoseHandle, FirehoseShared};

#[cfg(feature = "indexer")]
use crate::backfill::BackfillWorker;
#[cfg(feature = "indexer")]
use crate::db::load_persisted_crawler_sources;
#[cfg(feature = "indexer")]
use crate::ingest::indexer::FirehoseWorker;
#[cfg(feature = "indexer")]
use super::{crawler, BackfillHandle};

#[cfg(feature = "backlinks")]
use crate::backlinks::BacklinksControl;

use super::seed;

pub(crate) mod run;

/// the top-level handle to a hydrant instance.
///
/// `Hydrant` is cheaply cloneable. all sub-handles share the same underlying state.
/// construct it via [`Hydrant::new`] or [`Hydrant::from_env`], configure the filter
/// and repos as needed, then call [`Hydrant::run`] to start all background components.
#[derive(Clone)]
pub struct Hydrant {
    #[cfg(feature = "indexer")]
    pub crawler: crawler::CrawlerHandle,
    pub firehose: FirehoseHandle,
    #[cfg(feature = "indexer")]
    pub backfill: BackfillHandle,
    pub filter: FilterControl,
    pub pds: PdsControl,
    pub repos: ReposControl,
    pub db: DbControl,
    #[cfg(feature = "backlinks")]
    pub backlinks: crate::backlinks::BacklinksControl,
    pub(crate) state: Arc<AppState>,
    pub(crate) config: Arc<Config>,
    pub(crate) started: Arc<AtomicBool>,
    _priv: (),
}

impl Hydrant {
    /// Returns a reference to the internal DID resolver.
    pub fn resolver(&self) -> &crate::resolver::Resolver {
        &self.state.resolver
    }

    /// open the database and configure hydrant from `config`.
    ///
    /// this sets up the database, applies any filter configuration from `config`, and
    /// initializes all sub-handles. no background tasks are started yet: call
    /// [`run`](Self::run) to start all components and drive the instance.
    pub async fn new(config: Config) -> Result<Self> {
        info!("{config}");

        #[cfg(feature = "relay")]
        if config.only_index_links {
            miette::bail!("HYDRANT_ONLY_INDEX_LINKS is not supported in relay mode");
        }

        // 1. open database and construct AppState
        let state = AppState::new(&config)?;

        // 2. apply any filter config from env variables
        if config.full_network
            || config.filter_signals.is_some()
            || config.filter_collections.is_some()
            || config.filter_excludes.is_some()
        {
            let filter_ks = state.db.filter.clone();
            let inner = state.db.inner.clone();
            let mode = config.full_network.then_some(FilterMode::Full);
            let signals = config
                .filter_signals
                .clone()
                .map(crate::patch::SetUpdate::Set);
            let collections = config
                .filter_collections
                .clone()
                .map(crate::patch::SetUpdate::Set);
            let excludes = config
                .filter_excludes
                .clone()
                .map(crate::patch::SetUpdate::Set);

            tokio::task::spawn_blocking(move || {
                let mut batch = inner.batch();
                db_filter::apply_patch(
                    &mut batch,
                    &filter_ks,
                    mode,
                    signals,
                    collections,
                    excludes,
                )?;
                batch.commit().into_diagnostic()
            })
            .await
            .into_diagnostic()??;

            // 3. reload the live filter into the hot-path arc-swap
            let new_filter = tokio::task::spawn_blocking({
                let filter_ks = state.db.filter.clone();
                move || db_filter::load(&filter_ks)
            })
            .await
            .into_diagnostic()??;
            state.filter.store(Arc::new(new_filter));
        }

        #[cfg(feature = "indexer")]
        {
            // 4. set crawler enabled state from config, evaluated against the post-patch filter
            let post_patch_crawler = match config.enable_crawler {
                Some(b) => b,
                None => {
                    state.filter.load().mode == FilterMode::Full
                        || !config.crawler_sources.is_empty()
                }
            };
            state.crawler_enabled.send_replace(post_patch_crawler);
        }

        let state = Arc::new(state);

        Ok(Self {
            firehose: FirehoseHandle::new(state.clone()),
            filter: FilterControl(state.clone()),
            pds: PdsControl(state.clone()),
            repos: ReposControl(state.clone()),
            db: DbControl(state.clone()),
            #[cfg(feature = "indexer")]
            crawler: crawler::CrawlerHandle {
                state: state.clone(),
                shared: Arc::new(std::sync::OnceLock::new()),
                tasks: Arc::new(scc::HashMap::new()),
                persisted: Arc::new(scc::HashSet::new()),
            },
            #[cfg(feature = "indexer")]
            backfill: BackfillHandle::new(state.clone()),
            #[cfg(feature = "backlinks")]
            backlinks: crate::backlinks::BacklinksControl(state.clone()),
            state,
            config: Arc::new(config),
            started: Arc::new(AtomicBool::new(false)),
            _priv: (),
        })
    }

    /// reads config from environment variables and calls [`Hydrant::new`].
    pub async fn from_env() -> Result<Self> {
        Self::new(Config::from_env()?).await
    }

    /// returns a future that runs the HTTP management API server on the given bind addresses.
    ///
    /// the server exposes all management endpoints (`/filter`, `/repos`, `/ingestion`,
    /// `/stream`, `/stats`, `/db/*`, `/xrpc/*`). it runs indefinitely and resolves
    /// only on error.
    ///
    /// intended for `tokio::spawn` or inclusion in a `select!` / task list. the clone
    /// of `self` is deferred until the future is first polled.
    ///
    /// to disable the HTTP API entirely, simply don't call this method.
    pub fn serve(&self, binds: super::ApiBinds) -> impl Future<Output = Result<()>> {
        let hydrant = self.clone();
        async move { crate::api::serve(hydrant, binds).await }
    }

    /// returns a future that runs the debug HTTP API server on `127.0.0.1:{port}`.
    ///
    /// exposes internal inspection endpoints (`/debug/get`, `/debug/iter`, etc.).
    /// binds only to loopback.
    pub fn serve_debug(&self, port: u16) -> impl Future<Output = Result<()>> {
        let state = self.state.clone();
        async move { crate::api::serve_debug(state, port).await }
    }
}

impl axum::extract::FromRef<Hydrant> for Arc<AppState> {
    fn from_ref(h: &Hydrant) -> Self {
        h.state.clone()
    }
}
