#![allow(unused_imports)]

#[cfg(feature = "indexer")]
pub(crate) mod crawler;
pub(crate) mod filter;
pub(crate) mod firehose;
pub(crate) mod pds;
pub(crate) mod repos;
mod seed;
pub(crate) mod stream;

#[cfg(feature = "indexer")]
mod indexer;
#[cfg(feature = "indexer")]
pub use indexer::*;

#[cfg(feature = "relay")]
mod relay;
#[cfg(feature = "relay")]
pub use relay::*;

pub use filter::{FilterControl, FilterPatch, FilterSnapshot};
pub use firehose::{FirehoseHandle, FirehoseSourceInfo};
pub use pds::{PdsControl, PdsTierAssignment, PdsTierDefinition};
pub use repos::{ListedRecord, Record, RecordList, RepoHandle, RepoInfo, ReposControl};
use smol_str::{SmolStr, ToSmolStr};

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::{FutureExt, Stream};
use miette::{IntoDiagnostic, Result, WrapErr};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info};

#[cfg(feature = "indexer")]
use crate::backfill::BackfillWorker;
use crate::config::{Config, SignatureVerification};
#[cfg(feature = "indexer")]
use crate::db::load_persisted_crawler_sources;
use crate::db::{self, filter as db_filter, keys, load_persisted_firehose_sources};
use crate::filter::FilterMode;
#[cfg(feature = "indexer")]
use crate::ingest::indexer::FirehoseWorker;
use crate::state::AppState;
use crate::types::MarshallableEvt;

use firehose::{FirehoseShared, spawn_firehose_ingestor};
#[cfg(feature = "indexer")]
use stream::event_stream_thread;
#[cfg(feature = "relay")]
use stream::relay_stream_thread;

/// infromation about a host hydrant is consuming from.
pub struct Host {
    /// hostname of the host.
    pub name: SmolStr,
    /// latest seq hydrant has processed from this host.
    pub seq: i64,
    /// the amount of accounts hydrant has seen from this host.
    pub account_count: u64,
}

/// an event emitted by the hydrant event stream.
///
/// three variants are possible depending on the `type` field:
/// - `"record"`: a repo record was created, updated, or deleted. carries a [`RecordEvt`].
/// - `"identity"`: a DID's handle or PDS changed. carries an [`IdentityEvt`]. ephemeral, not replayable.
/// - `"account"`: a repo's active/inactive status changed. carries an [`AccountEvt`]. ephemeral, not replayable.
///
/// the `id` field is a monotonically increasing sequence number usable as a cursor for [`Hydrant::subscribe`].
pub type Event = MarshallableEvt<'static>;

/// the top-level handle to a hydrant instance.
///
/// `Hydrant` is cheaply cloneable. all sub-handles share the same underlying state.
/// construct it via [`Hydrant::new`] or [`Hydrant::from_env`], configure the filter
/// and repos as needed, then call [`Hydrant::run`] to start all background components.
///
/// # example
///
/// ```rust,no_run
/// use hydrant::control::Hydrant;
///
/// #[tokio::main]
/// async fn main() -> miette::Result<()> {
///     let hydrant = Hydrant::from_env().await?;
///
///     tokio::select! {
///         r = hydrant.run()?        => r,
///         r = hydrant.serve(3000)  => r,
///     }
/// }
/// ```
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
    config: Arc<Config>,
    started: Arc<AtomicBool>,
    _priv: (),
}

impl Hydrant {
    /// open the database and configure hydrant from `config`.
    ///
    /// this sets up the database, applies any filter configuration from `config`, and
    /// initializes all sub-handles. no background tasks are started yet: call
    /// [`run`](Self::run) to start all components and drive the instance.
    pub async fn new(config: Config) -> Result<Self> {
        info!("{config}");

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
            #[cfg(feature = "indexer")]
            crawler: crawler::CrawlerHandle {
                state: state.clone(),
                shared: Arc::new(std::sync::OnceLock::new()),
                tasks: Arc::new(scc::HashMap::new()),
                persisted: Arc::new(scc::HashSet::new()),
            },
            firehose: FirehoseHandle::new(state.clone()),
            #[cfg(feature = "indexer")]
            backfill: BackfillHandle::new(state.clone()),
            filter: FilterControl(state.clone()),
            pds: pds::PdsControl(state.clone()),
            repos: ReposControl(state.clone()),
            db: DbControl(state.clone()),
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

    /// start all background components and return a future that resolves when any
    /// fatal component exits.
    ///
    /// starts the backfill worker, firehose ingestors, crawler, and worker thread.
    /// resolves with `Ok(())` if a fatal component exits cleanly, or `Err(e)` if it
    /// fails. intended for use in `tokio::select!` alongside [`serve`](Self::serve).
    ///
    /// returns an error if called more than once on the same `Hydrant` instance.
    pub fn run(&self) -> Result<impl Future<Output = Result<()>>> {
        let state = self.state.clone();
        let config = self.config.clone();
        #[cfg(feature = "indexer")]
        let crawler = self.crawler.clone();
        let firehose = self.firehose.clone();

        if self.started.swap(true, Ordering::SeqCst) {
            miette::bail!("Hydrant::run() called more than once");
        }

        let fut = async move {
            // raw firehose events from pds/relay to RelayWorker
            let (buffer_tx, buffer_rx) = mpsc::channel::<crate::ingest::IngestMessage>(500);

            // validated IndexerMessages from RelayWorker/backfill to FirehoseWorker
            #[cfg(feature = "indexer")]
            let (indexer_tx, indexer_rx) =
                mpsc::channel::<crate::ingest::indexer::IndexerMessage>(500);

            // 5. spawn the backfill worker (not used in relay mode)
            #[cfg(feature = "indexer")]
            tokio::spawn({
                let state = state.clone();
                BackfillWorker::new(
                    state.clone(),
                    indexer_tx.clone(),
                    config.repo_fetch_timeout,
                    config.backfill_concurrency_limit,
                    matches!(
                        config.verify_signatures,
                        SignatureVerification::Full | SignatureVerification::BackfillOnly
                    ),
                    config.ephemeral,
                    state.backfill_enabled.subscribe(),
                )
                .run()
            });

            // 6. re-queue any repos that lost their backfill state, then start the retry worker
            #[cfg(feature = "indexer")]
            {
                if let Err(e) = tokio::task::spawn_blocking({
                    let state = state.clone();
                    move || crate::backfill::manager::queue_gone_backfills(&state)
                })
                .await
                .into_diagnostic()?
                {
                    error!(err = %e, "failed to queue gone backfills");
                    db::check_poisoned_report(&e);
                }

                std::thread::spawn({
                    let state = state.clone();
                    move || crate::backfill::manager::retry_worker(state)
                });
            }

            // 7. ephemeral GC thread (not used in relay mode)
            #[cfg(feature = "indexer")]
            if config.ephemeral {
                let state = state.clone();
                std::thread::Builder::new()
                    .name("ephemeral-gc".into())
                    .spawn(move || crate::db::ephemeral::ephemeral_ttl_worker(state))
                    .into_diagnostic()?;
            }

            // relay events TTL: relay_events keyspace grows unbounded without pruning
            #[cfg(feature = "relay")]
            {
                let state = state.clone();
                std::thread::Builder::new()
                    .name("relay-events-gc".into())
                    .spawn(move || crate::db::ephemeral::relay_events_ttl_worker(state))
                    .into_diagnostic()?;
            }

            // 8. cursor / counts persist thread
            std::thread::spawn({
                let state = state.clone();
                let persist_interval = config.cursor_save_interval;
                move || loop {
                    std::thread::sleep(persist_interval);

                    state.firehose_cursors.iter_sync(|relay, cursor| {
                        let seq = cursor.load(Ordering::SeqCst);
                        if seq > 0 {
                            if let Err(e) = db::set_firehose_cursor(&state.db, relay, seq) {
                                error!(relay = %relay, err = %e, "failed to save cursor");
                                db::check_poisoned_report(&e);
                            }
                        }
                        true
                    });

                    if let Err(e) = db::persist_counts(&state.db) {
                        error!(err = %e, "failed to persist counts");
                        db::check_poisoned_report(&e);
                    }

                    if let Err(e) = state.db.persist() {
                        error!(err = %e, "db persist failed");
                        db::check_poisoned_report(&e);
                    }
                }
            });

            // 9. events/sec stats ticker
            tokio::spawn({
                let state = state.clone();
                let get_id = |state: &AppState| {
                    #[cfg(feature = "indexer")]
                    let id = state.db.next_event_id.load(Ordering::Relaxed);
                    #[cfg(feature = "relay")]
                    let id = state.db.next_relay_seq.load(Ordering::Relaxed);
                    id
                };
                let mut last_id = get_id(&state);
                let mut last_time = std::time::Instant::now();
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                async move {
                    loop {
                        interval.tick().await;

                        let current_id = get_id(&state);
                        let current_time = std::time::Instant::now();
                        let delta = current_id.saturating_sub(last_id);

                        if delta == 0 {
                            debug!("no new events in 60s");
                            continue;
                        }

                        let elapsed = current_time.duration_since(last_time).as_secs_f64();
                        let rate = if elapsed > 0.0 {
                            delta as f64 / elapsed
                        } else {
                            0.0
                        };
                        info!("{rate:.2} events/s ({delta} events in {elapsed:.1}s)");

                        last_id = current_id;
                        last_time = current_time;
                    }
                }
            });

            let (fatal_tx_inner, mut fatal_rx) = watch::channel(None);
            let fatal_tx = Arc::new(fatal_tx_inner);

            // 10. set shared and spawn firehose ingestors
            firehose
                .shared
                .set(FirehoseShared {
                    buffer_tx: buffer_tx.clone(),
                    verify_signatures: matches!(
                        config.verify_signatures,
                        SignatureVerification::Full
                    ),
                })
                .ok()
                .expect("firehose shared already set");
            let fire_shared = firehose.shared.get().unwrap();

            let relay_hosts = config.relays.clone();
            if !relay_hosts.is_empty() {
                info!(
                    relay_count = relay_hosts.len(),
                    hosts = relay_hosts
                        .iter()
                        .map(|h| h.url.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    "starting firehose ingestor(s)"
                );
                for source in &relay_hosts {
                    let enabled_rx = state.firehose_enabled.subscribe();
                    let handle = spawn_firehose_ingestor(
                        &source.url,
                        source.is_pds,
                        &state,
                        fire_shared,
                        enabled_rx,
                    )
                    .await?;
                    let _ = firehose
                        .tasks
                        .insert_async(source.url.clone(), handle)
                        .await;
                }
            }

            let persisted_sources = tokio::task::spawn_blocking({
                let state = state.clone();
                move || load_persisted_firehose_sources(&state.db)
            })
            .await
            .into_diagnostic()??;

            for source in &persisted_sources {
                let _ = firehose.persisted.insert_async(source.url.clone()).await;
                if firehose.tasks.contains_async(&source.url).await {
                    continue;
                }
                let enabled_rx = state.firehose_enabled.subscribe();
                let handle = spawn_firehose_ingestor(
                    &source.url,
                    source.is_pds,
                    &state,
                    fire_shared,
                    enabled_rx,
                )
                .await?;
                let _ = firehose
                    .tasks
                    .insert_async(source.url.clone(), handle)
                    .await;
            }

            // 10c. seed firehose PDS sources from listHosts on configured seed URLs
            if !config.seed_hosts.is_empty() {
                let seed_urls = config.seed_hosts.clone();
                let firehose = firehose.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    seed::seed_from_list_hosts(&seed_urls, &firehose, &state).await;
                });
            }

            // 11. spawn crawler infrastructure
            #[cfg(feature = "indexer")]
            {
                use crate::crawler::{
                    CrawlerStats, CrawlerWorker, InFlight, RetryProducer, SignalChecker,
                };
                use crate::util::throttle::Throttler;

                let http = reqwest::Client::builder()
                    .user_agent(concat!(
                        env!("CARGO_PKG_NAME"),
                        "/",
                        env!("CARGO_PKG_VERSION")
                    ))
                    .gzip(true)
                    .build()
                    .expect("that reqwest will build");
                let pds_throttler = state.throttler.clone();
                let in_flight = InFlight::new();
                let stats = CrawlerStats::new(
                    state.clone(),
                    config
                        .crawler_sources
                        .iter()
                        .map(|s| s.url.clone())
                        .collect(),
                    pds_throttler.clone(),
                );
                let checker = SignalChecker {
                    http: http.clone(),
                    state: state.clone(),
                    throttler: pds_throttler,
                };

                info!(
                    max_pending = config.crawler_max_pending_repos,
                    resume_pending = config.crawler_resume_pending_repos,
                    enabled = *state.crawler_enabled.borrow(),
                    "starting crawler worker"
                );
                let (worker, tx) = CrawlerWorker::new(
                    state.clone(),
                    config.crawler_max_pending_repos,
                    config.crawler_resume_pending_repos,
                    stats.clone(),
                );
                tokio::spawn(async move {
                    worker.run().await;
                    error!("crawler worker exited unexpectedly, aborting");
                    std::process::abort();
                });

                let ticker = tokio::spawn(stats.clone().task());
                tokio::spawn(async move {
                    match ticker.await {
                        Err(e) => error!(err = ?e, "stats ticker panicked, aborting"),
                        Ok(()) => error!("stats ticker exited unexpectedly, aborting"),
                    }
                    std::process::abort();
                });

                tokio::spawn(
                    RetryProducer {
                        checker: checker.clone(),
                        in_flight: in_flight.clone(),
                        tx: tx.clone(),
                    }
                    .run(),
                );

                // set shared objects so CrawlerHandle methods can use them
                crawler
                    .shared
                    .set(crawler::CrawlerShared {
                        http,
                        checker,
                        in_flight,
                        tx,
                        stats,
                    })
                    .ok()
                    .expect("crawler shared already set");
                let shared = crawler.shared.get().unwrap();

                // spawn initial sources from config
                for source in config.crawler_sources.iter() {
                    let enabled_rx = state.crawler_enabled.subscribe();
                    let handle = crawler::spawn_crawler_producer(
                        source,
                        &shared.http,
                        &state,
                        &shared.checker,
                        &shared.in_flight,
                        &shared.tx,
                        &shared.stats,
                        enabled_rx,
                    );
                    let _ = crawler.tasks.insert_async(source.url.clone(), handle).await;
                }

                let persisted_sources = tokio::task::spawn_blocking({
                    let state = state.clone();
                    move || load_persisted_crawler_sources(&state.db)
                })
                .await
                .into_diagnostic()??;

                for source in &persisted_sources {
                    let _ = crawler.persisted.insert_async(source.url.clone()).await;
                    if crawler.tasks.contains_async(&source.url).await {
                        continue;
                    }
                    let enabled_rx = state.crawler_enabled.subscribe();
                    let handle = crawler::spawn_crawler_producer(
                        source,
                        &shared.http,
                        &state,
                        &shared.checker,
                        &shared.in_flight,
                        &shared.tx,
                        &shared.stats,
                        enabled_rx,
                    );
                    let _ = crawler.tasks.insert_async(source.url.clone(), handle).await;
                }
            }

            // 12. spawn the relay worker
            let relay_worker = std::thread::spawn({
                let state = state.clone();
                let handle = tokio::runtime::Handle::current();
                let config = config.clone();

                #[cfg(feature = "indexer")]
                let hook = indexer_tx.clone();

                move || {
                    crate::ingest::relay::RelayWorker::new(
                        state,
                        buffer_rx,
                        #[cfg(feature = "indexer")]
                        hook,
                        matches!(config.verify_signatures, SignatureVerification::Full),
                        config.firehose_workers,
                        crate::ingest::validation::ValidationOptions {
                            verify_mst: config.verify_mst,
                            rev_clock_skew_secs: config.rev_clock_skew_secs,
                        },
                    )
                    .run(handle)
                }
            });

            let tx = Arc::clone(&fatal_tx);
            tokio::spawn(
                tokio::task::spawn_blocking(move || {
                    relay_worker
                        .join()
                        .map_err(|e| miette::miette!("relay worker died: {e:?}"))
                })
                .map(move |r| {
                    let result = r.into_diagnostic().flatten().flatten();
                    let _ = tx.send(Some(result.map_err(|e| e.to_string())));
                }),
            );

            // 13. spawn the firehose worker (if enabled)
            #[cfg(feature = "indexer")]
            let firehose_worker = std::thread::spawn({
                let state = state.clone();
                let handle = tokio::runtime::Handle::current();
                let config = config.clone();
                move || {
                    FirehoseWorker::new(
                        state,
                        indexer_rx,
                        config.ephemeral,
                        config.firehose_workers,
                    )
                    .run(handle)
                }
            });

            #[cfg(feature = "indexer")]
            {
                let tx = Arc::clone(&fatal_tx);
                tokio::spawn(
                    tokio::task::spawn_blocking(move || {
                        firehose_worker
                            .join()
                            .map_err(|e| miette::miette!("firehose worker died: {e:?}"))
                    })
                    .map(move |r| {
                        let result = r.into_diagnostic().flatten().flatten();
                        let _ = tx.send(Some(result.map_err(|e| e.to_string())));
                    }),
                );
            }

            // drop the local fatal_tx so the watch channel is only kept alive by the
            // spawned tasks. when all fatal tasks exit (and drop their tx clones),
            // fatal_rx.changed() returns Err and we return Ok(()).
            drop(fatal_tx);

            loop {
                match fatal_rx.changed().await {
                    Ok(()) => {
                        if let Some(result) = fatal_rx.borrow().clone() {
                            return result.map_err(|s| miette::miette!("{s}"));
                        }
                    }
                    // all fatal_tx clones dropped: all tasks finished cleanly
                    Err(_) => return Ok(()),
                }
            }
        };
        Ok(fut)
    }

    /// return database counts and on-disk sizes for all keyspaces.
    ///
    /// counts include: `repos`, `pending`, `resync`, `records`, `blocks`, `events`,
    /// `error_ratelimited`, `error_transport`, `error_generic`.
    ///
    /// sizes are in bytes, reported per keyspace.
    pub async fn stats(&self) -> Result<StatsResponse> {
        let state = self.state.clone();

        #[allow(unused_mut)]
        let mut count_keys = vec![
            "repos",
            "error_ratelimited",
            "error_transport",
            "error_generic",
        ];

        #[cfg(feature = "indexer")]
        {
            count_keys.push("pending");
            count_keys.push("records");
            count_keys.push("blocks");
            count_keys.push("resync");
        }

        let mut counts: BTreeMap<&'static str, u64> =
            futures::future::join_all(count_keys.into_iter().map(|name| {
                let state = state.clone();
                async move { (name, state.db.get_count(name).await) }
            }))
            .await
            .into_iter()
            .collect();

        #[cfg(feature = "indexer")]
        counts.insert("events", state.db.events.approximate_len() as u64);

        #[cfg(feature = "relay")]
        counts.insert(
            "relay_events",
            state.db.relay_events.approximate_len() as u64,
        );

        let sizes = tokio::task::spawn_blocking(move || {
            let mut s = BTreeMap::new();
            s.insert("repos", state.db.repos.disk_space());
            s.insert("cursors", state.db.cursors.disk_space());
            s.insert("counts", state.db.counts.disk_space());
            s.insert("filter", state.db.filter.disk_space());
            s.insert("crawler", state.db.crawler.disk_space());

            #[cfg(feature = "indexer")]
            {
                s.insert("records", state.db.records.disk_space());
                s.insert("blocks", state.db.blocks.disk_space());
                s.insert("pending", state.db.pending.disk_space());
                s.insert("resync", state.db.resync.disk_space());
                s.insert("resync_buffer", state.db.resync_buffer.disk_space());
                s.insert("events", state.db.events.disk_space());
            }

            #[cfg(feature = "relay")]
            s.insert("relay_events", state.db.relay_events.disk_space());

            #[cfg(feature = "backlinks")]
            s.insert("backlinks", state.db.backlinks.disk_space());

            s
        })
        .await
        .into_diagnostic()?;

        Ok(StatsResponse { counts, sizes })
    }

    /// returns a future that runs the HTTP management API server on `0.0.0.0:{port}`.
    ///
    /// the server exposes all management endpoints (`/filter`, `/repos`, `/ingestion`,
    /// `/stream`, `/stats`, `/db/*`, `/xrpc/*`). it runs indefinitely and resolves
    /// only on error.
    ///
    /// intended for `tokio::spawn` or inclusion in a `select!` / task list. the clone
    /// of `self` is deferred until the future is first polled.
    ///
    /// to disable the HTTP API entirely, simply don't call this method.
    pub fn serve(&self, port: u16) -> impl Future<Output = Result<()>> {
        let hydrant = self.clone();
        async move { crate::api::serve(hydrant, port).await }
    }

    /// returns a future that runs the debug HTTP API server on `127.0.0.1:{port}`.
    ///
    /// exposes internal inspection endpoints (`/debug/get`, `/debug/iter`, etc.).
    /// binds only to loopback.
    pub fn serve_debug(&self, port: u16) -> impl Future<Output = Result<()>> {
        let state = self.state.clone();
        async move { crate::api::serve_debug(state, port).await }
    }

    /// get the status of a (firehose) host we are consuming from.
    ///
    /// returns the seq we are on for this host.
    pub async fn get_host_status(&self, hostname: &str) -> Result<Option<Host>> {
        let state = self.state.clone();
        let hostname = hostname.to_smolstr();

        tokio::task::spawn_blocking(move || {
            let key = keys::firehose_cursor_key(&hostname);
            let Some(seq) = state.db.cursors.get(&key).into_diagnostic()? else {
                return Ok(None);
            };
            let seq = i64::from_be_bytes(
                seq.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("cursor value is not 8 bytes")?,
            );
            let account_count = state
                .db
                .get_count_sync(&keys::pds_account_count_key(&hostname));

            Ok(Some(Host {
                name: hostname.into(),
                seq,
                account_count,
            }))
        })
        .await
        .into_diagnostic()?
    }

    /// enumerates all hosts hydrant is consuming from.
    ///
    /// returns hosts enumerated in this pagination and the cursor to paginate from.
    pub async fn list_hosts(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Host>, Option<SmolStr>)> {
        let state = self.state.clone();
        let cursor = cursor.map(str::to_string);

        tokio::task::spawn_blocking(move || {
            let prefix_end = {
                let mut end = keys::FIREHOSE_CURSOR_PREFIX.to_vec();
                *end.last_mut().unwrap() += 1;
                end
            };
            let start_bound = match cursor.as_deref() {
                Some(host) => std::ops::Bound::Excluded(keys::firehose_cursor_key(host)),
                None => std::ops::Bound::Included(keys::FIREHOSE_CURSOR_PREFIX.to_vec()),
            };

            // fetch one extra item to detect whether there is a next page
            let mut hosts: Vec<Host> = Vec::with_capacity(limit + 1);
            for item in state
                .db
                .cursors
                .range((start_bound, std::ops::Bound::Excluded(prefix_end)))
                .take(limit + 1)
            {
                let (k, v) = item.into_inner().into_diagnostic()?;
                let hostname = std::str::from_utf8(&k[keys::FIREHOSE_CURSOR_PREFIX.len()..])
                    .into_diagnostic()
                    .wrap_err("firehose cursor key contains non-utf8 hostname")?;
                let seq = i64::from_be_bytes(
                    v.as_ref()
                        .try_into()
                        .into_diagnostic()
                        .wrap_err("cursor value is not 8 bytes")?,
                );
                let account_count = state
                    .db
                    .get_count_sync(&keys::pds_account_count_key(hostname));
                hosts.push(Host {
                    name: hostname.into(),
                    seq,
                    account_count,
                });
            }

            let next_cursor = if hosts.len() > limit {
                hosts.pop();
                hosts.last().map(|h| h.name.clone())
            } else {
                None
            };

            Ok((hosts, next_cursor))
        })
        .await
        .into_diagnostic()?
    }
}

impl axum::extract::FromRef<Hydrant> for Arc<AppState> {
    fn from_ref(h: &Hydrant) -> Self {
        h.state.clone()
    }
}

/// database statistics returned by [`Hydrant::stats`].
#[derive(serde::Serialize)]
pub struct StatsResponse {
    /// record counts per logical category (repos, records, events, error kinds, etc.)
    pub counts: BTreeMap<&'static str, u64>,
    /// on-disk size in bytes per keyspace
    pub sizes: BTreeMap<&'static str, u64>,
}

/// control over database maintenance operations.
///
/// all methods pause the crawler, firehose, and backfill worker for the duration
/// of the operation and restore their prior state on completion, whether or not
/// the operation succeeds.
#[derive(Clone)]
pub struct DbControl(Arc<AppState>);

impl DbControl {
    /// trigger a major compaction of all keyspaces in parallel.
    ///
    /// compaction reclaims disk space from deleted/updated keys and improves
    /// read performance. can take several minutes on large datasets.
    pub async fn compact(&self) -> Result<()> {
        let state = self.0.clone();
        state
            .with_ingestion_paused(async || state.db.compact().await)
            .await
    }

    /// train zstd compression dictionaries for the `repos`, `blocks`, and `events` keyspaces.
    ///
    /// dictionaries are written to `dict_{name}.bin` files inside the database folder.
    /// a restart is required to apply them. training samples data blocks from the
    /// existing database, so the database must have a reasonable amount of data first.
    pub async fn train_dicts(&self) -> Result<()> {
        let state = self.0.clone();
        state
            .with_ingestion_paused(async || {
                let train = |name: &'static str| {
                    let state = state.clone();
                    tokio::task::spawn_blocking(move || state.db.train_dict(name))
                        .map(|res: Result<_, _>| res.into_diagnostic().flatten())
                };
                tokio::try_join!(train("repos"), train("blocks"), train("events")).map(|_| ())
            })
            .await
    }
}
