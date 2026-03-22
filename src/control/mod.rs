mod crawler;
mod filter;
mod firehose;
mod repos;
mod stream;

pub use crawler::{CrawlerHandle, CrawlerSourceInfo};
pub use filter::{FilterControl, FilterPatch, FilterSnapshot};
pub use firehose::{FirehoseHandle, FirehoseSourceInfo};
pub(crate) use repos::repo_state_to_info;
pub use repos::{ListedRecord, Record, RecordList, RepoHandle, RepoInfo, ReposControl};

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::{FutureExt, Stream};
use miette::{IntoDiagnostic, Result};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info};

use crate::backfill::BackfillWorker;
use crate::config::{Config, SignatureVerification};
use crate::db::{
    self, filter as db_filter, load_persisted_crawler_sources, load_persisted_firehose_sources,
};
use crate::filter::FilterMode;
use crate::ingest::worker::FirehoseWorker;
use crate::state::AppState;
use crate::types::MarshallableEvt;

use crawler::{CrawlerShared, spawn_crawler_producer};
use firehose::{FirehoseShared, spawn_firehose_ingestor};
use stream::event_stream_thread;

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
    pub crawler: CrawlerHandle,
    pub firehose: FirehoseHandle,
    pub backfill: BackfillHandle,
    pub filter: FilterControl,
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
                .map(crate::filter::SetUpdate::Set);
            let collections = config
                .filter_collections
                .clone()
                .map(crate::filter::SetUpdate::Set);
            let excludes = config
                .filter_excludes
                .clone()
                .map(crate::filter::SetUpdate::Set);

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

        // 4. set crawler enabled state from config, evaluated against the post-patch filter
        let post_patch_crawler = match config.enable_crawler {
            Some(b) => b,
            None => {
                state.filter.load().mode == FilterMode::Full || !config.crawler_sources.is_empty()
            }
        };
        state.crawler_enabled.send_replace(post_patch_crawler);

        let state = Arc::new(state);

        Ok(Self {
            crawler: CrawlerHandle {
                state: state.clone(),
                shared: Arc::new(std::sync::OnceLock::new()),
                tasks: Arc::new(scc::HashMap::new()),
                persisted: Arc::new(scc::HashSet::new()),
            },
            firehose: FirehoseHandle {
                state: state.clone(),
                shared: Arc::new(std::sync::OnceLock::new()),
                tasks: Arc::new(scc::HashMap::new()),
                persisted: Arc::new(scc::HashSet::new()),
            },
            backfill: BackfillHandle(state.clone()),
            filter: FilterControl(state.clone()),
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
        let crawler = self.crawler.clone();
        let firehose = self.firehose.clone();

        if self.started.swap(true, Ordering::SeqCst) {
            miette::bail!("Hydrant::run() called more than once");
        }

        let fut = async move {
            // internal buffered channel between ingestors / backfill and the firehose worker
            let (buffer_tx, buffer_rx) = mpsc::unbounded_channel();

            // 5. spawn the backfill worker
            tokio::spawn({
                let state = state.clone();
                BackfillWorker::new(
                    state.clone(),
                    buffer_tx.clone(),
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

            // 7. ephemeral GC thread
            if config.ephemeral {
                let state = state.clone();
                std::thread::Builder::new()
                    .name("ephemeral-gc".into())
                    .spawn(move || crate::db::ephemeral::ephemeral_ttl_worker(state))
                    .into_diagnostic()?;
            }

            // 8. cursor / counts persist thread
            std::thread::spawn({
                let state = state.clone();
                let persist_interval = config.cursor_save_interval;
                move || loop {
                    std::thread::sleep(persist_interval);

                    state.relay_cursors.iter_sync(|relay, cursor| {
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
                let mut last_id = state.db.next_event_id.load(Ordering::Relaxed);
                let mut last_time = std::time::Instant::now();
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                async move {
                    loop {
                        interval.tick().await;

                        let current_id = state.db.next_event_id.load(Ordering::Relaxed);
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

            info!(
                crawler_enabled = *state.crawler_enabled.borrow(),
                firehose_enabled = *state.firehose_enabled.borrow(),
                filter_mode = ?state.filter.load().mode,
                "starting ingestion"
            );

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
                        .map(|h| h.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    "starting firehose ingestor(s)"
                );
                for relay_url in &relay_hosts {
                    let enabled_rx = state.firehose_enabled.subscribe();
                    let handle =
                        spawn_firehose_ingestor(relay_url, &state, fire_shared, enabled_rx).await?;
                    let _ = firehose.tasks.insert_async(relay_url.clone(), handle).await;
                }
            }

            let persisted_relay_urls = tokio::task::spawn_blocking({
                let state = state.clone();
                move || load_persisted_firehose_sources(&state.db)
            })
            .await
            .into_diagnostic()??;

            for relay_url in &persisted_relay_urls {
                let _ = firehose.persisted.insert_async(relay_url.clone()).await;
                if firehose.tasks.contains_async(relay_url).await {
                    continue;
                }
                let enabled_rx = state.firehose_enabled.subscribe();
                let handle =
                    spawn_firehose_ingestor(relay_url, &state, fire_shared, enabled_rx).await?;
                let _ = firehose.tasks.insert_async(relay_url.clone(), handle).await;
            }

            // 11. spawn crawler infrastructure (always, to support dynamic source management)
            {
                use crate::crawler::throttle::Throttler;
                use crate::crawler::{
                    CrawlerStats, CrawlerWorker, InFlight, RetryProducer, SignalChecker,
                };

                let http = reqwest::Client::builder()
                    .user_agent(concat!(
                        env!("CARGO_PKG_NAME"),
                        "/",
                        env!("CARGO_PKG_VERSION")
                    ))
                    .gzip(true)
                    .build()
                    .expect("that reqwest will build");
                let pds_throttler = Throttler::new();
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
                    .set(CrawlerShared {
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
                    let handle = spawn_crawler_producer(
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
                    let handle = spawn_crawler_producer(
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

            // 12. spawn the firehose worker on a blocking thread (fatal task)
            let handle = tokio::runtime::Handle::current();
            let firehose_worker = std::thread::spawn({
                let state = state.clone();
                move || {
                    FirehoseWorker::new(
                        state,
                        buffer_rx,
                        matches!(config.verify_signatures, SignatureVerification::Full),
                        config.ephemeral,
                        config.firehose_workers,
                    )
                    .run(handle)
                }
            });

            {
                let tx = Arc::clone(&fatal_tx);
                tokio::spawn(
                    tokio::task::spawn_blocking(move || {
                        firehose_worker
                            .join()
                            .map_err(|e| miette::miette!("buffer processor died: {e:?}"))
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

    /// subscribe to the ordered event stream.
    ///
    /// returns an [`EventStream`] that implements [`futures::Stream`].
    ///
    /// - if `cursor` is `None`, streaming starts from the current head (live tail only).
    /// - if `cursor` is `Some(id)`, all persisted `record` events from that ID onward are
    ///   replayed first, then live events follow seamlessly.
    ///
    /// `identity` and `account` events are ephemeral and are never replayed from a cursor -
    /// only live occurrences are delivered. use [`ReposControl::get`] to fetch current
    /// identity/account state for a specific DID.
    ///
    /// multiple concurrent subscribers each receive a full independent copy of the stream.
    /// the stream ends when the `EventStream` is dropped.
    pub fn subscribe(&self, cursor: Option<u64>) -> EventStream {
        let (tx, rx) = mpsc::channel(500);
        let state = self.state.clone();
        let runtime = tokio::runtime::Handle::current();

        std::thread::Builder::new()
            .name("hydrant-stream".into())
            .spawn(move || {
                let _g = runtime.enter();
                event_stream_thread(state, tx, cursor);
            })
            .expect("failed to spawn stream thread");

        EventStream(rx)
    }

    /// return database counts and on-disk sizes for all keyspaces.
    ///
    /// counts include: `repos`, `pending`, `resync`, `records`, `blocks`, `events`,
    /// `error_ratelimited`, `error_transport`, `error_generic`.
    ///
    /// sizes are in bytes, reported per keyspace.
    pub async fn stats(&self) -> Result<StatsResponse> {
        let db = self.state.db.clone();

        let mut counts: BTreeMap<&'static str, u64> = futures::future::join_all(
            [
                "repos",
                "pending",
                "resync",
                "records",
                "blocks",
                "error_ratelimited",
                "error_transport",
                "error_generic",
            ]
            .into_iter()
            .map(|name| {
                let db = db.clone();
                async move { (name, db.get_count(name).await) }
            }),
        )
        .await
        .into_iter()
        .collect();

        counts.insert("events", db.events.approximate_len() as u64);

        let sizes = tokio::task::spawn_blocking(move || {
            let mut s = BTreeMap::new();
            s.insert("repos", db.repos.disk_space());
            s.insert("records", db.records.disk_space());
            s.insert("blocks", db.blocks.disk_space());
            s.insert("cursors", db.cursors.disk_space());
            s.insert("pending", db.pending.disk_space());
            s.insert("resync", db.resync.disk_space());
            s.insert("resync_buffer", db.resync_buffer.disk_space());
            s.insert("events", db.events.disk_space());
            s.insert("counts", db.counts.disk_space());
            s.insert("filter", db.filter.disk_space());
            s.insert("crawler", db.crawler.disk_space());
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
    /// exposes internal inspection endpoints (`/debug/get`, `/debug/iter`, etc.)
    /// that are not safe to expose publicly. binds only to loopback.
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

/// a stream of [`Event`]s. returned by [`Hydrant::subscribe`].
///
/// implements [`futures::Stream`] and can be used with `StreamExt::next`,
/// `while let Some(evt) = stream.next().await`, `forward`, etc.
/// the stream terminates when the underlying channel closes (i.e. hydrant shuts down).
pub struct EventStream(mpsc::Receiver<Event>);

impl Stream for EventStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
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

/// runtime control over the backfill worker component.
///
/// the backfill worker fetches full repo CAR files from each repo's PDS for any
/// repository in the pending queue, parses the MST, and inserts all matching records
/// into the database. concurrency is bounded by `HYDRANT_BACKFILL_CONCURRENCY_LIMIT`.
#[derive(Clone)]
pub struct BackfillHandle(Arc<AppState>);

impl BackfillHandle {
    /// enable the backfill worker, no-op if already enabled.
    pub fn enable(&self) {
        self.0.backfill_enabled.send_replace(true);
    }
    /// disable the backfill worker, in-flight repos complete before pausing.
    pub fn disable(&self) {
        self.0.backfill_enabled.send_replace(false);
    }
    /// returns the current enabled state of the backfill worker.
    pub fn is_enabled(&self) -> bool {
        *self.0.backfill_enabled.borrow()
    }
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
    /// dictionaries are written to `dict_{name}.bin` files next to the database.
    /// a restart is required to apply them. training samples data blocks from the
    /// existing database, so the database must have a reasonable amount of data first.
    pub async fn train_dicts(&self) -> Result<()> {
        let state = self.0.clone();
        state
            .with_ingestion_paused(async || {
                let train = |name: &'static str| {
                    let db = state.db.clone();
                    tokio::task::spawn_blocking(move || db.train_dict(name))
                        .map(|res| res.into_diagnostic().flatten())
                };
                tokio::try_join!(train("repos"), train("blocks"), train("events")).map(|_| ())
            })
            .await
    }
}
