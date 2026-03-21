use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use chrono::{DateTime, Utc};
use futures::{FutureExt, Stream};
use jacquard_common::types::cid::{ATP_CID_HASH, IpldCid};
use jacquard_common::types::string::Did;
use jacquard_common::{CowStr, IntoStatic, RawData};
use jacquard_repo::DAG_CBOR_CID_CODEC;
use miette::{IntoDiagnostic, Result};
use rand::Rng;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info};

use crate::backfill::BackfillWorker;
use crate::config::{Config, SignatureVerification};
use crate::crawler::Crawler;
use crate::db::{self, filter as db_filter, keys, ser_repo_state};
use crate::filter::{FilterMode, SetUpdate};
use crate::ingest::{firehose::FirehoseIngestor, worker::FirehoseWorker};
use crate::state::AppState;
use crate::types::{
    BroadcastEvent, GaugeState, MarshallableEvt, RecordEvt, RepoState, StoredData, StoredEvent,
};

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
///         r = hydrant.run()        => r,
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
            let signals = config.filter_signals.clone().map(SetUpdate::Set);
            let collections = config.filter_collections.clone().map(SetUpdate::Set);
            let excludes = config.filter_excludes.clone().map(SetUpdate::Set);

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
            None => state.filter.load().mode == FilterMode::Full,
        };
        state.crawler_enabled.send_replace(post_patch_crawler);

        let state = Arc::new(state);

        Ok(Self {
            crawler: CrawlerHandle(state.clone()),
            firehose: FirehoseHandle(state.clone()),
            backfill: BackfillHandle(state.clone()),
            filter: FilterControl(state.clone()),
            repos: ReposControl(state.clone()),
            db: DbControl(state.clone()),
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
    /// panics if called more than once on the same `Hydrant` instance.
    pub fn run(&self) -> impl Future<Output = Result<()>> {
        let state = self.state.clone();
        let config = self.config.clone();
        let started = self.started.clone();

        async move {
            if started.swap(true, Ordering::SeqCst) {
                panic!("Hydrant::run() called more than once");
            }

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

                    for (relay, cursor) in &state.relay_cursors {
                        let seq = cursor.load(Ordering::SeqCst);
                        if seq > 0 {
                            if let Err(e) = db::set_firehose_cursor(&state.db, relay, seq) {
                                error!(relay = %relay, err = %e, "failed to save cursor");
                                db::check_poisoned_report(&e);
                            }
                        }
                    }

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

            // 10. spawn one firehose ingestor per relay (fatal tasks)
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
                    let ingestor = FirehoseIngestor::new(
                        state.clone(),
                        buffer_tx.clone(),
                        relay_url.clone(),
                        state.filter.clone(),
                        state.firehose_enabled.subscribe(),
                        matches!(config.verify_signatures, SignatureVerification::Full),
                    );
                    let tx = Arc::clone(&fatal_tx);
                    tokio::spawn(async move {
                        let result = ingestor.run().await;
                        let _ = tx.send(Some(result.map_err(|e| e.to_string())));
                    });
                }
            }

            // 11. spawn the crawler if we have relay hosts to crawl
            if !relay_hosts.is_empty() {
                let crawler_rx = state.crawler_enabled.subscribe();
                info!(
                    relay_count = relay_hosts.len(),
                    hosts = relay_hosts
                        .iter()
                        .map(|h| h.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    enabled = *state.crawler_enabled.borrow(),
                    "starting crawler(s)"
                );
                let state = state.clone();
                let max_pending = config.crawler_max_pending_repos;
                let resume_pending = config.crawler_resume_pending_repos;
                tokio::spawn(async move {
                    let crawler =
                        Crawler::new(state, relay_hosts, max_pending, resume_pending, crawler_rx);
                    if let Err(e) = crawler.run().await {
                        error!(err = %e, "crawler error");
                        db::check_poisoned_report(&e);
                    }
                });
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
        }
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

// --- event stream ---

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

// --- stats ---

/// database statistics returned by [`Hydrant::stats`].
#[derive(serde::Serialize)]
pub struct StatsResponse {
    /// record counts per logical category (repos, records, events, error kinds, etc.)
    pub counts: BTreeMap<&'static str, u64>,
    /// on-disk size in bytes per keyspace
    pub sizes: BTreeMap<&'static str, u64>,
}

// --- ingestion handles ---

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
pub struct CrawlerHandle(Arc<AppState>);

impl CrawlerHandle {
    /// enable the crawler. no-op if already enabled.
    pub fn enable(&self) {
        self.0.crawler_enabled.send_replace(true);
    }
    /// disable the crawler. in-progress repo checks finish before the crawler pauses.
    pub fn disable(&self) {
        self.0.crawler_enabled.send_replace(false);
    }
    /// returns the current enabled state of the crawler.
    pub fn is_enabled(&self) -> bool {
        *self.0.crawler_enabled.borrow()
    }
}

/// runtime control over the firehose ingestor component.
///
/// the firehose connects to each configured relay's `com.atproto.sync.subscribeRepos`
/// websocket and processes commit, identity, account, and sync events in real time.
/// one independent connection is maintained per relay URL.
///
/// disabling the firehose closes the websocket after the current message is processed.
#[derive(Clone)]
pub struct FirehoseHandle(Arc<AppState>);

impl FirehoseHandle {
    /// enable the firehose. no-op if already enabled.
    pub fn enable(&self) {
        self.0.firehose_enabled.send_replace(true);
    }
    /// disable the firehose. the current message finishes processing before the connection closes.
    pub fn disable(&self) {
        self.0.firehose_enabled.send_replace(false);
    }
    /// returns the current enabled state of the firehose.
    pub fn is_enabled(&self) -> bool {
        *self.0.firehose_enabled.borrow()
    }
}

/// runtime control over the backfill worker component.
///
/// the backfill worker fetches full repo CAR files from each repo's PDS for any
/// repository in the pending queue, parses the MST, and inserts all matching records
/// into the database. concurrency is bounded by `HYDRANT_BACKFILL_CONCURRENCY_LIMIT`.
///
/// disabling backfill lets any in-flight repo fetches finish before pausing.
#[derive(Clone)]
pub struct BackfillHandle(Arc<AppState>);

impl BackfillHandle {
    /// enable the backfill worker. no-op if already enabled.
    pub fn enable(&self) {
        self.0.backfill_enabled.send_replace(true);
    }
    /// disable the backfill worker. in-flight repo fetches complete before pausing.
    pub fn disable(&self) {
        self.0.backfill_enabled.send_replace(false);
    }
    /// returns the current enabled state of the backfill worker.
    pub fn is_enabled(&self) -> bool {
        *self.0.backfill_enabled.borrow()
    }
}

// --- filter control ---

/// a point-in-time snapshot of the filter configuration. returned by all [`FilterControl`] methods.
///
/// because the filter is stored in the database and loaded on demand, this snapshot
/// may be stale if another caller modifies the filter concurrently. for the authoritative
/// live config use [`FilterControl::get`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FilterSnapshot {
    pub mode: FilterMode,
    pub signals: Vec<String>,
    pub collections: Vec<String>,
    pub excludes: Vec<String>,
}

/// runtime control over the indexing filter.
///
/// the filter has two orthogonal axes:
///
/// **mode** controls discovery:
/// - [`FilterMode::Filter`]: only indexes repos whose firehose commits touch a collection
///   matching a configured `signal`. explicit [`ReposControl::track`] always works regardless.
/// - [`FilterMode::Full`]: indexes the entire network. `signals` are ignored for discovery
///   but `collections` and `excludes` still apply.
///
/// **sets** are each independently configurable:
/// - `signals`: NSID patterns that trigger auto-discovery in `filter` mode (e.g. `app.bsky.feed.post`, `app.bsky.graph.*`)
/// - `collections`: NSID patterns that filter which records are *stored*. empty means store all.
/// - `excludes`: DIDs that are always skipped regardless of mode.
///
/// NSID patterns support an optional `.*` suffix to match an entire namespace.
/// all mutations are persisted to the database and take effect immediately.
#[derive(Clone)]
pub struct FilterControl(Arc<AppState>);

impl FilterControl {
    /// return the current filter configuration from the database.
    pub async fn get(&self) -> Result<FilterSnapshot> {
        let filter_ks = self.0.db.filter.clone();
        tokio::task::spawn_blocking(move || {
            let hot = db_filter::load(&filter_ks)?;
            let excludes = db_filter::read_set(&filter_ks, db_filter::EXCLUDE_PREFIX)?;
            Ok(FilterSnapshot {
                mode: hot.mode,
                signals: hot.signals.iter().map(|s| s.to_string()).collect(),
                collections: hot.collections.iter().map(|s| s.to_string()).collect(),
                excludes,
            })
        })
        .await
        .into_diagnostic()?
    }

    /// set the indexing mode. see [`FilterControl`] for mode semantics.
    pub async fn set_mode(&self, mode: FilterMode) -> Result<FilterSnapshot> {
        self.patch(Some(mode), None, None, None).await
    }

    /// replace the entire signals set. existing signals are removed.
    pub async fn set_signals(
        &self,
        signals: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<FilterSnapshot> {
        self.patch(
            None,
            Some(SetUpdate::Set(
                signals.into_iter().map(Into::into).collect(),
            )),
            None,
            None,
        )
        .await
    }

    /// add multiple signals without disturbing existing ones.
    pub async fn append_signals(
        &self,
        signals: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<FilterSnapshot> {
        self.patch(
            None,
            Some(SetUpdate::Patch(
                signals.into_iter().map(|s| (s.into(), true)).collect(),
            )),
            None,
            None,
        )
        .await
    }

    /// add a single signal. no-op if already present.
    pub async fn add_signal(&self, signal: impl Into<String>) -> Result<FilterSnapshot> {
        self.patch(
            None,
            Some(SetUpdate::Patch([(signal.into(), true)].into())),
            None,
            None,
        )
        .await
    }

    /// remove a single signal. no-op if not present.
    pub async fn remove_signal(&self, signal: impl Into<String>) -> Result<FilterSnapshot> {
        self.patch(
            None,
            Some(SetUpdate::Patch([(signal.into(), false)].into())),
            None,
            None,
        )
        .await
    }

    /// replace the entire collections set. pass an empty iterator to store all collections.
    pub async fn set_collections(
        &self,
        collections: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            Some(SetUpdate::Set(
                collections.into_iter().map(Into::into).collect(),
            )),
            None,
        )
        .await
    }

    /// add multiple collections without disturbing existing ones.
    pub async fn append_collections(
        &self,
        collections: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            Some(SetUpdate::Patch(
                collections.into_iter().map(|c| (c.into(), true)).collect(),
            )),
            None,
        )
        .await
    }

    /// add a single collection filter. no-op if already present.
    pub async fn add_collection(&self, collection: impl Into<String>) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            Some(SetUpdate::Patch([(collection.into(), true)].into())),
            None,
        )
        .await
    }

    /// remove a single collection filter. no-op if not present.
    pub async fn remove_collection(&self, collection: impl Into<String>) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            Some(SetUpdate::Patch([(collection.into(), false)].into())),
            None,
        )
        .await
    }

    /// replace the entire excludes set.
    pub async fn set_excludes(
        &self,
        excludes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            None,
            Some(SetUpdate::Set(
                excludes.into_iter().map(Into::into).collect(),
            )),
        )
        .await
    }

    /// add multiple DIDs to the excludes set without disturbing existing ones.
    pub async fn append_excludes(
        &self,
        excludes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            None,
            Some(SetUpdate::Patch(
                excludes.into_iter().map(|d| (d.into(), true)).collect(),
            )),
        )
        .await
    }

    /// add a single DID to the excludes set. no-op if already excluded.
    pub async fn add_exclude(&self, did: impl Into<String>) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            None,
            Some(SetUpdate::Patch([(did.into(), true)].into())),
        )
        .await
    }

    /// remove a single DID from the excludes set. no-op if not present.
    pub async fn remove_exclude(&self, did: impl Into<String>) -> Result<FilterSnapshot> {
        self.patch(
            None,
            None,
            None,
            Some(SetUpdate::Patch([(did.into(), false)].into())),
        )
        .await
    }

    /// apply a batch patch atomically. all provided fields are updated in a single db transaction.
    /// returns the updated [`FilterSnapshot`]. this is the primitive all other `FilterControl` methods delegate to.
    pub async fn patch(
        &self,
        mode: Option<FilterMode>,
        signals: Option<SetUpdate>,
        collections: Option<SetUpdate>,
        excludes: Option<SetUpdate>,
    ) -> Result<FilterSnapshot> {
        let filter_ks = self.0.db.filter.clone();
        let inner = self.0.db.inner.clone();
        let filter_handle = self.0.filter.clone();

        let new_filter = tokio::task::spawn_blocking(move || {
            let mut batch = inner.batch();
            db_filter::apply_patch(&mut batch, &filter_ks, mode, signals, collections, excludes)?;
            batch.commit().into_diagnostic()?;
            db_filter::load(&filter_ks)
        })
        .await
        .into_diagnostic()??;

        let excludes = {
            let filter_ks = self.0.db.filter.clone();
            tokio::task::spawn_blocking(move || {
                db_filter::read_set(&filter_ks, db_filter::EXCLUDE_PREFIX)
            })
            .await
            .into_diagnostic()??
        };

        let snapshot = FilterSnapshot {
            mode: new_filter.mode,
            signals: new_filter.signals.iter().map(|s| s.to_string()).collect(),
            collections: new_filter
                .collections
                .iter()
                .map(|s| s.to_string())
                .collect(),
            excludes,
        };

        filter_handle.store(Arc::new(new_filter));
        Ok(snapshot)
    }
}

// --- repos control ---

/// information about a tracked or known repository. returned by [`ReposControl`] methods.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RepoInfo {
    pub did: String,
    pub status: String,
    pub tracked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rev: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pds: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signing_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<DateTime<Utc>>,
}

/// control over which repositories are tracked and access to their state.
///
/// in `filter` mode, a repo is only indexed if it either matches a signal or is
/// explicitly tracked via [`ReposControl::track`]. in `full` mode all repos are indexed
/// and tracking is implicit.
///
/// tracking a DID that hydrant has never seen enqueues an immediate backfill.
/// tracking a DID that hydrant already knows about (but has marked untracked)
/// re-enqueues it for backfill.
#[derive(Clone)]
pub struct ReposControl(Arc<AppState>);

impl ReposControl {
    /// fetch the current state of a single repository. returns `None` if hydrant
    /// has never seen this DID.
    pub async fn get(&self, did: &Did<'_>) -> Result<Option<RepoInfo>> {
        let did_key = keys::repo_key(did);
        let did_str = did.as_str().to_owned();
        let db = self.0.db.clone();

        tokio::task::spawn_blocking(move || {
            let bytes = db.repos.get(&did_key).into_diagnostic()?;
            let state = bytes.as_deref().map(db::deser_repo_state).transpose()?;
            Ok(state.map(|s| repo_state_to_info(did_str, s)))
        })
        .await
        .into_diagnostic()?
    }

    /// explicitly track one or more repositories, enqueuing them for backfill if needed.
    ///
    /// - if a DID is new, a fresh [`RepoState`] is created and backfill is queued.
    /// - if a DID is already known but untracked, it is marked tracked and re-enqueued.
    /// - if a DID is already tracked, this is a no-op.
    pub async fn track(&self, dids: impl IntoIterator<Item = Did<'_>>) -> Result<()> {
        let dids: Vec<Did<'static>> = dids.into_iter().map(|d| d.into_static()).collect();
        let state = self.0.clone();

        let (new_count, transitions) = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut added = 0i64;
            let mut transitions: Vec<(GaugeState, GaugeState)> = Vec::new();
            let mut rng = rand::rng();

            for did in &dids {
                let did_key = keys::repo_key(did);
                let repo_bytes = db.repos.get(&did_key).into_diagnostic()?;
                let existing = repo_bytes
                    .as_deref()
                    .map(db::deser_repo_state)
                    .transpose()?;

                if let Some(mut repo_state) = existing {
                    if !repo_state.tracked {
                        let resync = db.resync.get(&did_key).into_diagnostic()?;
                        let old = db::Db::repo_gauge_state(&repo_state, resync.as_deref());
                        repo_state.tracked = true;
                        batch.insert(&db.repos, &did_key, ser_repo_state(&repo_state)?);
                        batch.insert(
                            &db.pending,
                            keys::pending_key(repo_state.index_id),
                            &did_key,
                        );
                        batch.remove(&db.resync, &did_key);
                        transitions.push((old, GaugeState::Pending));
                    }
                } else {
                    let repo_state = RepoState::backfilling(rng.next_u64());
                    batch.insert(&db.repos, &did_key, ser_repo_state(&repo_state)?);
                    batch.insert(
                        &db.pending,
                        keys::pending_key(repo_state.index_id),
                        &did_key,
                    );
                    added += 1;
                    transitions.push((GaugeState::Synced, GaugeState::Pending));
                }
            }

            batch.commit().into_diagnostic()?;
            Ok::<_, miette::Report>((added, transitions))
        })
        .await
        .into_diagnostic()??;

        if new_count > 0 {
            self.0.db.update_count_async("repos", new_count).await;
        }
        for (old, new) in transitions {
            self.0.db.update_gauge_diff_async(&old, &new).await;
        }
        self.0.notify_backfill();
        Ok(())
    }

    /// stop tracking one or more repositories. hydrant will stop processing new events
    /// for them and remove them from the pending/resync queues, but existing indexed
    /// records are **not** deleted.
    pub async fn untrack(&self, dids: impl IntoIterator<Item = Did<'_>>) -> Result<()> {
        let dids: Vec<Did<'static>> = dids.into_iter().map(|d| d.into_static()).collect();
        let state = self.0.clone();

        let gauge_decrements = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut gauge_decrements = Vec::new();

            for did in &dids {
                let did_key = keys::repo_key(did);
                let repo_bytes = db.repos.get(&did_key).into_diagnostic()?;
                let existing = repo_bytes
                    .as_deref()
                    .map(db::deser_repo_state)
                    .transpose()?;

                if let Some(repo_state) = existing {
                    if repo_state.tracked {
                        let resync = db.resync.get(&did_key).into_diagnostic()?;
                        let old = db::Db::repo_gauge_state(&repo_state, resync.as_deref());
                        let mut repo_state = repo_state.into_static();
                        repo_state.tracked = false;
                        batch.insert(&db.repos, &did_key, ser_repo_state(&repo_state)?);
                        batch.remove(&db.pending, keys::pending_key(repo_state.index_id));
                        batch.remove(&db.resync, &did_key);
                        if old != GaugeState::Synced {
                            gauge_decrements.push(old);
                        }
                    }
                }
            }

            batch.commit().into_diagnostic()?;
            Ok::<_, miette::Report>(gauge_decrements)
        })
        .await
        .into_diagnostic()??;

        for gauge in gauge_decrements {
            self.0
                .db
                .update_gauge_diff_async(&gauge, &GaugeState::Synced)
                .await;
        }
        Ok(())
    }
}

pub fn repo_state_to_info(did: String, s: RepoState<'_>) -> RepoInfo {
    RepoInfo {
        did,
        status: s.status.to_string(),
        tracked: s.tracked,
        rev: s.rev.as_ref().map(|r| r.to_string()),
        handle: s.handle.map(|h| h.to_string()),
        pds: s.pds.map(|p| p.to_string()),
        signing_key: s.signing_key.map(|k| k.encode()),
        last_updated_at: DateTime::from_timestamp_secs(s.last_updated_at),
        last_message_at: s.last_message_time.and_then(DateTime::from_timestamp_secs),
    }
}

// --- db control ---

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

// --- stream thread ---

fn event_stream_thread(state: Arc<AppState>, tx: mpsc::Sender<Event>, cursor: Option<u64>) {
    let db = &state.db;
    let mut event_rx = db.event_tx.subscribe();
    let ks = db.events.clone();
    let mut current_id = match cursor {
        Some(c) => c.saturating_sub(1),
        None => db.next_event_id.load(Ordering::SeqCst).saturating_sub(1),
    };

    loop {
        // catch up from db
        loop {
            let mut found = false;
            for item in ks.range(keys::event_key(current_id + 1)..) {
                let (k, v) = match item.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => {
                        error!(err = %e, "failed to read event from db");
                        break;
                    }
                };

                let id = match k.as_ref().try_into().map(u64::from_be_bytes) {
                    Ok(id) => id,
                    Err(_) => {
                        error!("failed to parse event id");
                        continue;
                    }
                };
                current_id = id;

                let stored: StoredEvent = match rmp_serde::from_slice(&v) {
                    Ok(e) => e,
                    Err(e) => {
                        error!(err = %e, "failed to deserialize stored event");
                        continue;
                    }
                };

                let Some(evt) = stored_to_event(&state, id, stored) else {
                    continue;
                };

                if tx.blocking_send(evt).is_err() {
                    return; // receiver dropped
                }
                found = true;
            }
            if !found {
                break;
            }
        }

        // wait for live events
        match event_rx.blocking_recv() {
            Ok(BroadcastEvent::Persisted(_)) => {} // re-run catch-up
            Ok(BroadcastEvent::Ephemeral(evt)) => {
                if tx.blocking_send(*evt).is_err() {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn stored_to_event(state: &AppState, id: u64, stored: StoredEvent<'_>) -> Option<Event> {
    let StoredEvent {
        live,
        did,
        rev,
        collection,
        rkey,
        action,
        data,
    } = stored;

    let record = match data {
        StoredData::Ptr(cid) => {
            let block = state
                .db
                .blocks
                .get(&keys::block_key(collection.as_str(), &cid.to_bytes()));
            match block {
                Ok(Some(bytes)) => match serde_ipld_dagcbor::from_slice::<RawData>(&bytes) {
                    Ok(val) => Some((cid, serde_json::to_value(val).ok()?)),
                    Err(e) => {
                        error!(err = %e, "cant parse block");
                        return None;
                    }
                },
                Ok(None) => {
                    error!("block not found, this is a bug");
                    return None;
                }
                Err(e) => {
                    error!(err = %e, "cant get block");
                    db::check_poisoned(&e);
                    return None;
                }
            }
        }
        StoredData::Block(block) => {
            let digest = Sha256::digest(&block);
            let hash =
                cid::multihash::Multihash::wrap(ATP_CID_HASH, &digest).expect("valid sha256 hash");
            let cid = IpldCid::new_v1(DAG_CBOR_CID_CODEC, hash);
            match serde_ipld_dagcbor::from_slice::<RawData>(&block) {
                Ok(val) => Some((cid, serde_json::to_value(val).ok()?)),
                Err(e) => {
                    error!(err = %e, "cant parse block");
                    return None;
                }
            }
        }
        StoredData::Nothing => None,
    };

    let (cid, record) = record
        .map(|(c, r)| (Some(c), Some(r)))
        .unwrap_or((None, None));

    Some(MarshallableEvt {
        id,
        event_type: "record".into(),
        record: Some(RecordEvt {
            live,
            did: did.to_did(),
            rev: CowStr::Owned(rev.to_tid().into()),
            collection: CowStr::Owned(collection.as_ref().to_string().into()),
            rkey: CowStr::Owned(rkey.to_smolstr().into()),
            action: CowStr::Borrowed(action.as_str()),
            record,
            cid: cid.map(|c| jacquard_common::types::cid::Cid::ipld(c).into()),
        }),
        identity: None,
        account: None,
    })
}
