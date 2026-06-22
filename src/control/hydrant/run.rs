use futures::FutureExt;
use miette::{IntoDiagnostic, Result};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use url::Url;

use super::super::firehose::FirehoseShared;
use super::super::seed;
use super::Hydrant;
use crate::config::SignatureVerification;
use crate::db::{self, load_persisted_firehose_sources};
use crate::state::AppState;

#[cfg(feature = "indexer")]
use super::super::crawler;
#[cfg(feature = "indexer")]
use crate::backfill::BackfillWorker;
#[cfg(feature = "indexer")]
use crate::db::load_persisted_crawler_sources;
#[cfg(feature = "indexer")]
use crate::ingest::indexer::FirehoseWorker;

impl Hydrant {
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
            // validated IndexerMessages from RelayWorker/backfill to FirehoseWorker.
            #[cfg(feature = "indexer")]
            let (indexer_tx, firehose_worker) =
                FirehoseWorker::new(state.clone(), config.firehose_workers);

            // raw firehose events from pds/relay to RelayWorker.
            let (buffer_tx, relay_worker) = crate::ingest::relay::RelayWorker::new(
                state.clone(),
                #[cfg(feature = "indexer")]
                indexer_tx.clone(),
                matches!(config.verify_signatures, SignatureVerification::Full),
                config.firehose_workers,
                crate::ingest::validation::ValidationOptions {
                    verify_mst: config.verify_mst,
                    rev_clock_skew_secs: config.rev_clock_skew_secs,
                    verify_cids: config.verify_cids,
                },
            );

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
                    config.backfill_strategy,
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
            #[cfg(feature = "indexer_stream")]
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
                        if seq > 0
                            && let Err(e) = db::set_firehose_cursor(&state.db, relay, seq)
                        {
                            error!(relay = %relay, err = %e, "failed to save cursor");
                            db::check_poisoned_report(&e);
                        }
                        true
                    });

                    let checkpoint_watermark = match state.db.checkpoint_count_deltas() {
                        Ok(watermark) => watermark,
                        Err(e) => {
                            error!(err = %e, "failed to checkpoint count deltas");
                            db::check_poisoned_report(&e);
                            None
                        }
                    };

                    if let Err(e) = state.db.persist() {
                        error!(err = %e, "db persist failed");
                        db::check_poisoned_report(&e);
                    } else {
                        let watermark = checkpoint_watermark
                            .map(Ok)
                            .unwrap_or_else(|| db::load_count_delta_watermark(&state.db));
                        match watermark {
                            Ok(watermark) => state.db.mark_count_checkpoint_persisted(watermark),
                            Err(e) => {
                                error!(err = %e, "failed to load durable count checkpoint watermark");
                                db::check_poisoned_report(&e);
                            }
                        }
                    }
                }
            });

            // 9. events/sec stats ticker
            #[cfg(any(feature = "indexer_stream", feature = "relay"))]
            tokio::spawn({
                let state = state.clone();
                let get_id = |state: &AppState| {
                    #[cfg(feature = "indexer_stream")]
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
                    max_failures: config.firehose_max_failures,
                })
                .ok()
                .expect("firehose shared already set");
            let fire_shared = firehose.shared.get().unwrap();

            // refresh seed snapshots before spawning any direct PDS sources. persisted
            // sources may predate cursor seeding, and an already-open websocket will not
            // pick up a later cursor update until it reconnects.
            if !config.seed_hosts.is_empty() {
                seed::refresh_seed_snapshots(&config.seed_hosts, &state).await;
            }

            // add hosts from config
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
                    let _ = firehose
                        .known_sources
                        .insert_async(source.url.clone(), source.is_pds)
                        .await;
                    firehose
                        .spawn_firehose_ingestor(source, fire_shared, true)
                        .await?;
                }
            }

            // add persisted hosts
            let persisted_sources = tokio::task::spawn_blocking({
                let state = state.clone();
                move || load_persisted_firehose_sources(&state.db)
            })
            .await
            .into_diagnostic()??;
            for source in &persisted_sources {
                let _ = firehose
                    .known_sources
                    .insert_async(source.url.clone(), source.is_pds)
                    .await;
                if firehose.tasks.contains_async(&source.url).await {
                    continue;
                }
                firehose
                    .spawn_firehose_ingestor(source, fire_shared, true)
                    .await?;
            }
            // we use spawn_firehose_ingestor directly here since we dont want
            // to go through the whole add_source machinery and checks. seed
            // cursor snapshots were refreshed before this loop.

            // 10c. seed firehose PDS sources from listHosts on configured seed URLs
            if !config.seed_hosts.is_empty() {
                let seed_urls = config.seed_hosts.clone();
                let firehose = firehose.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    seed::seed_from_list_hosts(&seed_urls, &firehose, &state).await;
                });
            }

            // 10d. periodic retry of offline firehose sources
            if let Some(retry_interval) = config.offline_host_retry_interval {
                tokio::spawn({
                    let firehose = firehose.clone();
                    async move {
                        loop {
                            tokio::time::sleep(retry_interval).await;

                            let mut to_restart: Vec<(Url, bool)> = Vec::new();
                            let mut banned = 0usize;
                            {
                                let meta = firehose.state.pds_meta.load();
                                firehose
                                    .known_sources
                                    .iter_async(|url, &is_pds| {
                                        if firehose.tasks.contains_sync(url) {
                                            return true;
                                        }
                                        let host = url.host_str().unwrap_or(url.as_str());
                                        if meta.is_banned(host) {
                                            banned += 1;
                                            return true;
                                        }
                                        to_restart.push((url.clone(), is_pds));
                                        true
                                    })
                                    .await;
                            }

                            if !to_restart.is_empty() || banned != 0 {
                                info!(
                                    restart_count = to_restart.len(),
                                    banned,
                                    retry_interval_secs = retry_interval.as_secs(),
                                    "offline firehose retry tick"
                                );
                            }

                            for (url, is_pds) in to_restart {
                                if let Err(e) = firehose.restart_source(url.clone(), is_pds).await {
                                    warn!(url = %url, is_pds, err = %e, "failed to restart firehose source");
                                }
                            }
                        }
                    }
                });
            }

            // 11. spawn crawler infrastructure
            #[cfg(feature = "indexer")]
            {
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
                let handle = tokio::runtime::Handle::current();
                move || relay_worker.run(handle)
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
                let handle = tokio::runtime::Handle::current();
                move || firehose_worker.run(handle)
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
}
