use futures::{FutureExt, future::BoxFuture};
use hydrant::config::{Config, SignatureVerification};
use hydrant::db;
use hydrant::ingest::firehose::FirehoseIngestor;
use hydrant::state::AppState;
use hydrant::{api, backfill::BackfillWorker, ingest::worker::FirehoseWorker};
use miette::IntoDiagnostic;
use mimalloc::MiMalloc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::{sync::mpsc, task::spawn_blocking};
use tracing::{debug, error, info};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> miette::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let cfg = Config::from_env()?;

    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    info!("{cfg}");

    let state = AppState::new(&cfg)?;

    if cfg.db_compact {
        info!("compacting database...");
        state.db.compact().await?;
    }

    if cfg.full_network
        || cfg.filter_signals.is_some()
        || cfg.filter_collections.is_some()
        || cfg.filter_excludes.is_some()
    {
        let filter_ks = state.db.filter.clone();
        let inner = state.db.inner.clone();
        let full_network = cfg.full_network;
        let signals = cfg.filter_signals.clone();
        let collections = cfg.filter_collections.clone();
        let excludes = cfg.filter_excludes.clone();

        tokio::task::spawn_blocking(move || {
            use hydrant::filter::{FilterMode, SetUpdate};
            let mut batch = inner.batch();

            let mode = if full_network {
                Some(FilterMode::Full)
            } else {
                None
            };

            let signals_update = signals.map(SetUpdate::Set);
            let collections_update = collections.map(SetUpdate::Set);
            let excludes_update = excludes.map(SetUpdate::Set);

            hydrant::db::filter::apply_patch(
                &mut batch,
                &filter_ks,
                mode,
                signals_update,
                collections_update,
                excludes_update,
            )?;

            batch.commit().into_diagnostic()
        })
        .await
        .into_diagnostic()??;

        let new_filter = hydrant::db::filter::load(&state.db.filter)?;
        state.filter.store(new_filter.into());
    }

    let (buffer_tx, buffer_rx) = mpsc::unbounded_channel();
    let state = Arc::new(state);

    if cfg.ephemeral {
        db::ephemeral::ephemeral_startup_load_refcounts(&state.db)?;

        let state_ttl = state.clone();
        std::thread::Builder::new()
            .name("ephemeral-ttl".into())
            .spawn(move || db::ephemeral::ephemeral_ttl_worker(state_ttl))
            .into_diagnostic()?;
    }

    if cfg.enable_backfill {
        tokio::spawn({
            let state = state.clone();
            let timeout = cfg.repo_fetch_timeout;
            BackfillWorker::new(
                state,
                buffer_tx.clone(),
                timeout,
                cfg.backfill_concurrency_limit,
                matches!(
                    cfg.verify_signatures,
                    SignatureVerification::Full | SignatureVerification::BackfillOnly
                ),
                cfg.ephemeral,
            )
            .run()
        });
    }

    if let Err(e) = spawn_blocking({
        let state = state.clone();
        move || hydrant::backfill::manager::queue_gone_backfills(&state)
    })
    .await
    .into_diagnostic()?
    {
        error!(err = %e, "failed to queue gone backfills");
        db::check_poisoned_report(&e);
    }

    std::thread::spawn({
        let state = state.clone();
        move || hydrant::backfill::manager::retry_worker(state)
    });

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

    std::thread::spawn({
        let state = state.clone();
        let persist_interval = cfg.cursor_save_interval;

        move || {
            loop {
                std::thread::sleep(persist_interval);

                // persist firehose cursors
                for (relay, cursor) in &state.relay_cursors {
                    let seq = cursor.load(Ordering::SeqCst);
                    if seq > 0 {
                        if let Err(e) = db::set_firehose_cursor(&state.db, relay, seq) {
                            error!(relay = %relay, err = %e, "failed to save cursor");
                            db::check_poisoned_report(&e);
                        }
                    }
                }

                // persist counts
                // TODO: make this more durable
                if let Err(e) = db::persist_counts(&state.db) {
                    error!(err = %e, "failed to persist counts");
                    db::check_poisoned_report(&e);
                }

                // persist journal
                if let Err(e) = state.db.persist() {
                    error!(err = %e, "db persist failed");
                    db::check_poisoned_report(&e);
                }
            }
        }
    });

    let post_patch_crawler = match cfg.enable_crawler {
        Some(b) => b,
        None => state.filter.load().mode == hydrant::filter::FilterMode::Full,
    };
    state.crawler_enabled.send_replace(post_patch_crawler);

    info!(
        crawler_enabled = *state.crawler_enabled.borrow(),
        firehose_enabled = *state.firehose_enabled.borrow(),
        filter_mode = ?state.filter.load().mode,
        "starting ingestion"
    );

    let relay_hosts = cfg.relays.clone();
    let crawler_max_pending = cfg.crawler_max_pending_repos;
    let crawler_resume_pending = cfg.crawler_resume_pending_repos;

    if !relay_hosts.is_empty() {
        let state_for_crawler = state.clone();
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
        tokio::spawn(async move {
            let crawler = hydrant::crawler::Crawler::new(
                state_for_crawler,
                relay_hosts,
                crawler_max_pending,
                crawler_resume_pending,
                crawler_rx,
            );
            if let Err(e) = crawler.run().await {
                error!(err = %e, "crawler error");
                db::check_poisoned_report(&e);
            }
        });
    }

    let firehose_worker = std::thread::spawn({
        let state = state.clone();
        let handle = tokio::runtime::Handle::current();
        move || {
            FirehoseWorker::new(
                state,
                buffer_rx,
                matches!(cfg.verify_signatures, SignatureVerification::Full),
                cfg.ephemeral,
                cfg.firehose_workers,
            )
            .run(handle)
        }
    });

    let mut tasks: Vec<BoxFuture<miette::Result<()>>> = vec![Box::pin(
        tokio::task::spawn_blocking(move || {
            firehose_worker
                .join()
                .map_err(|e| miette::miette!("buffer processor died: {e:?}"))
        })
        .map(|r| r.into_diagnostic().flatten().flatten()),
    )];

    for relay_url in &cfg.relays {
        let ingestor = FirehoseIngestor::new(
            state.clone(),
            buffer_tx.clone(),
            relay_url.clone(),
            state.filter.clone(),
            state.firehose_enabled.subscribe(),
            matches!(cfg.verify_signatures, SignatureVerification::Full),
        );
        tasks.push(Box::pin(ingestor.run()));
    }

    let state_api = state.clone();
    tasks.push(Box::pin(async move {
        api::serve(state_api, cfg.api_port)
            .await
            .map_err(|e| miette::miette!("API server failed: {e}"))
    }) as BoxFuture<_>);

    if cfg.enable_debug {
        let state_debug = state.clone();
        tasks.push(Box::pin(async move {
            api::serve_debug(state_debug, cfg.debug_port)
                .await
                .map_err(|e| miette::miette!("debug server failed: {e}"))
        }) as BoxFuture<_>);
    }

    let res = futures::future::select_all(tasks);
    if let (Err(e), _, _) = res.await {
        error!(err = %e, "critical worker died");
        db::check_poisoned_report(&e);
    }

    if let Err(e) = state.db.persist() {
        db::check_poisoned_report(&e);
        return Err(e);
    }

    Ok(())
}
