use futures::{FutureExt, TryFutureExt, future::BoxFuture};
use hydrant::config::{Config, SignatureVerification};
use hydrant::crawler::Crawler;
use hydrant::db::{self, set_firehose_cursor};
use hydrant::ingest::firehose::FirehoseIngestor;
use hydrant::state::AppState;
use hydrant::{api, backfill::BackfillWorker, ingest::worker::FirehoseWorker};
use miette::IntoDiagnostic;
use mimalloc::MiMalloc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::{sync::mpsc, task::spawn_blocking};
use tracing::{error, info};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> miette::Result<()> {
    let cfg = Config::from_env()?;

    let env_filter = tracing_subscriber::EnvFilter::new(&cfg.log_level);
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    info!("{cfg}");

    let state = AppState::new(&cfg)?;

    if cfg.full_network {
        let filter_ks = state.db.filter.clone();
        let inner = state.db.inner.clone();
        tokio::task::spawn_blocking(move || {
            use hydrant::filter::{FilterMode, MODE_KEY};
            let mut batch = inner.batch();
            batch.insert(
                &filter_ks,
                MODE_KEY,
                rmp_serde::to_vec(&FilterMode::Full).into_diagnostic()?,
            );
            batch.commit().into_diagnostic()
        })
        .await
        .into_diagnostic()??;

        let new_filter = hydrant::db::filter::load(&state.db.filter)?;
        state.filter.store(new_filter.into());
    }

    let (buffer_tx, buffer_rx) = mpsc::unbounded_channel();
    let state = Arc::new(state);

    if !cfg.disable_backfill {
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
        error!("failed to queue gone backfills: {e}");
        db::check_poisoned_report(&e);
    }

    std::thread::spawn({
        let state = state.clone();
        move || hydrant::backfill::manager::retry_worker(state)
    });

    tokio::spawn({
        let state = state.clone();
        async move {
            let mut last_id = state.db.next_event_id.load(Ordering::Relaxed);
            let mut last_time = std::time::Instant::now();
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                interval.tick().await;

                let current_id = state.db.next_event_id.load(Ordering::Relaxed);
                let current_time = std::time::Instant::now();

                let delta = current_id.saturating_sub(last_id);
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
            info!("persistence worker started");
            loop {
                std::thread::sleep(persist_interval);

                // persist firehose cursor
                let seq = state.cur_firehose.load(Ordering::SeqCst);
                if let Err(e) = set_firehose_cursor(&state.db, seq) {
                    error!("failed to save cursor: {e}");
                    db::check_poisoned_report(&e);
                }

                // persist counts
                // TODO: make this more durable
                if let Err(e) = db::persist_counts(&state.db) {
                    error!("failed to persist counts: {e}");
                    db::check_poisoned_report(&e);
                }

                // persist journal
                if let Err(e) = state.db.persist() {
                    error!("db persist failed: {e}");
                    db::check_poisoned_report(&e);
                }
            }
        }
    });

    if let hydrant::filter::FilterMode::Full | hydrant::filter::FilterMode::Signal =
        state.filter.load().mode
    {
        tokio::spawn(
            Crawler::new(
                state.clone(),
                cfg.relay_host.clone(),
                cfg.crawler_max_pending_repos,
                cfg.crawler_resume_pending_repos,
            )
            .run()
            .inspect_err(|e| {
                error!("crawler died: {e}");
                db::check_poisoned_report(&e);
            }),
        );
    }

    let mut tasks = if !cfg.disable_firehose {
        let firehose_worker = std::thread::spawn({
            let state = state.clone();
            let handle = tokio::runtime::Handle::current();
            move || {
                FirehoseWorker::new(
                    state,
                    buffer_rx,
                    matches!(cfg.verify_signatures, SignatureVerification::Full),
                    cfg.firehose_workers,
                )
                .run(handle)
            }
        });

        let ingestor = FirehoseIngestor::new(
            state.clone(),
            buffer_tx,
            cfg.relay_host,
            state.filter.clone(),
            matches!(cfg.verify_signatures, SignatureVerification::Full),
        );

        vec![
            Box::pin(
                tokio::task::spawn_blocking(move || {
                    firehose_worker
                        .join()
                        .map_err(|e| miette::miette!("buffer processor died: {e:?}"))
                })
                .map(|r| r.into_diagnostic().flatten().flatten()),
            ) as BoxFuture<_>,
            Box::pin(ingestor.run()),
        ]
    } else {
        info!("firehose ingestion disabled by config");
        // if firehose is disabled, we just wait indefinitely (or until signal)
        // essentially we just want to keep the main thread alive for the other components
        vec![Box::pin(futures::future::pending::<miette::Result<()>>()) as BoxFuture<_>]
    };

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
        error!("critical worker died: {e}");
        db::check_poisoned_report(&e);
    }

    if let Err(e) = state.db.persist() {
        db::check_poisoned_report(&e);
        return Err(e);
    }

    Ok(())
}
