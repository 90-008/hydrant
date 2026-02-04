mod api;
mod backfill;
mod config;
mod crawler;
mod db;
mod ingest;
mod ops;
mod resolver;
mod state;
mod types;

use crate::config::Config;
use crate::crawler::Crawler;
use crate::db::set_firehose_cursor;
use crate::ingest::firehose::FirehoseIngestor;
use crate::state::AppState;
use crate::{backfill::BackfillWorker, ingest::worker::FirehoseWorker};
use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use miette::IntoDiagnostic;
use mimalloc::MiMalloc;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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

    let (state, backfill_rx) = AppState::new(&cfg)?;
    let (buffer_tx, buffer_rx) = mpsc::unbounded_channel();
    let state = Arc::new(state);

    tokio::spawn(
        api::serve(state.clone(), cfg.api_port).inspect_err(|e| error!("API server failed: {e}")),
    );

    if cfg.enable_debug {
        tokio::spawn(
            api::serve_debug(state.clone(), cfg.debug_port)
                .inspect_err(|e| error!("debug server failed: {e}")),
        );
    }

    tokio::spawn({
        let state = state.clone();
        let timeout = cfg.repo_fetch_timeout;
        BackfillWorker::new(state, backfill_rx, timeout, cfg.backfill_concurrency_limit).run()
    });

    let firehose_worker = std::thread::spawn({
        let state = state.clone();
        let handle = tokio::runtime::Handle::current();
        move || FirehoseWorker::new(state, buffer_rx).run(handle)
    });

    if let Err(e) = spawn_blocking({
        let state = state.clone();
        move || crate::backfill::manager::queue_pending_backfills(&state)
    })
    .await
    .into_diagnostic()?
    {
        error!("failed to queue pending backfills: {e}");
        db::check_poisoned_report(&e);
    }

    if let Err(e) = spawn_blocking({
        let state = state.clone();
        move || crate::backfill::manager::queue_gone_backfills(&state)
    })
    .await
    .into_diagnostic()?
    {
        error!("failed to queue gone backfills: {e}");
        db::check_poisoned_report(&e);
    }

    std::thread::spawn({
        let state = state.clone();
        move || crate::backfill::manager::retry_worker(state)
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

    if cfg.full_network {
        tokio::spawn(
            Crawler::new(state.clone(), cfg.relay_host.clone())
                .run()
                .inspect_err(|e| {
                    error!("crawler died: {e}");
                    db::check_poisoned_report(&e);
                }),
        );
    }

    let ingestor =
        FirehoseIngestor::new(state.clone(), buffer_tx, cfg.relay_host, cfg.full_network);

    let res = futures::future::try_join_all::<[BoxFuture<_>; _]>([
        Box::pin(
            tokio::task::spawn_blocking(move || {
                firehose_worker
                    .join()
                    .map_err(|e| miette::miette!("buffer processor thread died: {e:?}"))
            })
            .map(|r| r.into_diagnostic().flatten().flatten()),
        ),
        Box::pin(ingestor.run()),
    ]);
    if let Err(e) = res.await {
        error!("ingestor or buffer processor died: {e}");
        db::check_poisoned_report(&e);
    }

    if let Err(e) = state.db.persist() {
        db::check_poisoned_report(&e);
        return Err(e);
    }

    Ok(())
}
