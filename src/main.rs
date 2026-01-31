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

use crate::backfill::Worker;
use crate::config::Config;
use crate::crawler::Crawler;
use crate::db::Db;
use crate::ingest::Ingestor;
use crate::state::AppState;
use mimalloc::MiMalloc;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> miette::Result<()> {
    let cfg = Config::from_env()?;

    let env_filter = tracing_subscriber::EnvFilter::new(&cfg.log_level);
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    info!("starting hydrant with config: {cfg:?}");

    let (state, backfill_rx) = AppState::new(&cfg)?;
    let state = Arc::new(state);

    tokio::spawn({
        let port = cfg.api_port;
        let state = state.clone();
        async move {
            if let Err(e) = api::serve(state, port).await {
                error!("API server failed: {e}");
            }
        }
    });

    tokio::spawn({
        let state = state.clone();
        let timeout = cfg.repo_fetch_timeout;
        async move {
            let worker = Worker::new(state, backfill_rx, timeout, cfg.backfill_concurrency_limit);
            worker.run().await;
        }
    });

    if let Err(e) = crate::backfill::manager::queue_pending_backfills(&state).await {
        error!("failed to queue pending backfills: {e}");
        Db::check_poisoned_report(&e);
    }

    tokio::spawn({
        let state = state.clone();
        async move {
            crate::backfill::manager::retry_worker(state).await;
        }
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

    tokio::spawn({
        let state = state.clone();
        let persist_interval = cfg.cursor_save_interval;

        async move {
            info!("persistence worker started");
            loop {
                tokio::time::sleep(persist_interval).await;

                let seq = state.cur_firehose.load(Ordering::SeqCst);
                const CURSOR_KEY: &[u8] = b"firehose_cursor";
                if let Err(e) = Db::insert(
                    state.db.cursors.clone(),
                    CURSOR_KEY,
                    seq.to_string().into_bytes(),
                )
                .await
                {
                    error!("failed to save cursor: {e}");
                    Db::check_poisoned_report(&e);
                }

                let state = state.clone();
                let res = tokio::task::spawn_blocking(move || state.db.persist()).await;

                match res {
                    Ok(Err(e)) => {
                        error!("db persist failed: {e}");
                        Db::check_poisoned_report(&e);
                    }
                    Err(e) => {
                        error!("persistence task join failed: {e}");
                    }
                    _ => {}
                }
            }
        }
    });

    if cfg.full_network {
        tokio::spawn({
            let state = state.clone();
            let crawler_host = cfg.relay_host.clone();
            async move {
                let crawler = Crawler::new(state, crawler_host);
                if let Err(e) = crawler.run().await {
                    error!("crawler died: {e}");
                    Db::check_poisoned_report(&e);
                }
            }
        });
    }

    let ingestor = Ingestor::new(state.clone(), cfg.relay_host.clone(), cfg.full_network);

    if let Err(e) = ingestor.run().await {
        error!("ingestor died: {e}");
        Db::check_poisoned_report(&e);
    }

    if let Err(e) = state.db.persist() {
        Db::check_poisoned_report(&e);
        return Err(e);
    }

    Ok(())
}
