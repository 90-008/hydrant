use crate::db::keys::reconstruct_did;
use crate::db::Db;
use crate::state::AppState;
use crate::types::ErrorState;
use fjall::Slice;
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub async fn queue_pending_backfills(state: &AppState) -> Result<()> {
    info!("scanning for pending backfills...");
    let mut count = 0;

    let ks = state.db.pending.clone();
    let items = tokio::task::spawn_blocking(move || {
        let mut collected: Vec<Slice> = Vec::new();
        for item in ks.iter() {
            let k = item.key().into_diagnostic()?;
            collected.push(k);
        }
        Ok::<Vec<Slice>, miette::Report>(collected)
    })
    .await
    .into_diagnostic()??;

    for key in items {
        let did_str = String::from_utf8_lossy(&key);
        let Ok(did) = reconstruct_did(&did_str) else {
            error!("invalid did in db, skipping: did:{did_str}");
            continue;
        };

        debug!("queuing did {did}");
        if let Err(e) = state.backfill_tx.send(did) {
            warn!("failed to queue pending backfill for did:{did_str}: {e}");
        } else {
            count += 1;
        }
    }

    info!("queued {count} pending backfills");
    Ok(())
}

pub async fn retry_worker(state: Arc<AppState>) {
    let db = &state.db;
    info!("retry worker started");
    loop {
        // sleep first (e.g., check every minute)
        tokio::time::sleep(Duration::from_secs(60)).await;

        let now = chrono::Utc::now().timestamp();
        let mut count = 0;

        let ks = db.errors.clone();
        let items = tokio::task::spawn_blocking(move || {
            let mut collected: Vec<(Slice, Slice)> = Vec::new();
            for item in ks.iter() {
                let (k, v) = item.into_inner().into_diagnostic()?;
                collected.push((k, v));
            }
            Ok::<_, miette::Report>(collected)
        })
        .await
        .into_diagnostic()
        .unwrap_or_else(|e| {
            warn!("failed to scan errors: {e}");
            Db::check_poisoned_report(&e);
            Ok(Vec::new())
        })
        .unwrap_or_else(|e| {
            warn!("failed to scan errors: {e}");
            Db::check_poisoned_report(&e);
            Vec::new()
        });

        for (key, value) in items {
            let did_str = String::from_utf8_lossy(&key);
            let Ok(did) = reconstruct_did(&did_str) else {
                error!("invalid did in db, skipping: {did_str}");
                continue;
            };
            if let Ok(err_state) = rmp_serde::from_slice::<ErrorState>(&value) {
                if err_state.next_retry <= now {
                    debug!("retrying backfill for {did}");

                    // move back to pending
                    if let Err(e) = Db::insert(db.pending.clone(), key, Vec::new()).await {
                        warn!("failed to move {did} to pending: {e}");
                        Db::check_poisoned_report(&e);
                        continue;
                    }

                    // queue
                    if let Err(e) = state.backfill_tx.send(did.to_owned()) {
                        warn!("failed to queue retry for {did}: {e}");
                    } else {
                        count += 1;
                    }
                }
            }
        }

        if count > 0 {
            info!("queued {count} retries");
        }
    }
}
