use crate::db::keys::reconstruct_did;
use crate::db::{deser_repo_state, ser_repo_state, Db};
use crate::state::AppState;
use crate::types::{ErrorState, RepoStatus};
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

pub fn queue_pending_backfills(state: &AppState) -> Result<()> {
    info!("scanning for pending backfills...");
    let mut count = 0;

    for guard in state.db.pending.iter() {
        let key = guard.key().into_diagnostic()?;
        let did_str = String::from_utf8_lossy(&key);
        let Ok(did) = reconstruct_did(&did_str) else {
            error!("invalid did in db, skipping: did:{did_str}");
            continue;
        };

        debug!("queuing did {did}");
        if let Err(e) = state.backfill_tx.send(did) {
            error!("failed to queue pending backfill for did:{did_str}: {e}");
        } else {
            count += 1;
        }
    }

    info!("queued {count} pending backfills");
    Ok(())
}

pub fn queue_gone_backfills(state: &Arc<AppState>) -> Result<()> {
    info!("scanning for deactivated/takendown repos to retry...");
    let mut count = 0;

    for guard in state.db.repos.iter() {
        let (key, val) = guard.into_inner().into_diagnostic()?;
        let did_str = String::from_utf8_lossy(&key);
        let Ok(did) = reconstruct_did(&did_str) else {
            error!("invalid did in db, skipping: did:{did_str}");
            continue;
        };
        if let Ok(repo_state) = deser_repo_state(&val) {
            if matches!(
                repo_state.status,
                RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended
            ) {
                info!("queuing retry for gone repo: {did}");

                let mut new_state = repo_state.clone();
                new_state.status = RepoStatus::Backfilling;
                let bytes = ser_repo_state(&new_state)?;

                let mut batch = state.db.inner.batch();
                batch.insert(&state.db.repos, key.clone(), bytes);
                batch.insert(&state.db.pending, key, Vec::new());
                batch.commit().into_diagnostic()?;

                if let Err(e) = state.backfill_tx.send(did.clone()) {
                    error!("failed to queue retry for {did}: {e}");
                } else {
                    count += 1;
                }
            }
        }
    }

    info!("queued {count} gone backfills");
    Ok(())
}

pub fn retry_worker(state: Arc<AppState>) {
    let db = &state.db;
    info!("retry worker started");
    loop {
        // sleep first (e.g., check every minute)
        std::thread::sleep(Duration::from_secs(60));

        let now = chrono::Utc::now().timestamp();
        let mut count = 0;

        for guard in db.errors.iter() {
            let (key, value) = match guard.into_inner() {
                Ok(t) => t,
                Err(e) => {
                    error!("failed to get error: {e}");
                    Db::check_poisoned(&e);
                    continue;
                }
            };
            let did_str = String::from_utf8_lossy(&key);
            let Ok(did) = reconstruct_did(&did_str) else {
                error!("invalid did in db, skipping: did:{did_str}");
                continue;
            };
            if let Ok(err_state) = rmp_serde::from_slice::<ErrorState>(&value) {
                if err_state.next_retry <= now {
                    info!("retrying backfill for {did}");

                    // move back to pending
                    if let Err(e) = db.pending.insert(key, Vec::new()) {
                        error!("failed to move {did} to pending: {e}");
                        Db::check_poisoned(&e);
                        continue;
                    }

                    // queue
                    if let Err(e) = state.backfill_tx.send(did.clone()) {
                        error!("failed to queue retry for {did}: {e}");
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
