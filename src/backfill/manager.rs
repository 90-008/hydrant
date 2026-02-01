use crate::db::keys::reconstruct_did;
use crate::db::{deser_repo_state, ser_repo_state, Db};
use crate::state::AppState;
use crate::types::{RepoStatus, ResyncState};
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

    for guard in state.db.resync.iter() {
        let (key, val) = guard.into_inner().into_diagnostic()?;
        let did_str = String::from_utf8_lossy(&key);
        let Ok(did) = reconstruct_did(&did_str) else {
            error!("invalid did in db, skipping: did:{did_str}");
            continue;
        };

        if let Ok(resync_state) = rmp_serde::from_slice::<ResyncState>(&val) {
            if matches!(resync_state, ResyncState::Gone { .. }) {
                info!("queuing retry for gone repo: {did}");

                // move back to pending
                let mut batch = state.db.inner.batch();
                batch.remove(&state.db.resync, key.clone());
                batch.insert(&state.db.pending, key, Vec::new());

                // update repo state back to backfilling
                let repo_key = crate::db::keys::repo_key(&did);
                if let Some(state_bytes) = state.db.repos.get(repo_key).into_diagnostic()? {
                    let mut repo_state = deser_repo_state(&state_bytes)?;
                    repo_state.status = RepoStatus::Backfilling;
                    batch.insert(&state.db.repos, repo_key, ser_repo_state(&repo_state)?);
                }

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

        for guard in db.resync.iter() {
            let (key, value) = match guard.into_inner() {
                Ok(t) => t,
                Err(e) => {
                    error!("failed to get resync state: {e}");
                    Db::check_poisoned(&e);
                    continue;
                }
            };
            let did_str = String::from_utf8_lossy(&key);
            let Ok(did) = reconstruct_did(&did_str) else {
                error!("invalid did in db, skipping: did:{did_str}");
                continue;
            };

            if let Ok(ResyncState::Error { next_retry, .. }) =
                rmp_serde::from_slice::<ResyncState>(&value)
            {
                if next_retry <= now {
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
