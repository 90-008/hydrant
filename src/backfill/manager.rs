use crate::db::types::TrimmedDid;
use crate::db::{self, deser_repo_state, keys, ser_repo_state};
use crate::state::AppState;
use crate::types::{RepoStatus, ResyncState};
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

pub fn queue_gone_backfills(state: &Arc<AppState>) -> Result<()> {
    debug!("scanning for deactivated/takendown repos to retry...");
    let mut count = 0;

    for guard in state.db.resync.iter() {
        let (key, val) = guard.into_inner().into_diagnostic()?;
        let did = match TrimmedDid::try_from(key.as_ref()) {
            Ok(did) => did.to_did(),
            Err(e) => {
                error!("invalid did in db, skipping: {e}");
                continue;
            }
        };

        if let Ok(resync_state) = rmp_serde::from_slice::<ResyncState>(&val) {
            if matches!(resync_state, ResyncState::Gone { .. }) {
                debug!("queuing retry for gone repo: {did}");

                // move back to pending
                let mut batch = state.db.inner.batch();
                batch.remove(&state.db.resync, key.clone());
                batch.insert(&state.db.pending, key.clone(), Vec::new());

                // update repo state back to backfilling
                if let Some(state_bytes) = state.db.repos.get(&key).into_diagnostic()? {
                    let mut repo_state = deser_repo_state(&state_bytes)?;
                    repo_state.status = RepoStatus::Backfilling;
                    batch.insert(&state.db.repos, key, ser_repo_state(&repo_state)?);
                }

                batch.commit().into_diagnostic()?;
                state.db.update_count("resync", -1);
                state.db.update_count("pending", 1);

                state.notify_backfill();
                count += 1;
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
                    db::check_poisoned(&e);
                    continue;
                }
            };
            let did = match TrimmedDid::try_from(key.as_ref()) {
                Ok(did) => did.to_did(),
                Err(e) => {
                    error!("invalid did in db, skipping: {e}");
                    continue;
                }
            };

            if let Ok(ResyncState::Error { next_retry, .. }) =
                rmp_serde::from_slice::<ResyncState>(&value)
            {
                if next_retry <= now {
                    debug!("retrying backfill for {did}");

                    // move back to pending
                    if let Err(e) = db.pending.insert(keys::repo_key(&did), Vec::new()) {
                        error!("failed to move {did} to pending: {e}");
                        db::check_poisoned(&e);
                        continue;
                    }
                    state.db.update_count("pending", 1);

                    state.notify_backfill();
                    count += 1;
                }
            }
        }

        if count > 0 {
            info!("queued {count} retries");
        }
    }
}
