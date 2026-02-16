use crate::db::types::TrimmedDid;
use crate::db::{self, deser_repo_state};
use crate::ops;
use crate::state::AppState;
use crate::types::{RepoStatus, ResyncState};
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

pub fn queue_gone_backfills(state: &Arc<AppState>) -> Result<()> {
    debug!("scanning for deactivated/takendown repos to retry...");
    let mut count = 0;

    let mut batch = state.db.inner.batch();

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

                let Some(state_bytes) = state.db.repos.get(&key).into_diagnostic()? else {
                    error!("repo state not found for {did}");
                    continue;
                };

                // update repo state back to backfilling
                let repo_state = deser_repo_state(&state_bytes)?;
                ops::update_repo_status(
                    &mut batch,
                    &state.db,
                    &did,
                    repo_state,
                    RepoStatus::Backfilling,
                )?;

                count += 1;
            }
        }
    }

    if count == 0 {
        return Ok(());
    }

    batch.commit().into_diagnostic()?;

    state.db.update_count("resync", -count);
    state.db.update_count("pending", count);

    state.notify_backfill();

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

        let mut batch = state.db.inner.batch();

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

            match rmp_serde::from_slice::<ResyncState>(&value) {
                Ok(ResyncState::Error { next_retry, .. }) => {
                    if next_retry <= now {
                        debug!("retrying backfill for {did}");

                        let state_bytes = match state.db.repos.get(&key).into_diagnostic() {
                            Ok(b) => b,
                            Err(err) => {
                                error!("failed to get repo state for {did}: {err}");
                                continue;
                            }
                        };
                        let Some(state_bytes) = state_bytes else {
                            error!("repo state not found for {did}");
                            continue;
                        };

                        let repo_state = match deser_repo_state(&state_bytes) {
                            Ok(s) => s,
                            Err(e) => {
                                error!("failed to deserialize repo state for {did}: {e}");
                                continue;
                            }
                        };
                        let res = ops::update_repo_status(
                            &mut batch,
                            &state.db,
                            &did,
                            repo_state,
                            RepoStatus::Backfilling,
                        );
                        if let Err(e) = res {
                            error!("failed to update repo status for {did}: {e}");
                            continue;
                        }

                        count += 1;
                    }
                }
                Ok(_) => {
                    // not an error state, do nothing
                }
                Err(e) => {
                    error!("failed to deserialize resync state for {did}: {e}");
                    continue;
                }
            }
        }

        if count == 0 {
            continue;
        }

        if let Err(e) = batch.commit() {
            error!("failed to commit batch: {e}");
            db::check_poisoned(&e);
            continue;
        }

        state.db.update_count("resync", -count);
        state.db.update_count("pending", count);
        state.notify_backfill();
        info!("queued {count} retries");
    }
}
