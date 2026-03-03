use crate::db::types::TrimmedDid;
use crate::db::{self, deser_repo_state};
use crate::ops;
use crate::state::AppState;
use crate::types::{GaugeState, RepoStatus, ResyncState};
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

pub fn queue_gone_backfills(state: &Arc<AppState>) -> Result<()> {
    debug!("scanning for deactivated/takendown repos to retry...");
    let mut transitions = Vec::new();

    let mut batch = state.db.inner.batch();

    for guard in state.db.resync.iter() {
        let (key, val) = guard.into_inner().into_diagnostic()?;
        let did = match TrimmedDid::try_from(key.as_ref()) {
            Ok(did) => did.to_did(),
            Err(e) => {
                error!(err = %e, "invalid did in db, skipping");
                continue;
            }
        };

        if let Ok(resync_state) = rmp_serde::from_slice::<ResyncState>(&val) {
            if matches!(resync_state, ResyncState::Gone { .. }) {
                debug!(did = %did, "queuing retry for gone repo");

                let Some(state_bytes) = state.db.repos.get(&key).into_diagnostic()? else {
                    error!(did = %did, "repo state not found");
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

                transitions.push((GaugeState::Resync(None), GaugeState::Pending));
            }
        }
    }

    if transitions.is_empty() {
        return Ok(());
    }

    batch.commit().into_diagnostic()?;

    for (old_gauge, new_gauge) in &transitions {
        state.db.update_gauge_diff(old_gauge, new_gauge);
    }

    state.notify_backfill();

    info!(count = transitions.len(), "queued gone backfills");
    Ok(())
}

pub fn retry_worker(state: Arc<AppState>) {
    let db = &state.db;
    info!("retry worker started");
    loop {
        // sleep first (e.g., check every minute)
        std::thread::sleep(Duration::from_secs(60));

        let now = chrono::Utc::now().timestamp();
        let mut transitions = Vec::new();

        let mut batch = state.db.inner.batch();

        for guard in db.resync.iter() {
            let (key, value) = match guard.into_inner() {
                Ok(t) => t,
                Err(e) => {
                    error!(err = %e, "failed to get resync state");
                    db::check_poisoned(&e);
                    continue;
                }
            };
            let did = match TrimmedDid::try_from(key.as_ref()) {
                Ok(did) => did.to_did(),
                Err(e) => {
                    error!(err = %e, "invalid did in db, skipping");
                    continue;
                }
            };

            match rmp_serde::from_slice::<ResyncState>(&value) {
                Ok(ResyncState::Error {
                    kind, next_retry, ..
                }) => {
                    if next_retry <= now {
                        debug!(did = %did, "retrying backfill");

                        let state_bytes = match state.db.repos.get(&key).into_diagnostic() {
                            Ok(b) => b,
                            Err(err) => {
                                error!(did = %did, err = %err, "failed to get repo state");
                                continue;
                            }
                        };
                        let Some(state_bytes) = state_bytes else {
                            error!(did = %did, "repo state not found");
                            continue;
                        };

                        let repo_state = match deser_repo_state(&state_bytes) {
                            Ok(s) => s,
                            Err(e) => {
                                error!(did = %did, err = %e, "failed to deserialize repo state");
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
                            error!(did = %did, err = %e, "failed to update repo status");
                            continue;
                        }

                        transitions.push((GaugeState::Resync(Some(kind)), GaugeState::Pending));
                    }
                }
                Ok(_) => {
                    // not an error state, do nothing
                }
                Err(e) => {
                    error!(did = %did, err = %e, "failed to deserialize resync state");
                    continue;
                }
            }
        }

        if transitions.is_empty() {
            continue;
        }

        if let Err(e) = batch.commit() {
            error!(err = %e, "failed to commit batch");
            db::check_poisoned(&e);
            continue;
        }

        for (old_gauge, new_gauge) in &transitions {
            state.db.update_gauge_diff(old_gauge, new_gauge);
        }
        state.notify_backfill();
        info!(count = transitions.len(), "queued retries");
    }
}
