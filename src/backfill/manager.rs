use crate::db::types::TrimmedDid;
use crate::db::{self, keys};
use crate::state::AppState;
use crate::types::{GaugeState, ResyncState};
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

                let metadata_key = keys::repo_metadata_key(&did);
                let metadata_bytes = match state
                    .db
                    .repo_metadata
                    .get(&metadata_key)
                    .map(|b| b.ok_or_else(|| miette::miette!("repo metadata not found")))
                    .into_diagnostic()
                    .flatten()
                {
                    Ok(b) => b,
                    Err(e) => {
                        error!(did = %did, err = %e, "failed to get repo metadata");
                        continue;
                    }
                };
                let mut metadata = crate::db::deser_repo_meta(&metadata_bytes)?;

                // move from resync back into pending
                batch.remove(&state.db.resync, key.clone());
                let old_pending = keys::pending_key(metadata.index_id);
                batch.remove(&state.db.pending, old_pending);
                metadata.index_id = rand::random::<u64>();
                batch.insert(
                    &state.db.pending,
                    keys::pending_key(metadata.index_id),
                    key.clone(),
                );
                batch.insert(
                    &state.db.repo_metadata,
                    &metadata_key,
                    crate::db::ser_repo_meta(&metadata)?,
                );

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

                        let metadata_key = keys::repo_metadata_key(&did);
                        let metadata_bytes = match state
                            .db
                            .repo_metadata
                            .get(&metadata_key)
                            .map(|b| b.ok_or_else(|| miette::miette!("repo metadata not found")))
                            .into_diagnostic()
                            .flatten()
                        {
                            Ok(b) => b,
                            Err(e) => {
                                error!(did = %did, err = %e, "failed to get repo metadata");
                                continue;
                            }
                        };
                        let mut metadata = match crate::db::deser_repo_meta(metadata_bytes.as_ref())
                        {
                            Ok(m) => m,
                            Err(e) => {
                                error!(did = %did, err = %e, "failed to deserialize repo metadata");
                                continue;
                            }
                        };

                        // move from resync back into pending
                        batch.remove(&state.db.resync, key.clone());
                        let old_pending = keys::pending_key(metadata.index_id);
                        batch.remove(&state.db.pending, old_pending);
                        metadata.index_id = rand::random::<u64>();
                        batch.insert(
                            &state.db.pending,
                            keys::pending_key(metadata.index_id),
                            key.clone(),
                        );
                        let serialized_metadata = match crate::db::ser_repo_meta(&metadata) {
                            Ok(s) => s,
                            Err(e) => {
                                error!(did = %did, err = %e, "failed to serialize repo metadata");
                                continue;
                            }
                        };
                        batch.insert(&state.db.repo_metadata, &metadata_key, serialized_metadata);

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
