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
    let mut transitions = 0usize;

    let mut txn = db::Txn::new(&state.db);

    for guard in state.db.indexer.resync.iter() {
        let (key, val) = guard.into_inner().into_diagnostic()?;
        let did = match TrimmedDid::try_from(key.as_ref()) {
            Ok(did) => did.to_did(),
            Err(e) => {
                error!(err = %e, "invalid did in db, skipping");
                continue;
            }
        };

        if let Ok(resync_state) = rmp_serde::from_slice::<ResyncState>(&val)
            && matches!(resync_state, ResyncState::Gone { .. })
        {
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
            txn.batch.remove(&state.db.indexer.resync, key.clone());
            let old_pending = keys::pending_key(metadata.index_id);
            txn.batch.remove(&state.db.indexer.pending, old_pending);
            metadata.index_id = rand::random::<u64>();
            txn.batch.insert(
                &state.db.indexer.pending,
                keys::pending_key(metadata.index_id),
                key.clone(),
            );
            txn.batch.insert(
                &state.db.repo_metadata,
                &metadata_key,
                crate::db::ser_repo_meta(&metadata)?,
            );

            txn.transition_lifecycle(&did, GaugeState::Pending)?;
            transitions += 1;
        }
    }

    if transitions == 0 {
        return Ok(());
    }

    txn.commit()?;

    state.notify_backfill();

    info!(count = transitions, "queued gone backfills");
    Ok(())
}

pub fn retry_worker(state: Arc<AppState>) {
    let db = &state.db;
    info!("retry worker started");
    loop {
        // sleep first (e.g., check every minute)
        std::thread::sleep(Duration::from_secs(60));

        let now = chrono::Utc::now().timestamp();
        let mut transitions = 0usize;

        let mut txn = db::Txn::new(&state.db);

        for guard in db.indexer.resync.iter() {
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
                Ok(ResyncState::Error { next_retry, .. }) => {
                    if next_retry <= now {
                        let did_key = keys::repo_key(&did);
                        let is_pds_throttled = if let Ok(Some(state_bytes)) = db.repos.get(&did_key)
                        {
                            if let Ok(repo_state) =
                                rmp_serde::from_slice::<crate::types::RepoState>(&state_bytes)
                            {
                                if let Some(pds_str) = &repo_state.pds {
                                    if let Ok(pds_url) = url::Url::parse(pds_str.as_ref()) {
                                        let now_ts = chrono::Utc::now().timestamp();
                                        state.throttler.snapshot(&pds_url).is_throttled(now_ts)
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if is_pds_throttled {
                            continue;
                        }

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

                        let old_pending = keys::pending_key(metadata.index_id);
                        metadata.index_id = rand::random::<u64>();
                        let new_pending = keys::pending_key(metadata.index_id);
                        let serialized_metadata = match crate::db::ser_repo_meta(&metadata) {
                            Ok(s) => s,
                            Err(e) => {
                                error!(did = %did, err = %e, "failed to serialize repo metadata");
                                continue;
                            }
                        };
                        if let Err(e) = txn.transition_lifecycle(&did, GaugeState::Pending) {
                            error!(did = %did, err = %e, "failed to transition lifecycle");
                            continue;
                        }

                        // move from resync back into pending
                        txn.batch.remove(&state.db.indexer.resync, key.clone());
                        txn.batch.remove(&state.db.indexer.pending, old_pending);
                        txn.batch
                            .insert(&state.db.indexer.pending, new_pending, key.clone());
                        txn.batch.insert(
                            &state.db.repo_metadata,
                            &metadata_key,
                            serialized_metadata,
                        );
                        transitions += 1;
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

        if transitions == 0 {
            continue;
        }

        if let Err(e) = txn.commit() {
            error!(err = %e, "failed to commit batch");
            db::check_poisoned_report(&e);
            continue;
        }
        state.notify_backfill();
        info!(count = transitions, "queued retries");
    }
}
