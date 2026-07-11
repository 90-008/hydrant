use fjall::Slice;
use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::backfill::client::ThrottledHttpClient;
use crate::backfill::error::BackfillError;
use crate::config::BackfillStrategy;
use crate::db::{Txn as DbTxn, keys};
use crate::ingest::indexer::{IndexerMessage, IndexerTx};
use crate::state::AppState;
use crate::types::{GaugeState, RepoState, RepoStatus, ResyncErrorKind, ResyncState};

use super::process::process_did;

pub(crate) async fn did_task(
    state: &Arc<AppState>,
    http: ThrottledHttpClient,
    buffer_tx: IndexerTx,
    did: &Did<'static>,
    pending_key: Slice,
    _permit: tokio::sync::OwnedSemaphorePermit,
    verify_signatures: bool,
    strategy: BackfillStrategy,
) -> Result<(), BackfillError> {
    let db = &state.db;

    match process_did(
        state,
        &http,
        did,
        pending_key.clone(),
        verify_signatures,
        strategy,
    )
    .await
    {
        Ok(Some(_repo_state)) => {
            let applied = state.db.run({
                let did = did.clone();
                let pending_key = pending_key.clone();
                move |db| {
                    let did_key = keys::repo_key(&did);
                    let mut txn = DbTxn::new(db);
                    let applied =
                        txn.transition_pending_key(&did, pending_key.as_ref(), GaugeState::Synced)?;
                    txn.batch.remove(&db.indexer.pending, pending_key.clone());
                    if applied {
                        txn.batch.remove(&db.indexer.resync, &did_key);
                    }
                    txn.commit()?;
                    Ok::<_, miette::Report>(applied)
                }
            })
            .await?;

            if !applied {
                return Ok(());
            }

            let state = state.clone();
            state.db.run(move |db| {
                db.inner
                    .persist(fjall::PersistMode::Buffer)
                    .into_diagnostic()
            })
            .await?;

            if let Err(e) = buffer_tx
                .send(IndexerMessage::BackfillFinished(did.clone()))
                .await
            {
                error!(err = %e, "failed to send BackfillFinished");
            }
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(BackfillError::Deleted) => {
            warn!("orphaned pending entry, cleaning up");
            state.db.run({
                let did = did.clone();
                let pending_key = pending_key.clone();
                move |db| {
                    let mut txn = DbTxn::new(db);
                    txn.transition_pending_key(&did, pending_key.as_ref(), GaugeState::Synced)?;
                    txn.batch.remove(&db.indexer.pending, pending_key);
                    txn.commit()?;
                    Ok::<_, miette::Report>(())
                }
            })
            .await?;
            Ok(())
        }
        Err(e) => {
            match &e {
                BackfillError::Ratelimited | BackfillError::PreemptivelyThrottled => {
                    debug!("too many requests");
                }
                BackfillError::Transport(reason) => {
                    error!(%reason, "transport error");
                }
                BackfillError::Generic(e) => {
                    error!(err = %e, "failed");
                }
                BackfillError::Deleted => unreachable!("already handled"),
            }

            let error_kind = match &e {
                BackfillError::Ratelimited | BackfillError::PreemptivelyThrottled => {
                    ResyncErrorKind::Ratelimited
                }
                BackfillError::Transport(_) => ResyncErrorKind::Transport,
                BackfillError::Generic(_) => ResyncErrorKind::Generic,
                BackfillError::Deleted => unreachable!("already handled"),
            };

            let did_key = keys::repo_key(did);

            // 1. get current retry count
            let did_key_clone = did_key.clone();
            let existing_state = db
                .run(move |db| {
                    db.indexer
                        .resync
                        .get(&did_key_clone)
                        .into_diagnostic()
                        .and_then(|b| {
                            b.map(|b| rmp_serde::from_slice::<ResyncState>(&b).into_diagnostic())
                                .transpose()
                        })
                })
                .await?;

            let mut retry_count = match existing_state {
                Some(ResyncState::Error { retry_count, .. }) => retry_count,
                Some(ResyncState::Gone { .. }) => return Ok(()), // should handle gone? original code didn't really?
                None => 0,
            };

            // Calculate new stats
            let next_retry = if matches!(e, BackfillError::PreemptivelyThrottled) {
                chrono::Utc::now().timestamp() + 60
            } else {
                retry_count += 1;
                ResyncState::next_backoff(retry_count)
            };

            let resync_state = ResyncState::Error {
                kind: error_kind,
                retry_count,
                next_retry,
            };
            let error_string = e.to_string();

            state.db.run({
                let did_key = did_key.into_static();
                let did = did.clone();
                let pending_key = pending_key.clone();
                move |db| {
                    // 3. save to resync
                    let serialized_resync_state =
                        rmp_serde::to_vec(&resync_state).into_diagnostic()?;

                    // 4. and update the main repo state
                    let serialized_repo_state = if let Some(state_bytes) =
                        db.repos.get(&did_key).into_diagnostic()?
                    {
                        let mut state: RepoState =
                            rmp_serde::from_slice(&state_bytes).into_diagnostic()?;
                        state.active = true;
                        state.status = RepoStatus::Error(error_string.into());
                        Some(rmp_serde::to_vec(&state).into_diagnostic()?)
                    } else {
                        None
                    };
                    let mut txn = DbTxn::new(db);
                    let applied = txn.transition_pending_key(
                        &did,
                        pending_key.as_ref(),
                        GaugeState::Resync(Some(error_kind)),
                    )?;
                    txn.batch.remove(&db.indexer.pending, pending_key.clone());
                    if applied {
                        txn.batch
                            .insert(&db.indexer.resync, &did_key, serialized_resync_state);
                        if let Some(state_bytes) = serialized_repo_state {
                            txn.batch.insert(&db.repos, &did_key, state_bytes);
                        }
                    }
                    txn.commit()?;
                    Ok::<_, miette::Report>(())
                }
            })
            .await?;

            Err(e)
        }
    }
}
