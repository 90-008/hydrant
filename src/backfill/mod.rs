use crate::db::{keys, Db};
use crate::ops;
use crate::state::{AppState, BackfillRx};
use crate::types::{AccountEvt, BroadcastEvent, RepoState, RepoStatus, ResyncState, StoredEvent};
use futures::TryFutureExt;
use jacquard::api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard::types::did::Did;
use jacquard::{prelude::*, IntoStatic};
use jacquard_common::xrpc::XrpcError;
use jacquard_repo::mst::Mst;
use jacquard_repo::MemoryBlockStore;
use miette::{IntoDiagnostic, Result};
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::iter::once;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, trace, warn};

pub mod manager;

pub struct Worker {
    state: Arc<AppState>,
    rx: BackfillRx,
    http: reqwest::Client,
    semaphore: Arc<Semaphore>,
}

impl Worker {
    pub fn new(
        state: Arc<AppState>,
        rx: BackfillRx,
        timeout: Duration,
        concurrency_limit: usize,
    ) -> Self {
        Self {
            state,
            rx,
            http: reqwest::Client::builder()
                .timeout(timeout)
                .zstd(true)
                .brotli(true)
                .gzip(true)
                .build()
                .expect("failed to build http client"),
            semaphore: Arc::new(Semaphore::new(concurrency_limit)),
        }
    }

    pub async fn run(mut self) {
        info!("backfill worker started");
        while let Some(did) = self.rx.recv().await {
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");

            tokio::spawn(
                Self::process_did_wrapper(
                    self.state.clone(),
                    self.http.clone(),
                    did.clone(),
                    permit,
                )
                .inspect_err(move |e| {
                    error!("backfill process failed for {did}: {e}");
                    Db::check_poisoned_report(e);
                }),
            );
        }
    }

    async fn process_did_wrapper(
        state: Arc<AppState>,
        http: reqwest::Client,
        did: Did<'static>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        let db = &state.db;

        // block buffer processing for this DID during backfill
        let _ = state.blocked_dids.insert_async(did.clone()).await;

        match Self::process_did(&state, &http, &did).await {
            Ok(previous_state) => {
                let did_key = keys::repo_key(&did);

                let is_pending = matches!(
                    previous_state.status,
                    RepoStatus::Backfilling | RepoStatus::New
                );
                let was_resync = matches!(
                    previous_state.status,
                    RepoStatus::Error(_)
                        | RepoStatus::Deactivated
                        | RepoStatus::Takendown
                        | RepoStatus::Suspended
                );

                let mut batch = db.inner.batch();
                // remove from pending
                if is_pending {
                    batch.remove(&db.pending, did_key);
                }
                // remove from resync
                if was_resync {
                    batch.remove(&db.resync, did_key);
                }
                tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                    .await
                    .into_diagnostic()??;

                tokio::spawn({
                    let pending_fut = is_pending.then(|| {
                        state
                            .db
                            .increment_count(keys::count_keyspace_key("pending"), -1)
                    });
                    let resync_fut = was_resync.then(|| {
                        state
                            .db
                            .increment_count(keys::count_keyspace_key("resync"), -1)
                    });
                    futures::future::join_all(pending_fut.into_iter().chain(resync_fut))
                });

                let state_for_persist = state.clone();
                tokio::task::spawn_blocking(move || {
                    state_for_persist
                        .db
                        .inner
                        .persist(fjall::PersistMode::Buffer)
                        .into_diagnostic()
                })
                .await
                .into_diagnostic()??;
            }
            Err(e) => {
                error!("backfill failed for {did}: {e}");
                let did_key = keys::repo_key(&did);

                // 1. get current retry count
                let mut retry_count = 0;
                if let Ok(Some(bytes)) = Db::get(db.resync.clone(), did_key).await {
                    if let Ok(ResyncState::Error {
                        retry_count: old_count,
                        ..
                    }) = rmp_serde::from_slice::<ResyncState>(&bytes)
                    {
                        retry_count = old_count + 1;
                    }
                }

                // 2. calculate backoff
                let next_retry = ResyncState::next_backoff(retry_count);

                let resync_state = ResyncState::Error {
                    message: e.to_string().into(),
                    retry_count,
                    next_retry,
                };

                tokio::task::spawn_blocking({
                    let state = state.clone();
                    let did_key = did_key.to_vec();
                    move || {
                        // 3. save to resync
                        let serialized_resync_state =
                            rmp_serde::to_vec(&resync_state).into_diagnostic()?;

                        // 4. and update the main repo state
                        let serialized_repo_state = if let Some(state_bytes) =
                            state.db.repos.get(&did_key).into_diagnostic()?
                        {
                            let mut state: RepoState =
                                rmp_serde::from_slice(&state_bytes).into_diagnostic()?;
                            state.status = RepoStatus::Error(e.to_string().into());
                            Some(rmp_serde::to_vec(&state).into_diagnostic()?)
                        } else {
                            None
                        };

                        let mut batch = state.db.inner.batch();

                        batch.insert(&state.db.resync, &did_key, serialized_resync_state);

                        if let Some(state_bytes) = serialized_repo_state {
                            batch.insert(&state.db.repos, &did_key, state_bytes);
                        }

                        // 5. remove from pending
                        batch.remove(&state.db.pending, &did_key);
                        batch.commit().into_diagnostic()
                    }
                })
                .await
                .into_diagnostic()??;
            }
        }

        // unblock buffer processing for this DID
        state.blocked_dids.remove_async(&did).await;
        Ok(())
    }

    // returns previous repo state if successful
    async fn process_did(
        app_state: &Arc<AppState>,
        http: &reqwest::Client,
        did: &Did<'static>,
    ) -> Result<RepoState> {
        debug!("backfilling {}", did);

        let db = &app_state.db;
        let did_key = keys::repo_key(did);
        let state_bytes = Db::get(db.repos.clone(), did_key)
            .await?
            .ok_or_else(|| miette::miette!("!!!THIS IS A BUG!!! repo state for {did} missing"))?;
        let mut state: RepoState = rmp_serde::from_slice(&state_bytes).into_diagnostic()?;
        let previous_state = state.clone();

        // 1. resolve pds
        let start = Instant::now();
        let (pds_url, handle) = app_state.resolver.resolve_identity_info(did).await?;
        trace!(
            "resolved {did} to pds {pds_url} handle {handle:?} in {:?}",
            start.elapsed()
        );

        if let Some(h) = handle {
            state.handle = Some(h.to_smolstr());
        }

        let emit_identity = |status: &RepoStatus| {
            let evt = AccountEvt {
                did: did.clone(),
                active: !matches!(
                    status,
                    RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended
                ),
                status: Some(
                    match status {
                        RepoStatus::Deactivated => "deactivated",
                        RepoStatus::Takendown => "takendown",
                        RepoStatus::Suspended => "suspended",
                        _ => "active",
                    }
                    .into(),
                ),
            };
            ops::emit_account_event(db, evt);
        };

        // 2. fetch repo (car)
        let start = Instant::now();
        let req = GetRepo::new().did(did.clone()).build();
        let resp = http.xrpc(pds_url).send(&req).await.into_diagnostic()?;

        let car_bytes = match resp.into_output() {
            Ok(o) => o,
            Err(XrpcError::Xrpc(e)) => {
                if matches!(e, GetRepoError::RepoNotFound(_)) {
                    warn!("repo {did} not found, deleting");
                    ops::delete_repo(db, did)?;
                    return Ok(previous_state); // stop backfill
                }

                let inactive_status = match e {
                    GetRepoError::RepoDeactivated(_) => Some(RepoStatus::Deactivated),
                    GetRepoError::RepoTakendown(_) => Some(RepoStatus::Takendown),
                    GetRepoError::RepoSuspended(_) => Some(RepoStatus::Suspended),
                    _ => None,
                };

                if let Some(status) = inactive_status {
                    warn!("repo {did} is {status:?}, stopping backfill");

                    emit_identity(&status);

                    let resync_state = ResyncState::Gone {
                        status: status.clone(),
                    };
                    let resync_bytes = rmp_serde::to_vec(&resync_state).into_diagnostic()?;

                    let app_state_clone = app_state.clone();
                    app_state
                        .db
                        .update_repo_state_async(did, move |state, (key, batch)| {
                            state.status = status;
                            batch.insert(&app_state_clone.db.resync, key, resync_bytes);
                            Ok((true, ()))
                        })
                        .await?;

                    // return success so wrapper stops retrying
                    return Ok(previous_state);
                }

                return Err(e).into_diagnostic();
            }
            Err(e) => return Err(e).into_diagnostic(),
        };

        // emit identity event so any consumers know
        emit_identity(&state.status);

        trace!(
            "fetched {} bytes for {} in {:?}",
            car_bytes.body.len(),
            did,
            start.elapsed()
        );

        // 3. import repo
        let start = Instant::now();
        let parsed = jacquard_repo::car::reader::parse_car_bytes(&car_bytes.body)
            .await
            .into_diagnostic()?;
        trace!("parsed car for {} in {:?}", did, start.elapsed());

        let start = Instant::now();
        let store = Arc::new(MemoryBlockStore::new());
        for (_cid, bytes) in &parsed.blocks {
            jacquard_repo::BlockStore::put(store.as_ref(), bytes)
                .await
                .into_diagnostic()?;
        }
        trace!(
            "stored {} blocks in memory for {} in {:?}",
            parsed.blocks.len(),
            did,
            start.elapsed()
        );

        // 4. parse root commit to get mst root
        let root_bytes = parsed
            .blocks
            .get(&parsed.root)
            .ok_or_else(|| miette::miette!("root block missing from CAR"))?;

        let commit = jacquard_repo::commit::Commit::from_cbor(root_bytes).into_diagnostic()?;
        debug!("backfilling repo at revision {}", commit.rev);

        // 5. walk mst
        let start = Instant::now();
        let mst: Mst<MemoryBlockStore> = Mst::load(store, commit.data, None);
        let leaves = mst.leaves().await.into_diagnostic()?;
        trace!("walked mst for {} in {:?}", did, start.elapsed());

        // 6. insert records into db
        let start = Instant::now();
        let (_state, added_records, added_blocks, collection_counts, count) = {
            let app_state = app_state.clone();
            let did = did.clone();
            let rev = commit.rev;
            let storage = mst.storage().clone();

            tokio::task::spawn_blocking(move || {
                let mut count = 0;
                let mut added_records = 0;
                let mut added_blocks = 0;
                let mut collection_counts: HashMap<SmolStr, i64> = HashMap::new();
                let mut batch = app_state.db.inner.batch();

                for (key, cid) in leaves {
                    let val_bytes = tokio::runtime::Handle::current()
                        .block_on(jacquard_repo::BlockStore::get(storage.as_ref(), &cid))
                        .into_diagnostic()?;

                    if let Some(val) = val_bytes {
                        let parts: Vec<&str> = key.splitn(2, '/').collect();
                        if parts.len() == 2 {
                            let collection = parts[0];
                            let rkey = parts[1];

                            let db_key = keys::record_key(&did, collection, rkey);
                            let cid_str = cid.to_smolstr();

                            let val_vec: Vec<u8> = val.to_vec();
                            batch.insert(
                                &app_state.db.blocks,
                                keys::block_key(&cid_str),
                                val_vec.clone(),
                            );

                            batch.insert(
                                &app_state.db.records,
                                db_key,
                                cid_str.as_bytes().to_vec(),
                            );

                            added_records += 1;
                            added_blocks += 1;
                            *collection_counts
                                .entry(collection.to_smolstr())
                                .or_default() += 1;

                            let event_id =
                                app_state.db.next_event_id.fetch_add(1, Ordering::SeqCst);
                            let evt = StoredEvent::Record {
                                live: false,
                                did: did.clone().into_static(),
                                rev: rev.as_str().into(),
                                collection: collection.into(),
                                rkey: rkey.into(),
                                action: "create".into(),
                                cid: Some(cid_str),
                            };

                            let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
                            batch.insert(
                                &app_state.db.events,
                                keys::event_key(event_id as i64),
                                bytes,
                            );

                            count += 1;
                        }
                    }
                }

                // 6. update status to synced
                state.status = RepoStatus::Synced;
                state.rev = rev.as_str().into();
                state.data = commit.data.to_smolstr();
                state.last_updated_at = chrono::Utc::now().timestamp();

                let did_key = keys::repo_key(&did);
                let bytes = rmp_serde::to_vec(&state).into_diagnostic()?;
                batch.insert(&app_state.db.repos, did_key, bytes);

                batch.commit().into_diagnostic()?;

                Ok::<_, miette::Report>((
                    state,
                    added_records,
                    added_blocks,
                    collection_counts,
                    count,
                ))
            })
            .await
            .into_diagnostic()??
        };

        trace!(
            "inserted {} records into db for {} in {:?}",
            count,
            did,
            start.elapsed()
        );

        // do the counts
        if added_records > 0 {
            tokio::spawn({
                let state = app_state.clone();
                let did = did.clone();
                let records_fut = state
                    .db
                    .increment_count(keys::count_keyspace_key("records"), added_records);
                let blocks_fut = state
                    .db
                    .increment_count(keys::count_keyspace_key("blocks"), added_blocks);
                let events_fut = state
                    .db
                    .increment_count(keys::count_keyspace_key("events"), added_records);
                let collections_futs = collection_counts.into_iter().map(|(col, cnt)| {
                    state
                        .db
                        .increment_count(keys::count_collection_key(&did, &col), cnt)
                });
                futures::future::join_all(
                    once(records_fut)
                        .chain(once(blocks_fut))
                        .chain(once(events_fut))
                        .chain(collections_futs),
                )
            });
        }
        trace!(
            "committed backfill batch for {did} in {:?}",
            start.elapsed()
        );

        let _ = db.event_tx.send(BroadcastEvent::Persisted(
            db.next_event_id.load(Ordering::SeqCst) - 1,
        ));

        // buffer processing is handled by BufferProcessor when blocked flag is cleared
        debug!("backfill complete for {did}");
        Ok(previous_state)
    }
}
