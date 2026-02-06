use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use crate::db::{self, Db, keys, ser_repo_state};
use crate::ops;
use crate::state::{AppState, BackfillRx};
use crate::types::{AccountEvt, BroadcastEvent, RepoState, RepoStatus, ResyncState, StoredEvent};
use futures::TryFutureExt;
use jacquard::api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard::types::cid::Cid;
use jacquard::types::did::Did;
use jacquard::{CowStr, IntoStatic, prelude::*};
use jacquard_common::xrpc::XrpcError;
use jacquard_repo::mst::Mst;
use jacquard_repo::{BlockStore, MemoryBlockStore};
use miette::{Context, IntoDiagnostic, Result};
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, trace, warn};

pub mod manager;

pub struct BackfillWorker {
    state: Arc<AppState>,
    rx: BackfillRx,
    http: reqwest::Client,
    semaphore: Arc<Semaphore>,
    verify_signatures: bool,
}

impl BackfillWorker {
    pub fn new(
        state: Arc<AppState>,
        rx: BackfillRx,
        timeout: Duration,
        concurrency_limit: usize,
        verify_signatures: bool,
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
            verify_signatures,
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
                    self.verify_signatures,
                )
                .inspect_err(move |e| {
                    error!("backfill process failed for {did}: {e}");
                    db::check_poisoned_report(e);
                }),
            );
        }
    }

    async fn process_did_wrapper(
        state: Arc<AppState>,
        http: reqwest::Client,
        did: Did<'static>,
        _permit: tokio::sync::OwnedSemaphorePermit,
        verify_signatures: bool,
    ) -> Result<()> {
        let db = &state.db;

        match Self::process_did(&state, &http, &did, verify_signatures).await {
            Ok(previous_state) => {
                let did_key = keys::repo_key(&did);

                let was_pending = matches!(previous_state.status, RepoStatus::Backfilling);
                let was_resync = matches!(
                    previous_state.status,
                    RepoStatus::Error(_)
                        | RepoStatus::Deactivated
                        | RepoStatus::Takendown
                        | RepoStatus::Suspended
                );

                let mut batch = db.inner.batch();
                // remove from pending
                if was_pending {
                    batch.remove(&db.pending, &did_key);
                }
                // remove from resync
                if was_resync {
                    batch.remove(&db.resync, &did_key);
                }
                tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                    .await
                    .into_diagnostic()??;
                if was_pending {
                    state.db.update_count_async("pending", -1).await;
                }
                if was_resync {
                    state.db.update_count_async("resync", -1).await;
                }

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
                if let Ok(Some(bytes)) = Db::get(db.resync.clone(), &did_key).await {
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
                    let did_key = did_key.into_static();
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

    async fn process_did<'i>(
        app_state: &Arc<AppState>,
        http: &reqwest::Client,
        did: &Did<'static>,
        verify_signatures: bool,
    ) -> Result<RepoState<'static>> {
        debug!("backfilling {}", did);

        let db = &app_state.db;
        let did_key = keys::repo_key(did);
        let state_bytes = Db::get(db.repos.clone(), did_key)
            .await?
            .ok_or_else(|| miette::miette!("!!!THIS IS A BUG!!! repo state for {did} missing"))?;
        let mut state: RepoState<'static> = rmp_serde::from_slice::<RepoState>(&state_bytes)
            .into_diagnostic()?
            .into_static();
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
            let _ = app_state.db.event_tx.send(ops::make_account_event(db, evt));
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
                    let mut batch = db.inner.batch();
                    ops::delete_repo(&mut batch, db, did)?;
                    batch.commit().into_diagnostic()?;
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
            "fetched {} bytes for {did} in {:?}",
            car_bytes.body.len(),
            start.elapsed()
        );

        // 3. import repo
        let start = Instant::now();
        let parsed = jacquard_repo::car::reader::parse_car_bytes(&car_bytes.body)
            .await
            .into_diagnostic()?;
        trace!("parsed car for {did} in {:?}", start.elapsed());

        let start = Instant::now();
        let store = Arc::new(MemoryBlockStore::new_from_blocks(parsed.blocks));
        trace!(
            "stored {} blocks in memory for {did} in {:?}",
            store.len(),
            start.elapsed()
        );

        // 4. parse root commit to get mst root
        let root_bytes = store
            .get(&parsed.root)
            .await
            .into_diagnostic()?
            .ok_or_else(|| miette::miette!("root block missing from CAR"))?;

        let root_commit =
            jacquard_repo::commit::Commit::from_cbor(&root_bytes).into_diagnostic()?;
        debug!(
            "backfilling repo at revision {}, root cid {}",
            root_commit.rev, root_commit.data
        );

        // 4.5. verify commit signature
        if verify_signatures {
            let pubkey = app_state.resolver.resolve_signing_key(did).await?;
            root_commit
                .verify(&pubkey)
                .map_err(|e| miette::miette!("signature verification failed for {did}: {e}"))?;
            trace!("signature verified for {did}");
        }

        // 5. walk mst
        let start = Instant::now();
        let mst: Mst<MemoryBlockStore> = Mst::load(store, root_commit.data, None);
        let leaves = mst.leaves().await.into_diagnostic()?;
        trace!("walked mst for {} in {:?}", did, start.elapsed());

        // 6. insert records into db
        let start = Instant::now();
        let (_state, records_cnt_delta, added_blocks, count) = {
            let app_state = app_state.clone();
            let did = did.clone();
            let rev = root_commit.rev;

            tokio::task::spawn_blocking(move || {
                let mut count = 0;
                let mut delta = 0;
                let mut added_blocks = 0;
                let mut collection_counts: HashMap<SmolStr, u64> = HashMap::new();
                let mut batch = app_state.db.inner.batch();
                let store = mst.storage();

                // pre-load existing record CIDs for this DID to detect duplicates/updates
                let prefix = keys::record_prefix(&did);
                let prefix_len = prefix.len();
                let mut existing_cids: HashMap<(SmolStr, SmolStr), SmolStr> = HashMap::new();
                for guard in app_state.db.records.prefix(&prefix) {
                    let (key, cid_bytes) = guard.into_inner().into_diagnostic()?;
                    // extract path (collection/rkey) from key by skipping the DID prefix
                    let mut path_split = key[prefix_len..].split(|b| *b == keys::SEP);
                    let collection =
                        std::str::from_utf8(path_split.next().wrap_err("collection not found")?)
                            .into_diagnostic()?
                            .to_smolstr();
                    let rkey = std::str::from_utf8(path_split.next().wrap_err("rkey not found")?)
                        .into_diagnostic()?
                        .to_smolstr();
                    let cid = std::str::from_utf8(&cid_bytes)
                        .into_diagnostic()?
                        .to_smolstr();
                    existing_cids.insert((collection, rkey), cid);
                }

                for (key, cid) in leaves {
                    let val_bytes = tokio::runtime::Handle::current()
                        .block_on(store.get(&cid))
                        .into_diagnostic()?;

                    if let Some(val) = val_bytes {
                        let (collection, rkey) = ops::parse_path(&key)?;
                        let path = (collection.to_smolstr(), rkey.to_smolstr());
                        let cid = Cid::ipld(cid);

                        // check if this record already exists with same CID
                        let (action, is_new) =
                            if let Some(existing_cid) = existing_cids.remove(&path) {
                                if existing_cid == cid.as_str() {
                                    debug!("skip {did}/{collection}/{rkey} ({cid})");
                                    continue; // skip unchanged record
                                }
                                ("update", false)
                            } else {
                                ("create", true)
                            };
                        debug!("{action} {did}/{collection}/{rkey} ({cid})");

                        let db_key = keys::record_key(&did, &collection, &rkey);

                        batch.insert(
                            &app_state.db.blocks,
                            keys::block_key(cid.as_str()),
                            val.as_ref(),
                        );
                        batch.insert(&app_state.db.records, db_key, cid.as_str().as_bytes());

                        added_blocks += 1;
                        if is_new {
                            delta += 1;
                            *collection_counts.entry(path.0.clone()).or_default() += 1;
                        }

                        let event_id = app_state.db.next_event_id.fetch_add(1, Ordering::SeqCst);
                        let evt = StoredEvent {
                            live: false,
                            did: TrimmedDid::from(&did),
                            rev: DbTid::from(rev.clone()),
                            collection: CowStr::Borrowed(collection),
                            rkey: DbRkey::new(rkey),
                            action: DbAction::from(action),
                            cid: Some(cid.to_ipld().expect("valid cid")),
                        };
                        let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
                        batch.insert(&app_state.db.events, keys::event_key(event_id), bytes);

                        count += 1;
                    }
                }

                // remove any remaining existing records (they weren't in the new MST)
                for ((collection, rkey), cid) in existing_cids {
                    debug!("remove {did}/{collection}/{rkey} ({cid})");
                    batch.remove(
                        &app_state.db.records,
                        keys::record_key(&did, &collection, &rkey),
                    );

                    let event_id = app_state.db.next_event_id.fetch_add(1, Ordering::SeqCst);
                    let evt = StoredEvent {
                        live: false,
                        did: TrimmedDid::from(&did),
                        rev: DbTid::from(rev.clone()),
                        collection: CowStr::Borrowed(&collection),
                        rkey: DbRkey::new(&rkey),
                        action: DbAction::Delete,
                        cid: None,
                    };
                    let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
                    batch.insert(&app_state.db.events, keys::event_key(event_id), bytes);

                    delta -= 1;
                    count += 1;
                }

                // 6. update status to synced
                state.status = RepoStatus::Synced;
                state.rev = Some(rev.clone().into());
                state.data = Some(root_commit.data);
                state.last_updated_at = chrono::Utc::now().timestamp();

                batch.insert(
                    &app_state.db.repos,
                    keys::repo_key(&did),
                    ser_repo_state(&state)?,
                );

                // add the counts
                for (col, cnt) in collection_counts {
                    db::set_record_count(&mut batch, &app_state.db, &did, &col, cnt);
                }

                batch.commit().into_diagnostic()?;

                Ok::<_, miette::Report>((state, delta, added_blocks, count))
            })
            .await
            .into_diagnostic()??
        };
        trace!("did {count} ops for {did} in {:?}", start.elapsed());

        // do the counts
        if records_cnt_delta > 0 {
            app_state
                .db
                .update_count_async("records", records_cnt_delta)
                .await;
            app_state
                .db
                .update_count_async("blocks", added_blocks)
                .await;
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
