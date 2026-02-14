use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use crate::db::{self, Db, keys, ser_repo_state};
use crate::ops;
use crate::resolver::ResolverError;
use crate::state::AppState;
use crate::types::{AccountEvt, BroadcastEvent, RepoState, RepoStatus, ResyncState, StoredEvent};

use fjall::Slice;
use jacquard::api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard::error::{ClientError, ClientErrorKind};
use jacquard::types::cid::Cid;
use jacquard::types::did::Did;
use jacquard::{CowStr, IntoStatic, prelude::*};
use jacquard_common::xrpc::XrpcError;
use jacquard_repo::mst::Mst;
use jacquard_repo::{BlockStore, MemoryBlockStore};
use miette::{Diagnostic, IntoDiagnostic, Result};
use reqwest::StatusCode;
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, trace, warn};

pub mod manager;

use crate::ingest::{BufferTx, IngestMessage};

struct AdaptiveLimiter {
    current_limit: usize,
    max_limit: usize,
    min_limit: usize,
}

impl AdaptiveLimiter {
    fn new(start: usize, max: usize) -> Self {
        Self {
            current_limit: start,
            max_limit: max,
            min_limit: 1,
        }
    }

    fn on_success(&mut self) {
        if self.current_limit < self.max_limit {
            self.current_limit += 1;
            debug!("adaptive limiter increased to {}", self.current_limit);
        }
    }

    fn on_failure(&mut self) {
        if self.current_limit > self.min_limit {
            self.current_limit = (self.current_limit / 2).max(self.min_limit);
            debug!("adaptive limiter decreased to {}", self.current_limit);
        }
    }
}

pub struct BackfillWorker {
    state: Arc<AppState>,
    buffer_tx: BufferTx,
    http: reqwest::Client,
    semaphore: Arc<Semaphore>,
    verify_signatures: bool,
    in_flight: Arc<scc::HashSet<Did<'static>>>,
}

impl BackfillWorker {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: BufferTx,
        timeout: Duration,
        concurrency_limit: usize,
        verify_signatures: bool,
    ) -> Self {
        Self {
            state,
            buffer_tx,
            http: reqwest::Client::builder()
                .timeout(timeout)
                .zstd(true)
                .brotli(true)
                .gzip(true)
                .build()
                .expect("failed to build http client"),
            semaphore: Arc::new(Semaphore::new(concurrency_limit)),
            verify_signatures,
            in_flight: Arc::new(scc::HashSet::new()),
        }
    }
}

struct InFlightGuard {
    did: Did<'static>,
    set: Arc<scc::HashSet<Did<'static>>>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let _ = self.set.remove_sync(&self.did);
    }
}

impl BackfillWorker {
    pub async fn run(self) {
        info!("backfill worker started");

        let (feedback_tx, mut feedback_rx) = tokio::sync::mpsc::channel(100);
        let mut limiter = AdaptiveLimiter::new(
            self.semaphore.available_permits(),
            self.semaphore.available_permits(),
        ); // assume start at max

        loop {
            // apply feedback from finished tasks
            while let Ok(was_ratelimited) = feedback_rx.try_recv() {
                if was_ratelimited {
                    limiter.on_failure();
                } else {
                    limiter.on_success();
                }
            }

            let mut spawned = 0;

            for guard in self.state.db.pending.iter() {
                if self.in_flight.len() >= limiter.current_limit {
                    break;
                }

                let key = match guard.key() {
                    Ok(k) => k,
                    Err(e) => {
                        error!("failed to read pending key: {e}");
                        db::check_poisoned(&e);
                        continue;
                    }
                };

                let did = match TrimmedDid::try_from(key.as_ref()) {
                    Ok(d) => d.to_did(),
                    Err(e) => {
                        error!("invalid did '{key:?}' in pending: {e}");
                        continue;
                    }
                };

                if self.in_flight.contains_sync(&did) {
                    continue;
                }
                let _ = self.in_flight.insert_sync(did.clone().into_static());

                let permit = match self.semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let guard = InFlightGuard {
                    did: did.clone().into_static(),
                    set: self.in_flight.clone(),
                };

                let state = self.state.clone();
                let http = self.http.clone();
                let did = did.clone();
                let buffer_tx = self.buffer_tx.clone();
                let verify = self.verify_signatures;
                let feedback_tx = feedback_tx.clone();

                tokio::spawn(async move {
                    let _guard = guard;
                    let res = did_task(&state, http, buffer_tx, &did, key, permit, verify).await;

                    let was_ratelimited = match &res {
                        Err(e) if matches!(e, BackfillError::Ratelimited) => true,
                        _ => false,
                    };

                    if let Err(e) = res {
                        error!("backfill process failed for {did}: {e}");
                        if let BackfillError::Generic(report) = &e {
                            db::check_poisoned_report(report);
                        }
                    }

                    // wake worker to pick up more (in case we were sleeping at limit)
                    state.backfill_notify.notify_one();

                    let _ = feedback_tx.try_send(was_ratelimited);
                });

                spawned += 1;
            }

            if spawned == 0 {
                // if we didn't spawn anything, wait for notification OR feedback
                tokio::select! {
                    _ = self.state.backfill_notify.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}, // poll for feedback if idle
                }
            } else {
                // if we spawned tasks, yield briefly to let them start and avoid tight loop
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

async fn did_task(
    state: &Arc<AppState>,
    http: reqwest::Client,
    buffer_tx: BufferTx,
    did: &Did<'static>,
    pending_key: Slice,
    _permit: tokio::sync::OwnedSemaphorePermit,
    verify_signatures: bool,
) -> Result<(), BackfillError> {
    let db = &state.db;

    match process_did(&state, &http, &did, verify_signatures).await {
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
                batch.remove(&db.pending, pending_key);
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

            let state = state.clone();
            tokio::task::spawn_blocking(move || {
                state
                    .db
                    .inner
                    .persist(fjall::PersistMode::Buffer)
                    .into_diagnostic()
            })
            .await
            .into_diagnostic()??;

            // Notify completion to worker shard
            if let Err(e) = buffer_tx.send(IngestMessage::BackfillFinished(did.clone())) {
                error!("failed to send BackfillFinished for {did}: {e}");
            }
            Ok(())
        }
        Err(e) => {
            let mut was_ratelimited = false;
            match &e {
                BackfillError::Ratelimited => {
                    was_ratelimited = true;
                    debug!("failed for {did}: too many requests");
                }
                BackfillError::Transport(reason) => {
                    error!("failed for {did}: transport error: {reason}");
                }
                BackfillError::Generic(e) => {
                    error!("failed for {did}: {e}");
                }
            }

            let did_key = keys::repo_key(&did);

            // 1. get current retry count
            let mut resync_state = Db::get(db.resync.clone(), &did_key)
                .await
                .and_then(|b| {
                    b.map(|b| rmp_serde::from_slice::<ResyncState>(&b).into_diagnostic())
                        .transpose()
                })?
                .and_then(|s| {
                    matches!(s, ResyncState::Gone { .. })
                        .then_some(None)
                        .unwrap_or(Some(s))
                })
                .unwrap_or_else(|| ResyncState::Error {
                    message: SmolStr::new_static(""),
                    retry_count: 0,
                    next_retry: 0,
                });

            let ResyncState::Error {
                message,
                retry_count,
                next_retry,
            } = &mut resync_state
            else {
                unreachable!("we handled the gone case above");
            };

            // 2. calculate backoff and update the other fields
            *retry_count += was_ratelimited.then_some(1).unwrap_or(1);
            *next_retry = ResyncState::next_backoff(*retry_count);
            *message = e.to_smolstr();

            let error_string = e.to_string();

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
                        state.status = RepoStatus::Error(error_string.into());
                        Some(rmp_serde::to_vec(&state).into_diagnostic()?)
                    } else {
                        None
                    };

                    let mut batch = state.db.inner.batch();
                    batch.insert(&state.db.resync, &did_key, serialized_resync_state);
                    batch.remove(&state.db.pending, pending_key);
                    if let Some(state_bytes) = serialized_repo_state {
                        batch.insert(&state.db.repos, &did_key, state_bytes);
                    }
                    batch.commit().into_diagnostic()
                }
            })
            .await
            .into_diagnostic()??;

            state.db.update_count_async("resync", 1).await;
            state.db.update_count_async("pending", -1).await;

            // add error stats
            if was_ratelimited {
                state.db.update_count_async("error_ratelimited", 1).await;
            } else if let BackfillError::Transport(_) = &e {
                state.db.update_count_async("error_transport", 1).await;
            } else {
                state.db.update_count_async("error_generic", 1).await;
            }
            Err(e)
        }
    }
}

#[derive(Debug, Diagnostic, Error)]
enum BackfillError {
    #[error("{0}")]
    Generic(miette::Report),
    #[error("too many requests")]
    Ratelimited,
    #[error("transport error: {0}")]
    Transport(SmolStr),
}

impl From<ClientError> for BackfillError {
    fn from(e: ClientError) -> Self {
        match e.kind() {
            ClientErrorKind::Http {
                status: StatusCode::TOO_MANY_REQUESTS,
            } => Self::Ratelimited,
            ClientErrorKind::Transport => Self::Transport(
                e.source_err()
                    .expect("transport error without source")
                    .to_smolstr(),
            ),
            _ => Self::Generic(e.into()),
        }
    }
}

impl From<miette::Report> for BackfillError {
    fn from(e: miette::Report) -> Self {
        Self::Generic(e)
    }
}

impl From<ResolverError> for BackfillError {
    fn from(e: ResolverError) -> Self {
        match e {
            ResolverError::Ratelimited => Self::Ratelimited,
            ResolverError::Transport(s) => Self::Transport(s),
            ResolverError::Generic(e) => Self::Generic(e),
        }
    }
}

async fn process_did<'i>(
    app_state: &Arc<AppState>,
    http: &reqwest::Client,
    did: &Did<'static>,
    verify_signatures: bool,
) -> Result<RepoState<'static>, BackfillError> {
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
    let resp = http.xrpc(pds_url).send(&req).await?;

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

            Err(e).into_diagnostic()?
        }
        Err(e) => Err(e).into_diagnostic()?,
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

    let root_commit = jacquard_repo::commit::Commit::from_cbor(&root_bytes).into_diagnostic()?;
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
    trace!("walked mst for {did} in {}", start.elapsed().as_secs_f64());

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

            let prefix = keys::record_prefix(&did);
            let mut existing_cids: HashMap<(SmolStr, DbRkey), SmolStr> = HashMap::new();

            let mut partitions = Vec::new();
            app_state.db.record_partitions.iter_sync(|col, ks| {
                partitions.push((col.clone(), ks.clone()));
                true
            });

            for (col_name, ks) in partitions {
                for guard in ks.prefix(&prefix) {
                    let (key, cid_bytes) = guard.into_inner().into_diagnostic()?;
                    let rkey = keys::parse_rkey(&key[prefix.len()..])
                        .map_err(|e| miette::miette!("invalid rkey '{key:?}' for {did}: {e}"))?;
                    let cid = cid::Cid::read_bytes(cid_bytes.as_ref())
                        .map_err(|e| miette::miette!("invalid cid '{cid_bytes:?}' for {did}: {e}"))?
                        .to_smolstr();

                    existing_cids.insert((col_name.as_str().into(), rkey), cid);
                }
            }

            for (key, cid) in leaves {
                let val_bytes = tokio::runtime::Handle::current()
                    .block_on(store.get(&cid))
                    .into_diagnostic()?;

                if let Some(val) = val_bytes {
                    let (collection, rkey) = ops::parse_path(&key)?;
                    let rkey = DbRkey::new(rkey);
                    let path = (collection.to_smolstr(), rkey.clone());
                    let cid_obj = Cid::ipld(cid);
                    let partition = app_state.db.record_partition(collection)?;

                    // check if this record already exists with same CID
                    let (action, is_new) = if let Some(existing_cid) = existing_cids.remove(&path) {
                        if existing_cid == cid_obj.as_str() {
                            debug!("skip {did}/{collection}/{rkey} ({cid})");
                            continue; // skip unchanged record
                        }
                        (DbAction::Update, false)
                    } else {
                        (DbAction::Create, true)
                    };
                    debug!("{action} {did}/{collection}/{rkey} ({cid})");

                    // Key is just did|rkey
                    let db_key = keys::record_key(&did, &rkey);

                    batch.insert(&app_state.db.blocks, cid.to_bytes(), val.as_ref());
                    batch.insert(&partition, db_key, cid.to_bytes());

                    added_blocks += 1;
                    if is_new {
                        delta += 1;
                        *collection_counts.entry(path.0.clone()).or_default() += 1;
                    }

                    let event_id = app_state.db.next_event_id.fetch_add(1, Ordering::SeqCst);
                    let evt = StoredEvent {
                        live: false,
                        did: TrimmedDid::from(&did),
                        rev: DbTid::from(&rev),
                        collection: CowStr::Borrowed(collection),
                        rkey,
                        action,
                        cid: Some(cid_obj.to_ipld().expect("valid cid")),
                    };
                    let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
                    batch.insert(&app_state.db.events, keys::event_key(event_id), bytes);

                    count += 1;
                }
            }

            // remove any remaining existing records (they weren't in the new MST)
            for ((collection, rkey), cid) in existing_cids {
                debug!("remove {did}/{collection}/{rkey} ({cid})");
                let partition = app_state.db.record_partition(collection.as_str())?;

                batch.remove(&partition, keys::record_key(&did, &rkey));

                let event_id = app_state.db.next_event_id.fetch_add(1, Ordering::SeqCst);
                let evt = StoredEvent {
                    live: false,
                    did: TrimmedDid::from(&did),
                    rev: DbTid::from(&rev),
                    collection: CowStr::Borrowed(&collection),
                    rkey,
                    action: DbAction::Delete,
                    cid: None,
                };
                let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
                batch.insert(&app_state.db.events, keys::event_key(event_id), bytes);

                delta -= 1;
                count += 1;
            }

            // 6. update data, status is updated in worker shard
            state.rev = Some((&rev).into());
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
