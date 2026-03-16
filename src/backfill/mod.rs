use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use crate::db::{self, Db, keys, ser_repo_state};
use crate::filter::FilterMode;
use crate::ops;
use crate::resolver::ResolverError;
use crate::state::AppState;
use crate::types::{
    AccountEvt, BroadcastEvent, GaugeState, RepoState, RepoStatus, ResyncErrorKind, ResyncState,
    StoredEvent,
};

use fjall::Slice;
use jacquard_api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard_common::error::{ClientError, ClientErrorKind};
use jacquard_common::types::cid::Cid;
use jacquard_common::types::did::Did;
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use jacquard_common::{CowStr, IntoStatic};
use jacquard_repo::mst::Mst;
use jacquard_repo::{BlockStore, MemoryBlockStore};
use miette::{Diagnostic, IntoDiagnostic, Result};
use reqwest::StatusCode;
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{Instrument, debug, error, info, trace, warn};

pub mod manager;

use crate::ingest::{BufferTx, IngestMessage};

pub struct BackfillWorker {
    state: Arc<AppState>,
    buffer_tx: BufferTx,
    http: reqwest::Client,
    semaphore: Arc<Semaphore>,
    verify_signatures: bool,
    ephemeral: bool,
    in_flight: Arc<scc::HashSet<Did<'static>>>,
}

impl BackfillWorker {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: BufferTx,
        timeout: Duration,
        concurrency_limit: usize,
        verify_signatures: bool,
        ephemeral: bool,
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
            ephemeral,
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

        loop {
            let mut spawned = 0;

            for guard in self.state.db.pending.iter() {
                let (key, value) = match guard.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => {
                        error!(err = %e, "failed to read pending entry");
                        db::check_poisoned(&e);
                        continue;
                    }
                };

                let did = match TrimmedDid::try_from(value.as_ref()) {
                    Ok(d) => d.to_did(),
                    Err(e) => {
                        error!(err = %e, "invalid did in pending value");
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
                let ephemeral = self.ephemeral;

                let span = tracing::info_span!("backfill", did = %did);
                tokio::spawn(
                    async move {
                        let _guard = guard;
                        let res = did_task(
                            &state, http, buffer_tx, &did, key, permit, verify, ephemeral,
                        )
                        .await;

                        if let Err(e) = res {
                            error!(err = %e, "process failed");
                            if let BackfillError::Generic(report) = &e {
                                db::check_poisoned_report(report);
                            }
                        }

                        // wake worker to pick up more (in case we were sleeping at limit)
                        state.backfill_notify.notify_one();
                    }
                    .instrument(span),
                );

                spawned += 1;
            }

            if spawned == 0 {
                // wait for new tasks
                self.state.backfill_notify.notified().await;
            }
            // loop immediately since we might have more tasks
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
    ephemeral: bool,
) -> Result<(), BackfillError> {
    let db = &state.db;

    match process_did(&state, &http, &did, verify_signatures, ephemeral).await {
        Ok(Some(repo_state)) => {
            let did_key = keys::repo_key(&did);

            // determine old gauge state
            // if it was error/suspended etc, we need to know which error kind it was to decrement correctly.
            // we have to peek at the resync state.
            let old_gauge = state.db.repo_gauge_state_async(&repo_state, &did_key).await;

            let mut batch = db.inner.batch();
            // remove from pending
            if old_gauge == GaugeState::Pending {
                batch.remove(&db.pending, pending_key);
            }
            // remove from resync
            if old_gauge.is_resync() {
                batch.remove(&db.resync, &did_key);
            }
            tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                .await
                .into_diagnostic()??;

            state
                .db
                .update_gauge_diff_async(&old_gauge, &GaugeState::Synced)
                .await;

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

            if let Err(e) = buffer_tx.send(IngestMessage::BackfillFinished(did.clone())) {
                error!(err = %e, "failed to send BackfillFinished");
            }
            Ok(())
        }
        Ok(None) => {
            // signal mode: repo had no matching records, was cleaned up by process_did
            state.db.update_count_async("repos", -1).await;
            state.db.update_count_async("pending", -1).await;
            Ok(())
        }
        Err(BackfillError::Deleted) => {
            warn!("orphaned pending entry, cleaning up");
            // orphaned pending entry, clean it up
            Db::remove(db.pending.clone(), pending_key).await?;
            state.db.update_count_async("pending", -1).await;
            Ok(())
        }
        Err(e) => {
            match &e {
                BackfillError::Ratelimited => {
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
                BackfillError::Ratelimited => ResyncErrorKind::Ratelimited,
                BackfillError::Transport(_) => ResyncErrorKind::Transport,
                BackfillError::Generic(_) => ResyncErrorKind::Generic,
                BackfillError::Deleted => unreachable!("already handled"),
            };

            let did_key = keys::repo_key(&did);

            // 1. get current retry count
            let existing_state = Db::get(db.resync.clone(), &did_key).await.and_then(|b| {
                b.map(|b| rmp_serde::from_slice::<ResyncState>(&b).into_diagnostic())
                    .transpose()
            })?;

            let (mut retry_count, prev_kind) = match existing_state {
                Some(ResyncState::Error {
                    kind, retry_count, ..
                }) => (retry_count, Some(kind)),
                Some(ResyncState::Gone { .. }) => return Ok(()), // should handle gone? original code didn't really?
                None => (0, None),
            };

            // Calculate new stats
            retry_count += 1;
            let next_retry = ResyncState::next_backoff(retry_count);

            let resync_state = ResyncState::Error {
                kind: error_kind.clone(),
                retry_count,
                next_retry,
            };

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
                    batch.remove(&state.db.pending, pending_key.clone());
                    if let Some(state_bytes) = serialized_repo_state {
                        batch.insert(&state.db.repos, &did_key, state_bytes);
                    }
                    batch.commit().into_diagnostic()
                }
            })
            .await
            .into_diagnostic()??;

            let old_gauge = prev_kind
                .map(|k| GaugeState::Resync(Some(k)))
                .unwrap_or(GaugeState::Pending);

            let new_gauge = GaugeState::Resync(Some(error_kind));

            state
                .db
                .update_gauge_diff_async(&old_gauge, &new_gauge)
                .await;

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
    #[error("repo was concurrently deleted")]
    Deleted,
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
    ephemeral: bool,
) -> Result<Option<RepoState<'static>>, BackfillError> {
    debug!("starting...");

    // always invalidate doc before backfilling
    app_state.resolver.invalidate(did).await;

    let db = &app_state.db;
    let did_key = keys::repo_key(did);
    let Some(state_bytes) = Db::get(db.repos.clone(), did_key).await? else {
        return Err(BackfillError::Deleted);
    };
    let mut state: RepoState<'static> = rmp_serde::from_slice::<RepoState>(&state_bytes)
        .into_diagnostic()?
        .into_static();
    let previous_state = state.clone();

    // 1. resolve pds
    let start = Instant::now();
    let doc = app_state.resolver.resolve_doc(did).await?;
    let pds = doc.pds.clone();
    trace!(
        pds = %doc.pds,
        handle = %doc.handle.as_deref().unwrap_or("handle.invalid"),
        elapsed = %start.elapsed().as_secs_f32(),
        "resolved to pds"
    );
    state.update_from_doc(doc);

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
    let resp = http.xrpc(pds).send(&req).await?;

    let car_bytes = match resp.into_output() {
        Ok(o) => o,
        Err(XrpcError::Xrpc(e)) => {
            if matches!(e, GetRepoError::RepoNotFound(_)) {
                warn!("repo not found, deleting");
                let mut batch = db::refcount::RefcountedBatch::new(db);
                if let Err(e) = crate::ops::delete_repo(&mut batch, db, did, &state) {
                    error!(err = %e, "failed to wipe repo during backfill");
                }
                batch.commit().into_diagnostic()?;
                return Ok(Some(previous_state)); // stop backfill
            }

            let inactive_status = match e {
                GetRepoError::RepoDeactivated(_) => Some(RepoStatus::Deactivated),
                GetRepoError::RepoTakendown(_) => Some(RepoStatus::Takendown),
                GetRepoError::RepoSuspended(_) => Some(RepoStatus::Suspended),
                _ => None,
            };

            if let Some(status) = inactive_status {
                warn!(?status, "repo is inactive, stopping backfill");

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
                return Ok(Some(previous_state));
            }

            Err(e).into_diagnostic()?
        }
        Err(e) => Err(e).into_diagnostic()?,
    };

    // emit identity event so any consumers know
    emit_identity(&state.status);

    trace!(
        bytes = car_bytes.body.len(),
        elapsed = ?start.elapsed(),
        "fetched car bytes"
    );

    // 3. import repo
    let start = Instant::now();
    let parsed = jacquard_repo::car::reader::parse_car_bytes(&car_bytes.body)
        .await
        .into_diagnostic()?;
    trace!(elapsed = %start.elapsed().as_secs_f32(), "parsed car");

    let start = Instant::now();
    let store = Arc::new(MemoryBlockStore::new_from_blocks(parsed.blocks));
    trace!(
        blocks = store.len(),
        elapsed = ?start.elapsed(),
        "stored blocks in memory"
    );

    // 4. parse root commit to get mst root
    let root_bytes = store
        .get(&parsed.root)
        .await
        .into_diagnostic()?
        .ok_or_else(|| miette::miette!("root block missing from CAR"))?;

    let root_commit = jacquard_repo::commit::Commit::from_cbor(&root_bytes).into_diagnostic()?;
    debug!(
        rev = %root_commit.rev,
        cid = %root_commit.data,
        "repo at revision"
    );

    // 4.5. verify commit signature
    if verify_signatures {
        let pubkey = app_state.resolver.resolve_signing_key(did).await?;
        root_commit
            .verify(&pubkey)
            .map_err(|e| miette::miette!("signature verification failed for {did}: {e}"))?;
        trace!("signature verified");
    }

    // 5. walk mst
    let start = Instant::now();
    let mst: Mst<MemoryBlockStore> = Mst::load(store, root_commit.data, None);
    let leaves = mst.leaves().await.into_diagnostic()?;
    trace!(elapsed = %start.elapsed().as_secs_f32(), "walked mst");

    // 6. insert records into db
    let start = Instant::now();
    let result = {
        let app_state = app_state.clone();
        let did = did.clone();
        let rev = root_commit.rev;

        tokio::task::spawn_blocking(move || {
            let filter = app_state.filter.load();
            let mut count = 0;
            let mut delta = 0;
            let mut added_blocks = 0;
            let mut collection_counts: HashMap<SmolStr, u64> = HashMap::new();
            let mut batch = db::refcount::RefcountedBatch::new(&app_state.db);
            let store = mst.storage();

            let prefix = keys::record_prefix_did(&did);
            let mut existing_cids: HashMap<(SmolStr, DbRkey), SmolStr> = HashMap::new();

            if !ephemeral {
                for guard in app_state.db.records.prefix(&prefix) {
                    let (key, cid_bytes) = guard.into_inner().into_diagnostic()?;
                    // key is did|collection|rkey
                    // skip did|
                    let mut remaining = key[prefix.len()..].splitn(2, |b| keys::SEP.eq(b));
                    let collection_raw = remaining
                        .next()
                        .ok_or_else(|| miette::miette!("invalid record key format: {key:?}"))?;
                    let rkey_raw = remaining
                        .next()
                        .ok_or_else(|| miette::miette!("invalid record key format: {key:?}"))?;

                    let collection = std::str::from_utf8(collection_raw)
                        .map_err(|e| miette::miette!("invalid collection utf8: {e}"))?;

                    let rkey = keys::parse_rkey(rkey_raw)
                        .map_err(|e| miette::miette!("invalid rkey '{key:?}' for {did}: {e}"))?;

                    let cid = cid::Cid::read_bytes(cid_bytes.as_ref())
                        .map_err(|e| miette::miette!("invalid cid '{cid_bytes:?}' for {did}: {e}"))?
                        .to_smolstr();

                    existing_cids.insert((collection.into(), rkey), cid);
                }
            }

            let mut signal_seen = filter.mode == FilterMode::Full || filter.signals.is_empty();

            for (key, cid) in leaves {
                let val_bytes = tokio::runtime::Handle::current()
                    .block_on(store.get(&cid))
                    .into_diagnostic()?;

                if let Some(val) = val_bytes {
                    let (collection, rkey) = ops::parse_path(&key)?;

                    if !filter.matches_collection(collection) {
                        continue;
                    }

                    if !signal_seen && filter.matches_signal(collection) {
                        debug!(collection = %collection, "signal matched");
                        signal_seen = true;
                    }

                    let rkey = DbRkey::new(rkey);
                    let path = (collection.to_smolstr(), rkey.clone());
                    let cid_obj = Cid::ipld(cid);

                    // check if this record already exists with same CID
                    let existing_cid = existing_cids.remove(&path);
                    let action = if let Some(existing_cid) = &existing_cid {
                        if existing_cid == cid_obj.as_str() {
                            trace!(collection = %collection, rkey = %rkey, cid = %cid, "skip unchanged record");
                            continue; // skip unchanged record
                        }
                        DbAction::Update
                    } else {
                        DbAction::Create
                    };
                    trace!(collection = %collection, rkey = %rkey, cid = %cid, ?action, "action record");

                    // key is did|collection|rkey
                    let db_key = keys::record_key(&did, collection, &rkey);

                    batch.batch_mut().insert(&app_state.db.blocks, cid.to_bytes(), val.as_ref());
                    if !ephemeral {
                        batch.batch_mut().insert(&app_state.db.records, db_key, cid.to_bytes());
                    }

                    added_blocks += 1;
                    if action == DbAction::Create {
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
                    batch.batch_mut().insert(&app_state.db.events, keys::event_key(event_id), bytes);

                    // update block refcount
                    let cid_bytes = Slice::from(cid.to_bytes());
                    // if ephemeral, we only care about events, so its 1
                    // if not, then its 2 since we also insert to records
                    batch.update_block_refcount(cid_bytes, ephemeral.then_some(1).unwrap_or(2))?;
                    // for Update, also decrement old CID refcount
                    // event will still be there, so we only decrement for records
                    // which means only if not ephemeral
                    if !ephemeral && action == DbAction::Update {
                        let existing_cid = existing_cid.expect("that cid exists since this is Update");
                        if existing_cid != cid_obj.as_str() {
                            let old_cid_bytes = Slice::from(
                                cid::Cid::from_str(&existing_cid)
                                    .expect("valid cid from existing_cids")
                                    .to_bytes()
                            );
                            batch.update_block_refcount(old_cid_bytes, -1)?;
                        }
                    }

                    count += 1;
                }
            }

            // remove any remaining existing records (they weren't in the new MST)
            for ((collection, rkey), cid) in existing_cids {
                trace!(collection = %collection, rkey = %rkey, cid = %cid, "remove existing record");

                // we dont have to put if ephemeral around here since
                // existing_cids will be empty anyyway
                batch.batch_mut().remove(
                    &app_state.db.records,
                    keys::record_key(&did, &collection, &rkey),
                );

                // decrement block refcount
                let cid_bytes = Slice::from(
                    cid::Cid::from_str(&cid)
                        .expect("valid cid from existing_cids")
                        .to_bytes()
                );
                batch.update_block_refcount(cid_bytes, -1)?;

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
                batch.batch_mut().insert(&app_state.db.events, keys::event_key(event_id), bytes);

                delta -= 1;
                count += 1;
            }

            if !signal_seen {
                trace!(signals = ?filter.signals, "no signal-matching records found, discarding repo");
                return Ok::<_, miette::Report>(None);
            }

            // 6. update data, status is updated in worker shard
            state.tracked = true;
            state.rev = Some((&rev).into());
            state.data = Some(root_commit.data);
            state.touch();

            batch.batch_mut().insert(
                &app_state.db.repos,
                keys::repo_key(&did),
                ser_repo_state(&state)?,
            );

            // add the counts
            if !ephemeral {
                for (col, cnt) in collection_counts {
                    db::set_record_count(batch.batch_mut(), &app_state.db, &did, &col, cnt);
                }
            }

            batch.commit().into_diagnostic()?;

            Ok::<_, miette::Report>(Some((state, delta, added_blocks, count)))
        })
        .await
        .into_diagnostic()??
    };

    let Some((_state, records_cnt_delta, added_blocks, count)) = result else {
        // signal mode: no signal-matching records found — clean up the optimistically-added repo
        let did_key = keys::repo_key(did);
        let backfill_pending_key = keys::pending_key(previous_state.index_id);
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = app_state.db.inner.batch();
            batch.remove(&app_state.db.repos, &did_key);
            batch.remove(&app_state.db.pending, backfill_pending_key);
            batch.commit().into_diagnostic()
        })
        .await
        .into_diagnostic()??;
        return Ok(None);
    };

    trace!(ops = count, elapsed = %start.elapsed().as_secs_f32(), "did ops");

    // do the counts
    if records_cnt_delta > 0 {
        app_state
            .db
            .update_count_async("records", records_cnt_delta)
            .await;
    }
    if added_blocks > 0 {
        app_state
            .db
            .update_count_async("blocks", added_blocks)
            .await;
    }
    trace!(
        elapsed = %start.elapsed().as_secs_f32(),
        "committed backfill batch"
    );

    let _ = db.event_tx.send(BroadcastEvent::Persisted(
        db.next_event_id.load(Ordering::SeqCst) - 1,
    ));

    trace!("complete");
    Ok(Some(previous_state))
}
