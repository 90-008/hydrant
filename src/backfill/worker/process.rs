use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use fjall::Slice;
use miette::{IntoDiagnostic, Result};
use tracing::{debug, error, trace, warn};
use smol_str::{SmolStr, ToSmolStr};

use jacquard_api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard_common::IntoStatic;
use jacquard_common::types::cid::Cid as AtCid;
use jacquard_common::types::did::Did;
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use jacquard_repo::mst::Mst;
use jacquard_repo::{BlockStore, MemoryBlockStore};

use crate::backfill::client::ThrottledHttpClient;
use crate::backfill::error::BackfillError;
use crate::backfill::sparse::{SparseBackfillResult, process_did_sparse};
use crate::config::BackfillStrategy;
use crate::db::types::{DbAction, DbRkey, TrimmedDid};
use crate::db::{self, CountDeltas, Db, keys, ser_repo_state};
use crate::filter::FilterMode;
use crate::ops;
use crate::sparse_mst::sparse_probe_collection;
use crate::state::AppState;
use crate::types::{Commit, GaugeState, RepoState, RepoStatus, ResyncState};
use crate::util::url_to_fluent_uri;

#[cfg(feature = "indexer_stream")]
use crate::types::{AccountEvt, BroadcastEvent, StoredData, StoredEvent};
#[cfg(feature = "indexer_stream")]
use jacquard_common::CowStr;
#[cfg(feature = "indexer_stream")]
use std::sync::atomic::Ordering;

pub(crate) async fn process_did(
    app_state: &Arc<AppState>,
    http: &ThrottledHttpClient,
    did: &Did<'static>,
    pending_key: Slice,
    verify_signatures: bool,
    strategy: BackfillStrategy,
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

    #[cfg(feature = "indexer_stream")]
    let emit_identity = |status: &RepoStatus, active: bool| {
        let status = match status {
            RepoStatus::Deactivated => "deactivated",
            RepoStatus::Takendown => "takendown",
            RepoStatus::Suspended => "suspended",
            RepoStatus::Deleted => "deleted",
            RepoStatus::Desynchronized => "desynchronized",
            RepoStatus::Throttled => "throttled",
            _ => "",
        };
        let evt = AccountEvt {
            did: did.clone(),
            active,
            status: status
                .is_empty()
                .then_some(None)
                .unwrap_or_else(|| Some(status.into())),
        };
        let _ = app_state.db.event_tx.send(ops::make_account_event(db, evt));
    };

    if strategy != BackfillStrategy::Full {
        let filter = app_state.filter.load();
        let sparse_supported = !filter.collections.is_empty()
            && sparse_probe_collection(&filter.collections).is_some();
        let should_try_sparse = match strategy {
            BackfillStrategy::Full => false,
            BackfillStrategy::SparseFilter => sparse_supported,
            BackfillStrategy::Auto => sparse_supported,
        };

        if should_try_sparse {
            match process_did_sparse(
                app_state,
                http,
                did,
                &pds,
                state.clone(),
                verify_signatures,
                strategy,
            )
            .await
            {
                Ok(SparseBackfillResult::Imported(sparse)) => {
                    #[cfg(feature = "indexer_stream")]
                    if sparse.state.active != previous_state.active
                        || sparse.state.status != previous_state.status
                        || previous_state.pds.is_none()
                    {
                        emit_identity(&sparse.state.status, sparse.state.active);
                    }

                    trace!(
                        records = sparse.records,
                        node_blocks = sparse.node_blocks,
                        node_bytes = sparse.node_bytes,
                        active = sparse.state.active,
                        status = ?sparse.state.status,
                        "sparse backfill complete"
                    );
                    return Ok(Some(previous_state));
                }
                Ok(SparseBackfillResult::Discarded) => {
                    let did_key = keys::repo_key(did);
                    let metadata_key = keys::repo_metadata_key(did);
                    let app_state_clone = app_state.clone();
                    let did = did.clone();
                    let pending_key = pending_key.clone();
                    tokio::task::spawn_blocking(move || {
                        let mut batch = app_state_clone.db.inner.batch();
                        let mut count_deltas = CountDeltas::default();
                        let mut lifecycle_counts = app_state_clone.db.lifecycle_counts();
                        let applied = lifecycle_counts.transition_pending_key(
                            &mut batch,
                            &did,
                            pending_key.as_ref(),
                            GaugeState::Synced,
                        )?;
                        batch.remove(&app_state_clone.db.pending, pending_key.clone());
                        if applied {
                            batch.remove(&app_state_clone.db.repos, &did_key);
                            batch.remove(&app_state_clone.db.repo_metadata, &metadata_key);
                            count_deltas.add_repos(-1);
                        }
                        let lifecycle_reservation = lifecycle_counts.stage(&mut batch);
                        let reservation = app_state_clone
                            .db
                            .stage_count_deltas(&mut batch, &count_deltas);
                        batch.commit().into_diagnostic().inspect(|_| {
                            app_state_clone
                                .db
                                .apply_lifecycle_counts(lifecycle_reservation);
                            app_state_clone.db.apply_count_deltas(&count_deltas);
                            drop(reservation);
                        })
                    })
                    .await
                    .into_diagnostic()??;

                    return Ok(None);
                }
                Ok(SparseBackfillResult::Skipped) => {
                    debug!("sparse backfill skipped, falling back to full getRepo");
                }
                Err(
                    e @ (BackfillError::Ratelimited
                    | BackfillError::PreemptivelyThrottled
                    | BackfillError::Transport(_)),
                ) => {
                    return Err(e);
                }
                Err(e) => {
                    warn!(err = %e, "sparse backfill failed, falling back to full getRepo");
                }
            }
        } else if strategy == BackfillStrategy::SparseFilter {
            debug!("sparse backfill requested but filter is not sparse-compatible");
        }
    }

    // 2. fetch repo (car)
    let start = Instant::now();
    let req = GetRepo::new().did(did.clone()).build();
    let throttle = app_state.throttler.get_handle(&pds).await;
    if throttle.is_throttled() {
        return Err(BackfillError::PreemptivelyThrottled);
    }
    let tier = app_state.resolve_pds_tier(pds.host_str().unwrap_or(""));
    let resp = {
        let _permit = throttle.acquire().await;
        if throttle.is_throttled() {
            return Err(BackfillError::PreemptivelyThrottled);
        }
        throttle.wait_for_allow(1, &tier).await;
        if throttle.is_throttled() {
            return Err(BackfillError::PreemptivelyThrottled);
        }
        match http.xrpc(url_to_fluent_uri(&pds)).send(&req).await {
            Ok(resp) => {
                if !throttle.is_throttled() {
                    throttle.record_success();
                }
                resp
            }
            Err(e) => return Err(BackfillError::from_sparse_client(e, &throttle)),
        }
    };

    let car_bytes = match resp.into_output() {
        Ok(o) => o,
        Err(XrpcError::Xrpc(e)) => {
            if matches!(e, GetRepoError::RepoNotFound(_)) {
                warn!("repo not found, deleting");
                let mut batch = db.inner.batch();
                let mut lifecycle_counts = db.lifecycle_counts();
                let applied = lifecycle_counts.transition_pending_key(
                    &mut batch,
                    did,
                    pending_key.as_ref(),
                    GaugeState::Synced,
                )?;
                batch.remove(&db.pending, pending_key.clone());
                if applied {
                    if let Err(e) = crate::ops::delete_repo(&mut batch, db, did, &state) {
                        error!(err = %e, "failed to wipe repo during backfill");
                    }
                }
                let lifecycle_reservation = lifecycle_counts.stage(&mut batch);
                batch.commit().into_diagnostic()?;
                db.apply_lifecycle_counts(lifecycle_reservation);
                // return None so did_task skips sending BackfillFinished (nothing to drain for a deleted repo)
                return Ok(None);
            }

            let inactive_status = match e {
                GetRepoError::RepoDeactivated(_) => Some(RepoStatus::Deactivated),
                GetRepoError::RepoTakendown(_) => Some(RepoStatus::Takendown),
                GetRepoError::RepoSuspended(_) => Some(RepoStatus::Suspended),
                _ => None,
            };

            if let Some(status) = inactive_status {
                warn!(?status, "repo is inactive, stopping backfill");

                #[cfg(feature = "indexer_stream")]
                emit_identity(&status, false);

                let resync_state = ResyncState::Gone {
                    status: status.clone(),
                };
                let resync_bytes = rmp_serde::to_vec(&resync_state).into_diagnostic()?;

                let app_state_clone = app_state.clone();
                let did = did.clone();
                let pending_key = pending_key.clone();
                tokio::task::spawn_blocking(move || {
                    let db = &app_state_clone.db;
                    let mut batch = db.inner.batch();
                    let mut lifecycle_counts = db.lifecycle_counts();
                    let applied = lifecycle_counts.transition_pending_key(
                        &mut batch,
                        &did,
                        pending_key.as_ref(),
                        GaugeState::Resync(None),
                    )?;
                    batch.remove(&db.pending, pending_key.clone());
                    if applied {
                        Db::update_repo_state(
                            &mut batch,
                            &db.repos,
                            &did,
                            move |state, (key, batch)| {
                                state.active = false;
                                state.status = status;
                                batch.insert(&db.resync, key, resync_bytes);
                                Ok((true, ()))
                             },
                        )?;
                    }
                    let lifecycle_reservation = lifecycle_counts.stage(&mut batch);
                    batch.commit().into_diagnostic()?;
                    db.apply_lifecycle_counts(lifecycle_reservation);
                    Ok::<_, miette::Report>(())
                })
                .await
                .into_diagnostic()??;

                return Ok(None);
            }

            Err(e).into_diagnostic()?
        }
        Err(e) => Err(e).into_diagnostic()?,
    };

    // emit identity event so any consumers know, but only if something changed
    #[cfg(feature = "indexer_stream")]
    if state.active != previous_state.active
        || state.status != previous_state.status
        || previous_state.pds.is_none()
    {
        emit_identity(&state.status, state.active);
    }

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
    let root_cid = parsed.root;
    let store = Arc::new(MemoryBlockStore::new_from_blocks(parsed.blocks));
    // parsed.blocks was moved into the store; parsed.root is now root_cid. drop the raw
    // CAR bytes here so we don't hold two copies of the block data (raw + parsed) for
    // the entire duration of the spawn_blocking call below.
    drop(car_bytes);
    trace!(
        blocks = store.len(),
        elapsed = ?start.elapsed(),
        "stored blocks in memory"
    );

    // 4. parse root commit to get mst root
    let root_bytes = store
        .get(&root_cid)
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

    let root_commit = Commit::from(root_commit);

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
        #[cfg(feature = "indexer_stream")]
        let rev = root_commit.rev;

        tokio::task::spawn_blocking(move || {
            let filter = app_state.filter.load();
            let ephemeral = app_state.ephemeral;
            let only_index_links = app_state.only_index_links;
            let mut count = 0;
            let mut delta = 0;
            let mut added_blocks = 0;
            let mut collection_counts: HashMap<SmolStr, u64> = HashMap::new();
            let mut batch = app_state.db.inner.batch();
            // clone the Arc so we hold an independent reference to the block store,
            // allowing mst (and its entire loaded node tree) to be freed immediately
            // rather than surviving until the end of spawn_blocking.
            let store = mst.storage().clone();
            drop(mst);

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
                    let cid_obj = AtCid::ipld(cid);

                    *collection_counts.entry(path.0.clone()).or_default() += 1;

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

                    let cid_raw = cid.to_bytes();
                    let block_key = Slice::from(keys::block_key(collection, &cid_raw));
                    if !ephemeral {
                        if !only_index_links {
                            batch.insert(&app_state.db.blocks, block_key.clone(), val.as_ref());
                        }
                        batch.insert(&app_state.db.records, db_key, cid_raw);
                        #[cfg(feature = "backlinks")]
                        if let Ok(value) = serde_ipld_dagcbor::from_slice::<jacquard_common::Data>(val.as_ref()) {
                            crate::backlinks::store::index_record(
                                &mut batch,
                                &app_state.db.backlinks,
                                did.as_str(),
                                collection,
                                &rkey.to_smolstr(),
                                &value,
                            )?;
                        }
                    }

                    added_blocks += 1;
                    if action == DbAction::Create {
                        delta += 1;
                    }

                    #[cfg(feature = "indexer_stream")]
                    {
                        let event_id = app_state.db.next_event_id.fetch_add(1, Ordering::SeqCst);
                        let evt = StoredEvent {
                            live: false,
                            did: TrimmedDid::from(&did),
                            rev,
                            collection: CowStr::Borrowed(collection),
                            rkey,
                            action,
                            data: if ephemeral {
                                StoredData::Block(val)
                            } else if only_index_links {
                                StoredData::Nothing
                            } else {
                                StoredData::Ptr(cid_obj.to_ipld().expect("valid cid"))
                            },
                        };
                        let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
                        batch.insert(&app_state.db.events, keys::event_key(event_id), bytes);

                        #[cfg(feature = "jetstream")]
                        {
                            let jetstream = crate::types::StoredJetstreamEvent::Commit {
                                did: TrimmedDid::from(&did).into_static(),
                                collection: CowStr::Borrowed(collection).into_static(),
                                event_id,
                                live: false,
                            };
                            crate::jetstream::stage_event(&mut batch, &app_state.db, jetstream, None)?;
                        }
                    }

                    count += 1;
                }
            }

            // all blocks have been read; free the MemoryBlockStore now so it does not
            // survive through the final batch commit or any fjall backpressure stall.
            drop(store);

            // remove any remaining existing records (they weren't in the new MST)
            for ((collection, rkey), cid) in existing_cids {
                trace!(collection = %collection, rkey = %rkey, cid = %cid, "remove existing record");

                // we dont have to put if ephemeral around here since
                // existing_cids will be empty anyway
                batch.remove(
                    &app_state.db.records,
                    keys::record_key(&did, &collection, &rkey),
                );
                #[cfg(feature = "backlinks")]
                crate::backlinks::store::delete_record(
                    &mut batch,
                    &app_state.db.backlinks,
                    did.as_str(),
                    &collection,
                    &rkey.to_smolstr(),
                )?;

                #[cfg(feature = "indexer_stream")]
                {
                    let event_id = app_state.db.next_event_id.fetch_add(1, Ordering::SeqCst);
                    let evt = StoredEvent {
                        live: false,
                        did: TrimmedDid::from(&did),
                        rev,
                        collection: CowStr::Borrowed(&collection),
                        rkey,
                        action: DbAction::Delete,
                        data: StoredData::Nothing,
                    };
                    let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
                    batch.insert(&app_state.db.events, keys::event_key(event_id), bytes);

                    #[cfg(feature = "jetstream")]
                    {
                        let jetstream = crate::types::StoredJetstreamEvent::Commit {
                            did: TrimmedDid::from(&did).into_static(),
                            collection: CowStr::Borrowed(&collection).into_static(),
                            event_id,
                            live: false,
                        };
                        crate::jetstream::stage_event(&mut batch, &app_state.db, jetstream, None)?;
                    }
                }

                delta -= 1;
                count += 1;
            }

            if !signal_seen {
                trace!(signals = ?filter.signals, "no signal-matching records found, discarding repo");
                return Ok::<_, miette::Report>(None);
            }

            // 6. update data, status is updated in worker shard
            state.root = Some(root_commit);
            state.touch();

            batch.insert(
                &app_state.db.repos,
                keys::repo_key(&did),
                ser_repo_state(&state)?,
            );

            let metadata_key = keys::repo_metadata_key(&did);
            let metadata_bytes = app_state
                .db
                .repo_metadata
                .get(&metadata_key)
                .into_diagnostic()?
                .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
            let mut metadata = crate::db::deser_repo_meta(&metadata_bytes)?;
            metadata.tracked = true;
            batch.insert(
                &app_state.db.repo_metadata,
                &metadata_key,
                crate::db::ser_repo_meta(&metadata)?,
            );

            // add the counts
            if !ephemeral {
                db::replace_record_counts(
                    &mut batch,
                    &app_state.db,
                    &did,
                    collection_counts.iter().map(|(col, cnt)| (col.as_str(), *cnt)),
                )?;
            }

            let mut count_deltas = CountDeltas::default();
            if delta != 0 {
                count_deltas.add_records(delta);
            }
            if added_blocks > 0 {
                count_deltas.add_blocks(added_blocks);
            }
            let reservation = app_state.db.stage_count_deltas(&mut batch, &count_deltas);
            batch.commit().into_diagnostic()?;
            app_state.db.apply_count_deltas(&count_deltas);
            drop(reservation);

            Ok::<_, miette::Report>(Some(count))
        })
        .await
        .into_diagnostic()??
    };

    let Some(count) = result else {
        // signal mode: no signal-matching records found, clean up the optimistically-added repo
        let did_key = keys::repo_key(did);
        let metadata_key = keys::repo_metadata_key(did);
        let backfill_pending_key = pending_key.clone();
        let app_state = app_state.clone();
        let did = did.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = app_state.db.inner.batch();
            let mut count_deltas = CountDeltas::default();
            let mut lifecycle_counts = app_state.db.lifecycle_counts();
            let applied = lifecycle_counts.transition_pending_key(
                &mut batch,
                &did,
                backfill_pending_key.as_ref(),
                GaugeState::Synced,
            )?;
            batch.remove(&app_state.db.pending, backfill_pending_key.clone());
            if applied {
                batch.remove(&app_state.db.repos, &did_key);
                batch.remove(&app_state.db.repo_metadata, &metadata_key);
                count_deltas.add_repos(-1);
            }
            let lifecycle_reservation = lifecycle_counts.stage(&mut batch);
            let reservation = app_state.db.stage_count_deltas(&mut batch, &count_deltas);
            batch.commit().into_diagnostic().inspect(|_| {
                app_state.db.apply_lifecycle_counts(lifecycle_reservation);
                app_state.db.apply_count_deltas(&count_deltas);
                drop(reservation);
            })
        })
        .await
        .into_diagnostic()??;
        return Ok(None);
    };

    trace!(ops = count, elapsed = %start.elapsed().as_secs_f32(), "did ops");
    trace!(
        elapsed = %start.elapsed().as_secs_f32(),
        "committed backfill batch"
    );

    #[cfg(feature = "indexer_stream")]
    let _ = db.event_tx.send(BroadcastEvent::Persisted(
        db.next_event_id.load(Ordering::SeqCst) - 1,
    ));

    trace!("complete");
    Ok(Some(previous_state))
}
