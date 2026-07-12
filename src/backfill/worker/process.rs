use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use fjall::Slice;
use miette::{IntoDiagnostic, Result};
use reqwest::StatusCode;
use smol_str::{SmolStr, ToSmolStr};
use tracing::{debug, error, trace, warn};

use jacquard_api::com_atproto::sync::get_repo::GetRepoError;
use jacquard_common::IntoStatic;
use jacquard_common::types::cid::Cid as AtCid;
use jacquard_common::types::did::Did;
use jacquard_repo::mst::Mst;
use jacquard_repo::{BlockStore, MemoryBlockStore};

use crate::backfill::client::{ThrottledHttpClient, collect_body_bounded};
use crate::backfill::error::BackfillError;
use crate::backfill::sparse::{SparseBackfillResult, process_did_sparse};
use crate::config::{BackfillStrategy, RateTier};
use crate::db::types::{DbAction, DbRkey};
use crate::db::{self, Txn as DbTxn, keys};
use crate::filter::FilterMode;
use crate::ops;
use crate::sparse_mst::sparse_probe_collection;
use crate::state::AppState;
use crate::types::{Commit, GaugeState, RepoState, RepoStatus, ResyncState};
use crate::util::{parse_retry_after, throttle::ThrottleHandle};

#[cfg(feature = "indexer_stream")]
use crate::types::{AccountEvt, BroadcastEvent};
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
    let Some(state_bytes) = db
        .run({
            let did_key = did_key.clone();
            move |db| {
                db.repos
                    .get(did_key)
                    .inspect_err(crate::db::check_poisoned)
                    .into_diagnostic()
            }
        })
        .await?
    else {
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
        let _ = app_state
            .db
            .stream
            .event_tx
            .send(ops::make_account_event(db, evt));
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
                    app_state_clone
                        .db
                        .run(move |db| {
                            let mut txn = DbTxn::new(db);
                            let applied = txn.transition_pending_key(
                                &did,
                                pending_key.as_ref(),
                                GaugeState::Synced,
                            )?;
                            txn.batch.remove(&db.indexer.pending, pending_key);
                            if applied {
                                txn.batch.remove(&db.repos, &did_key);
                                txn.batch.remove(&db.repo_metadata, &metadata_key);
                                txn.counts.add_repos(-1);
                            }
                            txn.commit()
                        })
                        .await?;

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
    let throttle = app_state.throttler.get_handle(&pds).await;
    let tier = app_state.resolve_pds_tier(pds.host_str().unwrap_or(""));
    let car_bytes = match fetch_full_repo_car(
        http,
        &pds,
        did,
        &throttle,
        &tier,
        app_state.max_car_body_bytes,
    )
    .await?
    {
        FullRepoOutcome::Car(body) => body,
        FullRepoOutcome::NotFound => {
            warn!("repo not found, deleting");
            let mut txn = DbTxn::new(db);
            let applied =
                txn.transition_pending_key(did, pending_key.as_ref(), GaugeState::Synced)?;
            txn.batch.remove(&db.indexer.pending, pending_key.clone());
            if applied {
                if let Err(e) = crate::ops::delete_repo(&mut txn.batch, db, did, &state) {
                    error!(err = %e, "failed to wipe repo during backfill");
                }
            }
            txn.commit()?;
            // return None so did_task skips sending BackfillFinished (nothing to drain for a deleted repo)
            return Ok(None);
        }
        FullRepoOutcome::Inactive(status) => {
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
            app_state_clone
                .db
                .run(move |db| {
                    let mut txn = DbTxn::new(db);
                    let applied = txn.transition_pending_key(
                        &did,
                        pending_key.as_ref(),
                        GaugeState::Resync(None),
                    )?;
                    txn.batch.remove(&db.indexer.pending, pending_key.clone());
                    if applied {
                        crate::db::Db::update_repo_state(
                            &mut txn.batch,
                            &db.repos,
                            &did,
                            move |state, (key, batch)| {
                                state.active = false;
                                state.status = status;
                                batch.insert(&db.indexer.resync, key, resync_bytes);
                                Ok((true, ()))
                            },
                        )?;
                    }
                    txn.commit()?;
                    Ok::<_, miette::Report>(())
                })
                .await?;

            return Ok(None);
        }
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
        bytes = car_bytes.len(),
        elapsed = ?start.elapsed(),
        "fetched car bytes"
    );

    // 3. import repo
    let start = Instant::now();
    let parsed = crate::car::parse_car(car_bytes)?;
    trace!(elapsed = %start.elapsed().as_secs_f32(), "parsed car");

    let start = Instant::now();
    let root_cid = parsed.root;
    if app_state.verify_cids {
        crate::car::validate_block_cids(&parsed.blocks)?;
    }
    let store = Arc::new(MemoryBlockStore::new_from_blocks(parsed.blocks));
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

    // 5. walk mst and fetch every record block under one store lock
    let start = Instant::now();
    let mst: Mst<MemoryBlockStore> = Mst::load(store, root_commit.data, None);
    let leaves = mst.leaves().await.into_diagnostic()?;
    let leaf_cids = leaves.iter().map(|(_, cid)| *cid).collect::<Vec<_>>();
    let leaf_blocks = mst.storage().get_many(&leaf_cids).await.into_diagnostic()?;
    let records = leaves.into_iter().zip(leaf_blocks);
    drop(mst);
    trace!(elapsed = %start.elapsed().as_secs_f32(), "walked mst");

    // 6. insert records into db
    let start = Instant::now();
    let result = {
        let app_state = app_state.clone();
        let did = did.clone();
        let rev = root_commit.rev;

        tokio::task::spawn_blocking(move || {
            let filter = app_state.filter.load();
            let ephemeral = app_state.ephemeral;
            let mut count = 0;
            let mut collection_counts: HashMap<SmolStr, u64> = HashMap::new();
            let mut txn = DbTxn::new(&app_state.db);
            let mut record_txn = txn.backfill_records(&app_state, &rev, &did);
            // the MST and non-record blocks were released before entering this blocking
            // persistence phase; records contains only leaf metadata and payload slices.

            let prefix = keys::record_prefix_did(&did);
            let mut existing_cids: HashMap<(SmolStr, DbRkey), SmolStr> = HashMap::new();

            if !ephemeral {
                for guard in app_state.db.indexer.record_prefix(&prefix) {
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

            for ((key, cid), val_bytes) in records {
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

                    record_txn.put_record(
                        collection,
                        &rkey,
                        cid_obj.to_ipld().expect("valid cid"),
                        &val,
                        action,
                    )?;

                    count += 1;
                }
            }


            // remove any remaining existing records (they weren't in the new MST)
            for ((collection, rkey), cid) in existing_cids {
                trace!(collection = %collection, rkey = %rkey, cid = %cid, "remove existing record");

                // existing_cids is empty in ephemeral mode.
                record_txn.delete_record(&collection, &rkey)?;
                count += 1;
            }

            if !signal_seen {
                trace!(signals = ?filter.signals, "no signal-matching records found, discarding repo");
                return Ok::<_, miette::Report>(None);
            }
            state.root = Some(root_commit);
            state.touch();
            record_txn.update_repo_state(&state)?;
            record_txn.finish()?;

            let metadata_key = keys::repo_metadata_key(&did);
            let metadata_bytes = app_state
                .db
                .repo_metadata
                .get(&metadata_key)
                .into_diagnostic()?
                .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
            let mut metadata = crate::db::deser_repo_meta(&metadata_bytes)?;
            metadata.tracked = true;
            txn.batch.insert(
                &app_state.db.repo_metadata,
                &metadata_key,
                crate::db::ser_repo_meta(&metadata)?,
            );

            // add the counts
            if !ephemeral {
                db::replace_record_counts(
                    &mut txn.batch,
                    &app_state.db,
                    &did,
                    collection_counts.iter().map(|(col, cnt)| (col.as_str(), *cnt)),
                )?;
            }

            txn.commit()?;

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
        app_state
            .db
            .run(move |db| {
                let mut txn = DbTxn::new(db);
                let applied = txn.transition_pending_key(
                    &did,
                    backfill_pending_key.as_ref(),
                    GaugeState::Synced,
                )?;
                txn.batch.remove(&db.indexer.pending, backfill_pending_key);
                if applied {
                    txn.batch.remove(&db.repos, &did_key);
                    txn.batch.remove(&db.repo_metadata, &metadata_key);
                    txn.counts.add_repos(-1);
                }
                txn.commit()
            })
            .await?;
        return Ok(None);
    };

    trace!(ops = count, elapsed = %start.elapsed().as_secs_f32(), "did ops");
    trace!(
        elapsed = %start.elapsed().as_secs_f32(),
        "committed backfill batch"
    );

    #[cfg(feature = "indexer_stream")]
    let _ = db.stream.event_tx.send(BroadcastEvent::Persisted(
        db.stream.next_event_id.load(Ordering::SeqCst) - 1,
    ));

    trace!("complete");
    Ok(Some(previous_state))
}

/// upper bound on a buffered xrpc error body. error responses are tiny json objects; this only
/// guards against a peer streaming an unbounded body on the error path.
const ERROR_BODY_MAX_BYTES: usize = 64 * 1024;

/// outcome of a full-repository `getRepo` fetch that the caller must act on. transport and
/// rate-limit failures are surfaced as [`BackfillError`] instead.
#[derive(Debug)]
enum FullRepoOutcome {
    /// the streamed CAR body, within the configured size ceiling.
    Car(Bytes),
    /// the PDS reported that the repository does not exist (`RepoNotFound`).
    NotFound,
    /// the PDS reported the repository is inactive; carries the status to record.
    Inactive(RepoStatus),
}

/// fetches a full repository CAR by streaming `com.atproto.sync.getRepo` directly through the
/// backfill client, enforcing `max_body_bytes` as chunks arrive so a runaway or malicious
/// response cannot exhaust memory. this bypasses the typed xrpc layer (which materializes the
/// entire decompressed body as one `Vec<u8>` before the caller sees it) while preserving the
/// same handling: `RepoNotFound`/`RepoDeactivated`/`RepoTakendown`/`RepoSuspended` are decoded
/// from the xrpc error body, and 429 responses feed the throttler from the rate-limit headers.
async fn fetch_full_repo_car(
    http: &ThrottledHttpClient,
    pds: &url::Url,
    did: &Did<'_>,
    throttle: &ThrottleHandle,
    tier: &RateTier,
    max_body_bytes: usize,
) -> Result<FullRepoOutcome, BackfillError> {
    let mut url = pds
        .join("xrpc/com.atproto.sync.getRepo")
        .map_err(|e| BackfillError::Generic(miette::miette!("invalid PDS URL: {e}")))?;
    url.query_pairs_mut().append_pair("did", did.as_str());

    // hold the per-pds permit for the entire download, not just until response headers: full
    // repo bodies are large, so releasing early would let unbounded concurrent downloads run
    // against a single pds and defeat per-pds concurrency (the original xrpc getRepo held it
    // for the whole download, since its transport read the full body before returning).
    let _permit = throttle.acquire().await;
    if throttle.is_throttled() {
        return Err(BackfillError::PreemptivelyThrottled);
    }
    throttle.wait_for_allow(1, tier).await;
    if throttle.is_throttled() {
        return Err(BackfillError::PreemptivelyThrottled);
    }
    let resp = match http
        .get(url)
        .header(reqwest::header::ACCEPT, "application/vnd.ipld.car")
        .send()
        .await
    {
        Ok(resp) => {
            if !throttle.is_throttled() {
                throttle.record_success();
            }
            resp
        }
        Err(e) => {
            let reason = e.to_string();
            if let Some(secs) = throttle.record_failure_detail("transport", reason.clone()) {
                warn!(%pds, reason, "PDS offline, blacklisting for {secs}s");
            }
            return Err(BackfillError::Transport(reason.into()));
        }
    };

    let status = resp.status();
    if status.is_success() {
        return match collect_body_bounded(resp, max_body_bytes)
            .await
            .map_err(|e| BackfillError::Transport(e.to_string().into()))?
        {
            Some(body) => Ok(FullRepoOutcome::Car(body)),
            None => Err(BackfillError::Generic(miette::miette!(
                "getRepo response for {did} exceeded max body size of {max_body_bytes} bytes"
            ))),
        };
    }

    // non-success: decode the (tiny) xrpc error body to preserve typed error handling.
    let retry_after = (status == StatusCode::TOO_MANY_REQUESTS)
        .then(|| parse_retry_after(&resp))
        .flatten();
    let body = collect_body_bounded(resp, ERROR_BODY_MAX_BYTES)
        .await
        .map_err(|e| BackfillError::Transport(e.to_string().into()))?
        .unwrap_or_default();

    if let Ok(err) = serde_json::from_slice::<GetRepoError>(&body) {
        match err {
            GetRepoError::RepoNotFound(_) => return Ok(FullRepoOutcome::NotFound),
            GetRepoError::RepoDeactivated(_) => {
                return Ok(FullRepoOutcome::Inactive(RepoStatus::Deactivated));
            }
            GetRepoError::RepoTakendown(_) => {
                return Ok(FullRepoOutcome::Inactive(RepoStatus::Takendown));
            }
            GetRepoError::RepoSuspended(_) => {
                return Ok(FullRepoOutcome::Inactive(RepoStatus::Suspended));
            }
            _ => {}
        }
    }

    if status == StatusCode::TOO_MANY_REQUESTS {
        throttle.record_ratelimit(retry_after);
        return Err(BackfillError::Ratelimited);
    }

    Err(BackfillError::Generic(miette::miette!(
        "getRepo failed with HTTP {status}: {}",
        String::from_utf8_lossy(&body)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::get;

    /// spawns a server answering `com.atproto.sync.getRepo` with a fixed status and body.
    async fn spawn_get_repo(status: StatusCode, body: Vec<u8>) -> url::Url {
        let app = Router::new().route(
            "/xrpc/com.atproto.sync.getRepo",
            get(move || {
                let body = body.clone();
                async move { (status, body) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    async fn fetch(
        pds: &url::Url,
        max_body_bytes: usize,
    ) -> Result<FullRepoOutcome, BackfillError> {
        let throttler = crate::util::throttle::Throttler::new(1, 10);
        let http = ThrottledHttpClient::new(vec![reqwest::Client::new()], throttler.clone());
        let throttle = throttler.get_handle(pds).await;
        let did = Did::new_static("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        fetch_full_repo_car(
            &http,
            pds,
            &did,
            &throttle,
            &RateTier::trusted(),
            max_body_bytes,
        )
        .await
    }

    #[tokio::test]
    async fn full_get_repo_streams_body_within_ceiling() {
        let pds = spawn_get_repo(StatusCode::OK, b"car-bytes".to_vec()).await;
        match fetch(&pds, 1024).await.unwrap() {
            FullRepoOutcome::Car(body) => assert_eq!(body.as_ref(), b"car-bytes"),
            other => panic!("expected Car, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn full_get_repo_rejects_oversized_body() {
        let pds = spawn_get_repo(StatusCode::OK, vec![0u8; 4096]).await;
        let err = fetch(&pds, 64).await.unwrap_err();
        assert!(
            err.to_string().contains("exceeded max body size"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn full_get_repo_maps_repo_not_found() {
        let pds = spawn_get_repo(
            StatusCode::BAD_REQUEST,
            br#"{"error":"RepoNotFound","message":"nope"}"#.to_vec(),
        )
        .await;
        assert!(matches!(
            fetch(&pds, 1024).await.unwrap(),
            FullRepoOutcome::NotFound
        ));
    }

    #[tokio::test]
    async fn full_get_repo_maps_inactive_status() {
        let pds = spawn_get_repo(
            StatusCode::BAD_REQUEST,
            br#"{"error":"RepoDeactivated"}"#.to_vec(),
        )
        .await;
        match fetch(&pds, 1024).await.unwrap() {
            FullRepoOutcome::Inactive(status) => assert_eq!(status, RepoStatus::Deactivated),
            other => panic!("expected Inactive, got {other:?}"),
        }
    }
}
