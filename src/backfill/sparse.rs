use crate::backfill::client::ThrottledHttpClient;
use crate::backfill::error::BackfillError;
use crate::config::{BackfillStrategy, RateTier};
use crate::db::types::{DbAction, DbRkey, TrimmedDid};
use crate::db::{self, CountDeltas, keys, ser_repo_state};
use crate::filter::FilterMode;
use crate::ops;
use crate::sparse_mst::{SparseScanner, mst_node_layer, sparse_probe_collection, sparse_ranges};
use crate::state::AppState;
use crate::types::{Commit, RepoState};

use fjall::Slice;
use futures::{StreamExt, stream};
use jacquard_api::com_atproto::sync::get_blocks::GetBlocksError;
use jacquard_api::com_atproto::sync::get_record::{GetRecord, GetRecordError};
#[cfg(feature = "jetstream")]
use jacquard_common::IntoStatic;
use jacquard_common::types::cid::{Cid as AtCid, IpldCid};
use jacquard_common::types::did::Did;
use jacquard_common::types::string::{Nsid, RecordKey};
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use miette::IntoDiagnostic;
use reqwest::StatusCode;

use smol_str::{SmolStr, ToSmolStr};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, trace, warn};

#[cfg(feature = "indexer_stream")]
use crate::types::{StoredData, StoredEvent};
use crate::util::throttle::ThrottleHandle;
use crate::util::url_to_fluent_uri;
#[cfg(feature = "indexer_stream")]
use jacquard_common::CowStr;

#[derive(Debug)]
pub(crate) struct SparseBackfillSuccess {
    pub(crate) state: RepoState<'static>,
    pub(crate) records: usize,
    pub(crate) node_blocks: usize,
    pub(crate) node_bytes: usize,
}

#[derive(Debug)]
pub(crate) enum SparseBackfillResult {
    Imported(SparseBackfillSuccess),
    Discarded,
    Skipped,
}

const SPARSE_GET_BLOCKS_CHUNK: usize = 100;
const SPARSE_GET_BLOCKS_PARALLELISM: usize = 1;
const SPARSE_MAX_SCAN_ROUNDS: usize = 256;
const SPARSE_AUTO_FULL_MAX_ROOT_LAYER: usize = 2;

pub(crate) async fn process_did_sparse(
    app_state: &Arc<AppState>,
    http: &ThrottledHttpClient,
    did: &Did<'static>,
    pds: &url::Url,
    state: RepoState<'static>,
    verify_signatures: bool,
    strategy: BackfillStrategy,
) -> Result<SparseBackfillResult, BackfillError> {
    let filter = app_state.filter.load();
    let Some(probe_collection) = sparse_probe_collection(&filter.collections) else {
        return Ok(SparseBackfillResult::Skipped);
    };
    let ranges = sparse_ranges(&filter.collections);
    if ranges.is_empty() {
        return Ok(SparseBackfillResult::Skipped);
    }

    let probe_collection = Nsid::new_owned(probe_collection.as_str()).into_diagnostic()?;
    let probe_rkey = RecordKey::any_static("-").into_diagnostic()?;
    let req = GetRecord::new()
        .did(did.clone())
        .collection(probe_collection)
        .rkey(probe_rkey)
        .build();

    let throttle = app_state.throttler.get_handle(pds).await;
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
        match http.xrpc(url_to_fluent_uri(pds)).send(&req).await {
            Ok(resp) => {
                if !throttle.is_throttled() {
                    throttle.record_success();
                }
                resp
            }
            Err(e) => return Err(BackfillError::from_sparse_client(e, &throttle)),
        }
    };
    let proof = match resp.into_output() {
        Ok(o) => o,
        Err(XrpcError::Xrpc(GetRecordError::RecordNotFound(_))) => {
            return Ok(SparseBackfillResult::Skipped);
        }
        Err(XrpcError::Xrpc(
            GetRecordError::RepoNotFound(_)
            | GetRecordError::RepoTakendown(_)
            | GetRecordError::RepoSuspended(_)
            | GetRecordError::RepoDeactivated(_),
        )) => return Ok(SparseBackfillResult::Skipped),
        Err(e) => Err(e).into_diagnostic()?,
    };

    let parsed = jacquard_repo::car::reader::parse_car_bytes(&proof.body)
        .await
        .into_diagnostic()?;
    let root_bytes = parsed
        .blocks
        .get(&parsed.root)
        .ok_or_else(|| miette::miette!("root block missing from sparse proof CAR"))?;
    let root_commit = jacquard_repo::commit::Commit::from_cbor(root_bytes).into_diagnostic()?;

    if verify_signatures {
        let pubkey = app_state.resolver.resolve_signing_key(did).await?;
        root_commit
            .verify(&pubkey)
            .map_err(|e| miette::miette!("signature verification failed for {did}: {e}"))?;
    }

    let root_cid = root_commit.data;
    if strategy == BackfillStrategy::Auto {
        if let Some(root_bytes) = parsed.blocks.get(&root_cid) {
            if let Some(root_layer) = mst_node_layer(root_bytes)? {
                if root_layer <= SPARSE_AUTO_FULL_MAX_ROOT_LAYER {
                    debug!(
                        root_layer,
                        max_sparse_layer = SPARSE_AUTO_FULL_MAX_ROOT_LAYER,
                        "sparse auto selected full getRepo for small repo"
                    );
                    return Ok(SparseBackfillResult::Skipped);
                }
            }
        }
    }

    let root_commit = Commit::from(root_commit);
    let mut scanner = SparseScanner::new(ranges, parsed.blocks);
    let mut scan_rounds = 0;
    let scan = loop {
        match scanner.scan(root_cid)? {
            Ok(scan) => break scan,
            Err(missing) => {
                scan_rounds += 1;
                if scan_rounds > SPARSE_MAX_SCAN_ROUNDS {
                    return Err(
                        miette::miette!("sparse mst scan exceeded fetch round limit").into(),
                    );
                }
                if missing.is_empty() {
                    return Ok(SparseBackfillResult::Skipped);
                }
                if missing.len() > SPARSE_MAX_SCAN_ROUNDS * SPARSE_GET_BLOCKS_CHUNK {
                    return Err(
                        miette::miette!("sparse mst scan exceeded missing block limit").into(),
                    );
                }
                let blocks = fetch_blocks(http, pds, did, &missing, &throttle, &tier).await?;
                if missing.iter().all(|cid| !blocks.contains_key(cid)) {
                    return Ok(SparseBackfillResult::Skipped);
                }
                scanner.insert_blocks(blocks);
            }
        }
    };

    let record_cids = scan
        .leaves
        .iter()
        .map(|(_, cid)| *cid)
        .collect::<std::collections::BTreeSet<_>>();
    let mut scanned_blocks = scanner.take_blocks();
    let missing_records = record_cids
        .iter()
        .filter_map(|cid| (!scanned_blocks.contains_key(cid)).then_some(*cid))
        .collect::<Vec<_>>();
    let mut record_blocks = record_cids
        .iter()
        .filter_map(|cid| scanned_blocks.remove(cid).map(|bytes| (*cid, bytes)))
        .collect::<BTreeMap<_, _>>();
    drop(scanned_blocks);
    record_blocks.extend(fetch_blocks(http, pds, did, &missing_records, &throttle, &tier).await?);

    if let Some((_, missing)) = scan
        .leaves
        .iter()
        .find(|(_, cid)| !record_blocks.contains_key(cid))
    {
        return Err(
            miette::miette!("sparse record block missing after getBlocks: {missing}").into(),
        );
    }

    let result = persist_sparse_backfill(
        app_state,
        did,
        state,
        root_commit,
        scan.leaves,
        record_blocks,
    )
    .await?;

    let Some((records, state)) = result else {
        return Ok(SparseBackfillResult::Discarded);
    };

    Ok(SparseBackfillResult::Imported(SparseBackfillSuccess {
        state,
        records,
        node_blocks: scan.node_blocks_seen,
        node_bytes: scan.node_bytes_seen,
    }))
}

async fn fetch_blocks(
    http: &ThrottledHttpClient,
    pds: &url::Url,
    did: &Did<'static>,
    cids: &[IpldCid],
    throttle: &ThrottleHandle,
    tier: &RateTier,
) -> Result<BTreeMap<IpldCid, bytes::Bytes>, BackfillError> {
    let mut out = BTreeMap::new();

    let fetches = cids
        .chunks(SPARSE_GET_BLOCKS_CHUNK)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| {
            fetch_block_chunk(
                http.clone(),
                pds.clone(),
                did.clone(),
                chunk.to_vec(),
                throttle,
                tier,
            )
        })
        .collect::<Vec<_>>();
    let fetches = stream::iter(fetches).buffer_unordered(SPARSE_GET_BLOCKS_PARALLELISM);
    futures::pin_mut!(fetches);

    while let Some(blocks) = fetches.next().await {
        out.extend(blocks?);
    }

    Ok(out)
}

async fn fetch_block_chunk(
    http: ThrottledHttpClient,
    pds: url::Url,
    did: Did<'static>,
    cids: Vec<IpldCid>,
    throttle: &ThrottleHandle,
    tier: &RateTier,
) -> Result<BTreeMap<IpldCid, bytes::Bytes>, BackfillError> {
    let mut url = pds
        .join("xrpc/com.atproto.sync.getBlocks")
        .map_err(|e| BackfillError::Generic(miette::miette!("Invalid URL: {e}")))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("did", did.as_str());
        for cid in &cids {
            query.append_pair("cids", &cid.to_string());
        }
    }

    let resp = {
        let _permit = throttle.acquire().await;
        throttle.wait_for_allow(1, tier).await;
        match http
            .client
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
                    warn!(
                        %pds,
                        reason,
                        "PDS offline, blacklisting for {secs}s"
                    );
                }
                return Err(BackfillError::Transport(reason.into()));
            }
        }
    };

    let status = resp.status();
    if status.is_success() {
        let body = resp
            .bytes()
            .await
            .map_err(|e| BackfillError::Transport(e.to_string().into()))?;
        crate::car::parse_car_blocks(&body)
            .await
            .map_err(BackfillError::from)
    } else {
        let retry_after = if status == StatusCode::TOO_MANY_REQUESTS {
            crate::util::parse_retry_after(&resp)
        } else {
            None
        };

        let body = resp
            .bytes()
            .await
            .map_err(|e| BackfillError::Transport(e.to_string().into()))?;
        if let Ok(err) = serde_json::from_slice::<GetBlocksError>(&body) {
            match err {
                GetBlocksError::BlockNotFound(_)
                | GetBlocksError::RepoNotFound(_)
                | GetBlocksError::RepoTakendown(_)
                | GetBlocksError::RepoSuspended(_)
                | GetBlocksError::RepoDeactivated(_) => {
                    return Ok(BTreeMap::new());
                }
                _ => {}
            }
        }

        if status == StatusCode::TOO_MANY_REQUESTS {
            throttle.record_ratelimit(retry_after);
            return Err(BackfillError::Ratelimited);
        }

        Err(miette::miette!(
            "getBlocks failed with HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        )
        .into())
    }
}

async fn persist_sparse_backfill(
    app_state: &Arc<AppState>,
    did: &Did<'static>,
    mut state: RepoState<'static>,
    root_commit: Commit,
    leaves: Vec<(SmolStr, IpldCid)>,
    blocks: BTreeMap<IpldCid, bytes::Bytes>,
) -> Result<Option<(usize, RepoState<'static>)>, BackfillError> {
    let app_state = app_state.clone();
    let did = did.clone();
    tokio::task::spawn_blocking(move || {
        let filter = app_state.filter.load();
        let ephemeral = app_state.ephemeral;
        let only_index_links = app_state.only_index_links;
        let mut count = 0;
        let mut delta = 0;
        let mut added_blocks = 0;
        let mut collection_counts: HashMap<SmolStr, u64> = HashMap::new();
        let mut batch = app_state.db.inner.batch();

        let prefix = keys::record_prefix_did(&did);
        let mut existing_cids: HashMap<(SmolStr, DbRkey), SmolStr> = HashMap::new();

        if !ephemeral {
            for guard in app_state.db.records.prefix(&prefix) {
                let (key, cid_bytes) = guard.into_inner().into_diagnostic()?;
                let mut remaining = key[prefix.len()..].splitn(2, |b| keys::SEP.eq(b));
                let collection_raw = remaining
                    .next()
                    .ok_or_else(|| miette::miette!("invalid record key format: {key:?}"))?;
                let rkey_raw = remaining
                    .next()
                    .ok_or_else(|| miette::miette!("invalid record key format: {key:?}"))?;

                let collection = std::str::from_utf8(collection_raw)
                    .map_err(|e| miette::miette!("invalid collection utf8: {e}"))?;
                if !filter.matches_collection(collection) {
                    continue;
                }

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
            let (collection, rkey) = ops::parse_path(&key)?;

            if !filter.matches_collection(collection) {
                continue;
            }

            let Some(val) = blocks.get(&cid).cloned() else {
                return Err(miette::miette!("missing sparse record block {cid}").into());
            };

            if !signal_seen && filter.matches_signal(collection) {
                debug!(collection = %collection, "signal matched");
                signal_seen = true;
            }

            let rkey = DbRkey::new(rkey);
            let path = (collection.to_smolstr(), rkey.clone());
            let cid_obj = AtCid::ipld(cid);

            *collection_counts.entry(path.0.clone()).or_default() += 1;

            let existing_cid = existing_cids.remove(&path);
            let action = if let Some(existing_cid) = &existing_cid {
                if existing_cid == cid_obj.as_str() {
                    trace!(collection = %collection, rkey = %rkey, cid = %cid, "skip unchanged sparse record");
                    continue;
                }
                DbAction::Update
            } else {
                DbAction::Create
            };
            trace!(collection = %collection, rkey = %rkey, cid = %cid, ?action, "action sparse record");

            let db_key = keys::record_key(&did, collection, &rkey);
            let cid_raw = cid.to_bytes();
            let block_key = Slice::from(keys::block_key(collection, &cid_raw));
            if !ephemeral {
                if !only_index_links {
                    batch.insert(&app_state.db.blocks, block_key.clone(), val.as_ref());
                }
                batch.insert(&app_state.db.records, db_key, cid_raw);
                #[cfg(feature = "backlinks")]
                if let Ok(value) =
                    serde_ipld_dagcbor::from_slice::<jacquard_common::Data>(val.as_ref())
                {
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
                    rev: root_commit.rev,
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

        for ((collection, rkey), cid) in existing_cids {
            trace!(collection = %collection, rkey = %rkey, cid = %cid, "remove sparse-stale record");

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
                    rev: root_commit.rev,
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
            trace!(signals = ?filter.signals, "no signal-matching sparse records found, discarding repo");
            return Ok::<_, miette::Report>(None);
        }

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

        if !ephemeral {
            db::replace_record_counts_matching(
                &mut batch,
                &app_state.db,
                &did,
                |collection| filter.matches_collection(collection),
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

        Ok::<_, miette::Report>(Some((count, state)))
    })
    .await
    .into_diagnostic()?
    .map_err(BackfillError::from)
}
