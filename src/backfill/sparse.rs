use crate::backfill::client::{ThrottledHttpClient, collect_body_bounded};
use crate::backfill::error::BackfillError;
use crate::config::{BackfillStrategy, RateTier};
use crate::db::types::{DbAction, DbRkey};
use crate::db::{self, Txn as DbTxn, keys};
use crate::filter::{FilterConfig, FilterMode};
use crate::ops;
use crate::sparse_mst::{SparseScanner, mst_node_layer, sparse_probe_collection, sparse_ranges};
use crate::state::AppState;
use crate::types::{Commit, RepoState};

use futures::{StreamExt, stream};
use jacquard_api::com_atproto::sync::get_blocks::GetBlocksError;
use jacquard_api::com_atproto::sync::get_record::{GetRecord, GetRecordError};
use jacquard_common::types::cid::{Cid as AtCid, IpldCid};
use jacquard_common::types::did::Did;
use jacquard_common::types::string::{Nsid, RecordKey};
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use miette::IntoDiagnostic;
use reqwest::StatusCode;

use smol_str::{SmolStr, ToSmolStr};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{debug, trace, warn};

use crate::util::throttle::ThrottleHandle;
use crate::util::url_to_fluent_uri;

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
const SPARSE_GET_BLOCKS_PARALLELISM: usize = 4;
const SPARSE_MAX_SCAN_ROUNDS: usize = 256;
const SPARSE_AUTO_FULL_MAX_ROOT_LAYER: usize = 2;
fn sparse_probe(filter: &FilterConfig) -> Option<(SmolStr, &'static str)> {
    filter
        .collections
        .iter()
        .find(|collection| !collection.ends_with(".*") && collection.ends_with(".profile"))
        .or_else(|| {
            filter
                .signals
                .iter()
                .find(|signal| signal.ends_with(".profile") && filter.matches_collection(signal))
        })
        .cloned()
        .map(|collection| (collection, "self"))
        .or_else(|| {
            sparse_probe_collection(&filter.collections).map(|collection| (collection, "-"))
        })
}

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
    let Some((probe_collection, probe_rkey)) = sparse_probe(&filter) else {
        return Ok(SparseBackfillResult::Skipped);
    };
    let ranges = sparse_ranges(&filter.collections);
    if ranges.is_empty() {
        return Ok(SparseBackfillResult::Skipped);
    }

    let probe_collection = Nsid::new_owned(probe_collection.as_str()).into_diagnostic()?;
    let probe_rkey = RecordKey::any_static(probe_rkey).into_diagnostic()?;
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
    if app_state.verify_cids {
        crate::car::validate_block_cids(&parsed.blocks)?;
    }
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
    let auto_full_layer = if strategy == BackfillStrategy::Auto {
        parsed
            .blocks
            .get(&root_cid)
            .map(|root_bytes| mst_node_layer(root_bytes))
            .transpose()?
            .flatten()
            .filter(|layer| *layer <= SPARSE_AUTO_FULL_MAX_ROOT_LAYER)
    } else {
        None
    };

    let root_commit = Commit::from(root_commit);
    let mut scanner = SparseScanner::new(ranges, parsed.blocks);
    let mut scan_rounds = 0;
    let scan = loop {
        match scanner.scan(root_cid)? {
            Ok(scan) => break scan,
            Err(missing) => {
                if let Some(root_layer) = auto_full_layer {
                    debug!(
                        root_layer,
                        max_sparse_layer = SPARSE_AUTO_FULL_MAX_ROOT_LAYER,
                        "sparse auto selected full getRepo because the probe omitted MST nodes"
                    );
                    return Ok(SparseBackfillResult::Skipped);
                }
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
                let blocks = fetch_blocks(
                    http,
                    pds,
                    did,
                    &missing,
                    &throttle,
                    &tier,
                    app_state.verify_cids,
                    app_state.max_car_body_bytes,
                )
                .await?;
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
    if !missing_records.is_empty()
        && let Some(root_layer) = auto_full_layer
    {
        debug!(
            root_layer,
            max_sparse_layer = SPARSE_AUTO_FULL_MAX_ROOT_LAYER,
            "sparse auto selected full getRepo because the probe omitted record blocks"
        );
        return Ok(SparseBackfillResult::Skipped);
    }
    let mut record_blocks = record_cids
        .iter()
        .filter_map(|cid| scanned_blocks.remove(cid).map(|bytes| (*cid, bytes)))
        .collect::<BTreeMap<_, _>>();
    drop(scanned_blocks);
    record_blocks.extend(
        fetch_blocks(
            http,
            pds,
            did,
            &missing_records,
            &throttle,
            &tier,
            app_state.verify_cids,
            app_state.max_car_body_bytes,
        )
        .await?,
    );

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
    verify_cids: bool,
    max_body_bytes: usize,
) -> Result<BTreeMap<IpldCid, bytes::Bytes>, BackfillError> {
    let mut out = BTreeMap::new();

    let fetches = stream::iter((0..cids.len()).step_by(SPARSE_GET_BLOCKS_CHUNK))
        .map(|start| {
            let end = start
                .saturating_add(SPARSE_GET_BLOCKS_CHUNK)
                .min(cids.len());
            fetch_block_chunk(
                http,
                pds,
                did,
                &cids[start..end],
                throttle,
                tier,
                verify_cids,
                max_body_bytes,
            )
        })
        .buffer_unordered(SPARSE_GET_BLOCKS_PARALLELISM);
    futures::pin_mut!(fetches);

    while let Some(blocks) = fetches.next().await {
        out.extend(blocks?);
    }

    Ok(out)
}

async fn fetch_block_chunk(
    http: &ThrottledHttpClient,
    pds: &url::Url,
    did: &Did<'_>,
    cids: &[IpldCid],
    throttle: &ThrottleHandle,
    tier: &RateTier,
    verify_cids: bool,
    max_body_bytes: usize,
) -> Result<BTreeMap<IpldCid, bytes::Bytes>, BackfillError> {
    let mut url = pds
        .join("xrpc/com.atproto.sync.getBlocks")
        .map_err(|e| BackfillError::Generic(miette::miette!("Invalid URL: {e}")))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("did", did.as_str());
        for cid in cids {
            query.append_pair("cids", &cid.to_string());
        }
    }

    let resp = {
        let _permit = throttle.acquire().await;
        throttle.wait_for_allow(1, tier).await;
        match http
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
        let Some(body) = collect_body_bounded(resp, max_body_bytes)
            .await
            .map_err(|e| BackfillError::Transport(e.to_string().into()))?
        else {
            return Err(BackfillError::Generic(miette::miette!(
                "getBlocks response for {did} exceeded max body size of {max_body_bytes} bytes"
            )));
        };
        let blocks = crate::car::parse_car_blocks(body).map_err(BackfillError::from)?;
        if verify_cids {
            crate::car::validate_block_cids(&blocks)?;
        }
        Ok(blocks)
    } else {
        let retry_after = (status == StatusCode::TOO_MANY_REQUESTS)
            .then_some(())
            .and_then(|()| crate::util::parse_retry_after(&resp));

        let body = resp
            .bytes()
            .await
            .map_err(|e| BackfillError::Transport(e.to_string().into()))?;
        if serde_json::from_slice::<GetBlocksError>(&body)
            .ok()
            .is_some_and(|err| {
                matches!(
                    err,
                    GetBlocksError::BlockNotFound(_)
                        | GetBlocksError::RepoNotFound(_)
                        | GetBlocksError::RepoTakendown(_)
                        | GetBlocksError::RepoSuspended(_)
                        | GetBlocksError::RepoDeactivated(_)
                )
            })
        {
            return Ok(BTreeMap::new());
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
    let db = app_state.db.clone();
    db.run(move |db| {
        let filter = app_state.filter.load();
        let ephemeral = app_state.ephemeral;
        let mut count = 0;
        let mut collection_counts: HashMap<SmolStr, u64> = HashMap::new();

        let prefix = keys::record_prefix_did(&did);
        let mut existing_cids: HashMap<(SmolStr, DbRkey), SmolStr> = HashMap::new();

        if !ephemeral {
            for guard in db.indexer.record_prefix(&prefix) {
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
        let mut txn = DbTxn::new(db);
        let mut record_txn = txn.backfill_records(&app_state, &root_commit.rev, &did);

        for (key, cid) in leaves {
            let (collection, rkey) = ops::parse_path(&key)?;

            if !filter.matches_collection(collection) {
                continue;
            }

            let Some(val) = blocks.get(&cid) else {
                return Err(miette::miette!("missing sparse record block {cid}"));
            };

            if !signal_seen && filter.matches_signal(collection) {
                debug!(collection = %collection, "signal matched");
                signal_seen = true;
            }

            let rkey = DbRkey::new(rkey);
            let path = (collection.to_smolstr(), rkey.clone());
            let cid_obj = AtCid::ipld(cid);

            *collection_counts.entry(path.0.clone()).or_default() += 1;

            let action = existing_cids.remove(&path).map_or(
                Some(DbAction::Create),
                |existing_cid| {
                    (existing_cid != cid_obj.as_str()).then_some(DbAction::Update)
                },
            );
            let Some(action) = action else {
                trace!(collection = %collection, rkey = %rkey, cid = %cid, "skip unchanged sparse record");
                continue;
            };
            trace!(collection = %collection, rkey = %rkey, cid = %cid, ?action, "action sparse record");

            record_txn.put_record(collection, &rkey, cid, val, action)?;
            count += 1;
        }

        for ((collection, rkey), cid) in existing_cids {
            trace!(collection = %collection, rkey = %rkey, cid = %cid, "remove sparse-stale record");
            record_txn.delete_record(&collection, &rkey)?;
            count += 1;
        }

        if !signal_seen {
            trace!(signals = ?filter.signals, "no signal-matching sparse records found, discarding repo");
            return Ok::<_, miette::Report>(None);
        }

        state.root = Some(root_commit);
        state.touch();
        record_txn.update_repo_state(&state)?;
        let _events = record_txn.finish()?;

        let metadata_key = keys::repo_metadata_key(&did);
        let metadata_bytes = db
            .repo_metadata
            .get(&metadata_key)
            .into_diagnostic()?
            .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
        let mut metadata = crate::db::deser_repo_meta(&metadata_bytes)?;
        metadata.tracked = true;
        txn.batch.insert(
            &db.repo_metadata,
            &metadata_key,
            crate::db::ser_repo_meta(&metadata)?,
        );

        if !ephemeral {
            db::replace_record_counts_matching(
                &mut txn.batch,
                db,
                &did,
                |collection| filter.matches_collection(collection),
                collection_counts.iter().map(|(col, cnt)| (col.as_str(), *cnt)),
            )?;
        }

        txn.commit()?;

        Ok::<_, miette::Report>(Some((count, state)))
    })
    .await
    .map_err(BackfillError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterConfig;
    use axum::{Router, extract::State, response::IntoResponse, routing::get};
    use cid::Cid;
    use cid::multihash::Multihash;
    use jacquard_common::types::crypto::{DAG_CBOR, SHA2_256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Barrier;

    #[derive(Clone)]
    struct FetchState {
        started: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
        car: bytes::Bytes,
    }

    async fn delayed_car(State(state): State<FetchState>) -> impl IntoResponse {
        let ordinal = state.started.fetch_add(1, Ordering::SeqCst) + 1;
        let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
        state.max_active.fetch_max(active, Ordering::SeqCst);
        if ordinal <= 2 {
            state.barrier.wait().await;
        }
        state.active.fetch_sub(1, Ordering::SeqCst);
        (
            [(
                reqwest::header::CONTENT_TYPE.as_str(),
                "application/vnd.ipld.car",
            )],
            state.car,
        )
    }

    fn cid(byte: u8) -> IpldCid {
        let hash = [byte; 32];
        let multihash = Multihash::<64>::wrap(SHA2_256, &hash).unwrap();
        Cid::new_v1(DAG_CBOR, multihash)
    }

    fn filter(collections: &[&str], signals: &[&str]) -> FilterConfig {
        let mut filter = FilterConfig::new(FilterMode::Filter);
        filter.collections = collections.iter().map(SmolStr::new).collect();
        filter.signals = signals.iter().map(SmolStr::new).collect();
        filter
    }

    #[test]
    fn sparse_probe_prefers_matching_profile_signal() {
        let filter = filter(
            &["sh.tangled.*"],
            &["sh.tangled.actor.profile", "app.bsky.actor.profile"],
        );

        assert_eq!(
            sparse_probe(&filter),
            Some((SmolStr::new("sh.tangled.actor.profile"), "self"))
        );
    }

    #[test]
    fn sparse_probe_ignores_profile_signal_outside_filter() {
        let filter = filter(&["sh.tangled.*"], &["app.bsky.actor.profile"]);

        assert_eq!(
            sparse_probe(&filter),
            Some((SmolStr::new("sh.tangled.probe"), "-"))
        );
    }

    #[test]
    fn sparse_probe_uses_self_for_exact_profile_collection() {
        let filter = filter(&["app.bsky.actor.profile"], &[]);

        assert_eq!(
            sparse_probe(&filter),
            Some((SmolStr::new("app.bsky.actor.profile"), "self"))
        );
    }

    #[tokio::test]
    async fn fetches_independent_get_blocks_chunks_concurrently() {
        let mut car = Vec::new();
        let mut writer =
            iroh_car::CarWriter::new(iroh_car::CarHeader::new_v1(Vec::new()), &mut car);
        let response_cid = cid(u8::MAX);
        writer.write(response_cid, b"block".to_vec()).await.unwrap();
        writer.finish().await.unwrap();
        let state = FetchState {
            started: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(Barrier::new(2)),
            car: car.into(),
        };
        let app = Router::new()
            .route("/xrpc/com.atproto.sync.getBlocks", get(delayed_car))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pds = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let throttler = crate::util::throttle::Throttler::new(1, 10);
        let http = ThrottledHttpClient::new(vec![reqwest::Client::new()], throttler.clone());
        let throttle = throttler.get_handle(&pds).await;
        let did = Did::new_static("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let cids = (0..=200).map(|byte| cid(byte as u8)).collect::<Vec<_>>();

        let blocks = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_blocks(
                &http,
                &pds,
                &did,
                &cids,
                &throttle,
                &RateTier::trusted(),
                false,
                64 * 1024 * 1024,
            ),
        )
        .await
        .expect("getBlocks chunks were fetched serially")
        .unwrap();

        assert_eq!(blocks.get(&response_cid).unwrap().as_ref(), b"block");
        assert!(
            state.max_active.load(Ordering::SeqCst) > 1,
            "expected more than one getBlocks chunk in flight"
        );
    }

    async fn spawn_car_server(car: bytes::Bytes) -> url::Url {
        let app = Router::new().route(
            "/xrpc/com.atproto.sync.getBlocks",
            get(move || {
                let car = car.clone();
                async move {
                    (
                        [(
                            reqwest::header::CONTENT_TYPE.as_str(),
                            "application/vnd.ipld.car",
                        )],
                        car,
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    async fn car_with_block(cid: IpldCid, payload: &[u8]) -> bytes::Bytes {
        let mut car = Vec::new();
        let mut writer =
            iroh_car::CarWriter::new(iroh_car::CarHeader::new_v1(Vec::new()), &mut car);
        writer.write(cid, payload.to_vec()).await.unwrap();
        writer.finish().await.unwrap();
        car.into()
    }

    async fn fetch_one(
        pds: &url::Url,
        wanted: IpldCid,
        verify_cids: bool,
        max_body_bytes: usize,
    ) -> Result<BTreeMap<IpldCid, bytes::Bytes>, BackfillError> {
        let throttler = crate::util::throttle::Throttler::new(1, 10);
        let http = ThrottledHttpClient::new(vec![reqwest::Client::new()], throttler.clone());
        let throttle = throttler.get_handle(pds).await;
        let did = Did::new_static("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        fetch_blocks(
            &http,
            pds,
            &did,
            &[wanted],
            &throttle,
            &RateTier::trusted(),
            verify_cids,
            max_body_bytes,
        )
        .await
    }

    #[tokio::test]
    async fn verify_cids_rejects_mismatched_get_blocks_car() {
        let pds = spawn_car_server(car_with_block(cid(1), b"forged").await).await;

        let err = fetch_one(&pds, cid(1), true, 64 * 1024 * 1024)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("CAR block CID mismatch"));
    }

    #[tokio::test]
    async fn verify_cids_accepts_matching_get_blocks_car() {
        let real_cid = jacquard_repo::mst::util::compute_cid(b"trusted").unwrap();
        let pds = spawn_car_server(car_with_block(real_cid, b"trusted").await).await;

        let blocks = fetch_one(&pds, real_cid, true, 64 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(blocks.get(&real_cid).unwrap().as_ref(), b"trusted");
    }

    #[tokio::test]
    async fn verify_cids_disabled_accepts_mismatched_get_blocks_car() {
        let pds = spawn_car_server(car_with_block(cid(1), b"forged").await).await;

        let blocks = fetch_one(&pds, cid(1), false, 64 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(blocks.get(&cid(1)).unwrap().as_ref(), b"forged");
    }

    #[tokio::test]
    async fn get_blocks_rejects_oversized_body() {
        let pds = spawn_car_server(car_with_block(cid(1), &[0u8; 4096]).await).await;

        let err = fetch_one(&pds, cid(1), false, 64).await.unwrap_err();
        assert!(err.to_string().contains("exceeded max body size"));
    }
}
