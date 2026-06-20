use crate::config::BackfillStrategy;
use crate::db::types::{DbAction, DbRkey, TrimmedDid};
use crate::db::{self, CountDeltas, Db, keys, ser_repo_state};
use crate::filter::FilterMode;
use crate::ops;
use crate::resolver::ResolverError;
use crate::sparse_mst::{SparseScanner, mst_node_layer, sparse_probe_collection, sparse_ranges};
use crate::state::AppState;
use crate::types::{Commit, GaugeState, RepoState, RepoStatus, ResyncErrorKind, ResyncState};

use fjall::Slice;
use futures::{StreamExt, stream};
use jacquard_api::com_atproto::sync::get_blocks::{GetBlocks, GetBlocksError};
use jacquard_api::com_atproto::sync::get_record::{GetRecord, GetRecordError};
use jacquard_api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard_common::IntoStatic;
use jacquard_common::error::{ClientError, ClientErrorKind};
use jacquard_common::types::cid::{Cid as AtCid, IpldCid};
use jacquard_common::types::did::Did;
use jacquard_common::types::string::{Nsid, RecordKey};
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use jacquard_repo::mst::Mst;
use jacquard_repo::{BlockStore, MemoryBlockStore};
use miette::{Diagnostic, IntoDiagnostic, Result};
use reqwest::StatusCode;
use smol_str::{SmolStr, ToSmolStr};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{Instrument, debug, error, info, trace, warn};
#[cfg(feature = "indexer_stream")]
use {
    crate::types::{AccountEvt, BroadcastEvent, StoredData, StoredEvent},
    jacquard_common::CowStr,
    std::sync::atomic::Ordering,
};

pub mod manager;

use crate::ingest::indexer::{IndexerMessage, IndexerTx};
use crate::util::throttle::ThrottleHandle;
use crate::util::{WatchEnabledExt, url_to_fluent_uri};

pub struct BackfillWorker {
    state: Arc<AppState>,
    buffer_tx: IndexerTx,
    http: reqwest::Client,
    semaphore: Arc<Semaphore>,
    verify_signatures: bool,
    strategy: BackfillStrategy,
    in_flight: Arc<scc::HashSet<Did<'static>>>,
    enabled: tokio::sync::watch::Receiver<bool>,
}

impl BackfillWorker {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: IndexerTx,
        timeout: Duration,
        concurrency_limit: usize,
        verify_signatures: bool,
        strategy: BackfillStrategy,
        enabled: tokio::sync::watch::Receiver<bool>,
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
            strategy,
            in_flight: Arc::new(scc::HashSet::new()),
            enabled,
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
    pub async fn run(mut self) {
        info!("backfill worker started");

        loop {
            self.enabled.wait_enabled("backfill").await;
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

                // check before trying to acquire a permit so we dont acquire a permit
                // for no reason, the read will be cheap anyhow
                if self.in_flight.contains_sync(&did) {
                    continue;
                }

                let permit = match self.semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // only mark as in flight if we can acquire a permit
                if self
                    .in_flight
                    .insert_sync(did.clone().into_static())
                    .is_err()
                {
                    // a task is already running, weh
                    // so we don't need this one anymore...
                    break;
                }

                let guard = InFlightGuard {
                    did: did.clone().into_static(),
                    set: self.in_flight.clone(),
                };

                let state = self.state.clone();
                let http = self.http.clone();
                let did = did.clone();
                let buffer_tx = self.buffer_tx.clone();
                let verify = self.verify_signatures;
                let strategy = self.strategy;

                let span = tracing::info_span!("backfill", did = %did);
                tokio::spawn(
                    async move {
                        let _guard = guard;
                        let res =
                            did_task(&state, http, buffer_tx, &did, key, permit, verify, strategy)
                                .await;

                        if let Err(e) = res {
                            match &e {
                                BackfillError::Ratelimited => {
                                    debug!(err = %e, "process failed");
                                }
                                _ => {
                                    error!(err = %e, "process failed");
                                }
                            }
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
            let applied = tokio::task::spawn_blocking({
                let state = state.clone();
                let did = did.clone();
                let pending_key = pending_key.clone();
                move || {
                    let db = &state.db;
                    let did_key = keys::repo_key(&did);
                    let mut batch = db.inner.batch();
                    let mut lifecycle_counts = db.lifecycle_counts();
                    let applied = lifecycle_counts.transition_pending_key(
                        &mut batch,
                        &did,
                        pending_key.as_ref(),
                        GaugeState::Synced,
                    )?;
                    batch.remove(&db.pending, pending_key.clone());
                    if applied {
                        batch.remove(&db.resync, &did_key);
                    }
                    let lifecycle_reservation = lifecycle_counts.stage(&mut batch);
                    batch.commit().into_diagnostic()?;
                    db.apply_lifecycle_counts(lifecycle_reservation);
                    Ok::<_, miette::Report>(applied)
                }
            })
            .await
            .into_diagnostic()??;

            if !applied {
                return Ok(());
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
            tokio::task::spawn_blocking({
                let state = state.clone();
                let did = did.clone();
                let pending_key = pending_key.clone();
                move || {
                    let db = &state.db;
                    let mut batch = db.inner.batch();
                    let mut lifecycle_counts = db.lifecycle_counts();
                    lifecycle_counts.transition_pending_key(
                        &mut batch,
                        &did,
                        pending_key.as_ref(),
                        GaugeState::Synced,
                    )?;
                    batch.remove(&db.pending, pending_key);
                    let lifecycle_reservation = lifecycle_counts.stage(&mut batch);
                    batch.commit().into_diagnostic()?;
                    db.apply_lifecycle_counts(lifecycle_reservation);
                    Ok::<_, miette::Report>(())
                }
            })
            .await
            .into_diagnostic()??;
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

            let did_key = keys::repo_key(did);

            // 1. get current retry count
            let existing_state = Db::get(db.resync.clone(), &did_key).await.and_then(|b| {
                b.map(|b| rmp_serde::from_slice::<ResyncState>(&b).into_diagnostic())
                    .transpose()
            })?;

            let mut retry_count = match existing_state {
                Some(ResyncState::Error { retry_count, .. }) => retry_count,
                Some(ResyncState::Gone { .. }) => return Ok(()), // should handle gone? original code didn't really?
                None => 0,
            };

            // Calculate new stats
            retry_count += 1;
            let next_retry = ResyncState::next_backoff(retry_count);

            let resync_state = ResyncState::Error {
                kind: error_kind,
                retry_count,
                next_retry,
            };
            let error_string = e.to_string();

            tokio::task::spawn_blocking({
                let state = state.clone();
                let did_key = did_key.into_static();
                let did = did.clone();
                let pending_key = pending_key.clone();
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
                        state.active = true;
                        state.status = RepoStatus::Error(error_string.into());
                        Some(rmp_serde::to_vec(&state).into_diagnostic()?)
                    } else {
                        None
                    };

                    let mut batch = state.db.inner.batch();
                    let mut lifecycle_counts = state.db.lifecycle_counts();
                    let applied = lifecycle_counts.transition_pending_key(
                        &mut batch,
                        &did,
                        pending_key.as_ref(),
                        GaugeState::Resync(Some(error_kind)),
                    )?;
                    batch.remove(&state.db.pending, pending_key.clone());
                    if applied {
                        batch.insert(&state.db.resync, &did_key, serialized_resync_state);
                        if let Some(state_bytes) = serialized_repo_state {
                            batch.insert(&state.db.repos, &did_key, state_bytes);
                        }
                    }
                    let lifecycle_reservation = lifecycle_counts.stage(&mut batch);
                    batch.commit().into_diagnostic()?;
                    state.db.apply_lifecycle_counts(lifecycle_reservation);
                    Ok::<_, miette::Report>(())
                }
            })
            .await
            .into_diagnostic()??;

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

impl BackfillError {
    fn from_sparse_client(e: ClientError, throttle: &ThrottleHandle) -> Self {
        match e.kind() {
            ClientErrorKind::Http {
                status: StatusCode::TOO_MANY_REQUESTS,
            } => {
                throttle.record_ratelimit(None);
                Self::Ratelimited
            }
            ClientErrorKind::Transport => {
                let reason = e
                    .source_err()
                    .expect("transport error without source")
                    .to_smolstr();
                if let Some(secs) = throttle.record_failure_detail("transport", reason.to_string())
                {
                    debug!(secs, reason = %reason, "throttling pds after sparse transport error");
                }
                Self::Transport(reason)
            }
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

#[derive(Debug)]
struct SparseBackfillSuccess {
    state: RepoState<'static>,
    records: usize,
    node_blocks: usize,
    node_bytes: usize,
}

#[derive(Debug)]
enum SparseBackfillResult {
    Imported(SparseBackfillSuccess),
    Discarded,
    Skipped,
}

const SPARSE_GET_BLOCKS_CHUNK: usize = 100;
const SPARSE_GET_BLOCKS_PARALLELISM: usize = 1;
const SPARSE_MAX_SCAN_ROUNDS: usize = 256;
const SPARSE_AUTO_FULL_MAX_ROOT_LAYER: usize = 2;

async fn process_did_sparse(
    app_state: &Arc<AppState>,
    http: &reqwest::Client,
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
        return Err(BackfillError::Ratelimited);
    }

    let resp = {
        let _permit = throttle.acquire().await;
        if throttle.is_throttled() {
            return Err(BackfillError::Ratelimited);
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
                let blocks = fetch_blocks(http, pds, did, &missing, &throttle).await?;
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
    record_blocks.extend(fetch_blocks(http, pds, did, &missing_records, &throttle).await?);

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
    http: &reqwest::Client,
    pds: &url::Url,
    did: &Did<'static>,
    cids: &[IpldCid],
    throttle: &ThrottleHandle,
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
    http: reqwest::Client,
    pds: url::Url,
    did: Did<'static>,
    cids: Vec<IpldCid>,
    throttle: &ThrottleHandle,
) -> Result<BTreeMap<IpldCid, bytes::Bytes>, BackfillError> {
    let req = GetBlocks::new()
        .did(did.clone())
        .cids(
            cids.iter()
                .map(|cid| AtCid::from(cid.to_string()))
                .collect::<Vec<_>>(),
        )
        .build();
    let resp = {
        let _permit = throttle.acquire().await;
        match http.xrpc(url_to_fluent_uri(&pds)).send(&req).await {
            Ok(resp) => {
                if !throttle.is_throttled() {
                    throttle.record_success();
                }
                resp
            }
            Err(e) => return Err(BackfillError::from_sparse_client(e, throttle)),
        }
    };
    let car = match resp.into_output() {
        Ok(o) => o,
        Err(XrpcError::Xrpc(GetBlocksError::BlockNotFound(_))) => {
            return Ok(BTreeMap::new());
        }
        Err(XrpcError::Xrpc(
            GetBlocksError::RepoNotFound(_)
            | GetBlocksError::RepoTakendown(_)
            | GetBlocksError::RepoSuspended(_)
            | GetBlocksError::RepoDeactivated(_),
        )) => return Ok(BTreeMap::new()),
        Err(e) => Err(e).into_diagnostic()?,
    };
    crate::car::parse_car_blocks(&car.body)
        .await
        .map_err(BackfillError::from)
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

async fn process_did(
    app_state: &Arc<AppState>,
    http: &reqwest::Client,
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
                        let reservation = app_state_clone.db.stage_count_deltas(&mut batch, &count_deltas);
                        batch.commit().into_diagnostic().inspect(|_| {
                            app_state_clone.db.apply_lifecycle_counts(lifecycle_reservation);
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
                Err(e @ (BackfillError::Ratelimited | BackfillError::Transport(_))) => {
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
    let resp = http.xrpc(url_to_fluent_uri(&pds)).send(&req).await?;

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
