use crate::db::{keys, Db};
use crate::ops;
use crate::state::{AppState, BackfillRx};
use crate::types::{ErrorState, RepoState, RepoStatus, StoredEvent};
use futures::TryFutureExt;
use jacquard::api::com_atproto::sync::get_repo::GetRepo;
use jacquard::api::com_atproto::sync::subscribe_repos::Commit;
use jacquard::prelude::*;
use jacquard::types::did::Did;
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
use tracing::{debug, error, info, trace};

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
                .inspect_err(move |e| error!("backfill process failed for {did}: {e}")),
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
        match Self::process_did(&state, &http, &did).await {
            Ok(previous_state) => {
                let did_key = keys::repo_key(&did);

                let is_pending = matches!(
                    previous_state.status,
                    RepoStatus::Backfilling | RepoStatus::New
                );
                let is_error = matches!(previous_state.status, RepoStatus::Error(_));

                let mut batch = db.inner.batch();
                // remove from pending
                if is_pending {
                    batch.remove(&db.pending, did_key);
                }
                // remove from errors (if it was a retry)
                if is_error {
                    batch.remove(&db.errors, did_key);
                }

                tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                    .await
                    .into_diagnostic()??;

                tokio::spawn({
                    let state = state.clone();
                    async move {
                        if is_pending {
                            let _ = state
                                .db
                                .increment_count(keys::count_keyspace_key("pending"), -1)
                                .await;
                        }
                        if is_error {
                            let _ = state
                                .db
                                .increment_count(keys::count_keyspace_key("errors"), -1)
                                .await;
                        }
                    }
                });

                Ok(())
            }
            Err(e) => {
                error!("backfill failed for {did}: {e}");
                let did_key = keys::repo_key(&did);

                // 1. get current retry count
                let mut retry_count = 0;
                if let Ok(Some(bytes)) = Db::get(db.errors.clone(), did_key).await {
                    if let Ok(old_err) = rmp_serde::from_slice::<ErrorState>(&bytes) {
                        retry_count = old_err.retry_count + 1;
                    }
                }

                // 2. calculate backoff
                let next_retry = ErrorState::next_backoff(retry_count);

                let err_state = ErrorState {
                    error: e.to_string().into(),
                    retry_count,
                    next_retry,
                };

                let mut batch = db.inner.batch();

                // 3. save to errors
                let bytes = rmp_serde::to_vec(&err_state).into_diagnostic()?;
                batch.insert(&db.errors, did_key, bytes);

                // 4. update main repo state
                if let Some(state_bytes) = Db::get(db.repos.clone(), did_key).await? {
                    let mut state: RepoState =
                        rmp_serde::from_slice(&state_bytes).into_diagnostic()?;
                    state.status = RepoStatus::Error(e.to_string().into());
                    let state_bytes = rmp_serde::to_vec(&state).into_diagnostic()?;
                    batch.insert(&db.repos, did_key, state_bytes);
                }

                // 5. remove from pending (it's now in errors)
                batch.remove(&db.pending, did_key);

                tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                    .await
                    .into_diagnostic()??;

                Ok(())
            }
        }
    }

    // returns previous repo state if successful
    async fn process_did(
        app_state: &Arc<AppState>,
        http: &reqwest::Client,
        did: &Did<'static>,
    ) -> Result<RepoState> {
        info!("backfilling {}", did);

        let db = &app_state.db;
        let did_key = keys::repo_key(did);
        let state_bytes = Db::get(db.repos.clone(), did_key)
            .await?
            .ok_or_else(|| miette::miette!("!!!THIS IS A BUG!!! repo state for {did} missing"))?;
        let mut state: RepoState = rmp_serde::from_slice(&state_bytes).into_diagnostic()?;
        let previous_state = state.clone();

        // 1. resolve pds
        let start = Instant::now();
        let pds_url = app_state.resolver.resolve_pds(did).await?;
        trace!(
            "resolved {} to pds {} in {:?}",
            did,
            pds_url,
            start.elapsed()
        );

        // 2. fetch repo (car)
        let start = Instant::now();
        let req = GetRepo::new().did(did.clone()).build();
        let car_bytes = http
            .xrpc(pds_url)
            .send(&req)
            .await
            .into_diagnostic()?
            .into_output()
            .into_diagnostic()?;
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
        info!("backfilling repo at revision {}", commit.rev);

        // 5. walk mst
        let start = Instant::now();
        let mst: Mst<MemoryBlockStore> = Mst::load(store, commit.data, None);
        let leaves = mst.leaves().await.into_diagnostic()?;
        trace!("walked mst for {} in {:?}", did, start.elapsed());

        // 6. insert records into db
        let start = Instant::now();
        let (added_records, added_blocks, collection_counts, count) = {
            let app_state = app_state.clone();
            let loop_did = did.clone();
            let loop_rev = commit.rev;
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

                            let db_key = keys::record_key(&loop_did, collection, rkey);
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
                                did: loop_did.as_str().into(),
                                rev: loop_rev.as_str().into(),
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

                // 6. update status to synced (inside batch)
                state.status = RepoStatus::Synced;
                state.rev = loop_rev.as_str().into();
                state.last_updated_at = chrono::Utc::now().timestamp();

                let did_key = keys::repo_key(&loop_did);
                let bytes = rmp_serde::to_vec(&state).into_diagnostic()?;
                batch.insert(&app_state.db.repos, did_key, bytes);

                batch.commit().into_diagnostic()?;

                Ok::<_, miette::Report>((added_records, added_blocks, collection_counts, count))
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
            "committed backfill batch for {} in {:?}",
            did,
            start.elapsed()
        );

        let _ = db
            .event_tx
            .send(db.next_event_id.load(Ordering::SeqCst) - 1);

        info!("marked {did} as synced, draining buffer...");

        // 7. drain buffer
        let start = Instant::now();
        let prefix = keys::buffer_prefix(did).to_vec();

        let num_buffered = tokio::task::spawn_blocking({
            let state = app_state.clone();
            let did = did.clone();
            move || -> Result<i64> {
                let mut batch = state.db.inner.batch();

                for res in state
                    .db
                    .buffer
                    .prefix(&prefix)
                    .map(|item| item.into_inner().into_diagnostic())
                {
                    let (key, value) = res?;
                    let commit: Commit = rmp_serde::from_slice(&value).into_diagnostic()?;
                    debug!("applying buffered commit seq: {}", commit.seq);

                    if let Err(e) = ops::apply_commit(&state.db, &commit, true) {
                        error!("failed to apply buffered commit for {did}: {e}");
                    }

                    // delete from buffer
                    batch.remove(&state.db.buffer, key);
                }

                batch.commit().into_diagnostic()?;

                Ok(count)
            }
        })
        .await
        .into_diagnostic()??;

        trace!(
            "drained {num_buffered} buffered commits for {did} in {:?}",
            start.elapsed()
        );

        info!("backfill complete for {did}");
        Ok(previous_state)
    }
}
