use futures::{FutureExt, TryFutureExt};
use rand::Rng;

use super::*;
use crate::db::CountDeltas;

impl ReposControl {
    /// iterates through pending repositories, returning their state.
    #[allow(dead_code)]
    pub(crate) fn iter_pending(
        &self,
        cursor: Option<u64>,
    ) -> impl Iterator<Item = Result<(u64, RepoInfo)>> {
        let start_bound = if let Some(cursor) = cursor {
            std::ops::Bound::Excluded(cursor.to_be_bytes().to_vec())
        } else {
            std::ops::Bound::Unbounded
        };

        let repos = self.0.db.repos.clone();
        let state = self.0.clone();
        self.0
            .db
            .pending
            .range((start_bound, std::ops::Bound::Unbounded))
            .map(move |g| {
                let (id_raw, did_key) = g.into_inner().into_diagnostic()?;
                let id = u64::from_be_bytes(
                    id_raw
                        .as_ref()
                        .try_into()
                        .into_diagnostic()
                        .wrap_err("can't parse pending key")?,
                );
                let Some(bytes) = repos.get(&did_key).into_diagnostic()? else {
                    // stale pending that we forgot to delete? shouldn't happen though
                    tracing::warn!(id, did = ?did_key, "stale pending???");
                    return Ok(None);
                };
                let repo_state = crate::db::deser_repo_state(bytes.as_ref())?;
                let did = TrimmedDid::try_from(did_key.as_ref())?.to_did();
                let metadata_key = keys::repo_metadata_key(&did);
                let metadata = state
                    .db
                    .repo_metadata
                    .get(&metadata_key)
                    .into_diagnostic()?
                    .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
                let metadata = crate::db::deser_repo_meta(metadata.as_ref())?;
                Ok(Some((
                    id,
                    repo_state_to_info(did, repo_state.into_static(), metadata.tracked),
                )))
            })
            .map(|b| b.transpose())
            .flatten()
    }

    #[allow(dead_code)]
    pub(crate) fn iter_resync(
        &self,
        cursor: Option<&Did<'_>>,
    ) -> impl Iterator<Item = Result<RepoInfo>> {
        let start_bound = if let Some(cursor) = cursor {
            let did_key = keys::repo_key(cursor);
            std::ops::Bound::Excluded(did_key)
        } else {
            std::ops::Bound::Unbounded
        };

        let repos = self.0.db.repos.clone();
        let state = self.0.clone();
        self.0
            .db
            .resync
            .range((start_bound, std::ops::Bound::Unbounded))
            .map(move |g| {
                let did_key = g.key().into_diagnostic()?;
                let Some(bytes) = repos.get(&did_key).into_diagnostic()? else {
                    // stale pending that we forgot to delete? shouldn't happen though
                    tracing::warn!(did = ?did_key, "stale resync???");
                    return Ok(None);
                };
                let repo_state = crate::db::deser_repo_state(bytes.as_ref())?;
                let did = TrimmedDid::try_from(did_key.as_ref())?.to_did();
                let metadata_key = keys::repo_metadata_key(&did);
                let metadata = state
                    .db
                    .repo_metadata
                    .get(&metadata_key)
                    .into_diagnostic()?
                    .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
                let metadata = crate::db::deser_repo_meta(metadata.as_ref())?;
                Ok(Some(repo_state_to_info(
                    did,
                    repo_state.into_static(),
                    metadata.tracked,
                )))
            })
            .map(|b| b.transpose())
            .flatten()
    }

    pub(crate) fn _resync(
        db: &Db,
        did: &Did<'_>,
        batch: &mut fjall::OwnedWriteBatch,
        count_deltas: &mut CountDeltas,
    ) -> Result<bool> {
        let did_key = keys::repo_key(did);
        let metadata_key = keys::repo_metadata_key(did);

        let repo_bytes = db.repos.get(&did_key).into_diagnostic()?;
        let existing = repo_bytes
            .as_deref()
            .map(db::deser_repo_state)
            .transpose()?;

        if let Some(repo_state) = existing {
            let metadata_bytes = db
                .repo_metadata
                .get(&metadata_key)
                .into_diagnostic()?
                .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
            let mut metadata = crate::db::deser_repo_meta(&metadata_bytes)?;

            // skip if already in pending queue
            let is_pending = db
                .pending
                .get(keys::pending_key(metadata.index_id))
                .into_diagnostic()?
                .is_some();
            if !is_pending {
                let resync = db.resync.get(&did_key).into_diagnostic()?;
                let old = db::Db::repo_gauge_state(&repo_state, resync.as_deref());
                metadata.tracked = true;
                // insert into pending with new index_id
                let old_pending = keys::pending_key(metadata.index_id);
                batch.remove(&db.pending, &old_pending);
                metadata.index_id = rand::Rng::next_u64(&mut rand::rng());
                batch.insert(&db.pending, keys::pending_key(metadata.index_id), &did_key);
                batch.remove(&db.resync, &did_key);
                batch.insert(
                    &db.repo_metadata,
                    &metadata_key,
                    crate::db::ser_repo_meta(&metadata)?,
                );
                count_deltas.add_gauge_diff(&old, &GaugeState::Pending);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// request one or more repositories to be resynced.
    ///
    /// note that they may not immediately start backfilling if:
    /// - other repos already filled the backfill concurrency limit,
    /// - or there are many repos pending already.
    ///
    /// this will also clear any error state the repo may have been in,
    /// allowing it to resync again.
    pub async fn resync(
        &self,
        dids: impl IntoIterator<Item = Did<'_>>,
    ) -> Result<Vec<Did<'static>>> {
        let dids: Vec<Did<'static>> = dids.into_iter().map(|d| d.into_static()).collect();
        let state = self.0.clone();

        let queued = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut queued: Vec<Did<'static>> = Vec::new();
            let mut count_deltas = CountDeltas::default();

            for did in dids {
                if Self::_resync(db, &did, &mut batch, &mut count_deltas)? {
                    queued.push(did);
                }
            }

            let reservation = db.stage_count_deltas(&mut batch, &count_deltas);
            batch.commit().into_diagnostic()?;
            db.apply_count_deltas(&count_deltas);
            drop(reservation);
            state.db.persist()?;
            Ok::<_, miette::Report>(queued)
        })
        .await
        .into_diagnostic()??;
        if !queued.is_empty() {
            self.0.notify_backfill();
        }

        Ok(queued)
    }

    /// explicitly track one or more repositories, enqueuing them for backfill if needed.
    ///
    /// - if a repo is new, a fresh [`RepoState`] is created and backfill is queued.
    /// - if a repo is already known but untracked, it is marked tracked and re-enqueued.
    /// - if a repo is already tracked, this is a no-op.
    pub async fn track(
        &self,
        dids: impl IntoIterator<Item = Did<'_>>,
    ) -> Result<Vec<Did<'static>>> {
        let dids: Vec<Did<'static>> = dids.into_iter().map(|d| d.into_static()).collect();
        let state = self.0.clone();

        let queued = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut queued: Vec<Did<'static>> = Vec::new();
            let mut count_deltas = CountDeltas::default();

            for did in dids {
                let did_key = keys::repo_key(&did);
                let metadata_key = keys::repo_metadata_key(&did);

                let metadata_bytes = db.repo_metadata.get(&metadata_key).into_diagnostic()?;
                let existing_metadata = metadata_bytes
                    .map(|b| crate::db::deser_repo_meta(&b))
                    .transpose()?;

                if let Some(metadata) = existing_metadata {
                    if !metadata.tracked && Self::_resync(db, &did, &mut batch, &mut count_deltas)?
                    {
                        queued.push(did);
                    }
                } else {
                    let repo_state = RepoState::backfilling();
                    let metadata = RepoMetadata::backfilling(rand::random());
                    batch.insert(&db.repos, &did_key, crate::db::ser_repo_state(&repo_state)?);
                    batch.insert(
                        &db.repo_metadata,
                        &metadata_key,
                        crate::db::ser_repo_meta(&metadata)?,
                    );
                    batch.insert(&db.pending, keys::pending_key(metadata.index_id), &did_key);
                    count_deltas.add("repos", 1);
                    count_deltas.add_gauge_diff(&GaugeState::Synced, &GaugeState::Pending);
                    queued.push(did);
                }
            }

            let reservation = db.stage_count_deltas(&mut batch, &count_deltas);
            batch.commit().into_diagnostic()?;
            db.apply_count_deltas(&count_deltas);
            drop(reservation);
            state.db.persist()?;
            Ok::<_, miette::Report>(queued)
        })
        .await
        .into_diagnostic()??;
        self.0.notify_backfill();
        Ok(queued)
    }

    /// stop tracking one or more repositories. hydrant will stop processing new events
    /// for them and remove them from the pending/resync queues, but existing indexed
    /// records are **not** deleted.
    pub async fn untrack(
        &self,
        dids: impl IntoIterator<Item = Did<'_>>,
    ) -> Result<Vec<Did<'static>>> {
        let dids: Vec<Did<'static>> = dids.into_iter().map(|d| d.into_static()).collect();
        let state = self.0.clone();

        let untracked = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut untracked: Vec<Did<'static>> = Vec::new();
            let mut count_deltas = CountDeltas::default();

            for did in dids {
                let did_key = keys::repo_key(&did);
                let metadata_key = keys::repo_metadata_key(&did);

                let repo_bytes = db.repos.get(&did_key).into_diagnostic()?;
                let existing = repo_bytes
                    .as_deref()
                    .map(db::deser_repo_state)
                    .transpose()?;

                if let Some(repo_state) = existing {
                    let metadata_bytes = db.repo_metadata.get(&metadata_key).into_diagnostic()?;
                    let existing_metadata = metadata_bytes
                        .map(|b| crate::db::deser_repo_meta(&b))
                        .transpose()?;

                    if let Some(mut metadata) = existing_metadata {
                        if metadata.tracked {
                            let resync = db.resync.get(&did_key).into_diagnostic()?;
                            let old = db::Db::repo_gauge_state(&repo_state, resync.as_deref());
                            metadata.tracked = false;
                            batch.insert(
                                &db.repo_metadata,
                                &metadata_key,
                                crate::db::ser_repo_meta(&metadata)?,
                            );
                            batch.remove(&db.pending, keys::pending_key(metadata.index_id));
                            batch.remove(&db.resync, &did_key);
                            if old != GaugeState::Synced {
                                count_deltas.add_gauge_diff(&old, &GaugeState::Synced);
                            }
                            untracked.push(did);
                        }
                    }
                }
            }

            let reservation = db.stage_count_deltas(&mut batch, &count_deltas);
            batch.commit().into_diagnostic()?;
            db.apply_count_deltas(&count_deltas);
            drop(reservation);
            state.db.persist()?;
            Ok::<_, miette::Report>(untracked)
        })
        .await
        .into_diagnostic()??;
        Ok(untracked)
    }
}

impl<'i> RepoHandle<'i> {
    /// gets a record from this repository.
    pub async fn get_record(&self, collection: &str, rkey: &str) -> Result<Option<Record>> {
        if !self.state.is_block_storage_enabled() {
            miette::bail!("block storage is not available in this mode");
        }

        let did = self.did.clone().into_static();
        let db_key = keys::record_key(&did, collection, &DbRkey::new(rkey));

        let collection = collection.to_smolstr();
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            use miette::WrapErr;

            let cid_bytes = state.db.records.get(db_key).into_diagnostic()?;
            let Some(cid_bytes) = cid_bytes else {
                return Ok(None);
            };

            // lookup block using col|cid key
            let block_key = keys::block_key(&collection, &cid_bytes);
            let Some(block_bytes) = state.db.blocks.get(block_key).into_diagnostic()? else {
                miette::bail!("block {cid_bytes:?} not found, this is a bug!!");
            };

            let value = serde_ipld_dagcbor::from_slice::<Data>(&block_bytes)
                .into_diagnostic()
                .wrap_err("cant parse block")?
                .into_static();
            let cid = Cid::new(&cid_bytes)
                .into_diagnostic()
                .wrap_err("cant parse block cid")?;
            let cid = Cid::Str(cid.to_cowstr().into_static());

            Ok(Some(Record { did, cid, value }))
        })
        .await
        .into_diagnostic()?
    }

    /// lists records from this repository.
    pub async fn list_records(
        &self,
        collection: &str,
        limit: usize,
        reverse: bool,
        cursor: Option<&str>,
    ) -> Result<RecordList> {
        if !self.state.is_block_storage_enabled() {
            miette::bail!("block storage is not available in this mode");
        }
        let did = self.did.clone().into_static();

        let state = self.state.clone();
        let prefix = keys::record_prefix_collection(&did, collection);
        let collection = collection.to_smolstr();
        let cursor = cursor.map(|c| c.to_smolstr());

        tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();
            let mut next_cursor = None;

            let iter: Box<dyn Iterator<Item = _>> = if !reverse {
                let mut end_prefix = prefix.clone();
                if let Some(last) = end_prefix.last_mut() {
                    *last += 1;
                }

                let end_key = if let Some(cursor) = &cursor {
                    let mut k = prefix.clone();
                    k.extend_from_slice(cursor.as_bytes());
                    k
                } else {
                    end_prefix
                };

                Box::new(
                    state
                        .db
                        .records
                        .range(prefix.as_slice()..end_key.as_slice())
                        .rev(),
                )
            } else {
                let start_key = if let Some(cursor) = &cursor {
                    let mut k = prefix.clone();
                    k.extend_from_slice(cursor.as_bytes());
                    k.push(0);
                    k
                } else {
                    prefix.clone()
                };

                Box::new(state.db.records.range(start_key.as_slice()..))
            };

            for item in iter {
                let (key, cid_bytes) = item.into_inner().into_diagnostic()?;

                if !key.starts_with(prefix.as_slice()) {
                    break;
                }

                let rkey = keys::parse_rkey(&key[prefix.len()..])?;
                if results.len() >= limit {
                    next_cursor = Some(rkey);
                    break;
                }

                // look up using col|cid key built from collection and binary cid bytes
                if let Ok(Some(block_bytes)) = state
                    .db
                    .blocks
                    .get(&keys::block_key(collection.as_str(), &cid_bytes))
                {
                    let value: Data =
                        serde_ipld_dagcbor::from_slice(&block_bytes).unwrap_or(Data::Null);
                    let cid = Cid::new(&cid_bytes).into_diagnostic()?;
                    let cid = Cid::Str(cid.to_cowstr().into_static());
                    results.push(ListedRecord {
                        rkey: Rkey::new_cow(CowStr::Owned(rkey.to_smolstr()))
                            .expect("that rkey is validated"),
                        cid,
                        value: value.into_static(),
                    });
                }
            }
            Result::<_, miette::Report>::Ok((results, next_cursor))
        })
        .await
        .into_diagnostic()?
        .map(|(records, next_cursor)| RecordList {
            records,
            cursor: next_cursor.map(|rkey| {
                Rkey::new_cow(CowStr::Owned(rkey.to_smolstr())).expect("that rkey is validated")
            }),
        })
    }

    /// generates a streaming CAR v1 response body for this repository.
    ///
    /// returns `None` if the repo has no commit yet (still backfilling) or is an
    /// unmigrated repo that does not have the necessary data to reconstruct the
    /// root commit from.
    ///
    /// ## notes
    /// - calling this if you are using collection allowlist will always result
    /// in an error since the commit root won't match the reconstructed CID.
    /// - calling this for big repositories will incur more resource cost due to
    /// hydrant's structure, the whole MST is always reconstructed.
    pub async fn generate_car(
        &self,
    ) -> Result<Option<impl futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static>>
    {
        if !self.state.is_block_storage_enabled() {
            miette::bail!("block storage is not available in this mode");
        }
        use iroh_car::{CarHeader, CarWriter};
        use jacquard_repo::{BlockStore, MemoryBlockStore, Mst};
        use miette::WrapErr;
        use std::sync::Arc;

        let commit = match self.state().await? {
            Some(state) => match state.root {
                Some(c) => c,
                None => return Ok(None),
            },
            None => return Ok(None),
        };

        let atp_commit = match commit.into_atp_commit(self.did.clone().into_static()) {
            Some(c) => c,
            None => return Ok(None),
        };
        let commit_cid = atp_commit.to_cid().into_diagnostic()?;
        let commit_cbor = atp_commit.to_cbor().into_diagnostic()?;

        let did = self.did.clone().into_static();
        let app_state = self.state.clone();

        // build mst and populate the block store in a single blocking pass
        let store = Arc::new(MemoryBlockStore::new());
        let mst = Mst::new(store.clone());
        let handle = tokio::runtime::Handle::current();

        let mst = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut mst = mst;
            let prefix = keys::record_prefix_did(&did);

            for guard in app_state.db.records.prefix(&prefix) {
                let (key, cid_bytes) = guard.into_inner().into_diagnostic()?;

                let rest = &key[prefix.len()..];
                let mut parts = rest.splitn(2, |b: &u8| *b == keys::SEP);
                let collection_raw = parts
                    .next()
                    .ok_or_else(|| miette::miette!("missing collection in record key"))?;
                let rkey_raw = parts
                    .next()
                    .ok_or_else(|| miette::miette!("missing rkey in record key"))?;

                let collection = std::str::from_utf8(collection_raw)
                    .into_diagnostic()
                    .wrap_err("collection is not valid utf8")?;
                let rkey = keys::parse_rkey(rkey_raw)?;
                let mst_key = format!("{collection}/{rkey}");

                let ipld_cid = cid::Cid::read_bytes(cid_bytes.as_ref())
                    .into_diagnostic()
                    .wrap_err_with(|| format!("invalid cid bytes for record {mst_key}"))?;

                let block_key = keys::block_key(collection, cid_bytes.as_ref());
                let block_bytes = app_state
                    .db
                    .blocks
                    .get(&block_key)
                    .into_diagnostic()?
                    .ok_or_else(|| miette::miette!("block missing for record {mst_key}"))?;

                handle
                    .block_on(mst.add_mut(&mst_key, ipld_cid))
                    .into_diagnostic()?;
                // we use put_many here to skip calculating the CID again
                handle
                    .block_on(mst.storage().put_many([(
                        ipld_cid,
                        bytes::Bytes::copy_from_slice(block_bytes.as_ref()),
                    )]))
                    .into_diagnostic()?;
            }

            handle.block_on(mst.persist()).into_diagnostic()?;

            Result::<_>::Ok(mst)
        })
        .await
        .into_diagnostic()??;

        // sanity check: rebuilt root should match stored commit data in full-index mode
        let computed_root = mst.get_pointer().await.into_diagnostic()?;
        if computed_root != atp_commit.data {
            tracing::warn!(
                computed = %computed_root,
                stored = %atp_commit.data,
                did = %self.did,
                "mst root mismatch (expected in filter mode)",
            );
        }

        store
            .put_many([(commit_cid, bytes::Bytes::from(commit_cbor))])
            .await
            .into_diagnostic()?;

        // stream the car directly to the response
        let (reader, writer) = tokio::io::duplex(64 * 1024);
        tokio::spawn(
            async move {
                let header = CarHeader::new_v1(vec![commit_cid]);
                let mut car_writer = CarWriter::new(header, writer);

                // write commit first, then mst nodes + leaf blocks
                let commit_data = store.get(&commit_cid).await?;
                if let Some(data) = commit_data {
                    car_writer
                        .write(commit_cid, &data)
                        .await
                        .into_diagnostic()?;
                }
                mst.write_blocks_to_car(&mut car_writer).await?;
                car_writer.finish().await.into_diagnostic()?;

                Result::<_, miette::Report>::Ok(())
            }
            .inspect_err(|e| tracing::error!("can't generate car: {e}")),
        );

        Ok(Some(tokio_util::io::ReaderStream::new(reader)))
    }

    /// gets how many records of a collection this repository has.
    pub async fn count_records(&self, collection: &str) -> Result<u64> {
        let did = self.did.clone().into_static();
        let state = self.state.clone();
        let collection = collection.to_string();
        tokio::task::spawn_blocking(move || db::get_record_count(&state.db, &did, &collection))
            .await
            .into_diagnostic()?
    }
}
