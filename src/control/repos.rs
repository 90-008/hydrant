use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use fjall::OwnedWriteBatch;
use futures::TryFutureExt;
use jacquard_common::cowstr::ToCowStr;
use jacquard_common::types::cid::{Cid, IpldCid};
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::types::nsid::Nsid;
use jacquard_common::types::string::{Did, Handle, Rkey};
use jacquard_common::types::tid::Tid;
use jacquard_common::{CowStr, Data, IntoStatic};
use miette::{Context, IntoDiagnostic, Result};
use rand::Rng;
use smol_str::ToSmolStr;
use url::Url;

use crate::db::types::{DbRkey, DidKey, TrimmedDid};
use crate::db::{self, Db, keys, ser_repo_state};
use crate::state::AppState;
use crate::types::{GaugeState, RepoState, RepoStatus};
use crate::util::invalid_handle;

/// information about a tracked or known repository. returned by [`ReposControl`] methods.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RepoInfo {
    /// the DID of the repository.
    pub did: Did<'static>,
    /// the status of the repository.
    #[serde(serialize_with = "crate::util::repo_status_serialize_str")]
    pub status: RepoStatus,
    /// whether this repository is tracked or not.
    /// untracked repositories are not updated and they stay frozen.
    pub tracked: bool,
    /// the revision of the root commit of this repository.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rev: Option<Tid>,
    /// the CID of the MST root of this repository.
    #[serde(serialize_with = "crate::util::opt_cid_serialize_str")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<IpldCid>,
    /// the handle for the DID of this repository.
    ///
    /// note that this handle is not bi-directionally verified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<Handle<'static>>,
    /// the URL for the PDS in which this repository is hosted on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pds: Option<Url>,
    /// ATProto signing key of this repository.
    #[serde(serialize_with = "crate::util::opt_did_key_serialize_str")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signing_key: Option<DidKey<'static>>,
    /// when this repository was last touched (status update, commit ingested, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_at: Option<DateTime<Utc>>,
    /// the time of the last message gotten from the firehose for this repository.
    /// this is equal to the `time` field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<DateTime<Utc>>,
}

/// control over which repositories are tracked and access to their state.
///
/// in `filter` mode, a repo is only indexed if it either matches a signal or is
/// explicitly tracked via [`ReposControl::track`]. in `full` mode all repos are
/// indexed and tracking is implicit.
///
/// tracking a DID that hydrant has never seen enqueues an immediate backfill.
/// tracking a DID that hydrant already knows about (but has marked untracked)
/// re-enqueues it for backfill.
#[derive(Clone)]
pub struct ReposControl(pub(super) Arc<AppState>);

impl ReposControl {
    pub(crate) fn iter_states(
        &self,
        cursor: Option<&Did<'_>>,
    ) -> impl Iterator<Item = Result<(Did<'static>, RepoState<'static>)>> {
        let start_bound = if let Some(cursor) = cursor {
            let did_key = keys::repo_key(cursor);
            std::ops::Bound::Excluded(did_key)
        } else {
            std::ops::Bound::Unbounded
        };

        self.0
            .db
            .repos
            .range((start_bound, std::ops::Bound::Unbounded))
            .map(|g| {
                let (k, v) = g.into_inner().into_diagnostic()?;
                let repo_state = crate::db::deser_repo_state(&v)?.into_static();
                let did = TrimmedDid::try_from(k.as_ref())?.to_did();
                Ok((did, repo_state))
            })
    }

    /// iterates through all repositories, returning their state.
    pub fn iter(&self, cursor: Option<&Did<'_>>) -> impl Iterator<Item = Result<RepoInfo>> {
        self.iter_states(cursor)
            .map(|r| r.map(|(did, s)| repo_state_to_info(did, s)))
    }

    #[allow(dead_code)]
    /// iterates through pending repositories, returning their state.
    fn iter_pending(&self, cursor: Option<u64>) -> impl Iterator<Item = Result<(u64, RepoInfo)>> {
        let start_bound = if let Some(cursor) = cursor {
            std::ops::Bound::Excluded(cursor.to_be_bytes().to_vec())
        } else {
            std::ops::Bound::Unbounded
        };

        let repos = self.0.db.repos.clone();
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
                let repo_state = crate::db::deser_repo_state(&bytes)?;
                let did = TrimmedDid::try_from(did_key.as_ref())?.to_did();
                Ok(Some((id, repo_state_to_info(did, repo_state))))
            })
            .map(|b| b.transpose())
            .flatten()
    }

    #[allow(dead_code)]
    fn iter_resync(&self, cursor: Option<&Did<'_>>) -> impl Iterator<Item = Result<RepoInfo>> {
        let start_bound = if let Some(cursor) = cursor {
            let did_key = keys::repo_key(cursor);
            std::ops::Bound::Excluded(did_key)
        } else {
            std::ops::Bound::Unbounded
        };

        let repos = self.0.db.repos.clone();
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
                let repo_state = crate::db::deser_repo_state(&bytes)?;
                let did = TrimmedDid::try_from(did_key.as_ref())?.to_did();
                Ok(Some(repo_state_to_info(did, repo_state)))
            })
            .map(|b| b.transpose())
            .flatten()
    }

    /// gets a handle for a repository to read from it.
    pub fn get<'i>(&self, did: &Did<'i>) -> RepoHandle<'i> {
        RepoHandle {
            state: self.0.clone(),
            did: did.clone(),
        }
    }

    /// same as [`ReposControl::get`] but allows you to pass in an identifier that can be
    /// either a handle or a DID.
    pub async fn resolve(&self, repo: &AtIdentifier<'_>) -> Result<RepoHandle<'static>> {
        let did = self.0.resolver.resolve_did(repo).await?;
        Ok(RepoHandle {
            state: self.0.clone(),
            did,
        })
    }

    /// fetch the current state of a repository.
    /// returns `None` if hydrant has never seen this repository.
    pub async fn info(&self, did: &Did<'_>) -> Result<Option<RepoInfo>> {
        self.get(did).info().await
    }

    fn _resync(
        db: &Db,
        did: &Did<'_>,
        batch: &mut OwnedWriteBatch,
        transitions: &mut Vec<(GaugeState, GaugeState)>,
    ) -> Result<bool> {
        let did_key = keys::repo_key(did);
        let repo_bytes = db.repos.get(&did_key).into_diagnostic()?;
        let existing = repo_bytes
            .as_deref()
            .map(db::deser_repo_state)
            .transpose()?;

        if let Some(mut repo_state) = existing
            && repo_state.status != RepoStatus::Backfilling
        {
            let resync = db.resync.get(&did_key).into_diagnostic()?;
            let old = db::Db::repo_gauge_state(&repo_state, resync.as_deref());
            repo_state.tracked = true;
            repo_state.status = RepoStatus::Backfilling;
            batch.insert(&db.repos, &did_key, ser_repo_state(&repo_state)?);
            batch.insert(
                &db.pending,
                keys::pending_key(repo_state.index_id),
                &did_key,
            );
            batch.remove(&db.resync, &did_key);
            transitions.push((old, GaugeState::Pending));
            return Ok(true);
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

        let (queued, transitions) = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut queued: Vec<Did<'static>> = Vec::new();
            let mut transitions: Vec<(GaugeState, GaugeState)> = Vec::new();

            for did in dids {
                if Self::_resync(db, &did, &mut batch, &mut transitions)? {
                    queued.push(did);
                }
            }

            batch.commit().into_diagnostic()?;
            Ok::<_, miette::Report>((queued, transitions))
        })
        .await
        .into_diagnostic()??;

        for (old, new) in transitions {
            self.0.db.update_gauge_diff_async(&old, &new).await;
        }
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

        let (new_count, queued, transitions) = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut added = 0i64;
            let mut queued: Vec<Did<'static>> = Vec::new();
            let mut transitions: Vec<(GaugeState, GaugeState)> = Vec::new();
            let mut rng = rand::rng();

            for did in dids {
                let did_key = keys::repo_key(&did);
                let repo_bytes = db.repos.get(&did_key).into_diagnostic()?;
                let existing = repo_bytes
                    .as_deref()
                    .map(db::deser_repo_state)
                    .transpose()?;

                if let Some(repo_state) = existing {
                    // the double read here is an ok tradeoff, the block will be in read-cache anyway
                    if !repo_state.tracked && Self::_resync(db, &did, &mut batch, &mut transitions)?
                    {
                        queued.push(did);
                    }
                } else {
                    let repo_state = RepoState::backfilling(rng.next_u64());
                    batch.insert(&db.repos, &did_key, ser_repo_state(&repo_state)?);
                    batch.insert(
                        &db.pending,
                        keys::pending_key(repo_state.index_id),
                        &did_key,
                    );
                    added += 1;
                    queued.push(did);
                    transitions.push((GaugeState::Synced, GaugeState::Pending));
                }
            }

            batch.commit().into_diagnostic()?;
            Ok::<_, miette::Report>((added, queued, transitions))
        })
        .await
        .into_diagnostic()??;

        if new_count > 0 {
            self.0.db.update_count_async("repos", new_count).await;
        }
        for (old, new) in transitions {
            self.0.db.update_gauge_diff_async(&old, &new).await;
        }
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

        let (untracked, gauge_decrements) = tokio::task::spawn_blocking(move || {
            let db = &state.db;
            let mut batch = db.inner.batch();
            let mut untracked: Vec<Did<'static>> = Vec::new();
            let mut gauge_decrements = Vec::new();

            for did in dids {
                let did_key = keys::repo_key(&did);
                let repo_bytes = db.repos.get(&did_key).into_diagnostic()?;
                let existing = repo_bytes
                    .as_deref()
                    .map(db::deser_repo_state)
                    .transpose()?;

                if let Some(repo_state) = existing {
                    if repo_state.tracked {
                        let resync = db.resync.get(&did_key).into_diagnostic()?;
                        let old = db::Db::repo_gauge_state(&repo_state, resync.as_deref());
                        let mut repo_state = repo_state.into_static();
                        repo_state.tracked = false;
                        batch.insert(&db.repos, &did_key, ser_repo_state(&repo_state)?);
                        batch.remove(&db.pending, keys::pending_key(repo_state.index_id));
                        batch.remove(&db.resync, &did_key);
                        if old != GaugeState::Synced {
                            gauge_decrements.push(old);
                        }
                        untracked.push(did);
                    }
                }
            }

            batch.commit().into_diagnostic()?;
            Ok::<_, miette::Report>((untracked, gauge_decrements))
        })
        .await
        .into_diagnostic()??;

        for gauge in gauge_decrements {
            self.0
                .db
                .update_gauge_diff_async(&gauge, &GaugeState::Synced)
                .await;
        }
        Ok(untracked)
    }
}

pub(crate) fn repo_state_to_info(did: Did<'static>, s: RepoState<'_>) -> RepoInfo {
    let (rev, data) = s
        .root
        .map(|c| (Some(c.rev.to_tid()), Some(c.data)))
        .unwrap_or_default();
    RepoInfo {
        did,
        status: s.status,
        tracked: s.tracked,
        rev,
        data,
        handle: s.handle.map(|h| h.into_static()),
        pds: s.pds.and_then(|p| p.parse().ok()),
        signing_key: s.signing_key.map(|k| k.into_static()),
        last_updated_at: DateTime::from_timestamp_secs(s.last_updated_at),
        last_message_at: s.last_message_time.and_then(DateTime::from_timestamp_secs),
    }
}

pub struct Record {
    pub did: Did<'static>,
    pub cid: Cid<'static>,
    pub value: Data<'static>,
}

pub struct ListedRecord {
    pub rkey: Rkey<'static>,
    pub cid: Cid<'static>,
    pub value: Data<'static>,
}

pub struct RecordList {
    pub records: Vec<ListedRecord>,
    pub cursor: Option<Rkey<'static>>,
}

#[derive(Debug, thiserror::Error)]
pub enum MiniDocError {
    #[error("repo is not synced yet")]
    NotSynced,
    #[error("repo not found")]
    RepoNotFound,
    #[error("could not resolve identity")]
    CouldNotResolveIdentity,
    #[error("{0}")]
    Other(miette::Error),
}

/// a mini doc with a bi-directionally verified handle.
pub struct MiniDoc<'i> {
    /// the did.
    pub did: Did<'i>,
    /// the handle. if verification fails or no handle is found,
    /// this will be "handle.invalid".
    pub handle: Handle<'i>,
    /// the url of the PDS of this repo.
    pub pds: Url,
    /// the atproto signing key of this repo.
    pub signing_key: DidKey<'i>,
}

/// handle to access data related to this repository.
#[derive(Clone)]
pub struct RepoHandle<'i> {
    state: Arc<AppState>,
    pub did: Did<'i>,
}

impl<'i> RepoHandle<'i> {
    pub(crate) async fn state(&self) -> Result<Option<RepoState<'static>>> {
        let did_key = keys::repo_key(&self.did);
        let app_state = self.state.clone();

        tokio::task::spawn_blocking(move || {
            let bytes = app_state.db.repos.get(&did_key).into_diagnostic()?;
            bytes
                .as_deref()
                .map(db::deser_repo_state)
                .transpose()
                .map(|opt| opt.map(IntoStatic::into_static))
        })
        .await
        .into_diagnostic()?
    }

    /// fetch the current state of this repository.
    /// returns `None` if hydrant has never seen this repository.
    pub async fn info(&self) -> Result<Option<RepoInfo>> {
        let did = self.did.clone().into_static();
        Ok(self.state().await?.map(|s| repo_state_to_info(did, s)))
    }

    /// returns the collections of this repository and the number of records it has in each.
    pub async fn collections(&self) -> Result<HashMap<Nsid<'static>, u64>> {
        let did = self.did.clone().into_static();
        let state = self.state.clone();

        tokio::task::spawn_blocking(move || {
            let prefix = keys::did_collection_prefix(&did);
            let mut res = HashMap::new();
            for item in state.db.counts.prefix(&prefix) {
                let (k, v) = item.into_inner().into_diagnostic()?;
                let col = k
                    .strip_prefix(prefix.as_slice())
                    .ok_or_else(|| miette::miette!("invalid collection count key: {k:?}"))
                    .and_then(|r| std::str::from_utf8(r).into_diagnostic())
                    .and_then(|n| Nsid::new(n).into_diagnostic())?
                    .into_static();
                let count = u64::from_be_bytes(
                    v.as_ref()
                        .try_into()
                        .into_diagnostic()
                        .wrap_err("expected to be count (8 bytes)")?,
                );
                res.insert(col, count);
            }
            Ok(res)
        })
        .await
        .into_diagnostic()?
    }

    /// returns a bi-directionally validated mini doc.
    pub async fn mini_doc(&self) -> Result<MiniDoc<'static>, MiniDocError> {
        let Some(info) = self.info().await.map_err(MiniDocError::Other)? else {
            return Err(MiniDocError::RepoNotFound);
        };

        if info.status == RepoStatus::Backfilling {
            return Err(MiniDocError::NotSynced);
        }

        let pds = info
            .pds
            .ok_or_else(|| MiniDocError::CouldNotResolveIdentity)?;
        let signing_key = info
            .signing_key
            .ok_or_else(|| MiniDocError::CouldNotResolveIdentity)?
            .into_static();

        let handle = if let Some(h) = info.handle {
            let is_valid = self
                .state
                .resolver
                .verify_handle(&self.did, &h)
                .await
                .into_diagnostic()
                .map_err(MiniDocError::Other)?;
            is_valid.then_some(h).unwrap_or_else(invalid_handle)
        } else {
            invalid_handle()
        };

        Ok(MiniDoc {
            did: self.did.clone().into_static(),
            handle,
            pds,
            signing_key,
        })
    }

    /// gets a record from this repository.
    pub async fn get_record(&self, collection: &str, rkey: &str) -> Result<Option<Record>> {
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
