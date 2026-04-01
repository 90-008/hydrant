use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use jacquard_common::cowstr::ToCowStr;
use jacquard_common::types::cid::{Cid, IpldCid};
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::types::nsid::Nsid;
use jacquard_common::types::string::{Did, Handle, Rkey};
use jacquard_common::types::tid::Tid;
use jacquard_common::{CowStr, Data, IntoStatic};
use miette::{Context, IntoDiagnostic, Result, WrapErr};
use smol_str::ToSmolStr;
use url::Url;

use crate::db::types::{DbRkey, DidKey, TrimmedDid};
use crate::db::{self, Db, keys};
use crate::state::AppState;
#[cfg(feature = "indexer")]
use crate::types::GaugeState;
use crate::types::{RepoMetadata, RepoState, RepoStatus};
use crate::util::invalid_handle;

#[cfg(feature = "indexer")]
mod indexer;

#[cfg(feature = "indexer")]
pub use indexer::*;

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
    ) -> impl Iterator<Item = Result<(Did<'static>, RepoState<'static>, crate::types::RepoMetadata)>>
    {
        let start_bound = if let Some(cursor) = cursor {
            let did_key = keys::repo_key(cursor);
            std::ops::Bound::Excluded(did_key)
        } else {
            std::ops::Bound::Unbounded
        };

        let state = self.0.clone();
        self.0
            .db
            .repos
            .range((start_bound, std::ops::Bound::Unbounded))
            .map(move |g| {
                let (k, v) = g.into_inner().into_diagnostic()?;
                let repo_state = crate::db::deser_repo_state(&v)?.into_static();
                let did = TrimmedDid::try_from(k.as_ref())?.to_did();
                let metadata_key = keys::repo_metadata_key(&did);
                let metadata = state
                    .db
                    .repo_metadata
                    .get(&metadata_key)
                    .into_diagnostic()?
                    .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
                let metadata = crate::db::deser_repo_meta(metadata.as_ref())?;
                Ok((did, repo_state, metadata))
            })
    }

    /// iterates through all repositories, returning their state.
    pub fn iter(&self, cursor: Option<&Did<'_>>) -> impl Iterator<Item = Result<RepoInfo>> {
        self.iter_states(cursor)
            .map(|r| r.map(|(did, s, m)| repo_state_to_info(did, s, m.tracked)))
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
}

pub(crate) fn repo_state_to_info(did: Did<'static>, s: RepoState<'_>, tracked: bool) -> RepoInfo {
    let (rev, data) = s
        .root
        .map(|c| (Some(c.rev.to_tid()), Some(c.data)))
        .unwrap_or_default();
    RepoInfo {
        did,
        status: s.status,
        tracked,
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
        let did_key = keys::repo_key(&did);
        let metadata_key = keys::repo_metadata_key(&did);
        let app_state = self.state.clone();

        tokio::task::spawn_blocking(move || {
            let state_bytes = app_state.db.repos.get(&did_key).into_diagnostic()?;
            let Some(state_bytes) = state_bytes else {
                return Ok(None);
            };
            let repo_state = crate::db::deser_repo_state(&state_bytes)?;

            let metadata_bytes = app_state
                .db
                .repo_metadata
                .get(&metadata_key)
                .into_diagnostic()?
                .ok_or_else(|| miette::miette!("repo metadata not found for {}", did))?;
            let metadata = crate::db::deser_repo_meta(&metadata_bytes)?;

            Ok(Some(repo_state_to_info(did, repo_state, metadata.tracked)))
        })
        .await
        .into_diagnostic()?
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

        // check if repo is still backfilling (in pending)
        #[cfg(feature = "indexer")]
        let is_pending = {
            let metadata_key = keys::repo_metadata_key(&self.did);
            let app_state = self.state.clone();
            tokio::task::spawn_blocking(move || {
                let metadata_bytes = app_state
                    .db
                    .repo_metadata
                    .get(&metadata_key)
                    .into_diagnostic()?;
                let Some(metadata_bytes) = metadata_bytes else {
                    return Ok::<_, miette::Report>(false);
                };

                let metadata = crate::db::deser_repo_meta(metadata_bytes.as_ref())?;
                return Ok(app_state
                    .db
                    .pending
                    .get(crate::db::keys::pending_key(metadata.index_id))
                    .into_diagnostic()?
                    .is_some());
            })
            .await
            .map_err(|e| MiniDocError::Other(miette::miette!(e)))?
            .map_err(MiniDocError::Other)?
        };
        #[cfg(feature = "relay")]
        let is_pending = false;

        if is_pending {
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
}
