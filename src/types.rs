use std::fmt::{Debug, Display};

use bytes::Bytes;
use jacquard_common::types::cid::IpldCid;
use jacquard_common::types::nsid::Nsid;
use jacquard_common::types::string::{Did, Rkey};
use jacquard_common::types::tid::Tid;
use jacquard_common::{CowStr, IntoStatic, types::string::Handle};
use jacquard_repo::commit::Commit as AtpCommit;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use smol_str::{SmolStr, ToSmolStr};

use crate::db::types::{DbAction, DbRkey, DbTid, DidKey, TrimmedDid};
use crate::resolver::MiniDoc;

pub(crate) mod v2 {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub enum RepoStatus {
        Backfilling,
        Synced,
        Error(SmolStr),
        Deactivated,
        Takendown,
        Suspended,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct Commit {
        pub version: i64,
        pub rev: DbTid,
        pub data: IpldCid,
        pub prev: Option<IpldCid>,
        #[serde(with = "jacquard_common::serde_bytes_helper")]
        pub sig: Bytes,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(bound(deserialize = "'i: 'de"))]
    pub(crate) struct RepoState<'i> {
        pub status: RepoStatus,
        pub root: Option<Commit>,
        pub last_message_time: Option<i64>,
        pub last_updated_at: i64,
        pub tracked: bool,
        pub index_id: u64,
        #[serde(borrow)]
        pub signing_key: Option<DidKey<'i>>,
        #[serde(borrow)]
        pub pds: Option<CowStr<'i>>,
        #[serde(borrow)]
        pub handle: Option<Handle<'i>>,
    }
}

pub(crate) mod v4 {
    use super::*;
    pub(crate) use v2::Commit;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub enum RepoStatus {
        /// repo is synced to latest commit from what we know of
        Synced,
        /// some unclassified fatal error
        Error(SmolStr),
        /// user has temporarily paused their overall account. content should
        /// not be displayed or redistributed, but does not need to be deleted
        /// from infrastructure. implied time-limited. also the initial state
        /// for an account after migrating to another pds instance.
        Deactivated,
        /// host or service has takendown the account. implied permanent or
        /// long-term, though may be reverted.
        Takendown,
        /// host or service has temporarily paused the account. implied
        /// time-limited.
        Suspended,
        /// user or host has deleted the account, and content should be removed
        /// from the network. implied permanent or long-term, though may be
        /// reverted (deleted accounts may reactivate on the same or another
        /// host).
        ///
        /// account is deleted; kept as a tombstone so stale commits arriving from the upstream
        /// backfill window are not forwarded. active=false per spec.
        Deleted,
        /// host detected a repo sync problem. active may be true or false per spec;
        /// the `active` field on `RepoState` is authoritative.
        Desynchronized,
        /// resource rate-limit exceeded. active may be true or false per spec;
        /// the `active` field on `RepoState` is authoritative.
        Throttled,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(bound(deserialize = "'i: 'de"))]
    pub(crate) struct RepoState<'i> {
        /// whether the upstream considers this account active.
        /// services should use the `active` flag to control overall account visibility
        pub active: bool,
        pub status: RepoStatus,
        pub root: Option<Commit>,
        /// ms since epoch of the last firehose message we processed for this repo.
        /// used to deduplicate identity / account events that can arrive from multiple relays at
        /// different wall-clock times but represent the same underlying PDS event.
        pub last_message_time: Option<i64>,
        /// this is when we *ingested* any last updates
        pub last_updated_at: i64, // unix timestamp
        #[serde(borrow)]
        pub signing_key: Option<DidKey<'i>>,
        #[serde(borrow)]
        pub pds: Option<CowStr<'i>>,
        #[serde(borrow)]
        pub handle: Option<Handle<'i>>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct RepoMetadata {
        /// whether we are ingesting events for this repo
        pub tracked: bool,
        /// index id in pending keyspace (if backfilling)
        pub index_id: u64,
    }
}

pub(crate) use v4::*;

impl<'c> From<AtpCommit<'c>> for Commit {
    fn from(value: AtpCommit<'c>) -> Self {
        Self {
            data: value.data,
            prev: value.prev,
            rev: DbTid::from(&value.rev),
            sig: value.sig,
            version: value.version,
        }
    }
}

impl Commit {
    pub(crate) fn into_atp_commit<'i>(self, did: Did<'i>) -> Option<AtpCommit<'i>> {
        // version < 0 is a sentinel used in v2 migration for repos with no commit data
        if self.version < 0 {
            return None;
        }
        Some(AtpCommit {
            did,
            rev: self.rev.to_tid(),
            data: self.data,
            prev: self.prev,
            sig: self.sig,
            version: self.version,
        })
    }
}

impl Display for RepoStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoStatus::Synced => write!(f, "synced"),
            RepoStatus::Error(e) => write!(f, "error({e})"),
            RepoStatus::Deactivated => write!(f, "deactivated"),
            RepoStatus::Takendown => write!(f, "takendown"),
            RepoStatus::Suspended => write!(f, "suspended"),
            RepoStatus::Deleted => write!(f, "deleted"),
            RepoStatus::Desynchronized => write!(f, "desynchronized"),
            RepoStatus::Throttled => write!(f, "throttled"),
        }
    }
}

impl RepoMetadata {
    pub fn backfilling(index_id: u64) -> Self {
        Self {
            index_id,
            tracked: true,
        }
    }
}

impl<'i> RepoState<'i> {
    pub fn backfilling() -> Self {
        Self {
            active: true,
            status: RepoStatus::Desynchronized,
            root: None,
            last_updated_at: chrono::Utc::now().timestamp(),
            handle: None,
            pds: None,
            signing_key: None,
            last_message_time: None,
        }
    }

    // advances the high-water mark to event_ms if it's newer than what we've seen
    pub fn advance_message_time(&mut self, event_ms: i64) {
        self.last_message_time = Some(event_ms.max(self.last_message_time.unwrap_or(0)));
    }

    // updates last_updated_at to now
    pub fn touch(&mut self) {
        self.last_updated_at = chrono::Utc::now().timestamp();
    }

    pub fn update_from_doc(&mut self, doc: MiniDoc) -> bool {
        let new_signing_key = doc.key.map(From::from);
        let changed = self.pds.as_deref() != Some(doc.pds.as_str())
            || self.handle != doc.handle
            || self.signing_key != new_signing_key;
        self.pds = Some(CowStr::Owned(doc.pds.to_smolstr()));
        self.handle = doc.handle;
        self.signing_key = new_signing_key;
        changed
    }
}

impl<'i> IntoStatic for RepoState<'i> {
    type Output = RepoState<'static>;

    fn into_static(self) -> Self::Output {
        RepoState {
            active: self.active,
            status: self.status,
            root: self.root,
            last_updated_at: self.last_updated_at,
            handle: self.handle.map(IntoStatic::into_static),
            pds: self.pds.map(IntoStatic::into_static),
            signing_key: self.signing_key.map(IntoStatic::into_static),
            last_message_time: self.last_message_time,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ResyncErrorKind {
    Ratelimited,
    Transport,
    Generic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ResyncState {
    Error {
        kind: ResyncErrorKind,
        retry_count: u32,
        next_retry: i64, // unix timestamp
    },
    Gone {
        status: RepoStatus, // deactivated, takendown, suspended
    },
}

impl ResyncState {
    pub fn next_backoff(retry_count: u32) -> i64 {
        // exponential backoff: 1m, 2m, 4m, 8m... up to 1h
        let base = 60;
        let cap = 3600;
        let mult = 2u64.pow(retry_count.min(10)) as i64;
        let delay = (base * mult).min(cap);

        // add +/- 10% jitter
        let jitter = (rand::random::<f64>() * 0.2 - 0.1) * delay as f64;
        let delay = (delay as f64 + jitter) as i64;

        chrono::Utc::now().timestamp() + delay
    }
}

#[derive(Debug, Serialize, Clone)]
pub enum EventType {
    Record,
    Identity,
    Account,
}

impl AsRef<str> for EventType {
    fn as_ref(&self) -> &str {
        match self {
            Self::Record => "record",
            Self::Identity => "identity",
            Self::Account => "account",
        }
    }
}

fn event_type_ser_str<S: Serializer>(v: &EventType, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(v.as_ref())
}

#[derive(Debug, Serialize, Clone)]
pub struct MarshallableEvt<'i> {
    pub id: u64,
    #[serde(rename = "type")]
    #[serde(serialize_with = "event_type_ser_str")]
    pub kind: EventType,
    #[serde(borrow)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<RecordEvt<'i>>,
    #[serde(borrow)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityEvt<'i>>,
    #[serde(borrow)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<AccountEvt<'i>>,
}

#[cfg(feature = "indexer")]
#[derive(Clone, Debug)]
pub(crate) enum BroadcastEvent {
    #[allow(dead_code)]
    Persisted(u64),
    Ephemeral(Box<MarshallableEvt<'static>>),
}

#[derive(Debug, Serialize, Clone)]
pub struct RecordEvt<'i> {
    pub live: bool,
    #[serde(borrow)]
    pub did: Did<'i>,
    pub rev: Tid,
    pub collection: Nsid<'i>,
    pub rkey: Rkey<'i>,
    pub action: CowStr<'i>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "crate::util::opt_cid_serialize_str")]
    pub cid: Option<IpldCid>,
}

#[derive(Debug, Serialize, Clone)]
pub struct IdentityEvt<'i> {
    #[serde(borrow)]
    pub did: Did<'i>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<Handle<'i>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct AccountEvt<'i> {
    #[serde(borrow)]
    pub did: Did<'i>,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<CowStr<'i>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) enum StoredData {
    Nothing,
    Ptr(IpldCid),
    #[serde(with = "jacquard_common::serde_bytes_helper")]
    Block(Bytes),
}

impl StoredData {
    pub fn is_nothing(&self) -> bool {
        matches!(self, StoredData::Nothing)
    }
}

impl Default for StoredData {
    fn default() -> Self {
        Self::Nothing
    }
}

impl Debug for StoredData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nothing => f.write_str("nothing"),
            Self::Block(_) => f.write_str("<block>"),
            Self::Ptr(cid) => write!(f, "{cid}"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(bound(deserialize = "'i: 'de"))]
pub(crate) struct StoredEvent<'i> {
    #[serde(default)]
    pub live: bool,
    #[serde(borrow)]
    pub did: TrimmedDid<'i>,
    pub rev: DbTid,
    #[serde(borrow)]
    pub collection: CowStr<'i>,
    pub rkey: DbRkey,
    pub action: DbAction,
    #[serde(default)]
    #[serde(skip_serializing_if = "StoredData::is_nothing")]
    pub data: StoredData,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum GaugeState {
    Synced,
    Pending,
    Resync(Option<ResyncErrorKind>),
}

impl GaugeState {
    pub fn is_resync(&self) -> bool {
        matches!(self, GaugeState::Resync(_))
    }
}

#[cfg(feature = "relay")]
#[derive(Clone)]
pub(crate) enum RelayBroadcast {
    Persisted(#[allow(dead_code)] u64),
    #[allow(dead_code)]
    Ephemeral(bytes::Bytes),
}
