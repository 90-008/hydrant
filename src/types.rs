use std::fmt::Display;

use jacquard_common::types::cid::IpldCid;
use jacquard_common::types::string::Did;
use jacquard_common::{CowStr, IntoStatic, types::string::Handle};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol_str::{SmolStr, ToSmolStr};

use crate::db::types::{DbAction, DbRkey, DbTid, DidKey, TrimmedDid};
use crate::resolver::MiniDoc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RepoStatus {
    Backfilling,
    Synced,
    Error(SmolStr),
    Deactivated,
    Takendown,
    Suspended,
}

impl Display for RepoStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoStatus::Backfilling => write!(f, "backfilling"),
            RepoStatus::Synced => write!(f, "synced"),
            RepoStatus::Error(e) => write!(f, "error({e})"),
            RepoStatus::Deactivated => write!(f, "deactivated"),
            RepoStatus::Takendown => write!(f, "takendown"),
            RepoStatus::Suspended => write!(f, "suspended"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "'i: 'de"))]
pub struct RepoState<'i> {
    pub status: RepoStatus,
    pub rev: Option<DbTid>,
    pub data: Option<IpldCid>,
    // todo: is this actually valid? the spec says this is informal and intermadiate
    // services may change it. we should probably document it. if we cant use this
    // then how do we dedup account / identity ops?
    /// ms since epoch of the last firehose message we processed for this repo.
    /// used to deduplicate identity / account events that can arrive from multiple relays at
    /// different wall-clock times but represent the same underlying PDS event.
    #[serde(default)]
    pub last_message_time: Option<i64>,
    /// this is when we *ingested* any last updates
    pub last_updated_at: i64, // unix timestamp
    /// whether we are ingesting events for this repo
    pub tracked: bool,
    /// index id in pending keyspace
    pub index_id: u64,
    #[serde(borrow)]
    pub signing_key: Option<DidKey<'i>>,
    #[serde(borrow)]
    pub pds: Option<CowStr<'i>>,
    #[serde(borrow)]
    pub handle: Option<Handle<'i>>,
}

impl<'i> RepoState<'i> {
    pub fn backfilling(index_id: u64) -> Self {
        Self {
            status: RepoStatus::Backfilling,
            rev: None,
            data: None,
            last_updated_at: chrono::Utc::now().timestamp(),
            index_id,
            tracked: true,
            handle: None,
            pds: None,
            signing_key: None,
            last_message_time: None,
        }
    }

    /// backfilling, but not tracked yet
    pub fn untracked(index_id: u64) -> Self {
        Self {
            tracked: false,
            ..Self::backfilling(index_id)
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
            status: self.status,
            rev: self.rev,
            data: self.data,
            last_updated_at: self.last_updated_at,
            index_id: self.index_id,
            tracked: self.tracked,
            handle: self.handle.map(IntoStatic::into_static),
            pds: self.pds.map(IntoStatic::into_static),
            signing_key: self.signing_key.map(IntoStatic::into_static),
            last_message_time: self.last_message_time,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResyncErrorKind {
    Ratelimited,
    Transport,
    Generic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResyncState {
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

// from src/api/event.rs

#[derive(Debug, Serialize, Clone)]
pub struct MarshallableEvt<'i> {
    pub id: u64,
    #[serde(rename = "type")]
    pub event_type: SmolStr,
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

#[derive(Clone, Debug)]
pub enum BroadcastEvent {
    #[allow(dead_code)]
    Persisted(u64),
    Ephemeral(Box<MarshallableEvt<'static>>),
}

#[derive(Debug, Serialize, Clone)]
pub struct RecordEvt<'i> {
    pub live: bool,
    #[serde(borrow)]
    pub did: Did<'i>,
    pub rev: CowStr<'i>,
    pub collection: CowStr<'i>,
    pub rkey: CowStr<'i>,
    pub action: CowStr<'i>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<CowStr<'i>>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(bound(deserialize = "'i: 'de"))]
pub struct StoredEvent<'i> {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<IpldCid>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum GaugeState {
    Synced,
    Pending,
    Resync(Option<ResyncErrorKind>),
}

impl GaugeState {
    pub fn is_resync(&self) -> bool {
        matches!(self, GaugeState::Resync(_))
    }
}
