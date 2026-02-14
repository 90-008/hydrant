use std::fmt::Display;

use jacquard::{CowStr, IntoStatic};
use jacquard_common::types::string::Did;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol_str::SmolStr;

use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use jacquard::types::cid::IpldCid;

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
    #[serde(borrow)]
    pub did: TrimmedDid<'i>,
    pub status: RepoStatus,
    pub rev: Option<DbTid>,
    pub data: Option<IpldCid>,
    pub last_seq: Option<i64>,
    pub last_updated_at: i64, // unix timestamp
    pub handle: Option<SmolStr>,
}

impl<'i> RepoState<'i> {
    pub fn backfilling(did: &'i Did<'i>) -> Self {
        Self {
            did: TrimmedDid::from(did),
            status: RepoStatus::Backfilling,
            rev: None,
            data: None,
            last_seq: None,
            last_updated_at: chrono::Utc::now().timestamp(),
            handle: None,
        }
    }
}

impl<'i> IntoStatic for RepoState<'i> {
    type Output = RepoState<'static>;

    fn into_static(self) -> Self::Output {
        RepoState {
            did: self.did.into_static(),
            status: self.status,
            rev: self.rev,
            data: self.data,
            last_seq: self.last_seq,
            last_updated_at: self.last_updated_at,
            handle: self.handle,
        }
    }
}

// from src/backfill/resync_state.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResyncState {
    Error {
        message: SmolStr,
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
    Ephemeral(MarshallableEvt<'static>),
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
    pub handle: Option<CowStr<'i>>,
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
