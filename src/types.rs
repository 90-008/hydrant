use jacquard_common::types::string::Did;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol_str::SmolStr;

// from src/state.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RepoStatus {
    New,
    Backfilling,
    Synced,
    Error(SmolStr),
    Deactivated,
    Takendown,
    Suspended,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoState {
    pub did: SmolStr,
    pub status: RepoStatus,
    pub rev: SmolStr,
    pub data: SmolStr,
    pub last_seq: Option<i64>,
    pub last_updated_at: i64, // unix timestamp
    pub handle: Option<SmolStr>,
}

impl RepoState {
    pub fn new(did: Did) -> Self {
        Self {
            did: did.as_str().into(),
            status: RepoStatus::New,
            rev: "".into(),
            data: "".into(),
            last_seq: None,
            last_updated_at: chrono::Utc::now().timestamp(),
            handle: None,
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
        chrono::Utc::now().timestamp() + delay
    }
}

// from src/api/event.rs

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    Persisted(u64),
    Ephemeral(MarshallableEvt<'static>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecordEvt<'i> {
    pub live: bool,
    #[serde(borrow)]
    pub did: Did<'i>,
    pub rev: SmolStr,
    pub collection: SmolStr,
    pub rkey: SmolStr,
    pub action: SmolStr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<SmolStr>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IdentityEvt<'i> {
    #[serde(borrow)]
    pub did: Did<'i>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<SmolStr>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AccountEvt<'i> {
    #[serde(borrow)]
    pub did: Did<'i>,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SmolStr>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StoredEvent<'i> {
    Record {
        live: bool,
        #[serde(borrow)]
        did: Did<'i>,
        rev: SmolStr,
        collection: SmolStr,
        rkey: SmolStr,
        action: SmolStr,
        cid: Option<SmolStr>,
    },
    #[serde(borrow)]
    Identity(IdentityEvt<'i>),
    #[serde(borrow)]
    Account(AccountEvt<'i>),
}
