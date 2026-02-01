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
pub struct MarshallableEvt {
    pub id: u64,
    #[serde(rename = "type")]
    pub event_type: SmolStr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<RecordEvt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityEvt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<AccountEvt>,
}

#[derive(Clone, Debug)]
pub enum BroadcastEvent {
    Persisted(u64),
    Ephemeral(MarshallableEvt),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecordEvt {
    pub live: bool,
    pub did: SmolStr,
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
pub struct IdentityEvt {
    pub did: SmolStr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<SmolStr>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AccountEvt {
    pub did: SmolStr,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SmolStr>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StoredEvent {
    Record {
        live: bool,
        did: SmolStr,
        rev: SmolStr,
        collection: SmolStr,
        rkey: SmolStr,
        action: SmolStr,
        cid: Option<SmolStr>,
    },
    Identity(IdentityEvt),
    Account(AccountEvt),
}
