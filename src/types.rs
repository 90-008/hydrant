use jacquard_common::types::string::Did;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol_str::SmolStr;

// From src/state.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RepoStatus {
    New,
    Backfilling,
    Synced,
    Error(SmolStr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoState {
    pub did: SmolStr,
    pub status: RepoStatus,
    pub rev: SmolStr,
    pub last_seq: Option<i64>,
    pub last_updated_at: i64, // Unix timestamp
}

impl RepoState {
    pub fn new(did: Did) -> Self {
        Self {
            did: did.as_str().into(),
            status: RepoStatus::New,
            rev: "".into(),
            last_seq: None,
            last_updated_at: chrono::Utc::now().timestamp(),
        }
    }
}

// From src/backfill/error_state.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorState {
    pub error: SmolStr,
    pub retry_count: u32,
    pub next_retry: i64, // unix timestamp
}

impl ErrorState {
    pub fn next_backoff(retry_count: u32) -> i64 {
        // exponential backoff: 1m, 2m, 4m, 8m... up to 1h
        let base = 60;
        let cap = 3600;
        let mult = 2u64.pow(retry_count.min(10)) as i64;
        let delay = (base * mult).min(cap);
        chrono::Utc::now().timestamp() + delay
    }
}

// From src/api/event.rs

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
    pub handle: SmolStr,
    pub is_active: bool,
    pub status: SmolStr,
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
}
