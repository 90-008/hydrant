use serde::{Deserialize, Serialize};
use jacquard_common::types::string::Handle;
use jacquard_common::CowStr;
use smol_str::SmolStr;
use crate::db::types::DidKey;
pub(crate) use super::v2::Commit;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RepoStatus {
    Synced,
    Error(SmolStr),
    Deactivated,
    Takendown,
    Suspended,
    Deleted,
    Desynchronized,
    Throttled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "'i: 'de"))]
pub(crate) struct RepoState<'i> {
    pub active: bool,
    pub status: RepoStatus,
    pub root: Option<Commit>,
    pub last_message_time: Option<i64>,
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
    pub tracked: bool,
    pub index_id: u64,
}
