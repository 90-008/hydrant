use serde::{Deserialize, Serialize};
use bytes::Bytes;
use jacquard_common::types::cid::IpldCid;
use jacquard_common::types::string::Handle;
use jacquard_common::CowStr;
use smol_str::SmolStr;
use crate::db::types::{DbTid, DidKey};

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
