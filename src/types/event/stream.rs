use bytes::Bytes;
use jacquard_common::types::cid::IpldCid;
use jacquard_common::types::nsid::Nsid;
use jacquard_common::types::string::{Did, Rkey};
use jacquard_common::types::tid::Tid;
use jacquard_common::{CowStr, types::string::Handle};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::value::RawValue;
use std::fmt::Debug;

use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};

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
    pub record: Option<std::sync::Arc<RawValue>>,
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

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) enum StoredData {
    #[default]
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

/// a durable record event that is also emitted on the in-memory stream after commit.
///
/// `inline_block` is only used for live tailing to avoid loading the record block from `blocks`.
/// cursor replay continues to read from the database.
#[derive(Debug, Clone)]
pub(crate) struct LiveRecordEvent {
    pub id: u64,
    pub stored: StoredEvent<'static>,
    pub inline_block: Option<Bytes>,
}
