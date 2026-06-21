use bytes::Bytes;
#[cfg(feature = "jetstream")]
use jacquard_common::IntoStatic;
use jacquard_common::types::cid::IpldCid;
use jacquard_common::types::nsid::Nsid;
use jacquard_common::types::string::{Did, Rkey};
use jacquard_common::types::tid::Tid;
use jacquard_common::{CowStr, types::string::Handle};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::value::RawValue;
use std::fmt::Debug;

#[cfg(any(feature = "indexer_stream", feature = "jetstream"))]
use crate::db::types::TrimmedDid;
#[cfg(feature = "indexer_stream")]
use crate::db::types::{DbAction, DbRkey, DbTid};
#[cfg(feature = "jetstream")]
use crate::ingest::stream::Datetime;

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

#[cfg(feature = "indexer_stream")]
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) enum StoredData {
    #[default]
    Nothing,
    Ptr(IpldCid),
    #[serde(with = "jacquard_common::serde_bytes_helper")]
    Block(Bytes),
}

#[cfg(feature = "indexer_stream")]
impl StoredData {
    pub fn is_nothing(&self) -> bool {
        matches!(self, StoredData::Nothing)
    }
}

#[cfg(feature = "indexer_stream")]
impl Debug for StoredData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nothing => f.write_str("nothing"),
            Self::Block(_) => f.write_str("<block>"),
            Self::Ptr(cid) => write!(f, "{cid}"),
        }
    }
}

#[cfg(feature = "indexer_stream")]
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
#[cfg(feature = "indexer_stream")]
#[derive(Debug, Clone)]
pub(crate) struct LiveRecordEvent {
    pub id: u64,
    pub stored: StoredEvent<'static>,
    pub inline_block: Option<Bytes>,
}

#[inline]
#[allow(dead_code)]
pub(crate) fn default_true() -> bool {
    true
}

#[cfg(feature = "jetstream")]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(bound(deserialize = "'i: 'de"))]
pub(crate) enum StoredJetstreamEvent<'i> {
    #[cfg(feature = "indexer_stream")]
    Commit {
        #[serde(borrow)]
        did: TrimmedDid<'i>,
        #[serde(borrow)]
        collection: CowStr<'i>,
        event_id: u64,
        #[serde(default = "default_true")]
        live: bool,
    },
    #[cfg(feature = "relay")]
    RelayCommit {
        #[serde(borrow)]
        did: TrimmedDid<'i>,
        #[serde(borrow)]
        collection: CowStr<'i>,
        relay_seq: u64,
        op_index: u32,
    },
    #[cfg(feature = "relay")]
    RelayAccount {
        #[serde(borrow)]
        did: TrimmedDid<'i>,
        relay_seq: u64,
    },
    #[cfg(feature = "relay")]
    RelayIdentity {
        #[serde(borrow)]
        did: TrimmedDid<'i>,
        relay_seq: u64,
    },
    Account {
        #[serde(borrow)]
        did: TrimmedDid<'i>,
        active: bool,
        #[serde(borrow)]
        status: Option<CowStr<'i>>,
        seq: i64,
        time: Datetime,
    },
    Identity {
        #[serde(borrow)]
        did: TrimmedDid<'i>,
        #[serde(borrow)]
        handle: Option<Handle<'i>>,
        seq: i64,
        time: Datetime,
    },
}

#[cfg(feature = "jetstream")]
#[derive(Clone, Debug)]
pub(crate) struct JetstreamBroadcast {
    pub id: u64,
    pub time_us: i64,
    pub event: StoredJetstreamEvent<'static>,
    /// pre-serialized json bytes for live tailing.
    /// replay events read from the database instead.
    pub ephemeral: Option<bytes::Bytes>,
}

#[cfg(feature = "jetstream")]
impl<'i> StoredJetstreamEvent<'i> {
    pub(crate) fn is_live(&self) -> bool {
        match self {
            #[cfg(feature = "indexer_stream")]
            Self::Commit { live, .. } => *live,
            _ => true,
        }
    }

    pub(crate) fn did(&self) -> &TrimmedDid<'i> {
        match self {
            #[cfg(feature = "indexer_stream")]
            Self::Commit { did, .. } => did,
            #[cfg(feature = "relay")]
            Self::RelayCommit { did, .. }
            | Self::RelayAccount { did, .. }
            | Self::RelayIdentity { did, .. } => did,
            Self::Account { did, .. } | Self::Identity { did, .. } => did,
        }
    }

    pub(crate) fn collection(&self) -> Option<&str> {
        match self {
            #[cfg(feature = "indexer_stream")]
            Self::Commit { collection, .. } => Some(collection.as_str()),
            #[cfg(feature = "relay")]
            Self::RelayCommit { collection, .. } => Some(collection.as_str()),
            #[cfg(feature = "relay")]
            Self::RelayAccount { .. } | Self::RelayIdentity { .. } => None,
            Self::Account { .. } | Self::Identity { .. } => None,
        }
    }

    pub(crate) fn into_static(self) -> StoredJetstreamEvent<'static> {
        match self {
            #[cfg(feature = "indexer_stream")]
            Self::Commit {
                did,
                collection,
                event_id,
                live,
            } => StoredJetstreamEvent::Commit {
                did: did.into_static(),
                collection: collection.into_static(),
                event_id,
                live,
            },
            #[cfg(feature = "relay")]
            Self::RelayCommit {
                did,
                collection,
                relay_seq,
                op_index,
            } => StoredJetstreamEvent::RelayCommit {
                did: did.into_static(),
                collection: collection.into_static(),
                relay_seq,
                op_index,
            },
            #[cfg(feature = "relay")]
            Self::RelayAccount { did, relay_seq } => StoredJetstreamEvent::RelayAccount {
                did: did.into_static(),
                relay_seq,
            },
            #[cfg(feature = "relay")]
            Self::RelayIdentity { did, relay_seq } => StoredJetstreamEvent::RelayIdentity {
                did: did.into_static(),
                relay_seq,
            },
            Self::Account {
                did,
                active,
                status,
                seq,
                time,
            } => StoredJetstreamEvent::Account {
                did: did.into_static(),
                active,
                status: status.map(IntoStatic::into_static),
                seq,
                time,
            },
            Self::Identity {
                did,
                handle,
                seq,
                time,
            } => StoredJetstreamEvent::Identity {
                did: did.into_static(),
                handle: handle.map(IntoStatic::into_static),
                seq,
                time,
            },
        }
    }
}

#[cfg(feature = "relay")]
#[derive(Clone)]
pub(crate) enum RelayBroadcast {
    Persisted(#[allow(dead_code)] u64),
    #[allow(dead_code)]
    Ephemeral(u64, bytes::Bytes),
}
