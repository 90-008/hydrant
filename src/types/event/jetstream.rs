use jacquard_common::IntoStatic;
use jacquard_common::{CowStr, types::string::Handle};
use serde::{Deserialize, Serialize};

use crate::db::types::TrimmedDid;
use crate::ingest::stream::Datetime;

#[inline]
#[allow(dead_code)]
pub(crate) fn default_true() -> bool {
    true
}

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

#[derive(Clone, Debug)]
pub(crate) struct JetstreamBroadcast {
    pub id: u64,
    pub time_us: i64,
    pub event: StoredJetstreamEvent<'static>,
    /// pre-serialized json bytes for live tailing.
    /// replay events read from the database instead.
    pub ephemeral: Option<bytes::Bytes>,
}

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
