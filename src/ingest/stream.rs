use std::convert::Infallible;
use std::option::Option;

use bytes::Bytes;
use futures::StreamExt;
use jacquard_common::error::DecodeError;
use jacquard_common::{
    CowStr,
    types::{
        cid::CidLink,
        string::{Did, Handle, Tid},
    },
};
use miette::Diagnostic;
use smol_str::format_smolstr;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::trace;
use url::Url;

#[derive(Debug, Error, Diagnostic)]
pub enum FirehoseError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("unknown scheme: {0}")]
    UnknownScheme(String),
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    #[error("empty frame")]
    EmptyFrame,
    #[error("relay error {error}: {message:?}")]
    RelayError {
        error: String,
        message: Option<String>,
    },
    #[error("unknown op: {0}")]
    UnknownOp(i64),
    #[error("missing type in header")]
    MissingType,
    #[error("unknown event type: {0}")]
    UnknownType(String),
    #[error("cbor decode error: {0}")]
    Cbor(String),
}

impl From<serde_ipld_dagcbor::DecodeError<Infallible>> for FirehoseError {
    fn from(e: serde_ipld_dagcbor::DecodeError<Infallible>) -> Self {
        Self::Cbor(e.to_string())
    }
}

pub struct FirehoseStream {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl FirehoseStream {
    pub async fn connect(mut relay: Url, cursor: Option<i64>) -> Result<Self, FirehoseError> {
        let scheme = match relay.scheme() {
            "https" | "wss" => "wss",
            "http" | "ws" => "ws",
            x => return Err(FirehoseError::UnknownScheme(x.to_string())),
        };
        relay.set_scheme(scheme).expect("to be valid url");
        relay.set_path("/xrpc/com.atproto.sync.subscribeRepos");
        let cursor = cursor.map(|c| format_smolstr!("cursor={c}"));
        relay.set_query(cursor.as_deref());

        let (ws, _) = connect_async(relay.as_str()).await?;
        Ok(Self { ws })
    }

    /// gets the next message bytes from the firehose
    pub async fn next(&mut self) -> Option<Result<Bytes, FirehoseError>> {
        loop {
            match self.ws.next().await? {
                Err(e) => return Some(Err(e.into())),
                Ok(Message::Binary(bytes)) => {
                    if bytes.is_empty() {
                        return Some(Err(FirehoseError::EmptyFrame));
                    }
                    return Some(Ok(bytes));
                }
                Ok(Message::Close(_)) => return None,
                Ok(x) => {
                    trace!(msg = ?x, "relay sent unexpected message");
                    continue;
                }
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(try_from = "String")]
pub struct Datetime(pub chrono::DateTime<chrono::FixedOffset>);

impl TryFrom<String> for Datetime {
    type Error = chrono::ParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        chrono::DateTime::parse_from_rfc3339(&s)
            .map(Self)
            .or_else(|_| {
                // no timezone, warn and assume UTC
                tracing::warn!(
                    value = %s,
                    "datetime missing timezone suffix, assuming UTC"
                );
                chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|ndt| Self(ndt.and_utc().fixed_offset()))
            })
    }
}

impl jacquard_common::IntoStatic for Datetime {
    type Output = Datetime;
    fn into_static(self) -> Self::Output {
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepoOpAction<'a> {
    Create,
    Update,
    Delete,
    Other(jacquard_common::CowStr<'a>),
}

impl<'a> RepoOpAction<'a> {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Other(s) => s.as_ref(),
        }
    }
}

impl<'a> From<&'a str> for RepoOpAction<'a> {
    fn from(s: &'a str) -> Self {
        match s {
            "create" => Self::Create,
            "update" => Self::Update,
            "delete" => Self::Delete,
            _ => Self::Other(jacquard_common::CowStr::from(s)),
        }
    }
}

impl<'a> From<String> for RepoOpAction<'a> {
    fn from(s: String) -> Self {
        match s.as_str() {
            "create" => Self::Create,
            "update" => Self::Update,
            "delete" => Self::Delete,
            _ => Self::Other(jacquard_common::CowStr::from(s)),
        }
    }
}

impl<'a> core::fmt::Display for RepoOpAction<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'a> AsRef<str> for RepoOpAction<'a> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<'a> serde::Serialize for RepoOpAction<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de, 'a> serde::Deserialize<'de> for RepoOpAction<'a>
where
    'de: 'a,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&'de str>::deserialize(deserializer)?;
        Ok(Self::from(s))
    }
}

impl<'a> Default for RepoOpAction<'a> {
    fn default() -> Self {
        Self::Other(Default::default())
    }
}

impl<'a> jacquard_common::IntoStatic for RepoOpAction<'a> {
    type Output = RepoOpAction<'static>;
    fn into_static(self) -> Self::Output {
        match self {
            RepoOpAction::Create => RepoOpAction::Create,
            RepoOpAction::Update => RepoOpAction::Update,
            RepoOpAction::Delete => RepoOpAction::Delete,
            RepoOpAction::Other(v) => RepoOpAction::Other(v.into_static()),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, jacquard_derive::IntoStatic)]
#[serde(rename_all = "camelCase")]
pub struct RepoOp<'a> {
    #[serde(borrow)]
    pub action: RepoOpAction<'a>,
    /// For creates and updates, the new record CID. For deletions, null.
    #[serde(borrow)]
    pub cid: Option<CidLink<'a>>,
    #[serde(borrow)]
    pub path: jacquard_common::CowStr<'a>,
    /// For updates and deletes, the previous record CID (required for inductive firehose). For creations, field should not be defined.
    #[serde(skip_serializing_if = "std::option::Option::is_none")]
    #[serde(default)]
    #[serde(borrow)]
    pub prev: Option<CidLink<'a>>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, jacquard_derive::IntoStatic)]
#[serde(rename_all = "camelCase")]
pub struct Commit<'a> {
    #[serde(borrow)]
    pub blobs: Vec<CidLink<'a>>,
    #[serde(with = "jacquard_common::serde_bytes_helper")]
    pub blocks: Bytes,
    #[serde(borrow)]
    pub commit: CidLink<'a>,
    #[serde(borrow)]
    pub ops: Vec<RepoOp<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[serde(borrow)]
    pub prev_data: Option<CidLink<'a>>,
    pub rebase: bool,
    #[serde(borrow)]
    pub repo: Did<'a>,
    pub rev: Tid,
    pub seq: i64,
    #[serde(deserialize_with = "deserialize_tid_or_empty")]
    pub since: Option<Tid>,
    pub time: Datetime,
    pub too_big: bool,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, jacquard_derive::IntoStatic)]
#[serde(rename_all = "camelCase")]
pub struct Identity<'a> {
    #[serde(borrow)]
    pub did: Did<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[serde(borrow)]
    pub handle: Option<Handle<'a>>,
    pub seq: i64,
    pub time: Datetime,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AccountStatus<'a> {
    Takendown,
    Suspended,
    Deleted,
    Deactivated,
    Desynchronized,
    Throttled,
    Other(jacquard_common::CowStr<'a>),
}

impl<'a> AccountStatus<'a> {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Takendown => "takendown",
            Self::Suspended => "suspended",
            Self::Deleted => "deleted",
            Self::Deactivated => "deactivated",
            Self::Desynchronized => "desynchronized",
            Self::Throttled => "throttled",
            Self::Other(s) => s.as_ref(),
        }
    }
}

impl<'a> From<&'a str> for AccountStatus<'a> {
    fn from(s: &'a str) -> Self {
        match s {
            "takendown" => Self::Takendown,
            "suspended" => Self::Suspended,
            "deleted" => Self::Deleted,
            "deactivated" => Self::Deactivated,
            "desynchronized" => Self::Desynchronized,
            "throttled" => Self::Throttled,
            _ => Self::Other(jacquard_common::CowStr::from(s)),
        }
    }
}

impl<'a> From<String> for AccountStatus<'a> {
    fn from(s: String) -> Self {
        match s.as_str() {
            "takendown" => Self::Takendown,
            "suspended" => Self::Suspended,
            "deleted" => Self::Deleted,
            "deactivated" => Self::Deactivated,
            "desynchronized" => Self::Desynchronized,
            "throttled" => Self::Throttled,
            _ => Self::Other(jacquard_common::CowStr::from(s)),
        }
    }
}

impl<'a> core::fmt::Display for AccountStatus<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'a> AsRef<str> for AccountStatus<'a> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<'a> serde::Serialize for AccountStatus<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de, 'a> serde::Deserialize<'de> for AccountStatus<'a>
where
    'de: 'a,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&'de str>::deserialize(deserializer)?;
        Ok(Self::from(s))
    }
}

impl<'a> Default for AccountStatus<'a> {
    fn default() -> Self {
        Self::Other(Default::default())
    }
}

impl jacquard_common::IntoStatic for AccountStatus<'_> {
    type Output = AccountStatus<'static>;
    fn into_static(self) -> Self::Output {
        match self {
            AccountStatus::Takendown => AccountStatus::Takendown,
            AccountStatus::Suspended => AccountStatus::Suspended,
            AccountStatus::Deleted => AccountStatus::Deleted,
            AccountStatus::Deactivated => AccountStatus::Deactivated,
            AccountStatus::Desynchronized => AccountStatus::Desynchronized,
            AccountStatus::Throttled => AccountStatus::Throttled,
            AccountStatus::Other(v) => AccountStatus::Other(v.into_static()),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, jacquard_derive::IntoStatic)]
#[serde(rename_all = "camelCase")]
pub struct Account<'a> {
    pub active: bool,
    #[serde(borrow)]
    pub did: Did<'a>,
    pub seq: i64,
    pub status: Option<AccountStatus<'a>>,
    pub time: Datetime,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, jacquard_derive::IntoStatic)]
#[serde(rename_all = "camelCase")]
pub struct Sync<'a> {
    #[serde(with = "jacquard_common::serde_bytes_helper")]
    pub blocks: Bytes,
    #[serde(borrow)]
    pub did: Did<'a>,
    #[serde(borrow)]
    pub rev: CowStr<'a>,
    pub seq: i64,
    pub time: Datetime,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InfoName<'a> {
    OutdatedCursor,
    Other(jacquard_common::CowStr<'a>),
}

impl<'a> InfoName<'a> {
    pub fn as_str(&self) -> &str {
        match self {
            Self::OutdatedCursor => "OutdatedCursor",
            Self::Other(s) => s.as_ref(),
        }
    }
}

impl<'a> From<&'a str> for InfoName<'a> {
    fn from(s: &'a str) -> Self {
        match s {
            "OutdatedCursor" => Self::OutdatedCursor,
            _ => Self::Other(jacquard_common::CowStr::from(s)),
        }
    }
}

impl<'a> From<String> for InfoName<'a> {
    fn from(s: String) -> Self {
        match s.as_str() {
            "OutdatedCursor" => Self::OutdatedCursor,
            _ => Self::Other(jacquard_common::CowStr::from(s)),
        }
    }
}

impl<'a> core::fmt::Display for InfoName<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'a> AsRef<str> for InfoName<'a> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<'a> serde::Serialize for InfoName<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de, 'a> serde::Deserialize<'de> for InfoName<'a>
where
    'de: 'a,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&'de str>::deserialize(deserializer)?;
        Ok(Self::from(s))
    }
}

impl<'a> Default for InfoName<'a> {
    fn default() -> Self {
        Self::Other(Default::default())
    }
}

impl<'a> jacquard_common::IntoStatic for InfoName<'a> {
    type Output = InfoName<'static>;
    fn into_static(self) -> Self::Output {
        match self {
            InfoName::OutdatedCursor => InfoName::OutdatedCursor,
            InfoName::Other(v) => InfoName::Other(v.into_static()),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, jacquard_derive::IntoStatic)]
#[serde(rename_all = "camelCase")]
pub struct Info<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(borrow)]
    pub message: Option<CowStr<'a>>,
    #[serde(borrow)]
    pub name: InfoName<'a>,
}

#[derive(Debug, Clone, jacquard_derive::IntoStatic)]
pub enum SubscribeReposMessage<'i> {
    Commit(Box<Commit<'i>>),
    Sync(Box<Sync<'i>>),
    Identity(Box<Identity<'i>>),
    Account(Box<Account<'i>>),
    Info(Box<Info<'i>>),
}

impl<'i> SubscribeReposMessage<'i> {
    pub fn did<'s>(&'s self) -> Option<&'s Did<'i>> {
        Some(match self {
            SubscribeReposMessage::Commit(c) => &c.repo,
            SubscribeReposMessage::Identity(i) => &i.did,
            SubscribeReposMessage::Account(a) => &a.did,
            SubscribeReposMessage::Sync(s) => &s.did,
            _ => return None,
        })
    }
}

use serde::Deserialize;
use serde_ipld_dagcbor::de::Deserializer;

// some relays send `""` for `since` when there is no previous revision instead of null
fn deserialize_tid_or_empty<'de, D>(deserializer: D) -> Result<Option<Tid>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = <Option<String>>::deserialize(deserializer)?;
    match s.as_deref() {
        None => Ok(None),
        Some("") => {
            tracing::warn!("received since with empty string instead of null");
            Ok(None)
        }
        Some(s) => s.parse::<Tid>().map(Some).map_err(serde::de::Error::custom),
    }
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct EventHeader {
    op: i64,
    t: Option<String>,
}

#[derive(Deserialize)]
struct ErrorFrame {
    error: String,
    message: Option<String>,
}

pub fn decode_frame<'i>(bytes: &'i [u8]) -> Result<SubscribeReposMessage<'i>, FirehoseError> {
    let mut de = Deserializer::from_slice(bytes);
    let header = EventHeader::deserialize(&mut de)?;

    match header.op {
        -1 => {
            let err = ErrorFrame::deserialize(&mut de)?;
            return Err(FirehoseError::RelayError {
                error: err.error,
                message: err.message,
            });
        }
        1 => {}
        op => return Err(FirehoseError::UnknownOp(op)),
    }

    let t = header.t.ok_or(FirehoseError::MissingType)?;

    let msg = match t.as_str() {
        "#commit" => SubscribeReposMessage::Commit(Box::new(Deserialize::deserialize(&mut de)?)),
        "#account" => SubscribeReposMessage::Account(Box::new(Deserialize::deserialize(&mut de)?)),
        "#identity" => {
            SubscribeReposMessage::Identity(Box::new(Deserialize::deserialize(&mut de)?))
        }
        "#sync" => SubscribeReposMessage::Sync(Box::new(Deserialize::deserialize(&mut de)?)),
        "#info" => SubscribeReposMessage::Info(Box::new(Deserialize::deserialize(&mut de)?)),
        other => return Err(FirehoseError::UnknownType(other.to_string())),
    };

    Ok(msg)
}

#[cfg(feature = "relay")]
#[derive(serde::Serialize)]
struct EncodeHeader<'a> {
    op: i64,
    t: &'a str,
}

#[cfg(feature = "relay")]
pub fn encode_frame<T: serde::Serialize>(t: &str, body: &T) -> miette::Result<bytes::Bytes> {
    let mut buf = serde_ipld_dagcbor::to_vec(&EncodeHeader { op: 1, t })
        .map_err(|e| miette::miette!("encode_frame header: {e}"))?;
    buf.extend_from_slice(
        &serde_ipld_dagcbor::to_vec(body).map_err(|e| miette::miette!("encode_frame body: {e}"))?,
    );
    Ok(bytes::Bytes::from(buf))
}

#[cfg(test)]
mod test {
    use super::{SubscribeReposMessage, decode_frame};

    #[test]
    fn test_decode_account() {
        const FRAME: &[u8] = b"omF0aCNhY2NvdW50Ym9wAaRjZGlkeCBkaWQ6cGxjOjNuNDNncWY2YTZua3J3MzU1cnNjNnJ0ZGNzZXEbAAAAAlP5Zt5kdGltZXgbMjAyNi0wMi0yNFQxNDowNToyMC41MjE1NDY5ZmFjdGl2ZfU=";
        let bytes = data_encoding::BASE64.decode(FRAME).unwrap();
        let msg = decode_frame(&bytes).unwrap();
        assert!(matches!(msg, SubscribeReposMessage::Account(_)));
    }

    /// regression: some relays send `since: ""` (empty string) instead of null/absent for the initial commit.
    /// this should decode cleanly with `since = None`.
    /// TODO: is this behaviour we should reject?
    #[test]
    fn test_decode_commit_empty_since() {
        const FRAME: &[u8] = b"omF0ZyNjb21taXRib3ABq2NvcHOAY3Jldm0zbWdkeWNmdWIyeTIyY3NlcRsAAAACZ8sHJmRyZXBveCBkaWQ6cGxjOmpxM3p2cmI1ZXdnMnFndXA3M3Fzb3V6ZWR0aW1leBsyMDI2LTAzLTA1VDE1OjQ3OjU0LjcxNjMyOFplYmxvYnOAZXNpbmNlYGZibG9ja3NZAUk6omVyb290c4HYKlglAAFxEiAlaO/rjabPL4/e2QlxkoxzCCwv69hE4P3Vdxpv7f6uEWd2ZXJzaW9uAeABAXESICVo7+uNps8vj97ZCXGSjHMILC/r2ETg/dV3Gm/t/q4RpmNkaWR4IGRpZDpwbGM6anEzenZyYjVld2cycWd1cDczcXNvdXplY3Jldm0zbWdkeWNmdWIyeTIyY3NpZ1hAwKfrZtwwbN7dW0uSbviOs65NWQRvlS9Qc7oRtiorybMTEYxKGJaFK2kHIMEWIJqumb4751En2aJEpsilWlaQOWRkYXRh2CpYJQABcRIgnf7+Yd126j3K5QI4gLCDedV63yBILW/b4nWSifZHZ3tkcHJldvZndmVyc2lvbgMrAXESIJ3+/mHdduo9yuUCOICwg3nVet8gSC1v2+J1kon2R2d7omFlgGFs9mZjb21taXTYKlglAAFxEiAlaO/rjabPL4/e2QlxkoxzCCwv69hE4P3Vdxpv7f6uEWZyZWJhc2X0ZnRvb0JpZ/Q=";
        let bytes = data_encoding::BASE64.decode(FRAME).unwrap();
        let msg = decode_frame(&bytes).unwrap();
        let SubscribeReposMessage::Commit(c) = msg else {
            panic!("expected Commit");
        };
        assert!(c.since.is_none(), "since should be None for empty string");
    }
}
