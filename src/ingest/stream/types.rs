use bytes::Bytes;
use jacquard_common::{
    CowStr,
    types::{
        cid::CidLink,
        string::{Did, Handle, Tid},
    },
};
use serde::{Deserialize, Serialize};

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

// some relays send `""` for `since` when there is no previous revision instead of null
pub(crate) fn deserialize_tid_or_empty<'de, D>(deserializer: D) -> Result<Option<Tid>, D::Error>
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
        Some(s) => s.parse().map(Some).map_err(serde::de::Error::custom),
    }
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
    #[serde(default)]
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

#[derive(Deserialize, Serialize, Debug, Clone, jacquard_derive::IntoStatic)]
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
