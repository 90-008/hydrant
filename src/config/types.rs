use std::fmt;
use std::str::FromStr;
use url::Url;
use serde::{Deserialize, Serialize};
use smol_str::ToSmolStr;
use miette::Result;

/// rate limit parameters for a named tier of PDS connections.
///
/// the per-second limit is `max(per_second_base, accounts * per_second_account_mul)`,
/// giving a floor at `per_second_base` that scales up with the PDS's active account count.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateTier {
    /// floor for the per-second limit, regardless of account count.
    pub per_second_base: u64,
    /// per-second events allowed per active account on this PDS.
    pub per_second_account_mul: f64,
    /// per-hour limit.
    pub per_hour: u64,
    /// per-day limit.
    pub per_day: u64,
    /// maximum active account limit for this host before dropping tracking of new accounts
    pub account_limit: Option<u64>,
}

impl RateTier {
    /// built-in "trusted" tier: high limits for well-behaved PDS operators.
    pub fn trusted() -> Self {
        Self {
            per_second_base: 5000,
            per_second_account_mul: 10.0,
            per_hour: 5000 * 3600,
            per_day: 5000 * 86400,
            account_limit: Some(10_000_000),
        }
    }

    /// built-in "default" tier: conservative limits for unknown PDS operators.
    pub fn default_tier() -> Self {
        Self {
            per_second_base: 50,
            per_second_account_mul: 0.5,
            per_hour: 1000 * 3600,
            per_day: 1000 * 86400,
            account_limit: Some(100),
        }
    }

    /// parse `base/mul/hourly/daily[/account_limit]` format used by `HYDRANT_RATE_TIERS`.
    pub(crate) fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() < 4 || parts.len() > 5 {
            return None;
        }
        Some(Self {
            per_second_base: parts[0].parse().ok()?,
            per_second_account_mul: parts[1].parse().ok()?,
            per_hour: parts[2].parse().ok()?,
            per_day: parts[3].parse().ok()?,
            account_limit: parts.get(4).and_then(|p| p.parse().ok()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrawlerMode {
    /// enumerate via `com.atproto.sync.listRepos`, check signals with `describeRepo`.
    ListRepos,
    /// enumerate via `com.atproto.sync.listReposByCollection` for each configured signal.
    /// note: if no signals are specified, this won't crawl for any repos.
    ByCollection,
}

impl CrawlerMode {
    pub(crate) fn default_for(full_network: bool) -> Self {
        full_network
            .then_some(Self::ListRepos)
            .unwrap_or(Self::ByCollection)
    }
}

impl Serialize for CrawlerMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_smolstr())
    }
}

impl<'de> Deserialize<'de> for CrawlerMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl FromStr for CrawlerMode {
    type Err = miette::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "list_repos" | "list-repos" => Ok(Self::ListRepos),
            "by_collection" | "by-collection" => Ok(Self::ByCollection),
            _ => Err(miette::miette!(
                "invalid crawler mode: expected 'list_repos' or 'by_collection'"
            )),
        }
    }
}

impl fmt::Display for CrawlerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ListRepos => write!(f, "list_repos"),
            Self::ByCollection => write!(f, "by_collection"),
        }
    }
}

/// a single crawler source: a URL and the mode used to enumerate it.
#[derive(Debug, Clone)]
pub struct CrawlerSource {
    pub url: Url,
    pub mode: CrawlerMode,
}

impl CrawlerSource {
    /// parse `[mode::]url`. mode prefix is optional, falls back to `default_mode`.
    pub(crate) fn parse(s: &str, default_mode: CrawlerMode) -> Option<Self> {
        if let Some((prefix, rest)) = s.split_once("::") {
            let mode = prefix.parse().ok()?;
            let url = Url::parse(rest).ok()?;
            Some(Self { url, mode })
        } else {
            let url = Url::parse(s).ok()?;
            Some(Self {
                url,
                mode: default_mode,
            })
        }
    }
}

/// a single firehose source: a URL and whether it is a direct PDS connection.
///
/// set via `HYDRANT_RELAY_HOSTS` as a comma-separated list of `[pds::]url` entries.
/// e.g. `wss://bsky.network,pds::wss://pds.example.com`.
/// a bare URL (no `pds::` prefix) is treated as an aggregating relay (`is_pds = false`).
#[derive(Debug, Clone)]
pub struct FirehoseSource {
    pub url: Url,
    /// true when this is a direct PDS connection; enables host authority enforcement.
    pub is_pds: bool,
}

impl FirehoseSource {
    /// parse `[pds::]url`. the `pds::` prefix marks the source as a direct PDS connection.
    pub fn parse(s: &str) -> Option<Self> {
        if let Some(url_str) = s.strip_prefix("pds::") {
            let url = Url::parse(url_str).ok()?;
            Some(Self { url, is_pds: true })
        } else {
            let url = Url::parse(s).ok()?;
            Some(Self { url, is_pds: false })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Lz4,
    Zstd,
    None,
}

impl FromStr for Compression {
    type Err = miette::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            "none" => Ok(Self::None),
            _ => Err(miette::miette!("invalid compression type")),
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
            Self::None => write!(f, "none"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureVerification {
    /// verify all commits, from the firehose and when backfilling a repo from a PDS.
    Full,
    /// only verify commits when backfilling a repo from a PDS.
    BackfillOnly,
    /// don't verify anything.
    None,
}

impl FromStr for SignatureVerification {
    type Err = miette::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "full" => Ok(Self::Full),
            "backfill-only" => Ok(Self::BackfillOnly),
            "none" => Ok(Self::None),
            _ => Err(miette::miette!("invalid signature verification level")),
        }
    }
}

impl fmt::Display for SignatureVerification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::BackfillOnly => write!(f, "backfill-only"),
            Self::None => write!(f, "none"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillStrategy {
    /// always fetch full repo cars with `com.atproto.sync.getRepo`.
    Full,
    /// use sparse collection backfill when possible, falling back to full repo cars.
    SparseFilter,
    /// choose sparse collection backfill for filtered repos unless the seed proof suggests
    /// the repo is small enough for full `getRepo` to be cheaper.
    Auto,
}

impl FromStr for BackfillStrategy {
    type Err = miette::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "full" => Ok(Self::Full),
            "sparse-filter" => Ok(Self::SparseFilter),
            "auto" => Ok(Self::Auto),
            _ => Err(miette::miette!("invalid backfill strategy")),
        }
    }
}

impl fmt::Display for BackfillStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::SparseFilter => write!(f, "sparse-filter"),
            Self::Auto => write!(f, "auto"),
        }
    }
}
