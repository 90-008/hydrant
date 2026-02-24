use std::sync::Arc;

use arc_swap::ArcSwap;
use fjall::Keyspace;
use miette::{IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

pub const MODE_KEY: &[u8] = b"m";
pub const DID_PREFIX: u8 = b'd';
pub const SIGNAL_PREFIX: u8 = b's';
pub const COLLECTION_PREFIX: u8 = b'c';
pub const EXCLUDE_PREFIX: u8 = b'x';
pub const SEP: u8 = b'|';

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    Dids = 0,
    Signal = 1,
    Full = 2,
}

/// hot-path in-memory config: only the small fields needed on every event.
/// dids and excludes are large sets kept in the filter keyspace only.
#[derive(Debug, Clone, Serialize)]
pub struct FilterConfig {
    pub mode: FilterMode,
    pub signals: Vec<SmolStr>,
    pub collections: Vec<SmolStr>,
}

impl FilterConfig {
    pub fn new(mode: FilterMode) -> Self {
        Self {
            mode,
            signals: Vec::new(),
            collections: Vec::new(),
        }
    }

    pub fn load(ks: &Keyspace) -> Result<Self> {
        let mode = ks
            .get(MODE_KEY)
            .into_diagnostic()?
            .map(|v| rmp_serde::from_slice(&v).into_diagnostic())
            .transpose()?
            .unwrap_or(FilterMode::Dids);

        let mut config = Self::new(mode);

        let signal_prefix = [SIGNAL_PREFIX, SEP];
        for guard in ks.prefix(signal_prefix) {
            let (k, _) = guard.into_inner().into_diagnostic()?;
            let val = std::str::from_utf8(&k[signal_prefix.len()..]).into_diagnostic()?;
            config.signals.push(SmolStr::new(val));
        }

        let col_prefix = [COLLECTION_PREFIX, SEP];
        for guard in ks.prefix(col_prefix) {
            let (k, _) = guard.into_inner().into_diagnostic()?;
            let val = std::str::from_utf8(&k[col_prefix.len()..]).into_diagnostic()?;
            config.collections.push(SmolStr::new(val));
        }

        Ok(config)
    }

    /// returns true if the collection matches the content filter.
    /// if collections is empty, all collections match.
    pub fn matches_collection(&self, collection: &str) -> bool {
        if self.collections.is_empty() {
            return true;
        }
        self.collections.iter().any(|p| nsid_matches(p, collection))
    }

    /// returns true if the commit touches a collection covered by a signal.
    pub fn matches_signal(&self, collection: &str) -> bool {
        self.signals.iter().any(|p| nsid_matches(p, collection))
    }
}

fn nsid_matches(pattern: &str, collection: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix(".*") {
        collection == prefix || collection.starts_with(prefix)
    } else {
        collection == pattern
    }
}

pub type FilterHandle = Arc<ArcSwap<FilterConfig>>;

pub fn new_handle(config: FilterConfig) -> FilterHandle {
    Arc::new(ArcSwap::new(Arc::new(config)))
}

/// apply a bool patch or set replacement for a single set update.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SetUpdate {
    /// replace the entire set with this list
    Set(Vec<String>),
    /// patch: true = add, false = remove
    Patch(std::collections::HashMap<String, bool>),
}

pub fn filter_key(prefix: u8, val: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + val.len());
    key.push(prefix);
    key.push(SEP);
    key.extend_from_slice(val.as_bytes());
    key
}
