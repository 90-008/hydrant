use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::sync::Arc;

pub type FilterHandle = Arc<arc_swap::ArcSwap<FilterConfig>>;

pub fn new_handle(config: FilterConfig) -> FilterHandle {
    Arc::new(arc_swap::ArcSwap::new(Arc::new(config)))
}

/// apply a bool patch or set replacement for a single set update.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SetUpdate {
    /// replace the entire set with this list
    Set(Vec<String>),
    /// patch: true = add, false = remove
    Patch(std::collections::HashMap<String, bool>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    Filter = 0,
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
