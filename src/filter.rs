use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::sync::Arc;

pub(crate) type FilterHandle = Arc<arc_swap::ArcSwap<FilterConfig>>;

pub(crate) fn new_handle(config: FilterConfig) -> FilterHandle {
    Arc::new(arc_swap::ArcSwap::new(Arc::new(config)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    Filter = 0,
    Full = 2,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FilterConfig {
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
}

#[cfg(feature = "indexer")]
mod indexer {
    use super::*;

    impl FilterConfig {
        pub fn matches_collection(&self, collection: &str) -> bool {
            if self.collections.is_empty() {
                return true;
            }
            self.collections.iter().any(|p| nsid_matches(p, collection))
        }

        pub fn matches_signal(&self, collection: &str) -> bool {
            self.signals.iter().any(|p| nsid_matches(p, collection))
        }

        pub fn check_signals(&self) -> bool {
            self.mode == FilterMode::Filter && !self.signals.is_empty()
        }
    }

    fn nsid_matches(pattern: &str, col: &str) -> bool {
        pattern
            .strip_suffix(".*")
            .map(|prefix| col == prefix || col.starts_with(prefix))
            .unwrap_or_else(|| col == pattern)
    }
}
