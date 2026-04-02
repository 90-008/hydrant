use crate::config::RateTier;
use arc_swap::ArcSwap;
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Default, Clone)]
pub(crate) struct PdsMeta {
    pub tiers: HashMap<String, SmolStr>,
    pub banned: HashSet<String>,
}

impl PdsMeta {
    pub fn tier_for(&self, host: &str, rate_tiers: &HashMap<String, RateTier>) -> RateTier {
        let default = rate_tiers
            .get("default")
            .copied()
            .unwrap_or_else(RateTier::default_tier);
        self.tiers
            .get(host)
            .and_then(|name| rate_tiers.get(name.as_str()).copied())
            .unwrap_or(default)
    }

    pub fn is_banned(&self, host: &str) -> bool {
        self.banned.contains(host)
    }
}

pub(crate) type PdsMetaHandle = Arc<ArcSwap<PdsMeta>>;

pub(crate) fn new_handle(meta: PdsMeta) -> PdsMetaHandle {
    Arc::new(ArcSwap::new(Arc::new(meta)))
}
