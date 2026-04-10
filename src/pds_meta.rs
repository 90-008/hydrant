use crate::config::RateTier;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum HostStatus {
    Active,
    Idle,
    Offline,
    Throttled,
    Banned,
}

impl From<HostStatus> for jacquard_api::com_atproto::sync::HostStatus<'static> {
    fn from(status: HostStatus) -> Self {
        match status {
            HostStatus::Active => Self::Active,
            HostStatus::Idle => Self::Idle,
            HostStatus::Offline => Self::Offline,
            HostStatus::Throttled => Self::Throttled,
            HostStatus::Banned => Self::Banned,
        }
    }
}

impl HostStatus {
    /// returns the new status to apply if the count dynamically crossed limits.
    pub fn check_limit_transition(
        self,
        current_count: u64,
        account_limit: Option<u64>,
    ) -> Option<Self> {
        if self == Self::Banned {
            return None;
        }
        match account_limit {
            Some(limit) if current_count >= limit && self != Self::Throttled => {
                Some(Self::Throttled)
            }
            Some(limit) if current_count < limit && self == Self::Throttled => Some(Self::Active),
            None if self == Self::Throttled => Some(Self::Active),
            _ => None,
        }
    }
}

impl Default for HostStatus {
    fn default() -> Self {
        Self::Active
    }
}

#[derive(Debug, Default, Clone)]
pub struct HostDesc {
    pub tier: Option<SmolStr>,
    pub status: HostStatus,
}

#[derive(Default, Clone)]
pub(crate) struct PdsMeta {
    pub hosts: HashMap<String, HostDesc>,
}

impl PdsMeta {
    /// update (or insert) the `HostDesc` for `host` by applying `f` to it.
    pub fn update_host_entry(&mut self, host: &str, f: impl FnOnce(&mut HostDesc)) {
        f(self.hosts.entry(host.to_string()).or_default());
    }

    /// atomically update (or insert) the `HostDesc` for `host` by applying `f` to it via RCU.
    pub(crate) fn update_host(
        cell: &arc_swap::ArcSwap<Self>,
        host: &str,
        mut f: impl FnMut(&mut HostDesc),
    ) {
        cell.rcu(|meta| {
            let mut next = (**meta).clone();
            next.update_host_entry(host, &mut f);
            next
        });
    }
}

impl PdsMeta {
    pub fn tier_for(&self, host: &str, rate_tiers: &HashMap<String, RateTier>) -> RateTier {
        let default = rate_tiers
            .get("default")
            .copied()
            .unwrap_or_else(RateTier::default_tier);
        self.hosts
            .get(host)
            .and_then(|h| h.tier.as_ref())
            .and_then(|name| rate_tiers.get(name.as_str()).copied())
            .unwrap_or(default)
    }

    pub fn status(&self, host: &str) -> HostStatus {
        self.hosts
            .get(host)
            .map(|h| h.status)
            .unwrap_or(HostStatus::Active)
    }

    pub fn is_banned(&self, host: &str) -> bool {
        self.status(host) == HostStatus::Banned
    }
}

pub(crate) type PdsMetaHandle = Arc<ArcSwap<PdsMeta>>;

pub(crate) fn new_handle(meta: PdsMeta) -> PdsMetaHandle {
    Arc::new(ArcSwap::new(Arc::new(meta)))
}
