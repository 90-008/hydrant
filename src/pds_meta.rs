use crate::config::RateTier;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

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

#[derive(Debug, Clone)]
pub struct TierRule {
    pub pattern: glob::Pattern,
    pub tier_name: SmolStr,
}

/// policy for resolving rate tiers for PDS hosts.
///
/// resolution order:
/// 1. explicit api-assigned override for the host (stored in `HostDesc.tier`)
/// 2. first matching rule in `rules` (config glob patterns, in order)
/// 3. built-in `"default"` tier
#[derive(Debug, Clone)]
pub struct TierPolicy {
    /// named rate tier definitions.
    pub tiers: HashMap<SmolStr, RateTier>,
    /// ordered glob rules, first match wins (for unassigned hosts).
    pub rules: Vec<TierRule>,
}

impl TierPolicy {
    /// resolves the effective `RateTier` for `host`.
    ///
    /// `override_name` is the api-assigned tier name from `HostDesc.tier`, if any.
    pub fn resolve(&self, host: &str, override_name: Option<&SmolStr>) -> RateTier {
        let default = self
            .tiers
            .get("default")
            .copied()
            .unwrap_or_else(RateTier::default_tier);

        if let Some(name) = override_name {
            let tier = self.tiers.get(name).copied().unwrap_or(default);
            debug!(host, override = %name, account_limit = ?tier.account_limit, "tier resolved via explicit override");
            return tier;
        }

        let matched = self.rules.iter().find(|r| r.pattern.matches(host));

        let tier = matched
            .and_then(|r| self.tiers.get(&r.tier_name).copied())
            .unwrap_or(default);

        debug!(
            host,
            matched_rule = matched.map(|r| format!("{}:{}", r.pattern, r.tier_name)).as_deref(),
            account_limit = ?tier.account_limit,
            "tier resolved via glob rules"
        );
        tier
    }
}
