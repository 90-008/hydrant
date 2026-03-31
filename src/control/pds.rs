use std::collections::HashMap;
use std::sync::Arc;

use miette::{IntoDiagnostic, Result};
use serde::Serialize;
use smol_str::SmolStr;

use crate::config::RateTier;
use crate::db::pds_tiers as db_pds;
use crate::state::AppState;

/// a single PDS-to-tier assignment.
#[derive(Debug, Clone, Serialize)]
pub struct PdsTierAssignment {
    pub host: String,
    pub tier: String,
}

/// a rate tier definition, as returned by the API.
#[derive(Debug, Clone, Serialize)]
pub struct PdsTierDefinition {
    pub per_second_base: u64,
    pub per_second_account_mul: f64,
    pub per_hour: u64,
    pub per_day: u64,
}

impl From<RateTier> for PdsTierDefinition {
    fn from(t: RateTier) -> Self {
        Self {
            per_second_base: t.per_second_base,
            per_second_account_mul: t.per_second_account_mul,
            per_hour: t.per_hour,
            per_day: t.per_day,
        }
    }
}

/// runtime control over pds related behaviour (eg. ratelimits).
#[derive(Clone)]
pub struct PdsControl(pub(super) Arc<AppState>);

impl PdsControl {
    /// list all current per-PDS tier assignments.
    pub async fn list_assignments(&self) -> Vec<PdsTierAssignment> {
        let snapshot = self.0.pds_tiers.load();
        snapshot
            .iter()
            .map(|(host, tier)| PdsTierAssignment {
                host: host.clone(),
                tier: tier.to_string(),
            })
            .collect()
    }

    /// list all configured rate tier definitions.
    pub fn list_rate_tiers(&self) -> HashMap<String, PdsTierDefinition> {
        self.0
            .rate_tiers
            .iter()
            .map(|(name, tier)| (name.clone(), PdsTierDefinition::from(*tier)))
            .collect()
    }

    /// assign `host` to `tier`, persisting the change to the database.
    /// returns an error if `tier` is not a known tier name.
    pub async fn set_tier(&self, host: String, tier: String) -> Result<()> {
        if !self.0.rate_tiers.contains_key(&tier) {
            miette::bail!(
                "unknown tier '{tier}'; known tiers: {:?}",
                self.0.rate_tiers.keys().collect::<Vec<_>>()
            );
        }

        let state = self.0.clone();
        let host_clone = host.clone();
        let tier_clone = tier.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = state.db.inner.batch();
            db_pds::set(&mut batch, &state.db.filter, &host_clone, &tier_clone);
            batch.commit().into_diagnostic()?;
            state.db.persist()
        })
        .await
        .into_diagnostic()??;

        let mut snapshot = (**self.0.pds_tiers.load()).clone();
        snapshot.insert(host, SmolStr::new(&tier));
        self.0.pds_tiers.store(Arc::new(snapshot));

        Ok(())
    }

    /// remove any explicit tier assignment for `host`, reverting it to the default tier.
    pub async fn remove_tier(&self, host: String) -> Result<()> {
        let state = self.0.clone();
        let host_clone = host.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = state.db.inner.batch();
            db_pds::remove(&mut batch, &state.db.filter, &host_clone);
            batch.commit().into_diagnostic()?;
            state.db.persist()
        })
        .await
        .into_diagnostic()??;

        let mut snapshot = (**self.0.pds_tiers.load()).clone();
        snapshot.remove(&host);
        self.0.pds_tiers.store(Arc::new(snapshot));

        Ok(())
    }
}
