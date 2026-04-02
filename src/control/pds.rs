use std::collections::HashMap;
use std::sync::Arc;

use miette::{IntoDiagnostic, Result};
use serde::Serialize;
use smol_str::SmolStr;

use crate::config::RateTier;
use crate::db::pds_meta as db_pds;
use crate::pds_meta::PdsMeta;
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
    async fn update<F, G>(&self, db_op: F, mem_op: G) -> Result<()>
    where
        F: FnOnce(&mut fjall::OwnedWriteBatch, &fjall::Keyspace) + Send + 'static,
        G: FnOnce(&mut PdsMeta),
    {
        let state = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = state.db.inner.batch();
            db_op(&mut batch, &state.db.filter);
            batch.commit().into_diagnostic()?;
            state.db.persist()
        })
        .await
        .into_diagnostic()??;

        let mut snapshot = (**self.0.pds_meta.load()).clone();
        mem_op(&mut snapshot);
        self.0.pds_meta.store(Arc::new(snapshot));

        Ok(())
    }

    /// list all current per-PDS tier assignments.
    pub async fn list_tiers(&self) -> HashMap<String, String> {
        let snapshot = self.0.pds_meta.load();
        snapshot
            .tiers
            .iter()
            .map(|(host, tier)| (host.clone(), tier.to_string()))
            .collect()
    }

    /// returns the assigned tier for `host`, or "default" if none is assigned.
    pub fn get_tier(&self, host: impl AsRef<str>) -> String {
        let snapshot = self.0.pds_meta.load();
        snapshot
            .tiers
            .get(host.as_ref())
            .map(|t| t.to_string())
            .unwrap_or_else(|| "default".to_string())
    }

    /// returns true if `host` is currently banned.
    pub fn is_banned(&self, host: impl AsRef<str>) -> bool {
        self.0.pds_meta.load().is_banned(host.as_ref())
    }

    /// list all currently banned PDS hosts.
    pub async fn list_banned(&self) -> Vec<String> {
        let snapshot = self.0.pds_meta.load();
        snapshot.banned.iter().cloned().collect()
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
    pub async fn set_tier(&self, host: impl AsRef<str>, tier: String) -> Result<()> {
        if !self.0.rate_tiers.contains_key(&tier) {
            miette::bail!(
                "unknown tier '{tier}'; known tiers: {:?}",
                self.0.rate_tiers.keys().collect::<Vec<_>>()
            );
        }

        let host = host.as_ref().to_string();
        let host_clone = host.clone();
        let tier_clone = tier.clone();
        self.update(
            move |batch, ks| db_pds::set_tier(batch, ks, &host_clone, &tier_clone),
            move |meta| {
                meta.tiers.insert(host, SmolStr::new(&tier));
            },
        )
        .await
    }

    /// remove any explicit tier assignment for `host`, reverting it to the default tier.
    pub async fn remove_tier(&self, host: impl AsRef<str>) -> Result<()> {
        let host = host.as_ref().to_string();
        let host_clone = host.clone();
        self.update(
            move |batch, ks| db_pds::remove_tier(batch, ks, &host_clone),
            move |meta| {
                meta.tiers.remove(&host);
            },
        )
        .await
    }

    /// ban `host`, persisting the change to the database.
    pub async fn ban(&self, host: impl AsRef<str>) -> Result<()> {
        let host = host.as_ref().to_string();
        let host_clone = host.clone();
        self.update(
            move |batch, ks| db_pds::set_banned(batch, ks, &host_clone),
            move |meta| {
                meta.banned.insert(host);
            },
        )
        .await
    }

    /// unban `host`, removing it from the database.
    pub async fn unban(&self, host: impl AsRef<str>) -> Result<()> {
        let host = host.as_ref().to_string();
        let host_clone = host.clone();
        self.update(
            move |batch, ks| db_pds::remove_banned(batch, ks, &host_clone),
            move |meta| {
                meta.banned.remove(&host);
            },
        )
        .await
    }
}
