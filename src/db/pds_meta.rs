use crate::pds_meta::HostStatus;
use fjall::{Keyspace, OwnedWriteBatch};
use miette::{IntoDiagnostic, Result};
use smol_str::SmolStr;

pub mod v5 {
    use super::*;

    // `{host}|tier` -> tier name
    pub fn pds_tier_key(host: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(host.len() + 5);
        key.extend_from_slice(host.as_bytes());
        key.extend_from_slice(b"|tier");
        key
    }

    /// load all PDS tier assignments from the filter keyspace
    pub fn load_tiers(ks: &Keyspace) -> Result<Vec<(SmolStr, SmolStr)>> {
        let mut out = Vec::new();
        for guard in ks.iter() {
            let (k, v) = guard.into_inner().into_diagnostic()?;
            if k.ends_with(b"|tier") {
                let host = std::str::from_utf8(&k[..k.len() - 5]).into_diagnostic()?;
                let tier = std::str::from_utf8(&v).into_diagnostic()?;
                out.push((SmolStr::new(host), SmolStr::new(tier)));
            }
        }
        Ok(out)
    }

    pub fn set_tier(batch: &mut OwnedWriteBatch, ks: &Keyspace, host: &str, tier: &str) {
        batch.insert(ks, pds_tier_key(host), tier.as_bytes());
    }

    pub fn remove_tier(batch: &mut OwnedWriteBatch, ks: &Keyspace, host: &str) {
        batch.remove(ks, pds_tier_key(host));
    }

    // `{host}|status` -> encoded HostStatus (msgpack)
    pub fn pds_status_key(host: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(host.len() + 7);
        key.extend_from_slice(host.as_bytes());
        key.extend_from_slice(b"|status");
        key
    }

    /// load all host statuses from the filter keyspace
    pub fn load_statuses(ks: &Keyspace) -> Result<Vec<(SmolStr, HostStatus)>> {
        let mut out = Vec::new();
        for guard in ks.iter() {
            let (k, v) = guard.into_inner().into_diagnostic()?;
            if k.ends_with(b"|status") {
                let host = std::str::from_utf8(&k[..k.len() - 7]).into_diagnostic()?;
                let status: HostStatus = rmp_serde::from_slice(&v).into_diagnostic()?;
                out.push((SmolStr::new(host), status));
            }
        }
        Ok(out)
    }

    pub fn set_status(
        batch: &mut OwnedWriteBatch,
        ks: &Keyspace,
        host: &str,
        status: HostStatus,
    ) -> Result<()> {
        let bytes = rmp_serde::to_vec(&status).into_diagnostic()?;
        batch.insert(ks, pds_status_key(host), bytes);
        Ok(())
    }

    pub fn remove_status(batch: &mut OwnedWriteBatch, ks: &Keyspace, host: &str) {
        batch.remove(ks, pds_status_key(host));
    }
}

pub use v5::*;
