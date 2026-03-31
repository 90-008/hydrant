use fjall::{Keyspace, OwnedWriteBatch};
use miette::{IntoDiagnostic, Result};
use smol_str::SmolStr;

pub const PDS_TIER_PREFIX: &[u8] = b"pt|";

// `pt|{host}` -> tier name
pub fn pds_tier_key(host: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(PDS_TIER_PREFIX.len() + host.len());
    key.extend_from_slice(PDS_TIER_PREFIX);
    key.extend_from_slice(host.as_bytes());
    key
}

/// load all PDS tier assignments from the filter keyspace
pub fn load(ks: &Keyspace) -> Result<Vec<(SmolStr, SmolStr)>> {
    let mut out = Vec::new();
    for guard in ks.prefix(PDS_TIER_PREFIX) {
        let (k, v) = guard.into_inner().into_diagnostic()?;
        let host = std::str::from_utf8(&k[PDS_TIER_PREFIX.len()..]).into_diagnostic()?;
        let tier = std::str::from_utf8(&v).into_diagnostic()?;
        out.push((SmolStr::new(host), SmolStr::new(tier)));
    }
    Ok(out)
}

pub fn set(batch: &mut OwnedWriteBatch, ks: &Keyspace, host: &str, tier: &str) {
    batch.insert(ks, pds_tier_key(host), tier.as_bytes());
}

pub fn remove(batch: &mut OwnedWriteBatch, ks: &Keyspace, host: &str) {
    batch.remove(ks, pds_tier_key(host));
}
