use fjall::{Keyspace, OwnedWriteBatch};
use miette::{IntoDiagnostic, Result};

use crate::filter::{
    COLLECTION_PREFIX, DID_PREFIX, EXCLUDE_PREFIX, FilterConfig, FilterMode, MODE_KEY, SEP,
    SIGNAL_PREFIX, SetUpdate, filter_key,
};

pub fn apply_patch(
    batch: &mut OwnedWriteBatch,
    ks: &Keyspace,
    mode: Option<FilterMode>,
    dids: Option<SetUpdate>,
    signals: Option<SetUpdate>,
    collections: Option<SetUpdate>,
    excludes: Option<SetUpdate>,
) -> Result<()> {
    if let Some(mode) = mode {
        batch.insert(ks, MODE_KEY, rmp_serde::to_vec(&mode).into_diagnostic()?);
    }

    apply_set_update(batch, ks, DID_PREFIX, dids)?;
    apply_set_update(batch, ks, SIGNAL_PREFIX, signals)?;
    apply_set_update(batch, ks, COLLECTION_PREFIX, collections)?;
    apply_set_update(batch, ks, EXCLUDE_PREFIX, excludes)?;

    Ok(())
}

fn apply_set_update(
    batch: &mut OwnedWriteBatch,
    ks: &Keyspace,
    prefix: u8,
    update: Option<SetUpdate>,
) -> Result<()> {
    let Some(update) = update else { return Ok(()) };

    match update {
        SetUpdate::Set(values) => {
            let scan_prefix = [prefix, SEP];
            for guard in ks.prefix(scan_prefix) {
                let (k, _) = guard.into_inner().into_diagnostic()?;
                batch.remove(ks, k);
            }
            for val in values {
                batch.insert(ks, filter_key(prefix, &val), []);
            }
        }
        SetUpdate::Patch(map) => {
            for (val, add) in map {
                let key = filter_key(prefix, &val);
                if add {
                    batch.insert(ks, key, []);
                } else {
                    batch.remove(ks, key);
                }
            }
        }
    }

    Ok(())
}

pub fn load(ks: &Keyspace) -> Result<FilterConfig> {
    FilterConfig::load(ks)
}

pub fn read_set(ks: &Keyspace, prefix: u8) -> Result<Vec<String>> {
    let scan_prefix = [prefix, SEP];
    let mut out = Vec::new();
    for guard in ks.prefix(scan_prefix) {
        let (k, _) = guard.into_inner().into_diagnostic()?;
        let val = std::str::from_utf8(&k[2..]).into_diagnostic()?.to_owned();
        out.push(val);
    }
    Ok(out)
}
