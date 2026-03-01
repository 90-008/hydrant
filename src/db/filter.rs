use fjall::{Keyspace, OwnedWriteBatch};
use miette::{IntoDiagnostic, Result};

use crate::filter::{FilterConfig, FilterMode, SetUpdate};

pub const MODE_KEY: &[u8] = b"m";
pub const SIGNAL_PREFIX: u8 = b's';
pub const COLLECTION_PREFIX: u8 = b'c';
pub const EXCLUDE_PREFIX: u8 = b'x';
pub const SEP: u8 = b'|';

pub fn filter_key(prefix: u8, val: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + val.len());
    key.push(prefix);
    key.push(SEP);
    key.extend_from_slice(val.as_bytes());
    key
}

pub fn apply_patch(
    batch: &mut OwnedWriteBatch,
    ks: &Keyspace,
    mode: Option<FilterMode>,
    signals: Option<SetUpdate>,
    collections: Option<SetUpdate>,
    excludes: Option<SetUpdate>,
) -> Result<()> {
    if let Some(mode) = mode {
        batch.insert(ks, MODE_KEY, rmp_serde::to_vec(&mode).into_diagnostic()?);
    }

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
    let mode = ks
        .get(MODE_KEY)
        .into_diagnostic()?
        .map(|v| rmp_serde::from_slice(&v).into_diagnostic())
        .transpose()?
        .unwrap_or(FilterMode::Filter);

    let mut config = FilterConfig::new(mode);

    let signal_prefix = [SIGNAL_PREFIX, SEP];
    for guard in ks.prefix(signal_prefix) {
        let (k, _) = guard.into_inner().into_diagnostic()?;
        let val = std::str::from_utf8(&k[signal_prefix.len()..]).into_diagnostic()?;
        config.signals.push(smol_str::SmolStr::new(val));
    }

    let col_prefix = [COLLECTION_PREFIX, SEP];
    for guard in ks.prefix(col_prefix) {
        let (k, _) = guard.into_inner().into_diagnostic()?;
        let val = std::str::from_utf8(&k[col_prefix.len()..]).into_diagnostic()?;
        config.collections.push(smol_str::SmolStr::new(val));
    }

    Ok(config)
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
