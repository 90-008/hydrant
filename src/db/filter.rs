use fjall::{Keyspace, OwnedWriteBatch};
use miette::{IntoDiagnostic, Result};

use crate::db::types::TrimmedDid;
use crate::filter::{FilterConfig, FilterMode, SetUpdate};
use jacquard_common::types::string::Did;

pub const MODE_KEY: &[u8] = b"m";
pub const SIGNAL_PREFIX: u8 = b's';
pub const COLLECTION_PREFIX: u8 = b'c';
pub const EXCLUDE_PREFIX: u8 = b'x';
pub const SEP: u8 = b'|';

pub fn signal_key(val: &str) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(2 + val.len());
    key.push(SIGNAL_PREFIX);
    key.push(SEP);
    key.extend_from_slice(val.as_bytes());
    Ok(key)
}

pub fn collection_key(val: &str) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(2 + val.len());
    key.push(COLLECTION_PREFIX);
    key.push(SEP);
    key.extend_from_slice(val.as_bytes());
    Ok(key)
}

pub fn exclude_key(val: &str) -> Result<Vec<u8>> {
    let did = Did::new(val).into_diagnostic()?;
    let trimmed = TrimmedDid::from(&did);
    let mut key = Vec::with_capacity(2 + trimmed.len());
    key.push(EXCLUDE_PREFIX);
    key.push(SEP);
    trimmed.write_to_vec(&mut key);
    Ok(key)
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

    let key_fn = match prefix {
        SIGNAL_PREFIX => signal_key,
        COLLECTION_PREFIX => collection_key,
        EXCLUDE_PREFIX => exclude_key,
        _ => unreachable!(),
    };

    match update {
        SetUpdate::Set(values) => {
            let scan_prefix = [prefix, SEP];
            for guard in ks.prefix(scan_prefix) {
                let (k, _) = guard.into_inner().into_diagnostic()?;
                batch.remove(ks, k);
            }
            for val in values {
                batch.insert(ks, key_fn(&val)?, []);
            }
        }
        SetUpdate::Patch(map) => {
            for (val, add) in map {
                let key = key_fn(&val)?;
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
        let val_bytes = &k[2..];
        let val = if prefix == EXCLUDE_PREFIX {
            TrimmedDid::try_from(val_bytes)?.to_did().to_string()
        } else {
            std::str::from_utf8(val_bytes).into_diagnostic()?.to_owned()
        };
        out.push(val);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_keys() {
        assert_eq!(
            signal_key("app.bsky.feed.like").unwrap(),
            b"s|app.bsky.feed.like"
        );
        assert_eq!(
            collection_key("app.bsky.feed.post").unwrap(),
            b"c|app.bsky.feed.post"
        );
    }

    #[test]
    fn test_exclude_key_trimmed() {
        let did = "did:plc:yk4q3id7id6p5z3bypvshc64";
        let key = exclude_key(did).unwrap();
        assert_eq!(key[0], EXCLUDE_PREFIX);
        assert_eq!(key[1], SEP);
        // TAG_PLC (1) + 15 bytes
        assert_eq!(key.len(), 2 + 1 + 15);

        let parsed = TrimmedDid::try_from(&key[2..]).unwrap();
        assert_eq!(parsed.to_did().as_str(), did);
    }

    #[test]
    fn test_apply_and_load() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let keyspace = fjall::Database::builder(tmp.path())
            .open()
            .into_diagnostic()?;
        let ks = keyspace
            .keyspace("filter", Default::default)
            .into_diagnostic()?;

        let mut batch = keyspace.batch();
        let signals = SetUpdate::Set(vec!["a.b.c".to_string()]);
        let collections = SetUpdate::Set(vec!["d.e.f".to_string()]);
        let excludes = SetUpdate::Set(vec!["did:plc:yk4q3id7id6p5z3bypvshc64".to_string()]);

        apply_patch(
            &mut batch,
            &ks,
            Some(FilterMode::Filter),
            Some(signals),
            Some(collections),
            Some(excludes),
        )?;
        batch.commit().into_diagnostic()?;

        let config = load(&ks)?;
        assert_eq!(config.mode, FilterMode::Filter);
        assert_eq!(config.signals, vec!["a.b.c"]);
        assert_eq!(config.collections, vec!["d.e.f"]);

        let excludes = read_set(&ks, EXCLUDE_PREFIX)?;
        assert_eq!(excludes, vec!["did:plc:yk4q3id7id6p5z3bypvshc64"]);

        Ok(())
    }
}
