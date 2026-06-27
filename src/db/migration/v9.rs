use fjall::OwnedWriteBatch;
use miette::Result;

use crate::db::Db;

#[cfg(feature = "indexer")]
use {
    crate::db::types::TrimmedDid,
    jacquard_common::types::did::Did,
    miette::IntoDiagnostic,
};

#[cfg(feature = "indexer")]
pub(crate) fn migrate_v9(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    // 1. Migrate excludes
    let exclude_prefix = [crate::db::filter::EXCLUDE_PREFIX, crate::db::filter::SEP];
    for guard in db.filter.prefix(exclude_prefix) {
        let (k, _) = guard.into_inner().into_diagnostic()?;
        let val_bytes = &k[exclude_prefix.len()..];
        if let Ok(s) = std::str::from_utf8(val_bytes) {
            if s.starts_with("did:") {
                let did = Did::new(s).into_diagnostic()?;
                let trimmed = TrimmedDid::from(&did);
                let mut new_key = Vec::with_capacity(2 + trimmed.len());
                new_key.push(crate::db::filter::EXCLUDE_PREFIX);
                new_key.push(crate::db::filter::SEP);
                trimmed.write_to_vec(&mut new_key);

                batch.insert(&db.filter, new_key, []);
                batch.remove(&db.filter, k);
            }
        }
    }

    // 2. Migrate PDS status and tier keys
    for guard in db.filter.iter() {
        let (k, v) = guard.into_inner().into_diagnostic()?;
        if k.ends_with(b"|status") && !k.starts_with(b"pds|status|") {
            let host_bytes = &k[..k.len() - 7];
            if let Ok(host) = std::str::from_utf8(host_bytes) {
                let mut new_key = Vec::with_capacity(11 + host.len());
                new_key.extend_from_slice(b"pds|status|");
                new_key.extend_from_slice(host.as_bytes());
                batch.insert(&db.filter, new_key, v);
                batch.remove(&db.filter, k);
            }
        } else if k.ends_with(b"|tier") && !k.starts_with(b"pds|tier|") {
            let host_bytes = &k[..k.len() - 5];
            if let Ok(host) = std::str::from_utf8(host_bytes) {
                let mut new_key = Vec::with_capacity(9 + host.len());
                new_key.extend_from_slice(b"pds|tier|");
                new_key.extend_from_slice(host.as_bytes());
                batch.insert(&db.filter, new_key, v);
                batch.remove(&db.filter, k);
            }
        }
    }

    Ok(())
}

#[cfg(not(feature = "indexer"))]
pub(crate) fn migrate_v9(_db: &Db, _batch: &mut OwnedWriteBatch) -> Result<()> {
    Ok(())
}
