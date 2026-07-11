use fjall::OwnedWriteBatch;
use miette::Result;

use crate::db::Db;

#[cfg(feature = "indexer")]
use {crate::db::types::TrimmedDid, jacquard_common::types::did::Did, miette::IntoDiagnostic};

#[cfg(feature = "indexer")]
pub(crate) fn migrate_v9(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    // 1. Migrate excludes
    let exclude_prefix = [crate::db::filter::EXCLUDE_PREFIX, crate::db::keys::SEP];
    for guard in db.filter.prefix(exclude_prefix) {
        let (k, _) = guard.into_inner().into_diagnostic()?;
        let val_bytes = &k[exclude_prefix.len()..];
        if let Ok(s) = std::str::from_utf8(val_bytes) {
            if s.starts_with("did:") {
                let did = Did::new(s).into_diagnostic()?;
                let trimmed = TrimmedDid::from(&did);
                let mut new_key = Vec::with_capacity(2 + trimmed.len());
                new_key.push(crate::db::filter::EXCLUDE_PREFIX);
                new_key.push(crate::db::keys::SEP);
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

#[cfg(all(test, feature = "indexer"))]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::tempdir;

    fn test_config(path: &std::path::Path) -> Config {
        Config {
            database_path: path.to_path_buf(),
            ..Default::default()
        }
    }

    #[test]
    fn test_migration_v9() -> Result<()> {
        let tmp = tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());

        let did = "did:plc:yk4q3id7id6p5z3bypvshc64";

        // 1. Prepare DB with legacy keys under version 8
        {
            let db = Db::open(&cfg)?;
            let mut batch = db.inner.batch();

            // Legacy exclude
            let legacy_exclude_key = format!("x|{}", did);
            batch.insert(&db.filter, legacy_exclude_key.as_bytes(), []);

            // Legacy PDS status & tier
            let legacy_status_key = "example.com|status";
            let status = crate::pds_meta::HostStatus::Offline;
            batch.insert(
                &db.filter,
                legacy_status_key.as_bytes(),
                rmp_serde::to_vec(&status).into_diagnostic()?,
            );

            let legacy_tier_key = "example.com|tier";
            batch.insert(&db.filter, legacy_tier_key.as_bytes(), b"tier1");

            batch.insert(
                &db.counts,
                crate::db::keys::VERSIONING_KEY,
                8_u64.to_be_bytes(),
            );
            batch.commit().into_diagnostic()?;
            db.persist()?;
        }

        // 2. Open DB again, which triggers v9 migration
        let db = Db::open(&cfg)?;

        // Verify version key is 9
        let version_bytes = db
            .counts
            .get(crate::db::keys::VERSIONING_KEY)
            .into_diagnostic()?
            .expect("db version should be set");
        assert_eq!(
            u64::from_be_bytes(version_bytes.as_ref().try_into().into_diagnostic()?),
            crate::db::migration::LATEST_VERSION
        );

        // Verify excludes migrated
        let legacy_exclude_key = format!("x|{}", did);
        assert!(
            !db.filter
                .contains_key(legacy_exclude_key.as_bytes())
                .into_diagnostic()?
        );

        let new_exclude_key = crate::db::filter::exclude_key(did)?;
        assert!(db.filter.contains_key(&new_exclude_key).into_diagnostic()?);

        // Verify PDS status and tier migrated
        assert!(
            !db.filter
                .contains_key(b"example.com|status")
                .into_diagnostic()?
        );
        assert!(
            !db.filter
                .contains_key(b"example.com|tier")
                .into_diagnostic()?
        );

        let new_status_key = crate::db::pds_meta::pds_status_key("example.com");
        assert!(db.filter.contains_key(&new_status_key).into_diagnostic()?);

        let new_tier_key = crate::db::pds_meta::pds_tier_key("example.com");
        assert!(db.filter.contains_key(&new_tier_key).into_diagnostic()?);

        Ok(())
    }
}
