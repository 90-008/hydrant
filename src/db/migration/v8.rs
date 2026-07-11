use fjall::OwnedWriteBatch;
use miette::Result;
#[cfg(feature = "indexer")]
use miette::{IntoDiagnostic, WrapErr};

use crate::db::Db;

#[cfg(feature = "indexer")]
use crate::db::{deser_repo_meta, keys, set_ks_count};
#[cfg(feature = "indexer")]
use crate::types::{GaugeState, ResyncErrorKind};

#[cfg(feature = "indexer")]
pub(crate) fn rebuild_lifecycle_counts(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    for guard in db.counts.prefix(keys::COUNT_GAUGE_PREFIX) {
        let key = guard.key().into_diagnostic()?;
        batch.remove(&db.counts, key);
    }

    for guard in db.counts.prefix(keys::COUNT_DELTA_PREFIX) {
        let key = guard.key().into_diagnostic()?;
        let (_, name) = keys::parse_count_delta_key(&key)?;
        if is_lifecycle_count(name) {
            batch.remove(&db.counts, key);
        }
    }

    let mut pending = 0_u64;
    let mut resync = 0_u64;
    let mut error_ratelimited = 0_u64;
    let mut error_transport = 0_u64;
    let mut error_generic = 0_u64;

    for guard in db.repos.iter() {
        let repo_key = guard.key().into_diagnostic()?;
        let gauge = primary_lifecycle_gauge(db, repo_key.as_ref())?;
        batch.insert(
            &db.counts,
            count_gauge_key_from_repo_key(repo_key.as_ref()),
            crate::db::lifecycle_counts::encode_gauge(gauge),
        );

        match gauge {
            GaugeState::Synced => {}
            GaugeState::Pending => pending += 1,
            GaugeState::Resync(None) => resync += 1,
            GaugeState::Resync(Some(ResyncErrorKind::Ratelimited)) => {
                resync += 1;
                error_ratelimited += 1;
            }
            GaugeState::Resync(Some(ResyncErrorKind::Transport)) => {
                resync += 1;
                error_transport += 1;
            }
            GaugeState::Resync(Some(ResyncErrorKind::Generic)) => {
                resync += 1;
                error_generic += 1;
            }
        }
    }

    set_ks_count(batch, db, "pending", pending);
    set_ks_count(batch, db, "resync", resync);
    set_ks_count(batch, db, "error_ratelimited", error_ratelimited);
    set_ks_count(batch, db, "error_transport", error_transport);
    set_ks_count(batch, db, "error_generic", error_generic);

    Ok(())
}

#[cfg(not(feature = "indexer"))]
pub(crate) fn rebuild_lifecycle_counts(_db: &Db, _batch: &mut OwnedWriteBatch) -> Result<()> {
    Ok(())
}

#[cfg(feature = "indexer")]
fn primary_lifecycle_gauge(db: &Db, repo_key: &[u8]) -> Result<GaugeState> {
    let metadata_key = repo_metadata_key_from_repo_key(repo_key);
    if let Some(metadata_bytes) = db.repo_metadata.get(&metadata_key).into_diagnostic()? {
        let metadata = deser_repo_meta(metadata_bytes.as_ref())
            .wrap_err("invalid repo metadata during lifecycle count rebuild")?;
        if db
            .pending
            .get(keys::pending_key(metadata.index_id))
            .into_diagnostic()?
            .is_some()
        {
            return Ok(GaugeState::Pending);
        }
    }

    db.resync
        .get(repo_key)
        .into_diagnostic()?
        .map(|bytes| crate::db::lifecycle_counts::gauge_from_resync(bytes.as_ref()))
        .map(Ok)
        .unwrap_or(Ok(GaugeState::Synced))
}

#[cfg(feature = "indexer")]
fn count_gauge_key_from_repo_key(repo_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(keys::COUNT_GAUGE_PREFIX.len() + repo_key.len());
    key.extend_from_slice(keys::COUNT_GAUGE_PREFIX);
    key.extend_from_slice(repo_key);
    key
}

#[cfg(feature = "indexer")]
fn repo_metadata_key_from_repo_key(repo_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(keys::REPO_METADATA_PREFIX.len() + repo_key.len());
    key.extend_from_slice(keys::REPO_METADATA_PREFIX);
    key.extend_from_slice(repo_key);
    key
}

#[cfg(feature = "indexer")]
fn is_lifecycle_count(name: &str) -> bool {
    matches!(
        name,
        "pending" | "resync" | "error_ratelimited" | "error_transport" | "error_generic"
    )
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "indexer")]
    use super::*;

    #[cfg(feature = "indexer")]
    mod indexer {
        use super::*;
        use crate::config::Config;
        use crate::db::{deser_repo_state, ser_repo_meta, ser_repo_state};
        use crate::types::{RepoMetadata, RepoState, RepoStatus, ResyncState};
        use jacquard_common::types::string::Did;

        fn test_config(path: &std::path::Path) -> Config {
            Config {
                database_path: path.to_path_buf(),
                ..Default::default()
            }
        }

        fn did(value: &str) -> Result<Did<'static>> {
            use jacquard_common::IntoStatic;
            Did::new(value)
                .into_diagnostic()
                .map(IntoStatic::into_static)
        }

        fn insert_repo(
            db: &Db,
            batch: &mut OwnedWriteBatch,
            did: &Did<'_>,
            index_id: u64,
        ) -> Result<()> {
            batch.insert(
                &db.repos,
                keys::repo_key(did),
                ser_repo_state(&RepoState::backfilling())?,
            );
            batch.insert(
                &db.repo_metadata,
                keys::repo_metadata_key(did),
                ser_repo_meta(&RepoMetadata::backfilling(index_id))?,
            );
            Ok(())
        }

        #[test]
        fn lifecycle_count_migration_rebuilds_skewed_counts_and_drops_old_deltas() -> Result<()> {
            let tmp = tempfile::tempdir().into_diagnostic()?;
            let cfg = test_config(tmp.path());

            let pending_did = did("did:web:pending.example")?;
            let ratelimited_did = did("did:web:ratelimited.example")?;
            let gone_did = did("did:web:gone.example")?;
            let synced_did = did("did:web:synced.example")?;

            {
                let db = Db::open(&cfg)?;
                let mut batch = db.inner.batch();

                insert_repo(&db, &mut batch, &pending_did, 10)?;
                insert_repo(&db, &mut batch, &ratelimited_did, 20)?;
                insert_repo(&db, &mut batch, &gone_did, 30)?;
                insert_repo(&db, &mut batch, &synced_did, 40)?;

                batch.insert(
                    &db.pending,
                    keys::pending_key(10),
                    keys::repo_key(&pending_did),
                );
                batch.insert(
                    &db.resync,
                    keys::repo_key(&ratelimited_did),
                    rmp_serde::to_vec(&ResyncState::Error {
                        kind: ResyncErrorKind::Ratelimited,
                        retry_count: 3,
                        next_retry: 123,
                    })
                    .into_diagnostic()?,
                );
                batch.insert(
                    &db.resync,
                    keys::repo_key(&gone_did),
                    rmp_serde::to_vec(&ResyncState::Gone {
                        status: RepoStatus::Deactivated,
                    })
                    .into_diagnostic()?,
                );

                set_ks_count(&mut batch, &db, "pending", 0);
                set_ks_count(&mut batch, &db, "resync", 99);
                set_ks_count(&mut batch, &db, "error_ratelimited", 42);
                set_ks_count(&mut batch, &db, "error_transport", 42);
                set_ks_count(&mut batch, &db, "error_generic", 42);
                batch.insert(
                    &db.counts,
                    keys::count_delta_key(1, "pending"),
                    10_i64.to_be_bytes(),
                );
                batch.insert(
                    &db.counts,
                    keys::count_delta_key(2, "error_ratelimited"),
                    10_i64.to_be_bytes(),
                );
                batch.insert(&db.counts, keys::VERSIONING_KEY, 7_u64.to_be_bytes());
                batch.commit().into_diagnostic()?;
                db.persist()?;
            }

            let db = Db::open(&cfg)?;

            assert_eq!(db.get_count_sync("pending"), 1);
            assert_eq!(db.get_count_sync("resync"), 2);
            assert_eq!(db.get_count_sync("error_ratelimited"), 1);
            assert_eq!(db.get_count_sync("error_transport"), 0);
            assert_eq!(db.get_count_sync("error_generic"), 0);
            assert_eq!(db.counts.prefix(keys::COUNT_GAUGE_PREFIX).count(), 4);

            for guard in db.counts.prefix(keys::COUNT_DELTA_PREFIX) {
                let key = guard.key().into_diagnostic()?;
                let (_, name) = keys::parse_count_delta_key(&key)?;
                assert!(!is_lifecycle_count(name));
            }

            let version_bytes = db
                .counts
                .get(keys::VERSIONING_KEY)
                .into_diagnostic()?
                .expect("db version should be set");
            assert_eq!(
                u64::from_be_bytes(version_bytes.as_ref().try_into().into_diagnostic()?),
                crate::db::migration::LATEST_VERSION
            );

            let synced_state = db
                .repos
                .get(keys::repo_key(&synced_did))
                .into_diagnostic()?
                .expect("synced repo should remain present");
            let _ = deser_repo_state(synced_state.as_ref())?;

            Ok(())
        }
    }
}
