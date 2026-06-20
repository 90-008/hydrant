use fjall::OwnedWriteBatch;
use miette::{Context, IntoDiagnostic, Result};

use crate::db::Db;
use crate::types::{RepoState, v4};

pub(crate) fn repo_state_event_clocks(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    for guard in db.repos.iter() {
        let (key, value) = guard.into_inner().into_diagnostic()?;
        let old: v4::RepoState = rmp_serde::from_slice(value.as_ref())
            .into_diagnostic()
            .wrap_err("invalid v6 repo state")?;

        let new_state = RepoState {
            active: old.active,
            status: old.status,
            root: old.root,
            last_message_time: old.last_message_time,
            last_identity_time: None,
            last_account_time: None,
            last_updated_at: old.last_updated_at,
            signing_key: old.signing_key,
            pds: old.pds,
            handle: old.handle,
        };

        batch.insert(
            &db.repos,
            key,
            rmp_serde::to_vec(&new_state)
                .into_diagnostic()
                .wrap_err("cant serialize v7 repo state")?,
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::db::{deser_repo_state, keys};
    use crate::types::{RepoStatus, v4};
    use jacquard_common::CowStr;
    use jacquard_common::types::string::Did;

    #[test]
    fn repo_state_event_clock_migration_rewrites_v6_repo_states() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = Config {
            database_path: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let did = Did::new("did:web:migration.test").into_diagnostic()?;
        let repo_key = keys::repo_key(&did);

        let legacy_bytes = {
            let db = Db::open(&cfg)?;
            let legacy = v4::RepoState {
                active: true,
                status: RepoStatus::Synced,
                root: None,
                last_message_time: Some(1_234),
                last_updated_at: 9_876,
                signing_key: None,
                pds: Some(CowStr::Borrowed("https://pds.example/")),
                handle: None,
            };
            let legacy_bytes = rmp_serde::to_vec(&legacy).into_diagnostic()?;

            let mut batch = db.inner.batch();
            batch.insert(&db.repos, &repo_key, &legacy_bytes);
            batch.insert(&db.counts, keys::VERSIONING_KEY, 6_u64.to_be_bytes());
            batch.commit().into_diagnostic()?;
            db.persist()?;

            legacy_bytes
        };

        let db = Db::open(&cfg)?;
        let migrated_bytes = db
            .repos
            .get(&repo_key)
            .into_diagnostic()?
            .expect("repo state should exist after migration");

        assert_ne!(migrated_bytes.as_ref(), legacy_bytes.as_slice());

        let migrated_state = deser_repo_state(migrated_bytes.as_ref())?;
        assert!(migrated_state.active);
        assert_eq!(migrated_state.status, RepoStatus::Synced);
        assert_eq!(migrated_state.last_message_time, Some(1_234));
        assert_eq!(migrated_state.last_updated_at, 9_876);
        assert_eq!(migrated_state.last_identity_time, None);
        assert_eq!(migrated_state.last_account_time, None);
        assert_eq!(migrated_state.pds.as_deref(), Some("https://pds.example/"));

        let version_bytes = db
            .counts
            .get(keys::VERSIONING_KEY)
            .into_diagnostic()?
            .expect("db version should be set");
        assert_eq!(
            u64::from_be_bytes(version_bytes.as_ref().try_into().into_diagnostic()?),
            8
        );

        Ok(())
    }
}
