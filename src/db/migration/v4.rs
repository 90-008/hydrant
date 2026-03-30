use fjall::OwnedWriteBatch;
use miette::{Context, IntoDiagnostic, Result};

use crate::db::Db;
use crate::types::v4;

#[derive(serde::Deserialize)]
#[serde(bound(deserialize = "'i: 'de"))]
pub(crate) struct OldRepoState<'i> {
    pub status: crate::types::v2::RepoStatus,
    pub root: Option<crate::types::v2::Commit>,
    pub last_message_time: Option<i64>,
    pub last_updated_at: i64,
    pub tracked: bool,
    pub index_id: u64,
    #[serde(borrow)]
    pub signing_key: Option<crate::db::types::DidKey<'i>>,
    #[serde(borrow)]
    pub pds: Option<jacquard_common::CowStr<'i>>,
    #[serde(borrow)]
    pub handle: Option<jacquard_common::types::string::Handle<'i>>,
}

pub(super) fn repo_state_active(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    for item in db.repos.iter() {
        let (k, v) = item.into_inner().into_diagnostic()?;
        let old: OldRepoState = rmp_serde::from_slice(&v)
            .into_diagnostic()
            .wrap_err("invalid repo state")?;

        // derive active from the old status: accounts in any inactive state had active=false;
        // everything else (synced, backfilling, error) was active from the upstream's perspective.
        let active = !matches!(
            old.status,
            crate::types::v2::RepoStatus::Deactivated
                | crate::types::v2::RepoStatus::Takendown
                | crate::types::v2::RepoStatus::Suspended
        );

        let status = match old.status {
            crate::types::v2::RepoStatus::Backfilling => v4::RepoStatus::Desynchronized,
            crate::types::v2::RepoStatus::Synced => v4::RepoStatus::Synced,
            crate::types::v2::RepoStatus::Error(s) => match s.as_str() {
                "desynchronized" => v4::RepoStatus::Desynchronized,
                "throttled" => v4::RepoStatus::Throttled,
                _ => v4::RepoStatus::Error(s),
            },
            crate::types::v2::RepoStatus::Deactivated => v4::RepoStatus::Deactivated,
            crate::types::v2::RepoStatus::Takendown => v4::RepoStatus::Takendown,
            crate::types::v2::RepoStatus::Suspended => v4::RepoStatus::Suspended,
        };

        let new_state = v4::RepoState {
            active,
            status,
            root: old.root,
            last_message_time: old.last_message_time,
            last_updated_at: old.last_updated_at,
            signing_key: old.signing_key,
            pds: old.pds,
            handle: old.handle,
        };

        let new_metadata = v4::RepoMetadata {
            tracked: old.tracked,
            index_id: old.index_id,
        };

        batch.insert(
            &db.repos,
            k.clone(),
            rmp_serde::to_vec(&new_state)
                .into_diagnostic()
                .wrap_err("cant serialize new repo state")?,
        );

        let did = crate::db::types::TrimmedDid::try_from(k.as_ref())?.to_did();
        let metadata_key = crate::db::keys::repo_metadata_key(&did);
        batch.insert(
            &db.repo_metadata,
            metadata_key,
            rmp_serde::to_vec(&new_metadata)
                .into_diagnostic()
                .wrap_err("cant serialize new repo metadata")?,
        );
    }

    Ok(())
}
