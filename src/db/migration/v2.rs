use bytes::Bytes;
use cid::Cid as IpldCid;
use fjall::OwnedWriteBatch;
use jacquard_common::{CowStr, types::string::Handle};
use miette::{Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};

use crate::db::{
    Db,
    types::{DbTid, DidKey},
};
use crate::types::Commit;
use crate::types::v2::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "'i: 'de"))]
pub(crate) struct OldRepoState<'i> {
    pub status: RepoStatus, // from v2, old is same as new
    pub rev: Option<DbTid>,
    pub data: Option<IpldCid>,
    pub last_message_time: Option<i64>,
    pub last_updated_at: i64,
    pub tracked: bool,
    pub index_id: u64,
    #[serde(borrow)]
    pub signing_key: Option<DidKey<'i>>,
    #[serde(borrow)]
    pub pds: Option<CowStr<'i>>,
    #[serde(borrow)]
    pub handle: Option<Handle<'i>>,
}

pub(super) fn repo_state_root_commit(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    for item in db.repos.iter() {
        let (k, v) = item.into_inner().into_diagnostic()?;
        let old: OldRepoState = rmp_serde::from_slice(&v)
            .into_diagnostic()
            .wrap_err("invalid old repo state")?;
        let new = RepoState {
            root: match (old.rev, old.data) {
                (Some(rev), Some(data)) => Some(Commit {
                    version: -1,
                    rev,
                    data,
                    prev: None,
                    sig: Bytes::new(),
                }),
                _ => None,
            },
            status: old.status,
            handle: old.handle,
            index_id: old.index_id,
            last_message_time: old.last_message_time,
            last_updated_at: old.last_updated_at,
            pds: old.pds,
            signing_key: old.signing_key,
            tracked: old.tracked,
        };
        batch.insert(
            &db.repos,
            k,
            rmp_serde::to_vec(&new)
                .into_diagnostic()
                .wrap_err("cant serialize new repo state")?,
        );
    }

    Ok(())
}
