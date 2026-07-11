//! backlinks index hooks for record ops. no-ops when the feature is off so
//! `apply_commit`/`delete_repo` stay unconditional.

use fjall::OwnedWriteBatch;
use jacquard_common::types::did::Did;
use miette::Result;

use crate::db::Db;

#[cfg(feature = "backlinks")]
pub(crate) fn index_record(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    did: &Did<'_>,
    collection: &str,
    rkey: &str,
    bytes: &[u8],
) -> Result<()> {
    if let Ok(value) = serde_ipld_dagcbor::from_slice::<jacquard_common::Data>(bytes) {
        crate::backlinks::store::index_record(
            batch,
            &db.backlinks,
            did.as_str(),
            collection,
            rkey,
            &value,
        )?;
    }
    Ok(())
}

#[cfg(not(feature = "backlinks"))]
#[inline(always)]
pub(crate) fn index_record(
    _batch: &mut OwnedWriteBatch,
    _db: &Db,
    _did: &Did<'_>,
    _collection: &str,
    _rkey: &str,
    _bytes: &[u8],
) -> Result<()> {
    Ok(())
}

#[cfg(feature = "backlinks")]
pub(crate) fn delete_record(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    did: &Did<'_>,
    collection: &str,
    rkey: &str,
) -> Result<()> {
    crate::backlinks::store::delete_record(batch, &db.backlinks, did.as_str(), collection, rkey)
}

#[cfg(not(feature = "backlinks"))]
#[inline(always)]
pub(crate) fn delete_record(
    _batch: &mut OwnedWriteBatch,
    _db: &Db,
    _did: &Did<'_>,
    _collection: &str,
    _rkey: &str,
) -> Result<()> {
    Ok(())
}

#[cfg(feature = "backlinks")]
pub(crate) fn delete_repo(batch: &mut OwnedWriteBatch, db: &Db, did: &Did<'_>) -> Result<()> {
    crate::backlinks::store::delete_repo(batch, &db.backlinks, did)
}

#[cfg(not(feature = "backlinks"))]
#[inline(always)]
pub(crate) fn delete_repo(_batch: &mut OwnedWriteBatch, _db: &Db, _did: &Did<'_>) -> Result<()> {
    Ok(())
}
