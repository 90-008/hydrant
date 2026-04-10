use crate::db::Db;
use fjall::OwnedWriteBatch;
use miette::{Context, IntoDiagnostic, Result};

pub mod v4 {
    pub const PDS_TIER_PREFIX: &[u8] = b"pt|";
    pub const PDS_BANNED_PREFIX: &[u8] = b"pb|";
}

pub(crate) fn pds_meta_layout(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    for guard in db.filter.prefix(v4::PDS_TIER_PREFIX) {
        let (k, v) = guard.into_inner().into_diagnostic()?;
        let host = std::str::from_utf8(&k[v4::PDS_TIER_PREFIX.len()..])
            .into_diagnostic()
            .wrap_err("failed to parse host as utf8")?;
        let tier = std::str::from_utf8(&v)
            .into_diagnostic()
            .wrap_err("failed to parse tier as utf8")?;
        crate::db::pds_meta::set_tier(batch, &db.filter, host, tier);
        batch.remove(&db.filter, k);
    }

    for guard in db.filter.prefix(v4::PDS_BANNED_PREFIX) {
        let (k, _) = guard.into_inner().into_diagnostic()?;
        let host = std::str::from_utf8(&k[v4::PDS_BANNED_PREFIX.len()..])
            .into_diagnostic()
            .wrap_err("failed to parse host as utf8")?;
        crate::db::pds_meta::set_status(
            batch,
            &db.filter,
            host,
            crate::pds_meta::HostStatus::Banned,
        )?;
        batch.remove(&db.filter, k);
    }

    Ok(())
}
