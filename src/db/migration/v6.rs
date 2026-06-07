use std::collections::BTreeMap;

use crate::db::keys::{self, COUNT_KS_PREFIX};
use crate::db::{Db, set_ks_count};
use crate::types::v4;
use fjall::OwnedWriteBatch;
use miette::{Context, IntoDiagnostic, Result};
use smol_str::SmolStr;
use url::Url;

pub(crate) fn rebuild_pds_account_counts(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    let mut counts: BTreeMap<SmolStr, u64> = BTreeMap::new();

    for guard in db.repos.iter() {
        let (_, value) = guard.into_inner().into_diagnostic()?;
        let state: v4::RepoState = rmp_serde::from_slice(value.as_ref())
            .into_diagnostic()
            .wrap_err("invalid v5 repo state")?;
        if !state.active {
            continue;
        }

        let Some(host) = state
            .pds
            .as_deref()
            .and_then(|pds| Url::parse(pds).ok())
            .and_then(|url| url.host_str().map(SmolStr::new))
        else {
            continue;
        };

        *counts.entry(host).or_insert(0) += 1;
    }

    for guard in db.counts.prefix(COUNT_KS_PREFIX) {
        let (key, _) = guard.into_inner().into_diagnostic()?;
        let name = std::str::from_utf8(&key[COUNT_KS_PREFIX.len()..])
            .into_diagnostic()
            .wrap_err("expected valid utf8 for count key")?;
        if name.starts_with("p|") {
            batch.remove(&db.counts, key);
        }
    }

    for guard in db.counts.prefix(keys::COUNT_DELTA_PREFIX) {
        let key = guard.key().into_diagnostic()?;
        let (_, name) = keys::parse_count_delta_key(&key)?;
        if name.starts_with("p|") {
            batch.remove(&db.counts, key);
        }
    }

    for (host, count) in counts {
        set_ks_count(batch, db, &keys::pds_account_count_key(&host), count);
    }

    Ok(())
}
