use fjall::OwnedWriteBatch;
use miette::{Context, IntoDiagnostic, Result};

use crate::db::Db;
use crate::db::keys::VERSIONING_KEY;

mod v1;
mod v2;
mod v3;
mod v4;
mod v5;

type MigrationFn = fn(&Db, &mut OwnedWriteBatch) -> Result<()>;

/// ordered list of migrations. migration at index `i` upgrades the schema from version `i` to `i+1`.
const MIGRATIONS: &[(&str, MigrationFn)] = &[
    ("stable_firehose_cursors", v1::stable_firehose_cursors),
    ("repo_state_root_commit", v2::repo_state_root_commit),
    ("firehose_source_is_pds", v3::firehose_source_is_pds),
    ("repo_state_active", v4::repo_state_active),
    ("pds_meta_layout", v5::pds_meta_layout),
];

fn read_version(db: &Db) -> Result<u64> {
    db.counts
        .get(VERSIONING_KEY)
        .into_diagnostic()?
        .map(|r| {
            r.as_ref()
                .try_into()
                .into_diagnostic()
                .wrap_err("db version key expected to be 8 bytes")
                .map(u64::from_be_bytes)
        })
        .transpose()
        .map(|v| v.unwrap_or(0))
}

/// run all pending database migrations in order.
///
/// each migration and its version bump are committed atomically. safe to run on a fresh
/// database (all migrations are no-ops when no relevant data exists). called during [`Db::open`].
pub(super) fn run(db: &Db) -> Result<()> {
    let version = read_version(db)? as usize;
    for (i, (name, migration)) in MIGRATIONS.iter().enumerate().skip(version) {
        tracing::info!("db: running migration {name} (v{i} -> v{})", i + 1);
        let mut batch = db.inner.batch();
        migration(db, &mut batch)?;
        let new_version = (i + 1) as u64;
        batch.insert(&db.counts, VERSIONING_KEY, new_version.to_be_bytes());
        batch.commit().into_diagnostic()?;
        tracing::info!("db: migration {name} complete");
    }
    Ok(())
}
