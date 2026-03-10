use crate::db::{Db, keys};

use fjall::{OwnedWriteBatch, Slice};
use miette::{IntoDiagnostic, Result};
use std::sync::atomic::Ordering;

/// a write batch that tracks block refcount deltas and applies them
/// automatically on commit. this prevents the bug class where callers
/// forget to call `apply_block_refcount_deltas` after committing.
pub struct RefcountedBatch<'db> {
    batch: OwnedWriteBatch,
    db: &'db Db,
    pending_deltas: Vec<(Slice, i8)>,
}

impl<'db> RefcountedBatch<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self {
            batch: db.inner.batch(),
            db,
            pending_deltas: Vec::new(),
        }
    }

    /// records a refcount delta for the given CID. writes to the reflog
    /// in the batch and accumulates the delta for in-memory application on commit.
    pub fn update_block_refcount(&mut self, cid_bytes: Slice, delta: i8) -> Result<()> {
        #[cfg(debug_assertions)]
        if let Ok(cid) = cid::Cid::read_bytes(cid_bytes.as_ref()) {
            tracing::debug!(delta, %cid, "update_block_refcount");
        }

        let value = rmp_serde::to_vec(&(cid_bytes.as_ref(), delta)).into_diagnostic()?;
        let seq = self.db.next_reflog_seq.fetch_add(1, Ordering::SeqCst);
        self.batch
            .insert(&self.db.block_reflog, keys::reflog_key(seq), value);
        self.pending_deltas.push((cid_bytes, delta));
        Ok(())
    }

    /// commits the batch and applies all accumulated refcount deltas to the in-memory map.
    pub fn commit(self) -> Result<(), fjall::Error> {
        self.batch.commit()?;
        apply_deltas(self.db, &self.pending_deltas);
        Ok(())
    }

    pub fn batch_mut(&mut self) -> &mut OwnedWriteBatch {
        &mut self.batch
    }
}

fn apply_deltas(db: &Db, deltas: &[(Slice, i8)]) {
    for (cid_bytes, delta) in deltas {
        let mut entry = db
            .block_refcounts
            .entry_sync(cid_bytes.clone())
            .or_insert(0);
        *entry += *delta as i64;
    }
}
