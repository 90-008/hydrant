//! ephemeral mode block lifecycle: in-memory refcounting and TTL-based event expiry.
//!
//! ## model
//!
//! every event that references a block CID holds one refcount entry. the TTL worker
//! decrements the count when the event expires. when the count hits zero the block is
//! deleted inline in the same batch as the event deletion.
//!
//! ## correctness
//!
//! - refcounts are rebuilt from `db.events` on startup before the server accepts requests.
//! - shared CIDs are handled correctly: two events referencing the same block each
//!   increment the counter; the block is deleted only when the second one expires.

use crate::db::{Db, keys};
use crate::types::StoredEvent;
use fjall::Slice;
use miette::{IntoDiagnostic, WrapErr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{debug, error, info, trace};

pub const EVENT_TTL_SECS: u64 = 60 * 10;

/// rebuilds `db.block_refcounts` by scanning all stored events.
/// must be called on startup in ephemeral mode before accepting requests.
pub fn ephemeral_startup_load_refcounts(db: &Db) -> miette::Result<()> {
    info!("rebuilding block refcounts from events (ephemeral mode)");
    for guard in db.events.iter() {
        let v = guard.value().into_diagnostic()?;
        let evt = rmp_serde::from_slice::<StoredEvent>(&v).into_diagnostic()?;
        let Some(cid) = evt.cid else { continue };
        let cid_bytes = Slice::from(cid.to_bytes());
        let mut entry = db.block_refcounts.entry_sync(cid_bytes).or_insert(0);
        *entry += 1;
    }
    trace!("ephemeral block refcounts ready");
    Ok(())
}

pub fn ephemeral_ttl_worker(state: Arc<crate::state::AppState>) {
    info!("ephemeral TTL worker started");
    loop {
        std::thread::sleep(Duration::from_secs(60));
        if let Err(e) = ephemeral_ttl_tick(&state.db) {
            error!(err = %e, "ephemeral TTL tick failed");
        }
    }
}

pub fn ephemeral_ttl_tick(db: &Db) -> miette::Result<()> {
    let now = chrono::Utc::now().timestamp() as u64;
    let cutoff_ts = now.saturating_sub(EVENT_TTL_SECS);

    // write current watermark
    let current_event_id = db.next_event_id.load(Ordering::SeqCst);
    db.cursors
        .insert(
            keys::event_watermark_key(now),
            current_event_id.to_be_bytes(),
        )
        .into_diagnostic()?;

    // find the watermark entry closest to and <= cutoff_ts
    let cutoff_key = keys::event_watermark_key(cutoff_ts);
    let cutoff_event_id = db
        .cursors
        .range(..=cutoff_key.clone())
        .next_back()
        .map(|g| g.into_inner().into_diagnostic())
        .transpose()?
        .filter(|(k, _)| k.starts_with(keys::EVENT_WATERMARK_PREFIX))
        .map(|(_, v)| {
            v.as_ref()
                .try_into()
                .into_diagnostic()
                .wrap_err("expected cutoff event id to be u64")
        })
        .transpose()?
        .map(u64::from_be_bytes);

    let Some(cutoff_event_id) = cutoff_event_id else {
        // no watermark old enough yet, nothing to prune
        return Ok(());
    };

    let cutoff_key_events = keys::event_key(cutoff_event_id);
    let mut batch = db.inner.batch();
    let mut pruned = 0usize;

    for guard in db.events.range(..cutoff_key_events) {
        let (k, v) = guard.into_inner().into_diagnostic()?;
        let evt = rmp_serde::from_slice::<StoredEvent>(&v).into_diagnostic()?;

        if let Some(cid) = evt.cid {
            let cid_bytes = Slice::from(cid.to_bytes());

            let remove_block = {
                let count = db
                    .block_refcounts
                    .entry_sync(cid_bytes.clone())
                    .and_modify(|c| {
                        *c = c.saturating_sub(1);
                    })
                    .or_default();
                *count == 0
            };

            if remove_block {
                db.block_refcounts.remove_sync(&cid_bytes);
                batch.remove(&db.blocks, cid_bytes);
            }
        }

        batch.remove(&db.events, k);
        pruned += 1;
    }

    // clean up consumed watermark entries (everything up to and including cutoff_ts)
    for guard in db.cursors.range(..=cutoff_key) {
        let k = guard.key().into_diagnostic()?;
        if k.starts_with(keys::EVENT_WATERMARK_PREFIX) {
            batch.remove(&db.cursors, k);
        }
    }

    batch.commit().into_diagnostic()?;

    debug!(pruned, "pruned old events");

    Ok(())
}
