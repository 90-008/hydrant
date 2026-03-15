use fjall::Slice;
use fjall::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};
use miette::{IntoDiagnostic, WrapErr};
use scc::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{error, info};

const EVENT_TTL_SECS: u64 = 3600;

use crate::db::{Db, keys};
use crate::types::StoredEvent;

pub struct BlocksGcFilterFactory {
    pub gc_ready: Arc<AtomicBool>,
    pub refcounts: Arc<HashMap<Slice, i64>>,
}

struct BlocksGcFilter {
    gc_ready: Arc<AtomicBool>,
    refcounts: Arc<HashMap<Slice, i64>>,
}

impl Factory for BlocksGcFilterFactory {
    fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
        Box::new(BlocksGcFilter {
            gc_ready: self.gc_ready.clone(),
            refcounts: self.refcounts.clone(),
        })
    }

    fn name(&self) -> &str {
        "blocks_gc"
    }
}

impl CompactionFilter for BlocksGcFilter {
    fn filter_item(&mut self, item: ItemAccessor<'_>, _ctx: &Context) -> lsm_tree::Result<Verdict> {
        if !self.gc_ready.load(Ordering::SeqCst) {
            return Ok(Verdict::Keep);
        }

        Ok(self
            .refcounts
            .read_sync(item.key().as_ref(), |_, count| {
                #[cfg(debug_assertions)]
                if let Ok(cid) = cid::Cid::read_bytes(item.key().as_ref()) {
                    tracing::debug!(cid = %cid, count, "BlocksGcFilter checking block");
                }

                (*count <= 0)
                    .then_some(Verdict::Destroy)
                    .unwrap_or(Verdict::Keep)
            })
            .unwrap_or(Verdict::Destroy))
    }
}

pub fn startup_load_refcounts(db: &Db) -> miette::Result<()> {
    let checkpoint_seq = db
        .cursors
        .get(keys::BLOCK_REFS_CHECKPOINT_SEQ_KEY)
        .into_diagnostic()?
        .map(|v| {
            u64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .expect("checkpoint seq should be 8 bytes"),
            )
        })
        .unwrap_or(0);

    // check if we need to run the one-time migration
    let needs_migration = db
        .counts
        .get(keys::count_keyspace_key("gc_schema_version"))
        .into_diagnostic()?
        .is_none();

    if needs_migration {
        migrate_build_refcounts(db)?;
    }

    // load snapshot
    for guard in db.block_refs.iter() {
        let (k, v) = guard.into_inner().into_diagnostic()?;
        let count = i64::from_be_bytes(
            v.as_ref()
                .try_into()
                .into_diagnostic()
                .wrap_err("invalid block_refs count bytes")?,
        );
        let _ = db.block_refcounts.insert_sync(k, count);
    }

    // replay WAL since checkpoint
    let start_key = keys::reflog_key(checkpoint_seq.saturating_add(1));
    for guard in db.block_reflog.range(start_key..) {
        let (_, v) = guard.into_inner().into_diagnostic()?;
        let (cid, delta): (Vec<u8>, i8) = rmp_serde::from_slice(&v).into_diagnostic()?;
        let cid = Slice::from(cid);
        let mut entry = db.block_refcounts.entry_sync(cid).or_insert(0);
        *entry += delta as i64;
    }

    db.gc_ready.store(true, Ordering::SeqCst);
    info!("block refcounts loaded, gc ready");
    Ok(())
}

fn migrate_build_refcounts(db: &Db) -> miette::Result<()> {
    info!("building initial block refcounts from existing records (one-time migration)");
    let mut batch = db.inner.batch();

    // scan records
    for guard in db.records.iter() {
        let cid_bytes = guard.value().into_diagnostic()?;
        let mut entry = db.block_refcounts.entry_sync(cid_bytes).or_insert(0);
        *entry += 1i64;
    }

    // events with cids
    for guard in db.events.iter() {
        let v = guard.value().into_diagnostic()?;
        let evt = rmp_serde::from_slice::<StoredEvent>(&v).into_diagnostic()?;
        let Some(cid) = evt.cid else {
            continue;
        };
        let cid_bytes = Slice::from(cid.to_bytes());
        let mut entry = db.block_refcounts.entry_sync(cid_bytes).or_insert(0);
        *entry += 1i64;
    }

    // persist as initial checkpoint
    db.block_refcounts.iter_sync(|k, v| {
        batch.insert(&db.block_refs, k.as_ref(), v.to_be_bytes());
        true
    });

    let seq = db.next_reflog_seq.load(Ordering::SeqCst);
    batch.insert(
        &db.cursors,
        keys::BLOCK_REFS_CHECKPOINT_SEQ_KEY,
        seq.to_be_bytes(),
    );
    // mark migration done
    let one: u64 = 1;
    batch.insert(
        &db.counts,
        keys::count_keyspace_key("gc_schema_version"),
        one.to_be_bytes(),
    );
    batch.commit().into_diagnostic()?;

    info!("block refcount migration complete");
    Ok(())
}

pub fn checkpoint_worker(state: Arc<crate::state::AppState>) {
    info!("block refs checkpoint worker started");
    loop {
        std::thread::sleep(Duration::from_secs(300));
        if let Err(e) = checkpoint(&state.db) {
            error!(err = %e, "block refs checkpoint failed");
        }
    }
}

fn checkpoint(db: &Db) -> miette::Result<()> {
    let checkpoint_seq = db.next_reflog_seq.load(Ordering::SeqCst).saturating_sub(1);

    let mut batch = db.inner.batch();

    db.block_refcounts.iter_sync(|k, v| {
        batch.insert(&db.block_refs, k.as_ref(), v.to_be_bytes());
        true
    });

    batch.insert(
        &db.cursors,
        keys::BLOCK_REFS_CHECKPOINT_SEQ_KEY,
        checkpoint_seq.to_be_bytes(),
    );

    // truncate reflog up to and including checkpoint_seq
    for guard in db.block_reflog.range(..=keys::reflog_key(checkpoint_seq)) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.block_reflog, k);
    }

    batch.commit().into_diagnostic()?;
    info!(seq = checkpoint_seq, "block refs checkpoint complete");
    Ok(())
}

pub fn ephemeral_startup_load_refcounts(db: &Db) -> miette::Result<()> {
    info!("rebuilding block refcounts from events (ephemeral mode)");

    for guard in db.events.iter() {
        let v = guard.value().into_diagnostic()?;
        let evt = rmp_serde::from_slice::<StoredEvent>(&v).into_diagnostic()?;
        let Some(cid) = evt.cid else {
            continue;
        };
        let cid_bytes = Slice::from(cid.to_bytes());
        let mut entry = db.block_refcounts.entry_sync(cid_bytes).or_insert(0);
        *entry += 1;
    }

    db.gc_ready.store(true, Ordering::SeqCst);
    info!("ephemeral block refcounts ready");
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

fn ephemeral_ttl_tick(db: &Db) -> miette::Result<()> {
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
        let Some(cid) = evt.cid else {
            continue;
        };
        let cid_bytes = Slice::from(cid.to_bytes());
        let mut entry = db.block_refcounts.entry_sync(cid_bytes).or_insert(0);
        *entry -= 1;
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

    if pruned > 0 {
        info!(pruned, "pruned old events");
    }

    Ok(())
}
