use crate::db::{Db, keys};
use fjall::Keyspace;
use miette::{IntoDiagnostic, WrapErr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{debug, error, info};

#[cfg(feature = "indexer")]
pub fn ephemeral_ttl_worker(state: Arc<crate::state::AppState>) {
    info!("ephemeral TTL worker started");
    loop {
        std::thread::sleep(Duration::from_secs(60));
        if let Err(e) = ephemeral_ttl_tick(&state.db, &state.ephemeral_ttl) {
            error!(err = %e, "ephemeral TTL tick failed");
        }
    }
}

#[cfg(feature = "relay")]
pub fn relay_events_ttl_worker(state: Arc<crate::state::AppState>) {
    info!("relay events TTL worker started");
    loop {
        std::thread::sleep(Duration::from_secs(60));
        if let Err(e) = relay_events_ttl_tick(&state.db, &state.ephemeral_ttl) {
            error!(err = %e, "relay events TTL tick failed");
        }
    }
}

#[cfg(feature = "indexer")]
pub fn ephemeral_ttl_tick(db: &Db, ttl: &Duration) -> miette::Result<()> {
    let current_seq = db.next_event_id.load(Ordering::SeqCst);
    ttl_tick_inner(
        db,
        ttl,
        keys::EVENT_WATERMARK_PREFIX,
        keys::event_watermark_key,
        &db.events,
        current_seq,
    )
}

#[cfg(feature = "relay")]
pub fn relay_events_ttl_tick(db: &Db, ttl: &Duration) -> miette::Result<()> {
    let current_seq = db.next_relay_seq.load(Ordering::SeqCst);
    ttl_tick_inner(
        db,
        ttl,
        keys::RELAY_EVENT_WATERMARK_PREFIX,
        keys::relay_event_watermark_key,
        &db.relay_events,
        current_seq,
    )
}

fn ttl_tick_inner(
    db: &Db,
    ttl: &Duration,
    watermark_prefix: &'static [u8],
    watermark_key: fn(u64) -> Vec<u8>,
    events_ks: &Keyspace,
    current_seq: u64,
) -> miette::Result<()> {
    let now = chrono::Utc::now().timestamp() as u64;
    let cutoff_ts = now.saturating_sub(ttl.as_secs());

    // write current watermark
    db.cursors
        .insert(watermark_key(now), current_seq.to_be_bytes())
        .into_diagnostic()?;

    // find the watermark entry closest to and <= cutoff_ts
    let cutoff_key = watermark_key(cutoff_ts);
    let cutoff_seq = db
        .cursors
        .range(..=cutoff_key.as_slice())
        .next_back()
        .map(|g| g.into_inner().into_diagnostic())
        .transpose()?
        .filter(|(k, _)| k.starts_with(watermark_prefix))
        .map(|(_, v)| {
            v.as_ref()
                .try_into()
                .into_diagnostic()
                .wrap_err("expected cutoff seq to be u64")
        })
        .transpose()?
        .map(u64::from_be_bytes);

    let Some(cutoff_seq) = cutoff_seq else {
        // no watermark old enough yet, nothing to prune
        return Ok(());
    };

    let cutoff_key_events = keys::event_key(cutoff_seq);
    let mut batch = db.inner.batch();
    let mut pruned = 0usize;

    for guard in events_ks.range(..cutoff_key_events) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(events_ks, k);
        pruned += 1;
    }

    // clean up consumed watermark entries (everything up to and including cutoff_ts)
    for guard in db.cursors.range(..=cutoff_key) {
        let k = guard.key().into_diagnostic()?;
        if k.starts_with(watermark_prefix) {
            batch.remove(&db.cursors, k);
        }
    }

    batch.commit().into_diagnostic()?;

    debug!(pruned, "pruned old events");

    Ok(())
}
