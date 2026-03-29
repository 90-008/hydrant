use crate::db::{Db, keys};
use miette::{IntoDiagnostic, WrapErr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{debug, error, info};

pub fn ephemeral_ttl_worker(state: Arc<crate::state::AppState>) {
    info!("ephemeral TTL worker started");
    loop {
        std::thread::sleep(Duration::from_secs(60));
        if let Err(e) = ephemeral_ttl_tick(&state.db, &state.ephemeral_ttl) {
            error!(err = %e, "ephemeral TTL tick failed");
        }
    }
}

pub fn ephemeral_ttl_tick(db: &Db, ttl: &Duration) -> miette::Result<()> {
    let now = chrono::Utc::now().timestamp() as u64;
    let cutoff_ts = now.saturating_sub(ttl.as_secs());

    // write current watermark
    #[cfg(feature = "events")]
    let current_event_id = db.next_event_id.load(Ordering::SeqCst);
    #[cfg(not(feature = "events"))]
    let current_event_id = 0u64;
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
        .range(..=cutoff_key.as_slice())
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
        let k = guard.key().into_diagnostic()?;
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
