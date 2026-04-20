#[cfg(any(feature = "indexer_stream", feature = "relay"))]
use {
    crate::db::{Db, keys},
    fjall::Keyspace,
    miette::{IntoDiagnostic, WrapErr},
    std::sync::Arc,
    std::sync::atomic::Ordering,
    std::time::Duration,
    tracing::{debug, error, info},
};

#[cfg(feature = "indexer_stream")]
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

#[cfg(feature = "indexer_stream")]
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

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
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
    let start: &[u8] = watermark_prefix;
    let end: &[u8] = cutoff_key.as_slice();

    let cutoff_seq = db
        .cursors
        .range(start..=end)
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

    let mut pruned_key = Vec::with_capacity(7 + watermark_prefix.len());
    pruned_key.extend_from_slice(b"pruned|");
    pruned_key.extend_from_slice(watermark_prefix);

    let last_pruned_seq = db
        .cursors
        .get(&pruned_key)
        .into_diagnostic()?
        .map(|v| {
            v.as_ref()
                .try_into()
                .into_diagnostic()
                .wrap_err("expected last pruned seq to be u64")
        })
        .transpose()?
        .map(u64::from_be_bytes)
        .unwrap_or(0);

    let start_key_events = keys::event_key(last_pruned_seq);
    let cutoff_key_events = keys::event_key(cutoff_seq);
    let mut batch = db.inner.batch();
    let mut pruned = 0usize;

    for guard in events_ks.range(start_key_events..cutoff_key_events) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(events_ks, k);
        pruned += 1;
    }

    batch.insert(&db.cursors, pruned_key, cutoff_seq.to_be_bytes());

    // clean up consumed watermark entries (everything up to and including cutoff_ts)
    let start: &[u8] = watermark_prefix;
    let end: &[u8] = cutoff_key.as_slice();
    for guard in db.cursors.range(start..=end) {
        let k = guard.key().into_diagnostic()?;
        if k.starts_with(watermark_prefix) {
            batch.remove(&db.cursors, k);
        }
    }

    batch.commit().into_diagnostic()?;

    if pruned > 0 {
        info!(pruned, "pruned old events");
    } else {
        debug!("no events were pruned");
    }

    Ok(())
}
