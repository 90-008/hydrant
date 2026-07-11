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
        #[cfg(feature = "jetstream")]
        if let Err(e) = jetstream_events_ttl_tick(&state.db, &state.ephemeral_ttl) {
            error!(err = %e, "jetstream TTL tick failed");
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
        #[cfg(feature = "jetstream")]
        if let Err(e) = jetstream_events_ttl_tick(&state.db, &state.ephemeral_ttl) {
            error!(err = %e, "jetstream TTL tick failed");
        }
    }
}

#[cfg(feature = "indexer_stream")]
pub fn ephemeral_ttl_tick(db: &Db, ttl: &Duration) -> miette::Result<()> {
    let current_seq = db.stream.next_event_id.load(Ordering::SeqCst);
    ttl_tick_inner(
        db,
        ttl,
        keys::EVENT_WATERMARK_PREFIX,
        keys::event_watermark_key,
        &db.stream.events,
        current_seq,
    )
}

#[cfg(feature = "relay")]
pub fn relay_events_ttl_tick(db: &Db, ttl: &Duration) -> miette::Result<()> {
    let current_seq = db.relay.next_seq.load(Ordering::SeqCst);
    ttl_tick_inner(
        db,
        ttl,
        keys::RELAY_EVENT_WATERMARK_PREFIX,
        keys::relay_event_watermark_key,
        &db.relay.events,
        current_seq,
    )
}

#[cfg(feature = "jetstream")]
pub fn jetstream_events_ttl_tick(db: &Db, ttl: &Duration) -> miette::Result<()> {
    let now = chrono::Utc::now().timestamp() as u64;
    let cutoff_ts = now.saturating_sub(ttl.as_secs());
    let cutoff_us = cutoff_ts.saturating_mul(1_000_000);

    db.jetstream.events
        .rotate_memtable_and_wait()
        .into_diagnostic()
        .wrap_err("failed to rotate memtable before Jetstream TTL range drop")?;

    let before_space = db.jetstream.events.disk_space();
    let before_tables = db.jetstream.events.table_count();
    db.jetstream.events
        .drop_range(..keys::jetstream_event_key(cutoff_us, 0))
        .into_diagnostic()
        .wrap_err("failed Jetstream TTL range drop for old events")?;
    let after_space = db.jetstream.events.disk_space();
    let after_tables = db.jetstream.events.table_count();

    info!(
        cutoff_us,
        reclaimed_bytes = before_space.saturating_sub(after_space),
        dropped_tables = before_tables.saturating_sub(after_tables),
        before_space,
        after_space,
        before_tables,
        after_tables,
        "dropped old Jetstream events for TTL"
    );

    Ok(())
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

    let mut dropped_key = Vec::with_capacity(8 + watermark_prefix.len());
    dropped_key.extend_from_slice(b"dropped|");
    dropped_key.extend_from_slice(watermark_prefix);

    let last_dropped_cutoff_seq = db
        .cursors
        .get(&dropped_key)
        .into_diagnostic()?
        .map(|v| {
            v.as_ref()
                .try_into()
                .into_diagnostic()
                .wrap_err("expected last dropped cutoff seq to be u64")
        })
        .transpose()?
        .map(u64::from_be_bytes)
        .unwrap_or(0);

    let drop_stats = (cutoff_seq > last_dropped_cutoff_seq)
        .then(|| -> miette::Result<_> {
            events_ks
                .rotate_memtable_and_wait()
                .into_diagnostic()
                .wrap_err("failed to rotate memtable before TTL range drop")?;

            let before_space = events_ks.disk_space();
            let before_tables = events_ks.table_count();

            events_ks
                .drop_range(..keys::event_key(cutoff_seq))
                .into_diagnostic()
                .wrap_err("failed TTL range drop for old events")?;

            let after_space = events_ks.disk_space();
            let after_tables = events_ks.table_count();

            Ok((before_space, after_space, before_tables, after_tables))
        })
        .transpose()?;

    let mut batch = db.inner.batch();
    if drop_stats.is_some() {
        batch.insert(&db.cursors, dropped_key, cutoff_seq.to_be_bytes());
    }

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

    if let Some((before_space, after_space, before_tables, after_tables)) = drop_stats {
        info!(
            cutoff_seq,
            reclaimed_bytes = before_space.saturating_sub(after_space),
            dropped_tables = before_tables.saturating_sub(after_tables),
            before_space,
            after_space,
            before_tables,
            after_tables,
            "dropped old event tables for TTL"
        );
    } else {
        debug!(cutoff_seq, "no new event TTL range to drop");
    }

    Ok(())
}

#[cfg(all(test, feature = "relay"))]
mod tests {
    use super::*;
    use crate::config::{Compression, Config};
    use miette::IntoDiagnostic;

    fn read_relay_seq(key: &[u8]) -> miette::Result<u64> {
        key.try_into()
            .into_diagnostic()
            .wrap_err("relay event key must be 8 bytes")
            .map(u64::from_be_bytes)
    }

    fn first_relay_seq(db: &crate::db::Db) -> miette::Result<u64> {
        let Some(guard) = db.relay.events.iter().next() else {
            miette::bail!("expected at least one relay event");
        };
        let key = guard.key().into_diagnostic()?;
        read_relay_seq(&key)
    }

    fn open_test_relay_db() -> miette::Result<(tempfile::TempDir, crate::db::Db)> {
        let dir = tempfile::tempdir().into_diagnostic()?;
        let cfg = Config {
            database_path: dir.path().to_path_buf(),
            cache_size: 16,
            data_compression: Compression::None,
            journal_compression: Compression::None,
            db_worker_threads: 2,
            db_events_memtable_size_mb: 1,
            ..Config::default()
        };
        let db = crate::db::Db::open(&cfg)?;

        Ok((dir, db))
    }

    fn insert_relay_events(
        db: &crate::db::Db,
        start_seq: u64,
        count: u64,
        payload: &[u8],
    ) -> miette::Result<()> {
        let mut batch = db.inner.batch();
        for seq in start_seq..start_seq + count {
            batch.insert(&db.relay.events, keys::relay_event_key(seq), payload);
        }
        batch.commit().into_diagnostic()?;
        db.relay.next_seq.store(start_seq + count, Ordering::SeqCst);
        Ok(())
    }

    fn seed_relay_event_table(
        db: &crate::db::Db,
        start_seq: u64,
        count: u64,
        payload: &[u8],
    ) -> miette::Result<()> {
        insert_relay_events(db, start_seq, count, payload)?;
        db.relay.events
            .rotate_memtable_and_wait()
            .into_diagnostic()?;
        Ok(())
    }

    fn seed_compacted_relay_events(
        db: &crate::db::Db,
        count: u64,
        payload: &[u8],
    ) -> miette::Result<()> {
        seed_relay_event_table(db, 0, count, payload)?;
        db.relay.events.major_compact().into_diagnostic()?;
        Ok(())
    }

    fn seed_past_relay_watermark(db: &crate::db::Db, cutoff_seq: u64) -> miette::Result<()> {
        let past_ts = chrono::Utc::now().timestamp() as u64 - 60 * 60;
        db.cursors
            .insert(
                keys::relay_event_watermark_key(past_ts),
                cutoff_seq.to_be_bytes(),
            )
            .into_diagnostic()
    }

    fn compact_relay_events_once(db: &crate::db::Db) -> miette::Result<()> {
        db.relay.events
            .compact(Arc::new(fjall::compaction::Leveled::default()))
            .into_diagnostic()
    }

    fn wait_for_relay_table_count(db: &crate::db::Db, min_tables: usize) -> miette::Result<()> {
        for _ in 0..200 {
            if db.relay.events.table_count() >= min_tables {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        miette::bail!(
            "timed out waiting for at least {min_tables} relay event tables, saw {}",
            db.relay.events.table_count()
        );
    }

    fn prune_prefix_with_per_key_tombstones(
        db: &crate::db::Db,
        cutoff_seq: u64,
    ) -> miette::Result<()> {
        let mut batch = db.inner.batch();
        for seq in 0..cutoff_seq {
            batch.remove(&db.relay.events, keys::relay_event_key(seq));
        }
        batch.commit().into_diagnostic()?;
        db.relay.events
            .rotate_memtable_and_wait()
            .into_diagnostic()?;
        Ok(())
    }

    #[test]
    fn relay_ttl_tick_drops_old_append_only_event_tables() -> miette::Result<()> {
        let (_dir, db) = open_test_relay_db()?;
        let old_tables = 2u64;
        let retained_tables = 1u64;
        let events_per_table = 64u64;
        let old_count = old_tables * events_per_table;
        let retained_count = retained_tables * events_per_table;
        let payload = vec![0x42; 4 * 1024];

        let mut start_seq = 0;
        for _ in 0..old_tables + retained_tables {
            seed_relay_event_table(&db, start_seq, events_per_table, &payload)?;
            start_seq += events_per_table;
        }

        let before_prune = db.relay.events.disk_space();
        let before_tables = db.relay.events.table_count();
        seed_past_relay_watermark(&db, old_count)?;

        relay_events_ttl_tick(&db, &Duration::from_secs(60 * 60))?;

        let after_prune = db.relay.events.disk_space();
        let after_tables = db.relay.events.table_count();
        assert_eq!(
            retained_count as usize,
            db.relay.events.iter().count(),
            "TTL should keep the tables at or after the cutoff sequence"
        );
        assert!(
            after_tables <= before_tables.saturating_sub(old_tables as usize),
            "TTL drop_range should remove old relay event tables; before={before_tables}, after={after_tables}"
        );
        assert!(
            after_prune < before_prune / 2,
            "TTL drop_range should reclaim old relay event tables; before={before_prune}, after={after_prune}"
        );

        Ok(())
    }

    #[test]
    fn relay_ttl_drop_range_retains_only_cutoff_boundary_table() -> miette::Result<()> {
        let (_dir, db) = open_test_relay_db()?;
        let table_count = 5u64;
        let events_per_table = 64u64;
        let fully_expired_tables = 2u64;
        let cutoff_seq = fully_expired_tables * events_per_table + events_per_table / 2;
        let payload = vec![0x42; 4 * 1024];

        let mut start_seq = 0;
        for _ in 0..table_count {
            seed_relay_event_table(&db, start_seq, events_per_table, &payload)?;
            compact_relay_events_once(&db)?;
            start_seq += events_per_table;
        }

        let before_tables = db.relay.events.table_count();
        seed_past_relay_watermark(&db, cutoff_seq)?;

        relay_events_ttl_tick(&db, &Duration::from_secs(60 * 60))?;

        let after_tables = db.relay.events.table_count();
        assert!(
            after_tables <= before_tables.saturating_sub(fully_expired_tables as usize),
            "drop_range should drop tables fully below the cutoff; before={before_tables}, after={after_tables}"
        );
        assert_eq!(
            fully_expired_tables * events_per_table,
            first_relay_seq(&db)?,
            "the table that straddles the cutoff should be retained"
        );
        assert_eq!(
            ((table_count - fully_expired_tables) * events_per_table) as usize,
            db.relay.events.iter().count(),
            "only the boundary table and newer tables should remain"
        );

        Ok(())
    }

    #[test]
    fn relay_ttl_drop_range_reclaims_after_sustained_auto_flushes() -> miette::Result<()> {
        let (_dir, db) = open_test_relay_db()?;
        let events_per_second = 600u64;
        let total_seconds = 8u64;
        let ttl_seconds = 4u64;
        let payload = vec![0x42; 2 * 1024];

        for second in 0..total_seconds {
            insert_relay_events(&db, second * events_per_second, events_per_second, &payload)?;
            wait_for_relay_table_count(&db, second as usize + 1)?;
            compact_relay_events_once(&db)?;
        }

        let cutoff_seq = events_per_second * ttl_seconds;
        let before_space = db.relay.events.disk_space();
        let before_tables = db.relay.events.table_count();
        assert!(
            before_tables >= total_seconds as usize,
            "sustained writes should have produced multiple SSTs; tables={before_tables}"
        );

        seed_past_relay_watermark(&db, cutoff_seq)?;
        relay_events_ttl_tick(&db, &Duration::from_secs(60 * 60))?;

        let after_space = db.relay.events.disk_space();
        let after_tables = db.relay.events.table_count();
        let first_seq = first_relay_seq(&db)?;
        let remaining_events = db.relay.events.iter().count() as u64;

        assert!(
            after_tables < before_tables,
            "TTL drop_range should remove old SSTs while newer writes remain; before={before_tables}, after={after_tables}"
        );
        assert!(
            after_space < before_space * 3 / 4,
            "TTL drop_range should reclaim a substantial old prefix; before={before_space}, after={after_space}"
        );
        assert!(
            first_seq >= cutoff_seq.saturating_sub(events_per_second),
            "boundary retention should be bounded to roughly one simulated second; cutoff={cutoff_seq}, first={first_seq}"
        );
        assert!(
            first_seq <= cutoff_seq,
            "drop_range should not drop newer-than-cutoff relay events; cutoff={cutoff_seq}, first={first_seq}"
        );
        assert!(
            remaining_events <= (total_seconds - ttl_seconds + 1) * events_per_second,
            "retained relay events should be bounded to the live window plus one boundary SST; remaining={remaining_events}"
        );

        Ok(())
    }

    #[test]
    fn relay_leveled_compaction_does_not_reclaim_prefix_tombstones() -> miette::Result<()> {
        let (_dir, db) = open_test_relay_db()?;
        let old_count = 512u64;
        let retained_count = 512u64;
        let payload = vec![0x42; 8 * 1024];

        seed_compacted_relay_events(&db, old_count + retained_count, &payload)?;
        let before_prune = db.relay.events.disk_space();
        prune_prefix_with_per_key_tombstones(&db, old_count)?;
        let after_delete = db.relay.events.disk_space();

        for _ in 0..16 {
            db.relay.events
                .compact(Arc::new(fjall::compaction::Leveled::default()))
                .into_diagnostic()?;
        }

        let after_leveled = db.relay.events.disk_space();
        assert_eq!(retained_count as usize, db.relay.events.iter().count());
        assert!(
            after_leveled >= before_prune * 9 / 10,
            "leveled compaction should not be expected to reclaim prefix tombstones quickly; before={before_prune}, after_delete={after_delete}, after_leveled={after_leveled}"
        );

        Ok(())
    }
}
