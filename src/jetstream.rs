use std::sync::atomic::Ordering;

use fjall::OwnedWriteBatch;
use miette::{IntoDiagnostic, Result};

use crate::db::{Db, keys};
use crate::types::{JetstreamBroadcast, StoredJetstreamEvent};

pub(crate) fn stage_event(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    event: StoredJetstreamEvent<'_>,
) -> Result<JetstreamBroadcast> {
    let id = db.next_jetstream_id.fetch_add(1, Ordering::SeqCst);
    let time_us = next_time_us(db);
    let event = event.into_static();
    let bytes = rmp_serde::to_vec(&event).into_diagnostic()?;
    batch.insert(
        &db.jetstream_events,
        keys::jetstream_event_key(time_us as u64, id),
        bytes,
    );
    Ok(JetstreamBroadcast { id, time_us, event })
}

fn next_time_us(db: &Db) -> i64 {
    loop {
        let last = db.last_jetstream_time_us.load(Ordering::SeqCst);
        let now = chrono::Utc::now().timestamp_micros();
        let next = now.max(last.saturating_add(1));
        if db
            .last_jetstream_time_us
            .compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return next;
        }
    }
}
