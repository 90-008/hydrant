use crate::db::{Db, keys};
use fjall::OwnedWriteBatch;
use miette::{Context, IntoDiagnostic, Result};
use smol_str::SmolStr;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use tracing::error;

#[derive(Debug, Clone, Default)]
pub struct CountDeltas {
    deltas: BTreeMap<SmolStr, i64>,
}

impl CountDeltas {
    fn add(&mut self, key: &str, delta: i64) {
        if delta == 0 {
            return;
        }

        let entry = self.deltas.entry(SmolStr::new(key)).or_insert(0);
        *entry += delta;
        if *entry == 0 {
            self.deltas.remove(key);
        }
    }

    pub(crate) fn add_repos(&mut self, delta: i64) {
        self.add("repos", delta);
    }

    #[cfg(feature = "indexer")]
    pub(crate) fn add_records(&mut self, delta: i64) {
        self.add("records", delta);
    }

    #[cfg(feature = "indexer")]
    pub(crate) fn add_blocks(&mut self, delta: i64) {
        self.add("blocks", delta);
    }

    pub(crate) fn add_pds_account(&mut self, host: &str, delta: i64) {
        self.add(&keys::pds_account_count_key(host), delta);
    }

    pub(crate) fn projected_pds_account_count(&self, db: &Db, host: &str) -> u64 {
        self.projected_count(db, &keys::pds_account_count_key(host))
    }

    #[cfg(feature = "indexer")]
    pub(crate) fn add_gauge_diff(
        &mut self,
        old: &crate::types::GaugeState,
        new: &crate::types::GaugeState,
    ) {
        use crate::types::GaugeState;

        if old == new {
            return;
        }

        match (old, new) {
            (GaugeState::Pending, GaugeState::Pending) => {}
            (GaugeState::Pending, _) => self.add("pending", -1),
            (_, GaugeState::Pending) => self.add("pending", 1),
            _ => {}
        }

        match (old.is_resync(), new.is_resync()) {
            (true, false) => self.add("resync", -1),
            (false, true) => self.add("resync", 1),
            _ => {}
        }

        if let GaugeState::Resync(Some(kind)) = old {
            self.add(
                match kind {
                    crate::types::ResyncErrorKind::Ratelimited => "error_ratelimited",
                    crate::types::ResyncErrorKind::Transport => "error_transport",
                    crate::types::ResyncErrorKind::Generic => "error_generic",
                },
                -1,
            );
        }

        if let GaugeState::Resync(Some(kind)) = new {
            self.add(
                match kind {
                    crate::types::ResyncErrorKind::Ratelimited => "error_ratelimited",
                    crate::types::ResyncErrorKind::Transport => "error_transport",
                    crate::types::ResyncErrorKind::Generic => "error_generic",
                },
                1,
            );
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.deltas.len()
    }

    pub(crate) fn get(&self, key: &str) -> i64 {
        self.deltas.get(key).copied().unwrap_or(0)
    }

    fn projected_count(&self, db: &Db, key: &str) -> u64 {
        apply_count_delta(db.get_count_sync(key), self.get(key))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SmolStr, &i64)> {
        self.deltas.iter()
    }
}

pub(crate) struct CountDeltaReservation {
    in_flight: Arc<Mutex<BTreeSet<u64>>>,
    start_id: u64,
}

impl Drop for CountDeltaReservation {
    fn drop(&mut self) {
        let Ok(mut in_flight) = self.in_flight.lock() else {
            error!(
                start_id = self.start_id,
                "count delta reservations poisoned"
            );
            return;
        };
        in_flight.remove(&self.start_id);
    }
}

pub(crate) fn apply_count_delta(current: u64, delta: i64) -> u64 {
    if delta >= 0 {
        current.saturating_add(delta as u64)
    } else {
        current.saturating_sub(delta.unsigned_abs())
    }
}

pub fn set_ks_count(batch: &mut OwnedWriteBatch, db: &Db, name: &str, count: u64) {
    let key = keys::count_keyspace_key(name);
    batch.insert(&db.counts, key, count.to_be_bytes());
}

pub fn set_count_delta_watermark(batch: &mut OwnedWriteBatch, db: &Db, watermark: u64) {
    batch.insert(
        &db.counts,
        keys::COUNT_DELTA_WATERMARK_KEY,
        watermark.to_be_bytes(),
    );
}

pub fn load_count_delta_watermark(db: &Db) -> Result<u64> {
    db.counts
        .get(keys::COUNT_DELTA_WATERMARK_KEY)
        .into_diagnostic()?
        .map(|value| read_u64_counter(&value))
        .transpose()
        .map(|watermark| watermark.unwrap_or(0))
}

pub(crate) fn read_u64_counter(value: &[u8]) -> Result<u64> {
    value
        .try_into()
        .into_diagnostic()
        .wrap_err("counter value must be 8 bytes")
        .map(u64::from_be_bytes)
}

fn read_i64_counter_delta(value: &[u8]) -> Result<i64> {
    value
        .try_into()
        .into_diagnostic()
        .wrap_err("counter delta must be 8 bytes")
        .map(i64::from_be_bytes)
}

pub(crate) fn replay_count_deltas(db: &Db, watermark: u64) -> Result<()> {
    let start = watermark.saturating_add(1);
    for (name, delta) in load_count_delta_range(db, start, u64::MAX)? {
        db.update_count(&name, delta);
    }
    Ok(())
}

fn load_count_delta_range(db: &Db, start: u64, end: u64) -> Result<BTreeMap<SmolStr, i64>> {
    let mut aggregated = BTreeMap::new();
    for guard in db.counts.range(keys::count_delta_start_key(start)..) {
        let (key, value) = guard.into_inner().into_diagnostic()?;
        if !key.starts_with(keys::COUNT_DELTA_PREFIX) {
            break;
        }

        let (id, name) = keys::parse_count_delta_key(&key)?;
        if id > end {
            break;
        }

        *aggregated.entry(SmolStr::new(name)).or_insert(0) += read_i64_counter_delta(&value)?;
    }
    Ok(aggregated)
}

fn get_persisted_ks_count(db: &Db, name: &str) -> Result<u64> {
    db.counts
        .get(keys::count_keyspace_key(name))
        .into_diagnostic()?
        .map(|value| read_u64_counter(&value))
        .transpose()
        .map(|count| count.unwrap_or(0))
}

impl Db {
    pub(crate) fn stage_count_deltas(
        &self,
        batch: &mut OwnedWriteBatch,
        deltas: &CountDeltas,
    ) -> Option<CountDeltaReservation> {
        if deltas.is_empty() {
            return None;
        }

        let start_id = self
            .next_count_delta_id
            .fetch_add(deltas.len() as u64, Ordering::SeqCst);
        self.count_delta_in_flight
            .lock()
            .expect("count delta reservations poisoned")
            .insert(start_id);

        for (offset, (key, delta)) in deltas.iter().enumerate() {
            batch.insert(
                &self.counts,
                keys::count_delta_key(start_id + offset as u64, key),
                delta.to_be_bytes(),
            );
        }

        Some(CountDeltaReservation {
            in_flight: self.count_delta_in_flight.clone(),
            start_id,
        })
    }

    pub(crate) fn apply_count_deltas(&self, deltas: &CountDeltas) {
        for (key, delta) in deltas.iter() {
            self.update_count(key, *delta);
        }
    }

    pub fn checkpoint_count_deltas(&self) -> Result<Option<u64>> {
        let start = self
            .count_delta_checkpoint_watermark
            .load(Ordering::SeqCst)
            .saturating_add(1);

        let mut end = self
            .next_count_delta_id
            .load(Ordering::SeqCst)
            .saturating_sub(1);

        let lowest_in_flight = self
            .count_delta_in_flight
            .lock()
            .expect("count delta reservations poisoned")
            .first()
            .copied();
        if let Some(lowest_in_flight) = lowest_in_flight {
            end = end.min(lowest_in_flight.saturating_sub(1));
        }

        if end < start {
            return Ok(None);
        }

        let aggregated = load_count_delta_range(self, start, end)?;
        let mut batch = self.inner.batch();

        for (name, delta) in aggregated {
            let current = get_persisted_ks_count(self, &name)?;
            set_ks_count(&mut batch, self, &name, apply_count_delta(current, delta));
        }

        set_count_delta_watermark(&mut batch, self, end);
        batch.commit().into_diagnostic()?;
        self.count_delta_checkpoint_watermark
            .store(end, Ordering::SeqCst);

        Ok(Some(end))
    }

    pub fn mark_count_checkpoint_persisted(&self, watermark: u64) {
        self.count_delta_gc_watermark
            .store(watermark, Ordering::SeqCst);
    }

    pub fn update_count(&self, key: &str, delta: i64) -> u64 {
        let mut entry = self.counts_map.entry_sync(SmolStr::new(key)).or_insert(0);
        if delta < 0 && *entry < delta.unsigned_abs() {
            error!(
                key,
                current = *entry,
                decrement = delta.unsigned_abs(),
                "count underflow !!! this is a bug"
            );
            *entry = 0;
        } else {
            *entry = apply_count_delta(*entry, delta);
        }
        *entry
    }

    pub async fn update_count_async(&self, key: &str, delta: i64) {
        let mut entry = self
            .counts_map
            .entry_async(SmolStr::new(key))
            .await
            .or_insert(0);
        if delta < 0 && *entry < delta.unsigned_abs() {
            error!(
                key,
                current = *entry,
                decrement = delta.unsigned_abs(),
                "count underflow !!! this is a bug"
            );
            *entry = 0;
        } else {
            *entry = apply_count_delta(*entry, delta);
        }
    }

    pub async fn get_count(&self, key: &str) -> u64 {
        self.counts_map
            .read_async(key, |_, v| *v)
            .await
            .unwrap_or(0)
    }

    pub fn get_count_sync(&self, key: &str) -> u64 {
        self.counts_map.read_sync(key, |_, v| *v).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(path: &std::path::Path) -> crate::config::Config {
        crate::config::Config {
            database_path: path.to_path_buf(),
            ..Default::default()
        }
    }

    #[test]
    fn count_deltas_replay_and_checkpoint_across_restart() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());

        {
            let db = Db::open(&cfg)?;
            let mut batch = db.inner.batch();
            set_ks_count(&mut batch, &db, "repos", 10);
            batch.commit().into_diagnostic()?;
            let mut batch = db.inner.batch();
            let mut deltas = CountDeltas::default();
            deltas.add_repos(2);
            #[cfg(feature = "indexer")]
            deltas.add_records(1);
            let reservation = db
                .stage_count_deltas(&mut batch, &deltas)
                .expect("count deltas should reserve ids");
            batch.commit().into_diagnostic()?;
            db.apply_count_deltas(&deltas);
            drop(reservation);
            db.persist()?;
        }

        {
            let db = Db::open(&cfg)?;
            assert_eq!(db.get_count_sync("repos"), 12);
            #[cfg(feature = "indexer")]
            assert_eq!(db.get_count_sync("records"), 1);
            let checkpointed_watermark = db
                .checkpoint_count_deltas()?
                .expect("checkpoint should fold pending deltas");
            db.persist()?;
            db.mark_count_checkpoint_persisted(checkpointed_watermark);
            assert_eq!(load_count_delta_watermark(&db)?, checkpointed_watermark);
        }

        {
            let db = Db::open(&cfg)?;
            assert_eq!(db.get_count_sync("repos"), 12);
            #[cfg(feature = "indexer")]
            assert_eq!(db.get_count_sync("records"), 1);
            assert!(load_count_delta_watermark(&db)? >= 1);
        }

        Ok(())
    }

    #[test]
    fn checkpoint_skips_inflight_deltas() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());
        let db = Db::open(&cfg)?;

        let mut batch = db.inner.batch();
        let mut deltas = CountDeltas::default();
        deltas.add_repos(1);
        let reservation = db
            .stage_count_deltas(&mut batch, &deltas)
            .expect("count deltas should reserve ids");

        assert!(db.checkpoint_count_deltas()?.is_none());

        batch.commit().into_diagnostic()?;
        db.apply_count_deltas(&deltas);
        drop(reservation);

        assert!(db.checkpoint_count_deltas()?.is_some());

        Ok(())
    }

    #[test]
    fn checkpointed_count_deltas_are_gcable() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());
        let db = Db::open(&cfg)?;

        let mut batch = db.inner.batch();
        let mut deltas = CountDeltas::default();
        deltas.add_repos(1);
        let reservation = db
            .stage_count_deltas(&mut batch, &deltas)
            .expect("count deltas should reserve ids");
        batch.commit().into_diagnostic()?;
        db.apply_count_deltas(&deltas);
        drop(reservation);

        let watermark = db
            .checkpoint_count_deltas()?
            .expect("checkpoint should fold pending deltas");
        db.persist()?;
        db.mark_count_checkpoint_persisted(watermark);

        db.counts.rotate_memtable_and_wait().into_diagnostic()?;
        db.counts.major_compact().into_diagnostic()?;

        assert_eq!(db.counts.prefix(keys::COUNT_DELTA_PREFIX).count(), 0);

        Ok(())
    }

    #[test]
    fn pds_account_count_migration_rebuilds_from_active_repos() -> Result<()> {
        use jacquard_common::CowStr;
        use jacquard_common::types::string::Did;

        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());

        {
            let db = Db::open(&cfg)?;
            let mut batch = db.inner.batch();

            let mut insert_repo = |did_str: &str, pds: &'static str, active: bool| -> Result<()> {
                let did = Did::new(did_str).into_diagnostic()?;
                let state = crate::types::v4::RepoState {
                    active,
                    status: crate::types::v4::RepoStatus::Synced,
                    root: None,
                    last_message_time: None,
                    last_updated_at: 0,
                    signing_key: None,
                    pds: Some(CowStr::Borrowed(pds)),
                    handle: None,
                };
                let bytes = rmp_serde::to_vec(&state).into_diagnostic()?;
                batch.insert(&db.repos, keys::repo_key(&did), bytes);
                Ok(())
            };

            insert_repo("did:web:one.test", "https://pds.example/", true)?;
            insert_repo("did:web:two.test", "https://pds.example/", true)?;
            insert_repo("did:web:inactive.test", "https://pds.example/", false)?;
            insert_repo("did:web:other.test", "https://other.example/", true)?;

            set_ks_count(
                &mut batch,
                &db,
                &keys::pds_account_count_key("pds.example"),
                100,
            );
            set_ks_count(
                &mut batch,
                &db,
                &keys::pds_account_count_key("other.example"),
                42,
            );
            set_ks_count(
                &mut batch,
                &db,
                &keys::pds_account_count_key("orphan.example"),
                7,
            );
            batch.insert(
                &db.counts,
                keys::count_delta_key(1, &keys::pds_account_count_key("pds.example")),
                5_i64.to_be_bytes(),
            );
            batch.insert(
                &db.counts,
                keys::count_delta_key(2, "repos"),
                1_i64.to_be_bytes(),
            );
            batch.insert(&db.counts, keys::VERSIONING_KEY, 5_u64.to_be_bytes());
            batch.commit().into_diagnostic()?;
            db.persist()?;
        }

        let db = Db::open(&cfg)?;
        assert_eq!(
            db.get_count_sync(&keys::pds_account_count_key("pds.example")),
            2
        );
        assert_eq!(
            db.get_count_sync(&keys::pds_account_count_key("other.example")),
            1
        );
        assert_eq!(
            db.get_count_sync(&keys::pds_account_count_key("orphan.example")),
            0
        );
        assert_eq!(db.get_count_sync("repos"), 1);

        let mut pds_delta_count = 0;
        for guard in db.counts.prefix(keys::COUNT_DELTA_PREFIX) {
            let key = guard.key().into_diagnostic()?;
            let (_, name) = keys::parse_count_delta_key(&key)?;
            if name.starts_with("p|") {
                pds_delta_count += 1;
            }
        }
        assert_eq!(pds_delta_count, 0);

        Ok(())
    }
}
