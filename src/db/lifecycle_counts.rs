use std::collections::BTreeMap;
use std::sync::MutexGuard;

use fjall::OwnedWriteBatch;
use jacquard_common::types::string::Did;
use miette::{IntoDiagnostic, Result, WrapErr};

use crate::types::{GaugeState, ResyncErrorKind, ResyncState};

use super::{CountDeltaReservation, CountDeltas, Db, deser_repo_meta, keys};

pub(crate) struct LifecycleCountBatch<'a> {
    db: &'a Db,
    _lock: MutexGuard<'a, ()>,
    deltas: CountDeltas,
    staged: BTreeMap<Vec<u8>, GaugeState>,
}

pub(crate) struct LifecycleCountReservation<'a> {
    _lock: MutexGuard<'a, ()>,
    _count_reservation: Option<CountDeltaReservation>,
    deltas: CountDeltas,
}

impl Db {
    pub(crate) fn lifecycle_counts(&self) -> LifecycleCountBatch<'_> {
        LifecycleCountBatch {
            db: self,
            _lock: self
                .lifecycle_count_lock
                .lock()
                .expect("lifecycle count lock poisoned"),
            deltas: CountDeltas::default(),
            staged: BTreeMap::new(),
        }
    }

    pub(crate) fn apply_lifecycle_counts(&self, reservation: LifecycleCountReservation<'_>) {
        self.apply_count_deltas(&reservation.deltas);
        drop(reservation);
    }
}

impl<'a> LifecycleCountBatch<'a> {
    pub(crate) fn transition(
        &mut self,
        batch: &mut OwnedWriteBatch,
        did: &Did<'_>,
        new: GaugeState,
    ) -> Result<bool> {
        let did_key = keys::repo_key(did);
        let (old, had_projection) = self.current_gauge(did, &did_key)?;

        if old != new {
            self.deltas.add_gauge_diff(&old, &new);
            self.stage_membership(batch, did, &did_key, new);
            return Ok(true);
        }

        if !had_projection {
            self.stage_membership(batch, did, &did_key, new);
        }

        Ok(false)
    }

    pub(crate) fn transition_pending_key(
        &mut self,
        batch: &mut OwnedWriteBatch,
        did: &Did<'_>,
        pending_key: &[u8],
        new: GaugeState,
    ) -> Result<bool> {
        if !self.pending_key_matches(did, pending_key)? {
            return Ok(false);
        }

        self.transition(batch, did, new)?;
        Ok(true)
    }

    pub(crate) fn stage(self, batch: &mut OwnedWriteBatch) -> LifecycleCountReservation<'a> {
        let count_reservation = self.db.stage_count_deltas(batch, &self.deltas);
        LifecycleCountReservation {
            _lock: self._lock,
            _count_reservation: count_reservation,
            deltas: self.deltas,
        }
    }

    fn current_gauge(&self, did: &Did<'_>, did_key: &[u8]) -> Result<(GaugeState, bool)> {
        if let Some(staged) = self.staged.get(did_key) {
            return Ok((*staged, true));
        }

        let projection_key = keys::count_gauge_key(did);
        if let Some(bytes) = self.db.counts.get(&projection_key).into_diagnostic()? {
            return decode_gauge(bytes.as_ref()).map(|gauge| (gauge, true));
        }

        if self.db.repos.get(did_key).into_diagnostic()?.is_some() {
            let metadata_key = keys::repo_metadata_key(did);
            let index_id = self
                .db
                .repo_metadata
                .get(&metadata_key)
                .into_diagnostic()?
                .and_then(|bytes| deser_repo_meta(bytes.as_ref()).ok())
                .map(|meta| meta.index_id);

            let mut is_pending = false;
            if let Some(index_id) = index_id {
                if self
                    .db
                    .pending
                    .get(keys::pending_key(index_id))
                    .into_diagnostic()?
                    .is_some()
                {
                    is_pending = true;
                }
            }

            let gauge = if is_pending {
                GaugeState::Pending
            } else if let Some(resync_bytes) = self.db.resync.get(did_key).into_diagnostic()? {
                gauge_from_resync(&resync_bytes)
            } else {
                GaugeState::Synced
            };

            return Ok((gauge, false));
        }

        Ok((GaugeState::Synced, false))
    }

    fn pending_key_matches(&self, did: &Did<'_>, pending_key: &[u8]) -> Result<bool> {
        let Some(current) = current_pending_key(self.db, did)? else {
            return Ok(false);
        };

        Ok(current.as_slice() == pending_key
            && self.db.pending.get(current).into_diagnostic()?.is_some())
    }

    fn stage_membership(
        &mut self,
        batch: &mut OwnedWriteBatch,
        did: &Did<'_>,
        did_key: &[u8],
        new: GaugeState,
    ) {
        let projection_key = keys::count_gauge_key(did);
        batch.insert(&self.db.counts, projection_key, encode_gauge(new));
        self.staged.insert(did_key.to_vec(), new);
    }
}

fn current_pending_key(db: &Db, did: &Did<'_>) -> Result<Option<[u8; 8]>> {
    db.repo_metadata
        .get(keys::repo_metadata_key(did))
        .into_diagnostic()?
        .map(|bytes| {
            deser_repo_meta(bytes.as_ref())
                .map(|metadata| keys::pending_key(metadata.index_id))
                .wrap_err_with(|| format!("invalid repo metadata for {did}"))
        })
        .transpose()
}

pub(super) fn gauge_from_resync(bytes: &[u8]) -> GaugeState {
    rmp_serde::from_slice::<ResyncState>(bytes)
        .map(|state| match state {
            ResyncState::Error { kind, .. } => GaugeState::Resync(Some(kind)),
            ResyncState::Gone { .. } => GaugeState::Resync(None),
        })
        .unwrap_or(GaugeState::Resync(None))
}

pub(super) fn encode_gauge(gauge: GaugeState) -> [u8; 1] {
    match gauge {
        GaugeState::Synced => [0],
        GaugeState::Pending => [1],
        GaugeState::Resync(None) => [2],
        GaugeState::Resync(Some(ResyncErrorKind::Ratelimited)) => [3],
        GaugeState::Resync(Some(ResyncErrorKind::Transport)) => [4],
        GaugeState::Resync(Some(ResyncErrorKind::Generic)) => [5],
    }
}

fn decode_gauge(bytes: &[u8]) -> Result<GaugeState> {
    match bytes {
        [0] => Ok(GaugeState::Synced),
        [1] => Ok(GaugeState::Pending),
        [2] => Ok(GaugeState::Resync(None)),
        [3] => Ok(GaugeState::Resync(Some(ResyncErrorKind::Ratelimited))),
        [4] => Ok(GaugeState::Resync(Some(ResyncErrorKind::Transport))),
        [5] => Ok(GaugeState::Resync(Some(ResyncErrorKind::Generic))),
        _ => miette::bail!("invalid lifecycle gauge projection bytes: {bytes:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::db::{ser_repo_meta, ser_repo_state};
    use crate::types::{RepoMetadata, RepoState};
    use jacquard_common::IntoStatic;

    fn test_config(path: &std::path::Path) -> Config {
        Config {
            database_path: path.to_path_buf(),
            ..Default::default()
        }
    }

    fn test_did() -> Result<Did<'static>> {
        Did::new("did:plc:yk4q3id7id6p5z3bypvshc64")
            .into_diagnostic()
            .map(IntoStatic::into_static)
    }

    fn enqueue_pending(db: &Db, did: &Did<'_>, index_id: u64) -> Result<[u8; 8]> {
        let did_key = keys::repo_key(did);
        let metadata_key = keys::repo_metadata_key(did);
        let pending_key = keys::pending_key(index_id);
        let state = RepoState::backfilling();
        let metadata = RepoMetadata::backfilling(index_id);

        let mut batch = db.inner.batch();
        batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
        batch.insert(&db.repo_metadata, metadata_key, ser_repo_meta(&metadata)?);
        batch.insert(&db.pending, pending_key, did_key);
        let mut lifecycle = db.lifecycle_counts();
        lifecycle.transition(&mut batch, did, GaugeState::Pending)?;
        let lifecycle_reservation = lifecycle.stage(&mut batch);
        batch.commit().into_diagnostic()?;
        db.apply_lifecycle_counts(lifecycle_reservation);
        Ok(pending_key)
    }

    fn complete_pending(db: &Db, did: &Did<'_>, pending_key: [u8; 8]) -> Result<bool> {
        let mut batch = db.inner.batch();
        let mut lifecycle = db.lifecycle_counts();
        let applied = lifecycle.transition_pending_key(
            &mut batch,
            did,
            pending_key.as_slice(),
            GaugeState::Synced,
        )?;
        if applied {
            batch.remove(&db.pending, pending_key);
        }
        let lifecycle_reservation = lifecycle.stage(&mut batch);
        batch.commit().into_diagnostic()?;
        db.apply_lifecycle_counts(lifecycle_reservation);
        Ok(applied)
    }

    #[test]
    fn duplicate_pending_completion_is_idempotent() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let db = Db::open(&test_config(tmp.path()))?;
        let did = test_did()?;
        let pending_key = enqueue_pending(&db, &did, 1)?;

        assert_eq!(db.get_count_sync("pending"), 1);
        assert!(complete_pending(&db, &did, pending_key)?);
        assert_eq!(db.get_count_sync("pending"), 0);

        assert!(!complete_pending(&db, &did, pending_key)?);
        assert_eq!(db.get_count_sync("pending"), 0);

        Ok(())
    }

    #[test]
    fn stale_pending_key_cannot_complete_current_pending_state() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let db = Db::open(&test_config(tmp.path()))?;
        let did = test_did()?;
        let stale_pending = enqueue_pending(&db, &did, 1)?;
        let current_pending = keys::pending_key(2);
        let did_key = keys::repo_key(&did);
        let metadata_key = keys::repo_metadata_key(&did);

        let mut metadata = RepoMetadata::backfilling(2);
        metadata.tracked = true;
        let mut batch = db.inner.batch();
        batch.remove(&db.pending, stale_pending);
        batch.insert(&db.pending, current_pending, &did_key);
        batch.insert(&db.repo_metadata, metadata_key, ser_repo_meta(&metadata)?);
        let mut lifecycle = db.lifecycle_counts();
        lifecycle.transition(&mut batch, &did, GaugeState::Pending)?;
        let lifecycle_reservation = lifecycle.stage(&mut batch);
        batch.commit().into_diagnostic()?;
        db.apply_lifecycle_counts(lifecycle_reservation);

        assert_eq!(db.get_count_sync("pending"), 1);
        assert!(!complete_pending(&db, &did, stale_pending)?);
        assert_eq!(db.get_count_sync("pending"), 1);
        assert!(db.pending.get(current_pending).into_diagnostic()?.is_some());

        Ok(())
    }

    #[test]
    fn lifecycle_delta_replays_after_crash_before_apply() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());
        let did = test_did()?;

        {
            let db = Db::open(&cfg)?;
            let pending_key = enqueue_pending(&db, &did, 1)?;
            db.persist()?;

            let mut batch = db.inner.batch();
            let mut lifecycle = db.lifecycle_counts();
            let applied = lifecycle.transition_pending_key(
                &mut batch,
                &did,
                pending_key.as_slice(),
                GaugeState::Synced,
            )?;
            assert!(applied);
            batch.remove(&db.pending, pending_key);
            let lifecycle_reservation = lifecycle.stage(&mut batch);
            batch.commit().into_diagnostic()?;
            db.persist()?;
            drop(lifecycle_reservation);
        }

        {
            let db = Db::open(&cfg)?;
            assert_eq!(db.get_count_sync("pending"), 0);
            assert!(
                db.pending
                    .get(keys::pending_key(1))
                    .into_diagnostic()?
                    .is_none()
            );
        }

        Ok(())
    }
}
