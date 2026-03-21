use crate::db::{keys, ser_repo_state};
use crate::state::AppState;
use crate::types::RepoState;
use miette::{IntoDiagnostic, Result};
use rand::Rng;
use rand::rngs::SmallRng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace};

use super::{CrawlerStats, InFlightGuard};

const WORKER_CHANNEL_CAPACITY: usize = 64;
const BLOCKING_TASK_TIMEOUT: Duration = Duration::from_secs(30);

/// a cursor write to include atomically with the batch.
pub(crate) struct CursorUpdate {
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
}

/// a batch of confirmed repos from any crawler source ready to enqueue.
///
/// `guards` hold the DIDs (via `Deref`) and keep their in-flight slots occupied
/// until the batch is committed to the database.  `cursor_update`, if present,
/// is committed atomically with the repos/pending inserts.
pub(crate) struct CrawlerBatch {
    pub(super) guards: Vec<InFlightGuard>,
    pub(super) cursor_update: Option<CursorUpdate>,
}

pub(crate) struct CrawlerWorker {
    pub(super) state: Arc<AppState>,
    pub(super) max_pending: usize,
    pub(super) resume_pending: usize,
    pub(super) stats: CrawlerStats,
    pub(super) rx: mpsc::Receiver<CrawlerBatch>,
    pub(super) was_throttled: bool,
}

impl CrawlerWorker {
    pub(crate) fn new(
        state: Arc<AppState>,
        max_pending: usize,
        resume_pending: usize,
        stats: CrawlerStats,
    ) -> (Self, mpsc::Sender<CrawlerBatch>) {
        let (tx, rx) = mpsc::channel(WORKER_CHANNEL_CAPACITY);
        (
            Self {
                state,
                max_pending,
                resume_pending,
                stats,
                rx,
                was_throttled: false,
            },
            tx,
        )
    }

    pub(crate) async fn run(mut self) {
        while let Some(batch) = self.rx.recv().await {
            self.wait_for_capacity().await;
            if let Err(e) = self.enqueue(batch).await {
                error!(err = ?e, "crawler worker: enqueue failed");
            }
        }
    }

    /// blocks until the pending queue has capacity.  mirrors the hysteresis logic
    /// that was previously duplicated in each crawler loop:
    ///   - above `max_pending`: hard stop, poll every 5s
    ///   - between `resume_pending` and `max_pending`: cooldown until below `resume_pending`
    ///   - below `resume_pending`: proceed (or release throttle)
    async fn wait_for_capacity(&mut self) {
        loop {
            let pending = self.state.db.get_count("pending").await;
            if pending > self.max_pending as u64 {
                if !self.was_throttled {
                    debug!(
                        pending,
                        max = self.max_pending,
                        "throttling: above max pending"
                    );
                    self.was_throttled = true;
                    self.stats.set_throttled(true);
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if pending > self.resume_pending as u64 {
                if !self.was_throttled {
                    debug!(
                        pending,
                        resume = self.resume_pending,
                        "throttling: entering cooldown"
                    );
                    self.was_throttled = true;
                    self.stats.set_throttled(true);
                }
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    if self.state.db.get_count("pending").await <= self.resume_pending as u64 {
                        break;
                    }
                }
                self.was_throttled = false;
                self.stats.set_throttled(false);
                info!("throttling released");
                break;
            } else {
                if self.was_throttled {
                    self.was_throttled = false;
                    self.stats.set_throttled(false);
                    info!("throttling released");
                }
                break;
            }
        }
    }

    /// filters already-known repos, commits them to `repos` + `pending` (plus
    /// an optional cursor update), then releases the in-flight guards.
    async fn enqueue(&mut self, batch: CrawlerBatch) -> Result<()> {
        let CrawlerBatch {
            guards,
            cursor_update,
        } = batch;

        // nothing to insert but still need to commit the cursor if present.
        if guards.is_empty() {
            if let Some(cursor) = cursor_update {
                self.commit_cursor(cursor).await?;
            }
            return Ok(());
        }

        // filter already-known repos, build and commit the write batch, then return
        // the surviving guards so they are dropped on the async side after commit.
        let db = self.state.db.clone();
        let surviving = tokio::time::timeout(
            BLOCKING_TASK_TIMEOUT,
            tokio::task::spawn_blocking(move || -> Result<Vec<InFlightGuard>> {
                let mut rng: SmallRng = rand::make_rng();
                let mut write_batch = db.inner.batch();
                let mut surviving = Vec::new();
                for guard in guards {
                    let did_key = keys::repo_key(&*guard);
                    if db.repos.contains_key(&did_key).into_diagnostic()? {
                        continue;
                    }
                    let state = RepoState::untracked(rng.next_u64());
                    write_batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
                    write_batch.insert(&db.pending, keys::pending_key(state.index_id), &did_key);
                    // clear any stale retry entry, this DID is confirmed and being enqueued
                    write_batch.remove(&db.crawler, keys::crawler_retry_key(&*guard));
                    trace!(did = %*guard, "enqueuing repo");
                    surviving.push(guard);
                }
                if let Some(cursor) = cursor_update {
                    write_batch.insert(&db.cursors, cursor.key, cursor.value);
                }
                write_batch.commit().into_diagnostic()?;
                Ok(surviving)
            }),
        )
        .await
        .into_diagnostic()?
        .map_err(|_| {
            error!("enqueue batch timed out after {BLOCKING_TASK_TIMEOUT:?}");
            miette::miette!("enqueue batch timed out")
        })?
        .inspect_err(|e| error!(err = ?e, "enqueue batch commit failed"))
        .unwrap_or_default();

        let count = surviving.len();
        // release in-flight slots now that the batch is committed
        drop(surviving);

        if count > 0 {
            self.stats.record_processed(count);
            self.state
                .db
                .update_count_async("repos", count as i64)
                .await;
            self.state
                .db
                .update_count_async("pending", count as i64)
                .await;
            self.state.notify_backfill();
        }

        Ok(())
    }

    async fn commit_cursor(&self, cursor: CursorUpdate) -> Result<()> {
        let db = self.state.db.clone();
        tokio::time::timeout(
            BLOCKING_TASK_TIMEOUT,
            tokio::task::spawn_blocking(move || {
                let mut batch = db.inner.batch();
                batch.insert(&db.cursors, cursor.key, cursor.value);
                batch.commit().into_diagnostic()
            }),
        )
        .await
        .into_diagnostic()?
        .map_err(|_| miette::miette!("cursor-only commit timed out"))?
    }
}
