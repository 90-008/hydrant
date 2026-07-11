use chrono::TimeDelta;
use jacquard_common::types::string::Did;
use miette::{IntoDiagnostic, Result};
use rand::RngExt;
use rand::rngs::SmallRng;
use std::collections::HashMap;
use std::ops::Mul;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use super::super::InFlight;
use super::super::worker::CrawlerBatch;
use super::checker::{MAX_RETRY_ATTEMPTS, RetryState, SignalChecker};
use crate::db::keys;
use crate::util::WatchEnabledExt;

const MAX_RETRY_BATCH: usize = 1000;

/// re-checks signal for repos whose `describeRepo` previously failed or timed out.
pub(crate) struct RetryProducer {
    pub(crate) checker: SignalChecker,
    pub(crate) in_flight: InFlight,
    pub(crate) tx: mpsc::Sender<CrawlerBatch>,
    pub(crate) enabled: tokio::sync::watch::Receiver<bool>,
}

impl RetryProducer {
    pub(crate) async fn run(mut self) {
        loop {
            self.enabled.wait_enabled("crawler retry").await;
            match self.process_queue().await {
                Ok(Some(dur)) => tokio::time::sleep(dur.max(Duration::from_secs(1))).await,
                Ok(None) => tokio::time::sleep(Duration::from_secs(60)).await,
                Err(e) => {
                    error!(err = %e, "retry loop failed");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        }
    }

    async fn process_queue(&self) -> Result<Option<Duration>> {
        let state = self.checker.state.clone();

        struct ScanResult {
            ready: Vec<Did<'static>>,
            existing: HashMap<Did<'static>, RetryState>,
            next_wake: Option<Duration>,
            had_more: bool,
        }

        // CPU-bound JSON parsing + read-only DB scan, stays on spawn_blocking
        let ScanResult {
            ready,
            existing,
            next_wake,
            had_more,
        } = tokio::task::spawn_blocking(move || -> Result<ScanResult> {
            let now = chrono::Utc::now();
            let mut rng: SmallRng = rand::make_rng();
            let mut ready: Vec<Did> = Vec::new();
            let mut existing: HashMap<Did<'static>, RetryState> = HashMap::new();
            let mut next_wake: Option<Duration> = None;
            let mut had_more = false;

            for guard in state.db.crawler.prefix(keys::CRAWLER_RETRY_PREFIX) {
                let (key, val) = guard.into_inner().into_diagnostic()?;
                let state: RetryState = rmp_serde::from_slice(&val).into_diagnostic()?;
                let did = keys::crawler_retry_parse_key(&key)?.to_did();

                if state.attempts >= MAX_RETRY_ATTEMPTS {
                    continue;
                }

                let backoff = TimeDelta::seconds(
                    state
                        .duration
                        .as_seconds_f64()
                        .mul(rng.random_range(0.01..0.07)) as i64,
                );
                let ready_at = state.after + backoff;
                if ready_at > now {
                    let wake = (ready_at - now).to_std().unwrap_or(Duration::ZERO);
                    next_wake = Some(next_wake.map(|w| w.min(wake)).unwrap_or(wake));
                    continue;
                }

                if ready.len() >= MAX_RETRY_BATCH {
                    had_more = true;
                    break;
                }

                ready.push(did.clone());
                existing.insert(did, state);
            }

            Ok(ScanResult {
                ready,
                existing,
                next_wake,
                had_more,
            })
        })
        .await
        .into_diagnostic()??;

        if ready.is_empty() {
            return Ok(next_wake);
        }

        debug!(count = ready.len(), "retrying pending repos");

        let filter = self.checker.state.filter.load();
        let in_flight = self.in_flight.acquire(ready).await;
        let mut retry_batch = self.checker.state.db.inner.batch();
        let confirmed = self
            .checker
            .check_signals_batch(in_flight, &filter, &mut retry_batch, &existing)
            .await?;

        self.checker.state.db.run(move |_db| retry_batch.commit().into_diagnostic())
            .await
            .inspect_err(|e| error!(err = ?e, "retry state commit failed"))
            .ok();

        if !confirmed.is_empty() {
            info!(count = confirmed.len(), "recovered from retry queue");
            self.tx
                .send(CrawlerBatch {
                    guards: confirmed,
                    cursor_update: None,
                })
                .await
                .ok();
        }

        Ok(had_more.then_some(Duration::ZERO).or(next_wake))
    }
}
