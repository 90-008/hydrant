use crate::backfill::client::ThrottledHttpClient;
use crate::backfill::error::BackfillError;
use crate::config::BackfillStrategy;
use crate::db::{self, types::TrimmedDid};
use crate::ingest::indexer::IndexerTx;
use crate::state::AppState;
use crate::util::WatchEnabledExt;
use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{Instrument, debug, error, info};

pub mod process;
pub mod task;

pub struct BackfillWorker {
    state: Arc<AppState>,
    buffer_tx: IndexerTx,
    http: ThrottledHttpClient,
    semaphore: Arc<Semaphore>,
    verify_signatures: bool,
    strategy: BackfillStrategy,
    in_flight: Arc<scc::HashSet<Did<'static>>>,
    enabled: tokio::sync::watch::Receiver<bool>,
}

impl BackfillWorker {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: IndexerTx,
        timeout: Duration,
        concurrency_limit: usize,
        verify_signatures: bool,
        strategy: BackfillStrategy,
        proxies: Vec<url::Url>,
        enabled: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        let build = |proxy: Option<reqwest::Proxy>| {
            let mut builder = reqwest::Client::builder()
                .timeout(timeout)
                .zstd(true)
                .brotli(true)
                .gzip(true);
            if let Some(proxy) = proxy {
                builder = builder.proxy(proxy);
            }
            builder.build().expect("failed to build http client")
        };

        // the direct connection is always in the pool; each proxy adds another egress IP.
        let mut clients = vec![build(None)];
        for url in &proxies {
            match reqwest::Proxy::all(url.as_str()) {
                Ok(proxy) => clients.push(build(Some(proxy))),
                Err(e) => error!(url = %url, err = %e, "invalid backfill proxy, skipping"),
            }
        }
        info!(egress_ips = clients.len(), "backfill client pool ready");

        Self {
            state: state.clone(),
            buffer_tx,
            http: ThrottledHttpClient::new(clients, state.throttler.clone()),
            semaphore: Arc::new(Semaphore::new(concurrency_limit)),
            verify_signatures,
            strategy,
            in_flight: Arc::new(scc::HashSet::new()),
            enabled,
        }
    }
}

struct InFlightGuard {
    did: Did<'static>,
    set: Arc<scc::HashSet<Did<'static>>>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let _ = self.set.remove_sync(&self.did);
    }
}

impl BackfillWorker {
    pub async fn run(mut self) {
        info!("backfill worker started");

        loop {
            self.enabled.wait_enabled("backfill").await;
            let mut spawned = 0;

            for guard in self.state.db.pending.iter() {
                let (key, value) = match guard.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => {
                        error!(err = %e, "failed to read pending entry");
                        db::check_poisoned(&e);
                        continue;
                    }
                };

                let did = match TrimmedDid::try_from(value.as_ref()) {
                    Ok(d) => d.to_did(),
                    Err(e) => {
                        error!(err = %e, "invalid did in pending value");
                        continue;
                    }
                };

                // check before trying to acquire a permit so we dont acquire a permit
                // for no reason, the read will be cheap anyhow
                if self.in_flight.contains_sync(&did) {
                    continue;
                }

                let permit = match self.semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // only mark as in flight if we can acquire a permit
                if self
                    .in_flight
                    .insert_sync(did.clone().into_static())
                    .is_err()
                {
                    // a task is already running, weh
                    // so we don't need this one anymore...
                    break;
                }

                let guard = InFlightGuard {
                    did: did.clone().into_static(),
                    set: self.in_flight.clone(),
                };

                let state = self.state.clone();
                let http = self.http.clone();
                let did = did.clone();
                let buffer_tx = self.buffer_tx.clone();
                let verify = self.verify_signatures;
                let strategy = self.strategy;

                let span = tracing::info_span!("backfill", did = %did);
                tokio::spawn(
                    async move {
                        let _guard = guard;
                        let res = task::did_task(
                            &state, http, buffer_tx, &did, key, permit, verify, strategy,
                        )
                        .await;

                        if let Err(e) = res {
                            match &e {
                                BackfillError::Ratelimited
                                | BackfillError::PreemptivelyThrottled => {
                                    debug!(err = %e, "process failed");
                                }
                                _ => {
                                    error!(err = %e, "process failed");
                                }
                            }
                            if let BackfillError::Generic(report) = &e {
                                db::check_poisoned_report(report);
                            }
                        }

                        // wake worker to pick up more (in case we were sleeping at limit)
                        state.backfill_notify.notify_one();
                    }
                    .instrument(span),
                );

                spawned += 1;
            }

            if spawned == 0 {
                // wait for new tasks
                self.state.backfill_notify.notified().await;
            }
            // loop immediately since we might have more tasks
        }
    }
}
