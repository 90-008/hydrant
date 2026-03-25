use crate::db::keys::by_collection_cursor_key;
use crate::db::{Db, keys};
use crate::state::AppState;
use crate::util::{ErrorForStatus, RetryOutcome, RetryWithBackoff, WatchEnabledExt};
use jacquard_api::com_atproto::sync::list_repos_by_collection::ListReposByCollectionOutput;
use jacquard_common::{IntoStatic, types::string::Did};
use miette::{Context, IntoDiagnostic, Result};
use smol_str::SmolStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, trace, warn};
use url::Url;

use super::worker::{CrawlerBatch, CursorUpdate};
use super::{CrawlerStats, InFlight, base_url};

const BLOCKING_TASK_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct ByCollectionProducer {
    pub(crate) index_url: Url,
    pub(crate) http: reqwest::Client,
    pub(crate) state: Arc<AppState>,
    pub(crate) in_flight: InFlight,
    pub(crate) tx: mpsc::Sender<CrawlerBatch>,
    pub(crate) enabled: watch::Receiver<bool>,
    pub(crate) stats: CrawlerStats,
}

impl ByCollectionProducer {
    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            self.enabled.wait_enabled("by-collection crawler").await;

            let filter = self.state.filter.load();
            if filter.signals.is_empty() {
                trace!("no signals configured, by-collection crawler sleeping 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let filter = arc_swap::Guard::into_inner(filter);

            info!(
                host = self.index_url.host_str(),
                signal_count = filter.signals.len(),
                "starting by-collection discovery pass"
            );

            // todo: make this parallel or make use of lightrail taking multiple collections
            // https://github.com/bluesky-social/atproto/pull/4733
            for collection in &filter.signals {
                self.enabled.wait_enabled("by-collection crawler").await;
                let span = tracing::info_span!("by_collection", %collection);
                use tracing::Instrument as _;
                if let Err(e) = self.crawl(collection).instrument(span).await {
                    error!(err = %e, %collection, "by-collection crawl error, continuing");
                }
            }

            info!("by-collection pass complete, sleeping 1h");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
                _ = self.enabled.changed() => {
                    if !*self.enabled.borrow() { return Ok(()); }
                }
            }
        }
    }

    async fn crawl(&mut self, collection: &str) -> Result<()> {
        let db = &self.state.db;
        let base = base_url(&self.index_url)?;
        let cursor_key = by_collection_cursor_key(self.index_url.as_str(), collection);

        // resume from any persisted cursor, so a restart mid-pass doesn't rescan from scratch.
        let mut cursor: Option<SmolStr> = Db::get(db.cursors.clone(), &cursor_key)
            .await
            .ok()
            .flatten()
            .and_then(|b| String::from_utf8(b.to_vec()).ok().map(SmolStr::from));

        if cursor.is_some() {
            debug!(%collection, "resuming by-collection pass from cursor");
        }

        let mut total_count = 0usize;
        let mut new_count = 0usize;

        loop {
            self.enabled.wait_enabled("by-collection crawler").await;

            let mut url = base
                .join("/xrpc/com.atproto.sync.listReposByCollection")
                .into_diagnostic()?;
            url.query_pairs_mut()
                .append_pair("collection", collection)
                .append_pair("limit", "2000");
            if let Some(c) = &cursor {
                url.query_pairs_mut().append_pair("cursor", c.as_str());
            }

            let fetch_result = (|| self.http.get(url.clone()).send().error_for_status())
                .retry(5, |e: &reqwest::Error, attempt| {
                    matches!(e.status(), Some(reqwest::StatusCode::TOO_MANY_REQUESTS))
                        .then(|| Duration::from_secs(1 << attempt.min(5)))
                })
                .await;

            let res = match fetch_result {
                Ok(r) => r,
                Err(RetryOutcome::Ratelimited) => {
                    warn!(%collection, "rate limited by collection index after retries");
                    continue;
                }
                Err(RetryOutcome::Failed(e)) => {
                    error!(err = %e, %collection, "by-collection fetch failed");
                    continue;
                }
            };

            let bytes = match res.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    error!(err = %e, "can't read listReposByCollection response");
                    continue;
                }
            };

            struct PageResult {
                unknown_dids: Vec<Did<'static>>,
                next_cursor: Option<SmolStr>,
                count: usize,
            }

            let page_result = {
                let repos_ks = db.repos.clone();
                let filter_ks = db.filter.clone();

                tokio::time::timeout(
                    BLOCKING_TASK_TIMEOUT,
                    tokio::task::spawn_blocking(move || -> miette::Result<Option<PageResult>> {
                        let output =
                            match serde_json::from_slice::<ListReposByCollectionOutput>(&bytes) {
                                Ok(out) => out.into_static(),
                                Err(e) => {
                                    error!(
                                        err = %e,
                                        "failed to parse listReposByCollection response"
                                    );
                                    return Ok(None);
                                }
                            };

                        if output.repos.is_empty() {
                            return Ok(None);
                        }

                        let count = output.repos.len();
                        let next_cursor = output.cursor.map(|c| c.as_str().into());
                        let mut unknown = Vec::new();
                        for repo in output.repos {
                            let excl_key = crate::db::filter::exclude_key(repo.did.as_str())?;
                            if filter_ks.contains_key(&excl_key).into_diagnostic()? {
                                continue;
                            }
                            let did_key = keys::repo_key(&repo.did);
                            if !repos_ks.contains_key(&did_key).into_diagnostic()? {
                                unknown.push(repo.did.into_static());
                            }
                        }

                        Ok(Some(PageResult {
                            unknown_dids: unknown,
                            next_cursor,
                            count,
                        }))
                    }),
                )
                .await
            }
            .into_diagnostic()?
            .map_err(|_| {
                error!(
                    "spawn_blocking task for listReposByCollection timed out after {}s",
                    BLOCKING_TASK_TIMEOUT.as_secs()
                );
                miette::miette!("spawn_blocking task for listReposByCollection timed out")
            })?;

            let PageResult {
                unknown_dids,
                next_cursor,
                count,
            } = match page_result {
                Ok(Some(r)) => r,
                Ok(None) => {
                    info!(
                        %collection,
                        total = total_count,
                        new = new_count,
                        "finished by-collection pass (empty page)"
                    );
                    return Ok(());
                }
                Err(e) => return Err(e).wrap_err("error parsing by-collection page"),
            };

            total_count += count;
            trace!(%collection, count, "fetched repos from by-collection index");

            let in_flight = self.in_flight.acquire(unknown_dids).await;
            new_count += in_flight.len();
            self.stats.record_crawled(count);

            cursor = next_cursor;
            let cursor_update = cursor.as_ref().map(|c| CursorUpdate {
                key: cursor_key.clone(),
                value: c.as_bytes().to_vec(),
            });

            self.tx
                .send(CrawlerBatch {
                    guards: in_flight,
                    cursor_update,
                })
                .await
                .ok();

            if cursor.is_none() {
                info!(
                    %collection,
                    total = total_count,
                    new = new_count,
                    "finished by-collection pass"
                );
                return Ok(());
            }
        }
    }
}
