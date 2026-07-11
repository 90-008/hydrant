use jacquard_api::com_atproto::sync::list_repos::ListReposOutput;
use jacquard_common::{IntoStatic, types::string::Did};
use miette::{Context, IntoDiagnostic, Result};
use reqwest::StatusCode;
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};
use url::Url;

use super::super::worker::{CrawlerBatch, CursorUpdate};
use super::super::{CrawlerStats, InFlight, base_url};
use super::checker::SignalChecker;
use crate::db::keys::crawler_cursor_key;
use crate::db::{Db, keys};
use crate::state::AppState;
use crate::util::WatchEnabledExt;
use crate::util::{ErrorForStatus, RetryOutcome, RetryWithBackoff};

const BLOCKING_TASK_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct ListReposProducer {
    pub(crate) url: Url,
    pub(crate) checker: SignalChecker,
    pub(crate) in_flight: InFlight,
    pub(crate) tx: mpsc::Sender<CrawlerBatch>,
    pub(crate) enabled: watch::Receiver<bool>,
    pub(crate) stats: CrawlerStats,
}

impl ListReposProducer {
    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            if let Err(e) = self.crawl().await {
                error!(err = ?e, relay = %self.url, "fatal relay crawl error, restarting in 30s");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }

    async fn get_cursor(&self) -> Result<Option<SmolStr>> {
        let key = crawler_cursor_key(self.url.as_str());
        let cursor_bytes = self
            .checker
            .state
            .db
            .run(move |db| db.cursors.get(key).into_diagnostic())
            .await?;
        Ok(cursor_bytes
            .as_deref()
            .and_then(|b| rmp_serde::from_slice::<SmolStr>(b).ok()))
    }

    async fn crawl(&mut self) -> Result<()> {
        let db = &self.checker.state.db;
        let base = base_url(&self.url)?;

        let mut cursor = self.get_cursor().await?;
        match &cursor {
            Some(c) => info!(cursor = %c, "resuming"),
            None => info!("starting from scratch"),
        }

        loop {
            self.enabled.wait_enabled("crawler").await;

            let mut url = base
                .join("/xrpc/com.atproto.sync.listRepos")
                .into_diagnostic()?;
            url.query_pairs_mut().append_pair("limit", "1000");
            if let Some(c) = &cursor {
                url.query_pairs_mut().append_pair("cursor", c.as_str());
            }

            let fetch_result = (|| self.checker.http.get(url.clone()).send().error_for_status())
                .retry(5, |e: &reqwest::Error, attempt| {
                    matches!(e.status(), Some(StatusCode::TOO_MANY_REQUESTS))
                        .then(|| Duration::from_secs(1 << attempt.min(5)))
                })
                .await;

            let res = match fetch_result {
                Ok(r) => r,
                Err(RetryOutcome::Ratelimited) => {
                    warn!("rate limited by relay after retries");
                    miette::bail!("rate limited by relay after retries");
                }
                Err(RetryOutcome::Failed(e)) => {
                    error!(err = %e, "crawler failed to fetch listRepos");
                    return Err(e)
                        .into_diagnostic()
                        .wrap_err("crawler failed to fetch listRepos");
                }
            };

            let bytes = match res.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    error!(err = %e, "cant read listRepos response");
                    return Err(e)
                        .into_diagnostic()
                        .wrap_err("cant read listRepos response");
                }
            };

            let filter = self.checker.state.filter.load_full();

            struct ParseResult {
                unknown_dids: Vec<Did<'static>>,
                cursor: Option<SmolStr>,
                count: usize,
            }

            let parse_result = {
                let repos = db.repos.clone();
                let filter_ks = db.filter.clone();
                let crawler_ks = db.crawler.clone();

                match tokio::time::timeout(
                    BLOCKING_TASK_TIMEOUT,
                    // CPU-bound JSON parsing + read-only DB scan, stays on spawn_blocking
                    tokio::task::spawn_blocking(move || -> Result<Option<ParseResult>> {
                        let output = serde_json::from_slice::<ListReposOutput>(&bytes)
                            .into_diagnostic()
                            .wrap_err("failed to parse listRepos response")?
                            .into_static();
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
                            let retry_key = keys::crawler_retry_key(&repo.did);
                            if crawler_ks.contains_key(&retry_key).into_diagnostic()? {
                                continue;
                            }
                            let did_key = keys::repo_key(&repo.did);
                            if !repos.contains_key(&did_key).into_diagnostic()? {
                                unknown.push(repo.did.into_static());
                            }
                        }
                        Ok(Some(ParseResult {
                            unknown_dids: unknown,
                            cursor: next_cursor,
                            count,
                        }))
                    }),
                )
                .await
                {
                    Ok(res) => res
                        .into_diagnostic()
                        .wrap_err("blocking task failed to parse listRepos")?,
                    Err(_) => {
                        error!(
                            "spawn_blocking task for parsing listRepos timed out after {}s",
                            BLOCKING_TASK_TIMEOUT.as_secs()
                        );
                        miette::bail!("spawn_blocking task for parsing listRepos timed out");
                    }
                }
            };

            let ParseResult {
                unknown_dids,
                cursor: next_cursor,
                count,
            } = match parse_result {
                Ok(Some(res)) => res,
                Ok(None) => {
                    info!("enumeration pass complete, sleeping 1h");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(3600)) => {
                            info!("resuming after 1h sleep");
                        }
                        _ = self.enabled.changed() => {
                            if !*self.enabled.borrow() { return Ok(()); }
                        }
                    }
                    continue;
                }
                Err(e) => return Err(e).wrap_err("error while crawling"),
            };

            debug!(count, "fetched repos");
            self.stats.record_crawled(count);

            let pass_complete;
            if let Some(new_cursor) = next_cursor {
                cursor = Some(new_cursor.as_str().into());
                pass_complete = false;
            } else {
                info!("reached end of list.");
                pass_complete = true;
            }

            let cursor_update = cursor
                .as_ref()
                .map(|c| -> Result<CursorUpdate> {
                    Ok(CursorUpdate {
                        key: crawler_cursor_key(self.url.as_str()),
                        value: rmp_serde::to_vec(c.as_str())
                            .into_diagnostic()
                            .wrap_err("cant serialize cursor")?,
                    })
                })
                .transpose()?;

            let in_flight = self.in_flight.acquire(unknown_dids).await;
            let confirmed = if filter.check_signals() && !in_flight.is_empty() {
                let mut retry_batch = db.inner.batch();
                let confirmed = self
                    .checker
                    .check_signals_batch(in_flight, &filter, &mut retry_batch, &HashMap::new())
                    .await?;
                tokio::time::timeout(
                    BLOCKING_TASK_TIMEOUT,
                    db.run(move |_db| retry_batch.commit().into_diagnostic()),
                )
                .await
                .map_err(|_| miette::miette!("retry state commit timed out"))??;
                confirmed
            } else {
                in_flight
            };

            self.tx
                .send(CrawlerBatch {
                    guards: confirmed,
                    cursor_update,
                })
                .await
                .ok();

            if pass_complete {
                info!("enumeration complete, sleeping 1h before next pass");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(3600)) => {
                        info!("resuming after 1h sleep");
                    }
                    _ = self.enabled.changed() => {
                        if !*self.enabled.borrow() { return Ok(()); }
                    }
                }
            }
        }
    }
}

pub(crate) async fn cursor_display(state: &AppState, relay_host: &Url) -> SmolStr {
    let key = crawler_cursor_key(relay_host.as_str());
    let cursor_bytes = match state
        .db
        .run(move |db| db.cursors.get(key).into_diagnostic())
        .await
    {
        Ok(b) => b,
        Err(e) => return e.to_smolstr(),
    };
    match cursor_bytes
        .as_deref()
        .map(rmp_serde::from_slice::<SmolStr>)
    {
        None | Some(Err(_)) => "none".to_smolstr(),
        Some(Ok(c)) => c,
    }
}
