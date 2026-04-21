use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use jacquard_api::com_atproto::sync::HostStatus;
use jacquard_api::com_atproto::sync::list_hosts::ListHostsOutput;
use miette::IntoDiagnostic;
use tracing::{info, warn};
use url::Url;

use super::firehose::FirehoseHandle;
use crate::db::{self, CountDeltas, keys};
use crate::state::AppState;

const MAX_CONCURRENT_SEEDS: usize = 4;

/// seed firehose pds sources by calling `com.atproto.sync.listHosts` on each seed URL.
/// banned pds' are not added, everything else is (including offline)
pub(crate) async fn seed_from_list_hosts(
    seed_urls: &[Url],
    firehose: &FirehoseHandle,
    state: &Arc<AppState>,
) {
    info!("will seed urls...");

    let http = reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("that reqwest will build");

    let mut futs = futures::stream::iter(seed_urls.iter().cloned())
        .map(|seed_url| {
            let firehose = firehose.clone();
            let state = state.clone();
            let http = http.clone();
            async move { seed_one(&seed_url, &firehose, &state, &http).await }
        })
        .buffer_unordered(MAX_CONCURRENT_SEEDS);

    while let Some(_) = futs.next().await {}
}

#[tracing::instrument(skip_all, fields(seed_url = %seed_url))]
async fn seed_one(
    seed_url: &Url,
    firehose: &FirehoseHandle,
    state: &Arc<AppState>,
    http: &reqwest::Client,
) {
    let cursor_key = keys::seed_cursor_key(seed_url.as_str());

    // resume from the last saved cursor so we don't re-page through already-seen hosts
    let mut cursor: Option<String> = {
        let ks = state.db.cursors.clone();
        let key = cursor_key.clone();
        match db::Db::get(ks, key).await {
            Ok(Some(b)) => rmp_serde::from_slice::<String>(b.as_ref()).ok(),
            Ok(None) => None,
            Err(e) => {
                warn!(err = %e, "failed to load seed cursor, starting from scratch");
                None
            }
        }
    };

    if cursor.is_some() {
        info!(cursor = ?cursor, "resuming seed from saved cursor");
    } else {
        info!("seeding firehose sources from listHosts");
    }

    let mut total = 0usize;
    let mut added = 0usize;

    loop {
        let url = list_hosts_url(seed_url, cursor.as_deref());
        let resp = match http.get(url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(err = %e, "failed to fetch listHosts, stopping");
                break;
            }
        };

        if !resp.status().is_success() {
            warn!(status = %resp.status(), "listHosts returned error status, stopping");
            break;
        }

        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                warn!(err = %e, "failed to read listHosts response, stopping");
                break;
            }
        };

        let body: ListHostsOutput<'_> = match serde_json::from_slice(&bytes) {
            Ok(b) => b,
            Err(e) => {
                warn!(err = %e, "failed to parse listHosts response, stopping");
                break;
            }
        };

        let next_cursor = body.cursor.as_deref().map(str::to_owned);
        total += body.hosts.len();

        for host in &body.hosts {
            // skip banned hosts; everything else (active, idle, offline, throttled) is included
            // since the firehose ingestor handles reconnection for transiently-unavailable hosts
            if matches!(host.status, Some(HostStatus::Banned)) {
                continue;
            }

            let wss_url_str = format!("wss://{}/", host.hostname);
            let wss_url = match Url::parse(&wss_url_str) {
                Ok(u) => u,
                Err(e) => {
                    warn!(hostname = %host.hostname, err = %e, "invalid hostname in listHosts response, skipping");
                    continue;
                }
            };

            // skip sources that are already running
            if firehose.tasks.contains_async(&wss_url).await {
                continue;
            }

            // initialise account count for hosts we haven't seen before
            if let Some(count) = host.account_count.filter(|&c| c > 0) {
                let count_key = keys::pds_account_count_key(host.hostname.as_ref());
                let current = state.db.get_count(&count_key).await;
                if current == 0 {
                    let state = state.clone();
                    let count_key = count_key.clone();
                    let result = tokio::task::spawn_blocking(move || -> miette::Result<()> {
                        let mut batch = state.db.inner.batch();
                        let mut count_deltas = CountDeltas::default();
                        count_deltas.add(&count_key, count);
                        let reservation = state.db.stage_count_deltas(&mut batch, &count_deltas);
                        batch.commit().into_diagnostic()?;
                        state.db.apply_count_deltas(&count_deltas);
                        drop(reservation);
                        Ok(())
                    })
                    .await
                    .into_diagnostic()
                    .flatten();
                    if let Err(e) = result {
                        warn!(hostname = %host.hostname, err = %e, "failed to seed host account count");
                    }
                }
            }

            match firehose.add_source(wss_url, true).await {
                Ok(()) => added += 1,
                Err(e) => {
                    warn!(hostname = %host.hostname, err = %e, "failed to add firehose source");
                }
            }
        }

        cursor = next_cursor;

        // persist cursor after each page so a restart can resume where we left off
        if let Some(ref c) = cursor {
            let value = match rmp_serde::to_vec(c) {
                Ok(v) => v,
                Err(e) => {
                    warn!(err = %e, "failed to serialize seed cursor");
                    continue;
                }
            };
            let state = state.clone();
            let key: Vec<u8> = cursor_key.clone();
            let result = tokio::task::spawn_blocking(move || -> miette::Result<()> {
                let mut batch = state.db.inner.batch();
                batch.insert(&state.db.cursors, key, &value);
                batch.commit().into_diagnostic()
            })
            .await
            .into_diagnostic()
            .flatten();
            if let Err(e) = result {
                warn!(err = %e, "failed to persist seed cursor");
            }
        }

        if cursor.is_none() {
            break;
        }
    }

    info!(
        total,
        added, "finished seeding firehose sources from listHosts"
    );
}

fn list_hosts_url(base: &Url, cursor: Option<&str>) -> Url {
    let mut url = base.clone();
    url.set_path("/xrpc/com.atproto.sync.listHosts");
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("limit", "1000");
        if let Some(c) = cursor {
            pairs.append_pair("cursor", c);
        }
    }
    url
}
