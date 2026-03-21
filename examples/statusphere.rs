//! a statusphere indexer: tracks xyz.statusphere.status records across the ATProto network.
//!
//! statusphere is a demo app where users set a single-emoji status on their bluesky profile.
//! this example indexes those status records in real time, maintaining the current status
//! per user and printing a periodic leaderboard of the top emoji statuses in use.
//!
//! see: https://github.com/bluesky-social/statusphere-example-app
//!
//! run with:
//!   HYDRANT_DATABASE_PATH=./statusphere.db cargo run --example statusphere
//!
//! the database persists records across restarts. on each start the full event
//! history is replayed from the database to rebuild the in-memory index.
//! (in a better app, we could for example use the ephemeral mode of hydrant,
//! and use our db, or we could use hydrant to backfill multiple instances of the app.)

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use futures::StreamExt;
use hydrant::config::Config;
use hydrant::control::{EventStream, Hydrant, ReposControl};
use hydrant::filter::FilterMode;
use jacquard_common::types::did::Did;
use jacquard_common::types::tid::Tid;
use scc::HashMap;

const COLLECTION: &str = "xyz.statusphere.status";

struct StatusEntry {
    emoji: String,
    created_at: String,
}

struct StatusIndex {
    /// current status per DID: only the latest by createdAt is kept.
    current: HashMap<String, StatusEntry>,
}

impl StatusIndex {
    fn new() -> Self {
        Self {
            current: HashMap::new(),
        }
    }

    fn set(&self, did: String, emoji: String, created_at: &str) -> bool {
        let is_newer = self
            .current
            .read_sync(&did, |_, e| created_at > e.created_at.as_str())
            .unwrap_or(true);
        if is_newer {
            self.current.upsert_sync(
                did,
                StatusEntry {
                    emoji,
                    created_at: created_at.to_owned(),
                },
            );
        }
        is_newer
    }

    fn delete(&self, did: &str) {
        self.current.remove_sync(did);
    }

    fn top(&self, n: usize) -> Vec<(String, usize)> {
        use std::collections::HashMap;
        let mut counts: HashMap<String, usize> = HashMap::with_capacity(self.current.capacity());
        self.current.iter_sync(|_, e| {
            *counts.entry(e.emoji.clone()).or_default() += 1;
            true
        });
        let mut ranked: Vec<_> = counts.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1));
        ranked.truncate(n);
        ranked
    }
}

async fn run_ticker(index: Arc<StatusIndex>) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.tick().await;
    loop {
        interval.tick().await;
        let top = index.top(10);
        if top.is_empty() {
            continue;
        }
        println!(
            "\n--- top statuses ({} users tracked) ---",
            index.current.len()
        );
        for (emoji, count) in &top {
            println!("  {emoji}  ×{count}");
        }
        println!("----------------------------------------\n");
    }
}

async fn handle_stream(index: Arc<StatusIndex>, repos: ReposControl, mut stream: EventStream) {
    // get handle of did through the hydrant api
    let get_handle = async |did: &Did<'_>| {
        repos
            .get(did)
            .await
            .ok()
            .flatten()
            .and_then(|info| info.handle)
            .map(|h| h.to_string())
            .unwrap_or_else(|| did.to_string())
    };
    while let Some(event) = stream.next().await {
        if let Some(rec) = event.record {
            let did = rec.did.as_str().to_owned();
            match rec.action.as_str() {
                "create" | "update" => {
                    let Some(record) = rec.record else { continue };
                    let Some(emoji) = record
                        .get("status")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_owned())
                    else {
                        continue;
                    };
                    let created_at = record
                        .get("createdAt")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if index.set(did.clone(), emoji.clone(), created_at) {
                        let name = get_handle(&rec.did).await;
                        println!("[{created_at}] {name}: {emoji}");
                    }
                }
                "delete" => {
                    let name = get_handle(&rec.did).await;
                    index.delete(&did);
                    // parse the tid to use as date since createdAt doesnt make sense here
                    let date = Tid::from_str(&rec.rkey)
                        .ok()
                        .and_then(|tid| DateTime::from_timestamp_micros(tid.timestamp() as i64))
                        .map(|date| date.to_string())
                        .unwrap_or_else(|| "invalid rkey".to_string());
                    println!("[{date}] {name} cleared status");
                }
                _ => {}
            }
        } else if let Some(account) = event.account {
            // when an account is deactivated or deleted, drop their status.
            if !account.active {
                index.delete(account.did.as_str());
            }
        }
    }
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("hydrant=info")
        .init();

    // config is loaded from environment variables (all prefixed with HYDRANT_).
    // key defaults for this example:
    // DATABASE_PATH=./hydrant.db                    | where to store the database.
    // RELAY_HOST=wss://relay.fire.hose.cam/         | firehose source.
    // CRAWLER_URLS=https://lightrail.microcosm.blue | crawler sources. in filter mode this defaults to `by-collection`.
    let cfg = Config::from_env()?;
    let hydrant = Hydrant::new(cfg).await?;

    // discover only repos that publish xyz.statusphere.status records,
    // and only store that collection (all other record types are dropped).
    hydrant
        .filter
        .set_mode(FilterMode::Filter)
        .set_signals([COLLECTION])
        .set_collections([COLLECTION])
        .apply()
        .await?;

    // replay all persisted events from the start to rebuild the in-memory index,
    // then switch to live tail. since the index is in-memory, we always need the
    // full replay on startup.
    let stream = hydrant.subscribe(Some(0));

    let index = Arc::new(StatusIndex::new());
    tokio::select! {
        // this finally starts hydrant, so it will start crawling and backfilling etc.
        r = hydrant.run()? => r,
        _ = run_ticker(index.clone()) => Ok(()),
        _ = handle_stream(index.clone(), hydrant.repos.clone(), stream) => Ok(()),
    }
}
