use crate::db::{keys, Db};
use crate::state::AppState;
use crate::types::{RepoState, RepoStatus};
use jacquard::api::com_atproto::sync::list_repos::{ListRepos, ListReposOutput};
use jacquard::prelude::*;
use jacquard::types::did::Did;
use jacquard_common::CowStr;
use miette::{IntoDiagnostic, Result};
use smol_str::SmolStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};
use url::Url;

pub struct Crawler {
    state: Arc<AppState>,
    relay_host: SmolStr,
    http: reqwest::Client,
}

impl Crawler {
    pub fn new(state: Arc<AppState>, relay_host: SmolStr) -> Self {
        Self {
            state,
            relay_host,
            http: reqwest::Client::new(),
        }
    }

    pub async fn run(self) -> Result<()> {
        info!("crawler started");

        let db = &self.state.db;

        let relay_url = Url::parse(&self.relay_host).into_diagnostic()?;

        // 1. load cursor
        let cursor_key = b"crawler_cursor";
        let mut cursor: Option<SmolStr> =
            if let Ok(Some(bytes)) = Db::get(db.cursors.clone(), cursor_key.to_vec()).await {
                let s = String::from_utf8_lossy(&bytes);
                info!("resuming crawler from cursor: {}", s);
                Some(s.into())
            } else {
                None
            };

        loop {
            // 2. fetch listrepos
            let req = ListRepos::new()
                .limit(1000)
                .maybe_cursor(cursor.clone().map(|c| CowStr::from(c.to_string())))
                .build();

            let res_result = self.http.xrpc(relay_url.clone()).send(&req).await;

            let output: ListReposOutput = match res_result {
                Ok(res) => res.into_output().into_diagnostic()?,
                Err(e) => {
                    error!("crawler failed to list repos: {}. retrying in 30s...", e);
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            };

            if output.repos.is_empty() {
                info!("crawler finished enumeration (or empty page). sleeping for 1 hour.");
                tokio::time::sleep(Duration::from_secs(3600)).await;
                // we might want to reset cursor to start over? tap seems to loop.
                // for now, just wait.
                continue;
            }

            info!("crawler fetched {} repos...", output.repos.len());

            let mut batch = db.inner.batch();
            let mut to_queue = Vec::new();

            // 3. process repos
            for repo in output.repos {
                let did_str = smol_str::SmolStr::from(repo.did.as_str());
                let did_key = keys::repo_key(&repo.did);

                // check if known
                if !Db::contains_key(db.repos.clone(), did_key).await? {
                    debug!("crawler found new repo: {}", did_str);

                    // create state (backfilling)
                    let mut state = RepoState::new(repo.did.to_owned());
                    state.status = RepoStatus::Backfilling;
                    let bytes = rmp_serde::to_vec(&state).into_diagnostic()?;

                    batch.insert(&db.repos, did_key, bytes);
                    batch.insert(&db.pending, did_key, Vec::new());
                    to_queue.push(did_str);
                }
            }

            // update counts if we found new repos
            if !to_queue.is_empty() {
                let count = to_queue.len() as i64;
                tokio::spawn({
                    let state = self.state.clone();
                    async move {
                        let _ = state
                            .db
                            .increment_count(keys::count_keyspace_key("repos"), count)
                            .await;
                        let _ = state
                            .db
                            .increment_count(keys::count_keyspace_key("pending"), count)
                            .await;
                    }
                });
            }

            // 4. update cursor
            if let Some(new_cursor) = output.cursor {
                cursor = Some(new_cursor.as_str().into());

                batch.insert(
                    &db.cursors,
                    cursor_key.to_vec(),
                    new_cursor.as_bytes().to_vec(),
                );
            } else {
                // end of pagination
                info!("crawler reached end of list.");
                cursor = None;
            }

            tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                .await
                .into_diagnostic()??;

            // 5. queue for backfill
            for did_str in to_queue {
                let did = match Did::new_owned(did_str.as_str()) {
                    Ok(d) => d,
                    Err(e) => {
                        error!("got invalid DID ({did_str}) from relay, skipping this: {e}");
                        continue;
                    }
                };
                if let Err(e) = self.state.backfill_tx.send(did) {
                    error!("crawler failed to queue {did_str}: {e}");
                }
            }

            if cursor.is_none() {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    }
}
