use crate::db::{Db, keys, ser_repo_state};
use crate::state::AppState;
use crate::types::RepoState;
use jacquard::api::com_atproto::sync::list_repos::{ListRepos, ListReposOutput};
use jacquard::prelude::*;
use jacquard_common::CowStr;
use miette::{IntoDiagnostic, Result};
use rand::Rng;
use rand::rngs::SmallRng;
use smol_str::SmolStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace};
use url::Url;

pub struct Crawler {
    state: Arc<AppState>,
    relay_host: Url,
    http: reqwest::Client,
    max_pending: usize,
    resume_pending: usize,
}

impl Crawler {
    pub fn new(
        state: Arc<AppState>,
        relay_host: Url,
        max_pending: usize,
        resume_pending: usize,
    ) -> Self {
        Self {
            state,
            relay_host,
            http: reqwest::Client::new(),
            max_pending,
            resume_pending,
        }
    }

    pub async fn run(self) -> Result<()> {
        info!("crawler started");

        let mut rng: SmallRng = rand::make_rng();

        let db = &self.state.db;

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
        let mut was_throttled = false;

        loop {
            // check throttling
            loop {
                let pending = self.state.db.get_count("pending").await;
                if pending > self.max_pending as u64 {
                    if !was_throttled {
                        debug!(
                            "crawler throttling: pending repos {} > max {}, sleeping...",
                            pending, self.max_pending
                        );
                        was_throttled = true;
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                } else if pending > self.resume_pending as u64 {
                    if !was_throttled {
                        debug!(
                            "crawler throttling: pending repos {} > max {}, entering cooldown...",
                            pending, self.max_pending
                        );
                        was_throttled = true;
                    }

                    while self.state.db.get_count("pending").await > self.resume_pending as u64 {
                        debug!(
                            "crawler cooldown: pending repos {} > resume {}, sleeping...",
                            self.state.db.get_count("pending").await,
                            self.resume_pending
                        );
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                    break;
                } else {
                    if was_throttled {
                        info!("crawler resuming: throttling released");
                        was_throttled = false;
                    }
                    break;
                }
            }

            // 2. fetch listrepos
            let req = ListRepos::new()
                .limit(1000)
                .maybe_cursor(cursor.clone().map(|c| CowStr::from(c.to_string())))
                .build();

            let mut url = self.relay_host.clone();
            if url.scheme() == "wss" {
                url.set_scheme("https")
                    .map_err(|_| miette::miette!("invalid url: {url}"))?;
            } else if url.scheme() == "ws" {
                url.set_scheme("http")
                    .map_err(|_| miette::miette!("invalid url: {url}"))?;
            }
            let res_result = self.http.xrpc(url).send(&req).await;

            let output: ListReposOutput = match res_result {
                Ok(res) => res.into_output().into_diagnostic()?,
                Err(e) => {
                    let e = e
                        .source_err()
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| e.to_string());
                    error!("crawler failed to list repos: {e}. retrying in 30s...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            };

            if output.repos.is_empty() {
                info!("crawler finished enumeration (or empty page). sleeping for 1 hour.");
                tokio::time::sleep(Duration::from_secs(3600)).await;
                continue;
            }

            debug!("crawler fetched {} repos...", output.repos.len());

            let mut batch = db.inner.batch();
            let mut to_queue = Vec::new();

            // 3. process repos
            for repo in output.repos {
                let did_key = keys::repo_key(&repo.did);

                // check if known
                if !Db::contains_key(db.repos.clone(), &did_key).await? {
                    trace!("crawler found new repo: {}", repo.did);

                    let state = RepoState::backfilling(rng.next_u64());
                    batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
                    batch.insert(&db.pending, keys::pending_key(state.index_id), &did_key);
                    to_queue.push(repo.did.clone());
                }
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

            // update counts if we found new repos
            if !to_queue.is_empty() {
                let count = to_queue.len() as i64;
                self.state.db.update_count_async("repos", count).await;
                self.state.db.update_count_async("pending", count).await;
            }

            // 5. notify backfill worker
            if !to_queue.is_empty() {
                self.state.notify_backfill();
            }

            if cursor.is_none() {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    }
}
