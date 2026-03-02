use crate::db::{Db, keys, ser_repo_state};
use crate::state::AppState;
use crate::types::RepoState;
use jacquard_api::com_atproto::repo::list_records::ListRecordsOutput;
use jacquard_api::com_atproto::sync::list_repos::ListReposOutput;
use jacquard_common::{IntoStatic, types::string::Did};
use miette::{IntoDiagnostic, Result};
use rand::Rng;
use rand::rngs::SmallRng;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::Jitter;
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use smol_str::SmolStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace};
use url::Url;

pub struct Crawler {
    state: Arc<AppState>,
    relay_host: Url,
    http: Arc<ClientWithMiddleware>,
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
        let retry_policy = ExponentialBackoff::builder()
            .jitter(Jitter::Bounded)
            .build_with_max_retries(8);
        let reqwest_client = reqwest::Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .gzip(true)
            .build()
            .expect("that reqwest will build");

        let http = ClientBuilder::new(reqwest_client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let http = Arc::new(http);

        Self {
            state,
            relay_host,
            http,
            max_pending,
            resume_pending,
        }
    }

    pub async fn run(self) -> Result<()> {
        info!("crawler started");

        let mut api_url = self.relay_host.clone();
        if api_url.scheme() == "wss" {
            api_url
                .set_scheme("https")
                .map_err(|_| miette::miette!("invalid url: {api_url}"))?;
        } else if api_url.scheme() == "ws" {
            api_url
                .set_scheme("http")
                .map_err(|_| miette::miette!("invalid url: {api_url}"))?;
        }

        let mut rng: SmallRng = rand::make_rng();

        let db = &self.state.db;

        // 1. load cursor
        let cursor_key = b"crawler_cursor";
        let mut cursor: Option<SmolStr> = Db::get(db.cursors.clone(), cursor_key.to_vec())
            .await?
            .map(|bytes| {
                let s = String::from_utf8_lossy(&bytes);
                info!("resuming crawler from cursor: {}", s);
                s.into()
            });
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
            let mut list_repos_url = api_url
                .join("/xrpc/com.atproto.sync.listRepos")
                .into_diagnostic()?;
            list_repos_url
                .query_pairs_mut()
                .append_pair("limit", "1000");
            if let Some(c) = &cursor {
                list_repos_url
                    .query_pairs_mut()
                    .append_pair("cursor", c.as_str());
            }

            let res_result = self.http.get(list_repos_url.clone()).send().await;
            let bytes = match res_result {
                Ok(res) => match res.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        error!(
                            "crawler failed to parse list repos response: {e}. retrying in 30s..."
                        );
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }
                },
                Err(e) => {
                    error!("crawler failed to list repos: {e}. retrying in 30s...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            };
            let output = serde_json::from_slice::<ListReposOutput>(&bytes)
                .into_diagnostic()?
                .into_static();

            if output.repos.is_empty() {
                info!("crawler finished enumeration (or empty page). sleeping for 1 hour.");
                tokio::time::sleep(Duration::from_secs(3600)).await;
                continue;
            }

            debug!("crawler fetched {} repos...", output.repos.len());

            let mut batch = db.inner.batch();
            let mut to_queue = Vec::new();
            let filter = self.state.filter.load();
            // we can check whether or not to backfill repos faster if we only have to check
            // certain known signals, since we can just listRecords for those signals
            // if we have glob signals we cant do this since we dont know what signals to check
            let check_signals = filter.mode == crate::filter::FilterMode::Filter
                && !filter.signals.is_empty()
                && !filter.has_glob_signals();

            // 3. process repos
            let mut unknown_repos = Vec::new();
            for repo in output.repos {
                let parsed_did: Did = repo.did.parse().unwrap();
                let did_key = keys::repo_key(&parsed_did);

                let excl_key = crate::db::filter::exclude_key(repo.did.as_str())?;
                if db.filter.contains_key(&excl_key).into_diagnostic()? {
                    continue;
                }

                // check if known
                if !Db::contains_key(db.repos.clone(), &did_key).await? {
                    unknown_repos.push(repo);
                }
            }

            let mut valid_repos = Vec::new();
            if check_signals && !unknown_repos.is_empty() {
                let mut set = tokio::task::JoinSet::new();
                for repo in unknown_repos {
                    let http = self.http.clone();
                    let api_url = api_url.clone();
                    let filter = filter.clone();
                    set.spawn(async move {
                        let mut found_signal = false;
                        for signal in filter.signals.iter() {
                            let mut list_records_url =
                                api_url.join("/xrpc/com.atproto.repo.listRecords").unwrap();
                            list_records_url
                                .query_pairs_mut()
                                .append_pair("repo", &repo.did)
                                .append_pair("collection", signal)
                                .append_pair("limit", "1");

                            match http.get(list_records_url).send().await {
                                Ok(res) => {
                                    let Ok(bytes) = res.bytes().await else {
                                        error!("failed to read bytes from listRecords response for repo {}, signal {signal}", repo.did);
                                        continue;
                                    };
                                    if let Ok(out) = serde_json::from_slice::<ListRecordsOutput>(&bytes) {
                                        if !out.records.is_empty() {
                                            found_signal = true;
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "failed to listRecords for repo {}, signal {signal}: {e}",
                                        repo.did
                                    );
                                    continue;
                                }
                            }
                        }

                        if !found_signal {
                            trace!(
                                "crawler skipped repo {}: no records match signals",
                                repo.did
                            );
                        }

                        (repo, found_signal)
                    });
                }

                while let Some(res) = set.join_next().await {
                    let (repo, found_signal) = res.into_diagnostic()?;
                    if found_signal {
                        valid_repos.push(repo);
                    }
                }
            } else {
                valid_repos = unknown_repos;
            }

            for repo in valid_repos {
                let parsed_did: Did = repo.did.parse().unwrap();
                let did_key = keys::repo_key(&parsed_did);
                trace!("crawler found new repo: {}", repo.did);

                let state = RepoState::backfilling(rng.next_u64());
                batch.insert(&db.repos, &did_key, ser_repo_state(&state)?);
                batch.insert(&db.pending, keys::pending_key(state.index_id), &did_key);
                to_queue.push(repo.did.clone());
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
