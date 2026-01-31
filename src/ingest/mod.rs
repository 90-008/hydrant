use crate::db::{keys, Db};
use crate::ops;
use crate::state::AppState;
use crate::types::{RepoState, RepoStatus};
use jacquard::api::com_atproto::sync::subscribe_repos::{SubscribeRepos, SubscribeReposMessage};
use jacquard_common::xrpc::{SubscriptionClient, TungsteniteSubscriptionClient};
use jacquard_common::IntoStatic;
use miette::{IntoDiagnostic, Result};
use n0_future::StreamExt;
use smol_str::SmolStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use url::Url;

pub struct Ingestor {
    state: Arc<AppState>,
    relay_host: SmolStr,
    buffer_tx: mpsc::Sender<(Vec<u8>, Vec<u8>)>,
    full_network: bool,
}

impl Ingestor {
    pub fn new(state: Arc<AppState>, relay_host: SmolStr, full_network: bool) -> Self {
        let (buffer_tx, mut buffer_rx) = mpsc::channel::<(Vec<u8>, Vec<u8>)>(1000);

        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut batch_items = Vec::with_capacity(100);
            const MAX_BATCH_SIZE: usize = 100;
            const BATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(10);

            loop {
                // wait for at least one item
                match buffer_rx.recv().await {
                    Some(item) => batch_items.push(item),
                    None => break,
                }

                // collect more items until batch is full or timeout
                let deadline = tokio::time::Instant::now() + BATCH_TIMEOUT;
                while batch_items.len() < MAX_BATCH_SIZE {
                    match tokio::time::timeout_at(deadline, buffer_rx.recv()).await {
                        Ok(Some(item)) => batch_items.push(item),
                        Ok(None) => break, // channel closed
                        Err(_) => break,   // timeout reached
                    }
                }

                if !batch_items.is_empty() {
                    let mut batch = state_clone.db.inner.batch();
                    for (k, v) in batch_items.drain(..) {
                        batch.insert(&state_clone.db.buffer, k, v);
                    }

                    let res = tokio::task::spawn_blocking(move || batch.commit()).await;
                    match res {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => error!("failed to persist buffer batch: {}", e),
                        Err(e) => error!("buffer worker join error: {}", e),
                    }
                }
            }
        });

        Self {
            state,
            relay_host,
            buffer_tx,
            full_network,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let base_url = Url::parse(&self.relay_host).into_diagnostic()?;

        loop {
            // 1. load cursor
            let current_cursor = self.state.cur_firehose.load(Ordering::SeqCst);
            let start_cursor = if current_cursor > 0 {
                Some(current_cursor)
            } else {
                let cursor_key = b"firehose_cursor";
                if let Ok(Some(bytes)) =
                    Db::get(self.state.db.cursors.clone(), cursor_key.to_vec()).await
                {
                    let s = String::from_utf8_lossy(&bytes);
                    debug!("resuming from cursor: {}", s);
                    s.parse::<i64>().ok()
                } else {
                    info!("no cursor found, live tailing");
                    None
                }
            };

            if let Some(c) = start_cursor {
                self.state.cur_firehose.store(c, Ordering::SeqCst);
            }

            // 2. connect
            let client = TungsteniteSubscriptionClient::from_base_uri(base_url.clone());
            let params = if let Some(c) = start_cursor {
                SubscribeRepos::new().cursor(c).build()
            } else {
                SubscribeRepos::new().build()
            };

            let stream = match client.subscribe(&params).await {
                Ok(s) => s,
                Err(e) => {
                    error!("failed to connect to firehose: {e}, retrying in 5s...");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let (_sink, mut messages) = stream.into_stream();

            info!("firehose connected");

            // 3. process loop
            while let Some(msg_res) = messages.next().await {
                match msg_res {
                    Ok(msg) => {
                        if let Err(e) = self.handle_message(msg).await {
                            error!("failed to handle firehose message: {e}");
                        }
                    }
                    Err(e) => {
                        error!("firehose stream error: {e}");
                        break;
                    }
                }
            }

            error!("firehose disconnected, reconnecting in 5s...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }

    async fn handle_message(&mut self, msg: SubscribeReposMessage<'_>) -> Result<()> {
        match msg {
            SubscribeReposMessage::Commit(commit) => {
                self.state.cur_firehose.store(commit.seq, Ordering::SeqCst);

                if let Err(e) = self.process_commit(&commit).await {
                    error!("failed to process commit {}: {e}", commit.seq);
                    // buffer for later inspection/retry
                    let _ = self.buffer_event(&commit).await;
                }
            }
            _ => {} // ignore identity/account/etc for now
        }
        Ok(())
    }

    async fn process_commit(
        &mut self,
        commit: &jacquard::api::com_atproto::sync::subscribe_repos::Commit<'_>,
    ) -> Result<()> {
        let db = self.state.db.clone();
        let did = &commit.repo;

        let mut should_process = self.full_network;
        let did_key = keys::repo_key(&did);

        if !should_process {
            if Db::contains_key(db.repos.clone(), did_key).await? {
                should_process = true;
            }
        }

        if !should_process {
            return Ok(());
        }

        // check repo state
        let state_bytes = Db::get(db.repos.clone(), did_key).await?;

        let repo_state = if let Some(bytes) = state_bytes {
            rmp_serde::from_slice::<RepoState>(&bytes).ok()
        } else {
            None
        };

        let status = repo_state
            .as_ref()
            .map(|s| s.status.clone())
            .unwrap_or(RepoStatus::New);

        match status {
            RepoStatus::New => {
                debug!("new repo detected: {}", did);
                // 1. save state as backfilling
                let mut new_state = RepoState::new(commit.repo.clone().into_static());
                new_state.status = RepoStatus::Backfilling;
                let bytes = rmp_serde::to_vec(&new_state).into_diagnostic()?;

                let mut batch = db.inner.batch();
                batch.insert(&db.repos, did_key, bytes);
                batch.insert(&db.pending, did_key, Vec::new());

                tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                    .await
                    .into_diagnostic()??;

                // 2. queue for backfill
                if let Err(e) = self.state.backfill_tx.send(did.clone().into_static()) {
                    error!("failed to queue backfill for {}: {}", did, e);
                }

                // 3. buffer this event
                self.buffer_event(commit).await?;
            }
            RepoStatus::Backfilling => {
                debug!("buffering event for backfilling repo: {}", did);
                self.buffer_event(commit).await?;
            }
            RepoStatus::Synced => {
                // check revision
                if let Some(state) = repo_state {
                    if !state.rev.is_empty() && commit.rev.as_str() <= state.rev.as_str() {
                        debug!(
                            "skipping replayed event for {}: {} <= {}",
                            did, commit.rev, state.rev
                        );
                        return Ok(());
                    }
                }

                // apply immediately
                let db = db.clone();
                let commit_static = commit.clone().into_static();
                let did_static = did.clone().into_static();

                let res = tokio::task::spawn_blocking(move || {
                    ops::apply_commit(&db, &commit_static, true)
                })
                .await
                .into_diagnostic()?;

                if let Err(e) = res {
                    error!("failed to apply live commit for {}: {}", did_static, e);
                    self.buffer_event(commit).await?;
                } else {
                    debug!("synced event for {}, {} ops", did_static, commit.ops.len());
                }
            }
            RepoStatus::Error(_) => {
                // maybe retry? for now ignore.
            }
        }
        Ok(())
    }

    async fn buffer_event(
        &mut self,
        commit: &jacquard::api::com_atproto::sync::subscribe_repos::Commit<'_>,
    ) -> Result<()> {
        // we need to store the event to replay it later.
        // key: {DID}\x00{SEQ} -> guarantees ordering
        let mut key = Vec::new();
        key.extend_from_slice(keys::buffer_prefix(&commit.repo));
        key.push(0x00);
        key.extend_from_slice(&commit.seq.to_be_bytes());

        // value: serialized commit
        let val = rmp_serde::to_vec(commit).into_diagnostic()?;

        if let Err(e) = self.buffer_tx.send((key, val)).await {
            error!("failed to buffer event (channel closed): {}", e);
        }

        Ok(())
    }
}
