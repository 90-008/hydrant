use crate::db::{self, Db, keys};
use crate::ingest::BufferTx;
use crate::state::AppState;
use jacquard::api::com_atproto::sync::subscribe_repos::{SubscribeRepos, SubscribeReposMessage};
use jacquard::types::did::Did;
use jacquard_common::xrpc::{SubscriptionClient, TungsteniteSubscriptionClient};
use miette::Result;
use n0_future::StreamExt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info};
use url::Url;

pub struct FirehoseIngestor {
    state: Arc<AppState>,
    buffer_tx: BufferTx,
    relay_host: Url,
    full_network: bool,
    verify_signatures: bool,
}

impl FirehoseIngestor {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: BufferTx,
        relay_host: Url,
        full_network: bool,
        verify_signatures: bool,
    ) -> Self {
        Self {
            state,
            buffer_tx,
            relay_host,
            full_network,
            verify_signatures,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            // 1. load cursor
            let current_cursor = self.state.cur_firehose.load(Ordering::SeqCst);
            let start_cursor = if current_cursor > 0 {
                Some(current_cursor)
            } else {
                db::get_firehose_cursor(&self.state.db).await?
            };
            match start_cursor {
                Some(c) => info!("resuming from cursor: {c}"),
                None => info!("no cursor found, live tailing"),
            }

            if let Some(c) = start_cursor {
                self.state.cur_firehose.store(c, Ordering::SeqCst);
            }

            // 2. connect
            let client = TungsteniteSubscriptionClient::from_base_uri(self.relay_host.clone());
            let params = SubscribeRepos::new;
            let params = start_cursor
                .map(|c| params().cursor(c))
                .unwrap_or_else(params)
                .build();

            let stream = match client.subscribe(&params).await {
                Ok(s) => s,
                Err(e) => {
                    error!("failed to connect to firehose: {e}, retrying in 5s...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let (_sink, mut messages) = stream.into_stream();

            info!("firehose connected");

            // 3. process loop
            while let Some(msg_res) = messages.next().await {
                match msg_res {
                    Ok(msg) => self.handle_message(msg).await,
                    Err(e) => {
                        error!("firehose stream error: {e}");
                        break;
                    }
                }
            }

            error!("firehose disconnected, reconnecting in 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn handle_message(&mut self, msg: SubscribeReposMessage<'static>) {
        let did = match &msg {
            SubscribeReposMessage::Commit(commit) => &commit.repo,
            SubscribeReposMessage::Identity(identity) => &identity.did,
            SubscribeReposMessage::Account(account) => &account.did,
            SubscribeReposMessage::Sync(sync) => &sync.did,
            _ => return,
        };

        if !self.should_process(did).await.unwrap_or(false) {
            return;
        }

        // pre-warm the key cache for commit events
        if self.verify_signatures && matches!(&msg, SubscribeReposMessage::Commit(_)) {
            let state = self.state.clone();
            let did = did.clone();
            tokio::spawn(async move {
                let _ = state.resolver.resolve_signing_key(&did).await;
            });
        }

        if let Err(e) = self.buffer_tx.send(msg) {
            error!("failed to send message to buffer processor: {e}");
        }
    }

    async fn should_process(&self, did: &Did<'_>) -> Result<bool> {
        if self.full_network {
            return Ok(true);
        }
        let did_key = keys::repo_key(did);
        Db::contains_key(self.state.db.repos.clone(), did_key).await
    }
}
