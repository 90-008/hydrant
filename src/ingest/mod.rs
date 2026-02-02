use crate::db::{keys, Db};
use crate::state::AppState;
use jacquard::api::com_atproto::sync::subscribe_repos::{SubscribeRepos, SubscribeReposMessage};
use jacquard::types::did::Did;
use jacquard_common::xrpc::{SubscriptionClient, TungsteniteSubscriptionClient};
use jacquard_common::IntoStatic;
use miette::Result;
use n0_future::StreamExt;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, error, info};
use url::Url;

pub struct Ingestor {
    state: Arc<AppState>,
    relay_host: Url,
    full_network: bool,
}

impl Ingestor {
    pub fn new(state: Arc<AppState>, relay_host: Url, full_network: bool) -> Self {
        Self {
            state,
            relay_host,
            full_network,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            // 1. load cursor
            let current_cursor = self.state.cur_firehose.load(Ordering::SeqCst);
            let start_cursor = if current_cursor > 0 {
                Some(current_cursor)
            } else {
                let cursor_key = b"firehose_cursor";
                if let Ok(Some(bytes)) =
                    crate::db::Db::get(self.state.db.cursors.clone(), cursor_key.to_vec()).await
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
            let client = TungsteniteSubscriptionClient::from_base_uri(self.relay_host.clone());
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
                    Ok(msg) => self.handle_message(msg).await,
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

    async fn handle_message(&mut self, msg: SubscribeReposMessage<'_>) {
        let (did, seq) = match &msg {
            SubscribeReposMessage::Commit(commit) => (&commit.repo, commit.seq),
            SubscribeReposMessage::Identity(identity) => (&identity.did, identity.seq),
            SubscribeReposMessage::Account(account) => (&account.did, account.seq),
            _ => return,
        };

        if !self.should_process(did).await.unwrap_or(false) {
            return;
        }

        self.state.cur_firehose.store(seq, Ordering::SeqCst);

        let buffered_at = chrono::Utc::now().timestamp_millis();

        // persist to DB for crash recovery
        let db_key = keys::buffer_key(&did, buffered_at);
        if let Ok(bytes) = rmp_serde::to_vec(&msg) {
            if let Err(e) = Db::insert(self.state.db.buffer.clone(), db_key, bytes).await {
                error!("failed to persist buffered message: {e}");
            }
        }

        // always buffer through the BufferProcessor
        let buffered_msg = crate::buffer::BufferedMessage {
            did: did.clone().into_static(),
            msg: msg.into_static(),
            buffered_at,
        };

        if let Err(e) = self.state.buffer_tx.send(buffered_msg) {
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
