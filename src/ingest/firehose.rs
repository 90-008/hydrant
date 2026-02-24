use crate::db::{self, Db, keys};
use crate::filter::{FilterHandle, FilterMode};
use crate::ingest::{BufferTx, IngestMessage};
use crate::state::AppState;
use jacquard::api::com_atproto::sync::subscribe_repos::{SubscribeRepos, SubscribeReposMessage};
use jacquard::types::did::Did;
use jacquard_common::xrpc::{SubscriptionClient, TungsteniteSubscriptionClient};
use miette::{IntoDiagnostic, Result};
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
    filter: FilterHandle,
    _verify_signatures: bool,
}

impl FirehoseIngestor {
    pub fn new(
        state: Arc<AppState>,
        buffer_tx: BufferTx,
        relay_host: Url,
        filter: FilterHandle,
        verify_signatures: bool,
    ) -> Self {
        Self {
            state,
            buffer_tx,
            relay_host,
            filter,
            _verify_signatures: verify_signatures,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
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

        if !self
            .should_process(did)
            .await
            .inspect_err(|e| error!("failed to check if we should process {did}: {e}"))
            .unwrap_or(false)
        {
            return;
        }

        if let Err(e) = self.buffer_tx.send(IngestMessage::Firehose(msg)) {
            error!("failed to send message to buffer processor: {e}");
        }
    }

    async fn should_process(&self, did: &Did<'_>) -> Result<bool> {
        let filter = self.filter.load();

        let excl_key = crate::filter::filter_key(crate::filter::EXCLUDE_PREFIX, did.as_str());
        if self
            .state
            .db
            .filter
            .contains_key(&excl_key)
            .into_diagnostic()?
        {
            return Ok(false);
        }

        match filter.mode {
            FilterMode::Full => Ok(true),
            FilterMode::Dids | FilterMode::Signal => {
                let did_key = crate::filter::filter_key(crate::filter::DID_PREFIX, did.as_str());
                if self
                    .state
                    .db
                    .filter
                    .contains_key(&did_key)
                    .into_diagnostic()?
                {
                    return Ok(true);
                }
                Db::contains_key(self.state.db.repos.clone(), keys::repo_key(did)).await
            }
        }
    }
}
