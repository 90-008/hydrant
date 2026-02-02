use crate::db::{keys, Db};
use crate::ops;
use crate::state::AppState;
use crate::types::{AccountEvt, IdentityEvt};

use super::BufferedMessage;
use jacquard::api::com_atproto::sync::subscribe_repos::SubscribeReposMessage;
use jacquard::types::did::Did;
use jacquard_common::IntoStatic;
use miette::{IntoDiagnostic, Result};
use smol_str::ToSmolStr;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tracing::{debug, error, info, trace, warn};

pub struct BufferProcessor {
    state: Arc<AppState>,
    rx: mpsc::UnboundedReceiver<BufferedMessage>,
}

impl BufferProcessor {
    pub fn new(state: Arc<AppState>, rx: mpsc::UnboundedReceiver<BufferedMessage>) -> Self {
        Self { state, rx }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut queues: HashMap<Did<'static>, VecDeque<BufferedMessage>> = HashMap::new();

        // recover from DB
        let recovered = self.recover_from_db().await?;
        if !recovered.is_empty() {
            info!("recovered {} buffered messages from db", recovered.len());
            for msg in recovered {
                queues.entry(msg.did.clone()).or_default().push_back(msg);
            }
        }

        let mut to_remove: Vec<Did<'static>> = Vec::new();

        loop {
            while let Ok(msg) = self.rx.try_recv() {
                queues.entry(msg.did.clone()).or_default().push_back(msg);
            }

            for (did, queue) in &mut queues {
                if self.state.blocked_dids.contains_sync(did) {
                    continue;
                }

                while let Some(buffered) = queue.pop_front() {
                    if let Err(e) = self.process_message(buffered).await {
                        error!("failed to process buffered message for {did}: {e}");
                        Db::check_poisoned_report(&e);
                    }
                }

                if queue.is_empty() {
                    to_remove.push(did.clone());
                }
            }

            for did in to_remove.drain(..) {
                queues.remove(&did);
            }

            // wait until we receive a new message
            match self.rx.recv().await {
                Some(msg) => {
                    queues.entry(msg.did.clone()).or_default().push_back(msg);
                }
                None => {
                    debug!("buffer processor channel closed, exiting");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn process_message(&self, buffered: BufferedMessage) -> Result<()> {
        let did = buffered.did;
        let buffered_at = buffered.buffered_at;

        match buffered.msg {
            SubscribeReposMessage::Commit(commit) => {
                trace!("processing buffered commit for {did}");
                let state = self.state.clone();
                tokio::task::spawn_blocking(move || ops::apply_commit(&state.db, &commit, true))
                    .await
                    .into_diagnostic()??;
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!("processing buffered identity for {did}");
                let handle = identity.handle.as_ref().map(|h| h.as_str().to_smolstr());
                let evt = IdentityEvt {
                    did: did.clone(),
                    handle,
                };
                ops::emit_identity_event(&self.state.db, evt);
            }
            SubscribeReposMessage::Account(account) => {
                debug!("processing buffered account for {did}");
                let evt = AccountEvt {
                    did: did.clone(),
                    active: account.active,
                    status: account.status.as_ref().map(|s| s.to_smolstr()),
                };
                ops::emit_account_event(&self.state.db, evt);

                let did = did.clone();
                let state = self.state.clone();
                let account = account.clone();

                tokio::task::spawn_blocking(move || -> Result<()> {
                    if !account.active {
                        use jacquard::api::com_atproto::sync::subscribe_repos::AccountStatus;
                        if let Some(status) = &account.status {
                            match status {
                                AccountStatus::Deleted => {
                                    info!("account {did} deleted, wiping data");
                                    ops::delete_repo(&state.db, &did)?;
                                }
                                AccountStatus::Takendown => {
                                    ops::update_repo_status(
                                        &state.db,
                                        &did,
                                        crate::types::RepoStatus::Takendown,
                                    )?;
                                }
                                AccountStatus::Suspended => {
                                    ops::update_repo_status(
                                        &state.db,
                                        &did,
                                        crate::types::RepoStatus::Suspended,
                                    )?;
                                }
                                AccountStatus::Deactivated => {
                                    ops::update_repo_status(
                                        &state.db,
                                        &did,
                                        crate::types::RepoStatus::Deactivated,
                                    )?;
                                }
                                AccountStatus::Throttled | AccountStatus::Desynchronized => {
                                    let status_str = status.as_str().to_smolstr();
                                    ops::update_repo_status(
                                        &state.db,
                                        &did,
                                        crate::types::RepoStatus::Error(status_str),
                                    )?;
                                }
                                AccountStatus::Other(s) => {
                                    warn!("unknown account status for {did}: {s}");
                                }
                            }
                        } else {
                            warn!("account {did} inactive but no status provided");
                        }
                    } else {
                        // active account, clear any error/suspension states if they exist
                        // we set it to Synced because we are receiving live events for it
                        ops::update_repo_status(&state.db, &did, crate::types::RepoStatus::Synced)?;
                    }
                    Ok(())
                })
                .await
                .into_diagnostic()??;
            }
            _ => {
                warn!("unknown message type in buffer for {did}");
            }
        }

        // remove from DB buffer
        self.remove_from_db_buffer(&did, buffered_at).await?;

        Ok(())
    }

    async fn recover_from_db(&self) -> Result<Vec<BufferedMessage>> {
        let state = self.state.clone();

        spawn_blocking(move || {
            let mut recovered = Vec::new();
            for item in state.db.buffer.iter() {
                let (key, value) = item.into_inner().into_diagnostic()?;
                let (did, ts) = keys::parse_buffer_key(&key)?;
                let msg =
                    rmp_serde::from_slice::<SubscribeReposMessage>(&value).into_diagnostic()?;
                recovered.push(BufferedMessage {
                    did,
                    msg: msg.into_static(),
                    buffered_at: ts,
                });
            }
            // ensure chronological order across all DIDs
            recovered.sort_by_key(|m| m.buffered_at);
            Ok(recovered)
        })
        .await
        .into_diagnostic()
        .flatten()
    }

    async fn remove_from_db_buffer(&self, did: &Did<'_>, buffered_at: i64) -> Result<()> {
        let key = keys::buffer_key(did, buffered_at);
        self.state.db.buffer.remove(key).into_diagnostic()?;
        Ok(())
    }
}
