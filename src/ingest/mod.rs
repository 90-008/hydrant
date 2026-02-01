use crate::db::{deser_repo_state, keys, ser_repo_state, Db};
use crate::ops::{self, emit_identity_event};
use crate::state::AppState;
use crate::types::{IdentityEvt, RepoState, RepoStatus};
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
                    let db_inner = state_clone.db.inner.clone();
                    let buffer = state_clone.db.buffer.clone();

                    let mut items_to_persist = Vec::with_capacity(batch_items.len());
                    items_to_persist.extend(batch_items.drain(..));

                    let res = tokio::task::spawn_blocking(move || {
                        let mut batch = db_inner.batch();
                        for (k, v) in items_to_persist {
                            batch.insert(&buffer, k, v);
                        }
                        batch.commit()
                    })
                    .await;

                    match res {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            Db::check_poisoned(&e);
                            error!("failed to persist buffer batch: {}", e)
                        }
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
        match msg {
            SubscribeReposMessage::Commit(commit) => {
                self.state.cur_firehose.store(commit.seq, Ordering::SeqCst);

                if let Err(e) = self.process_commit(&commit).await {
                    error!("failed to process commit {}: {e}", commit.seq);
                    Db::check_poisoned_report(&e);
                    // buffer for later inspection/retry
                    let _ = self.buffer_event(&commit).await;
                }
            }
            SubscribeReposMessage::Identity(identity) => {
                self.state
                    .cur_firehose
                    .store(identity.seq, Ordering::SeqCst);
                self.process_identity(*identity).await;
            }
            SubscribeReposMessage::Account(account) => {
                self.state.cur_firehose.store(account.seq, Ordering::SeqCst);
                self.process_account(&account).await;
            }
            _ => {} // ignore info/sync for now
        }
    }

    async fn process_identity(
        &mut self,
        identity: jacquard::api::com_atproto::sync::subscribe_repos::Identity<'_>,
    ) {
        // identity update implies activity
        self.check_reactivation(&identity.did).await;

        let Some(handle) = identity.handle else {
            return;
        };

        let handle_str = handle.as_str();

        let res = self
            .state
            .db
            .update_repo_state_async(&identity.did, {
                let handle_str = handle_str.to_string();
                move |state, _| {
                    if state.handle.as_deref() == Some(&handle_str) {
                        return Ok((false, ()));
                    }
                    info!("updating handle for {} to {}", state.did, handle_str);
                    state.handle = Some(handle_str.into());
                    Ok((true, ()))
                }
            })
            .await;

        match res {
            Ok(Some((state, _))) => {
                // emit identity event
                let evt = IdentityEvt {
                    did: identity.did.as_str().into(),
                    handle: handle_str.into(),
                    is_active: matches!(
                        state.status,
                        RepoStatus::Synced | RepoStatus::Backfilling | RepoStatus::New
                    ),
                    status: match state.status {
                        RepoStatus::Deactivated => "deactivated".into(),
                        RepoStatus::Takendown => "takendown".into(),
                        RepoStatus::Suspended => "suspended".into(),
                        _ => "active".into(),
                    },
                };

                if state.status == RepoStatus::Synced {
                    self.emit_identity_event(evt).await;
                }
            }
            Ok(None) => {}
            Err(e) => {
                error!("failed to update repo state for {}: {e}", identity.did);
                Db::check_poisoned_report(&e);
            }
        }
    }

    async fn process_account(
        &mut self,
        account: &jacquard::api::com_atproto::sync::subscribe_repos::Account<'_>,
    ) {
        use jacquard::api::com_atproto::sync::subscribe_repos::AccountStatus;

        if account.active {
            self.check_reactivation(&account.did).await;
        }

        if let Some(status) = &account.status {
            match status {
                AccountStatus::Deleted => {
                    info!("repo {} deleted, removing", account.did);
                    // emit deletion event FIRST if live
                    let did_key = keys::repo_key(&account.did);
                    if let Ok(Some(bytes)) = Db::get(self.state.db.repos.clone(), did_key).await {
                        if let Ok(state) = deser_repo_state(bytes) {
                            if state.status == RepoStatus::Synced {
                                let evt = IdentityEvt {
                                    did: account.did.as_str().into(),
                                    handle: state.handle.unwrap_or_default(),
                                    is_active: false,
                                    status: "deleted".into(),
                                };
                                self.emit_identity_event(evt).await;
                            }
                        }
                    }

                    if let Err(e) = tokio::task::spawn_blocking({
                        let state = self.state.clone();
                        let did = account.did.clone().into_static();
                        move || ops::delete_repo(&state.db, &did)
                    })
                    .await
                    .into_diagnostic()
                    .and_then(|r| r)
                    {
                        error!("failed to delete repo {}: {e}", account.did);
                        Db::check_poisoned_report(&e);
                    }
                }
                AccountStatus::Deactivated
                | AccountStatus::Takendown
                | AccountStatus::Suspended => {
                    let new_status = match status {
                        AccountStatus::Deactivated => RepoStatus::Deactivated,
                        AccountStatus::Takendown => RepoStatus::Takendown,
                        AccountStatus::Suspended => RepoStatus::Suspended,
                        _ => unreachable!(),
                    };

                    let res = self
                        .state
                        .db
                        .update_repo_state_async(&account.did, {
                            let new_status = new_status.clone();
                            move |state, _| {
                                if state.status != new_status {
                                    let was_synced = state.status == RepoStatus::Synced;
                                    state.status = new_status;
                                    Ok((true, was_synced))
                                } else {
                                    Ok((false, false))
                                }
                            }
                        })
                        .await;

                    match res {
                        Ok(Some((state, was_synced))) => {
                            if was_synced {
                                let evt = IdentityEvt {
                                    did: account.did.as_str().into(),
                                    handle: state.handle.unwrap_or_default(),
                                    is_active: false,
                                    status: match state.status {
                                        RepoStatus::Deactivated => "deactivated".into(),
                                        RepoStatus::Takendown => "takendown".into(),
                                        RepoStatus::Suspended => "suspended".into(),
                                        _ => "active".into(),
                                    },
                                };
                                self.emit_identity_event(evt).await;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            error!("failed to update repo status for {}: {e}", account.did);
                            Db::check_poisoned_report(&e);
                        }
                    }
                }
                _ => {} // ignore others
            }
        } else if account.active {
            if let Ok(Some(bytes)) =
                Db::get(self.state.db.repos.clone(), keys::repo_key(&account.did)).await
            {
                if let Ok(state) = rmp_serde::from_slice::<RepoState>(&bytes) {
                    if matches!(
                        state.status,
                        RepoStatus::Backfilling | RepoStatus::Synced | RepoStatus::New
                    ) {
                        let evt = IdentityEvt {
                            did: account.did.as_str().into(),
                            handle: state.handle.unwrap_or_default(),
                            is_active: true,
                            status: "active".into(),
                        };
                        self.emit_identity_event(evt).await;
                    }
                }
            }
        }
    }

    async fn emit_identity_event(&self, evt: IdentityEvt) {
        emit_identity_event(&self.state.db, evt);
    }

    async fn check_reactivation(&mut self, did: &jacquard::types::did::Did<'_>) {
        let did_key = keys::repo_key(did);
        let Ok(Some(bytes)) = Db::get(self.state.db.repos.clone(), did_key).await else {
            return;
        };
        let Ok(mut state) = deser_repo_state(bytes) else {
            return;
        };

        match state.status {
            RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended => {
                info!("reactivating repo {did} (was {:?})", state.status);

                state.status = RepoStatus::Backfilling;
                let Ok(bytes) = ser_repo_state(&state) else {
                    return;
                };

                let mut batch = self.state.db.inner.batch();
                batch.insert(&self.state.db.repos, did_key, bytes);
                batch.insert(&self.state.db.pending, did_key, Vec::new());
                let res = tokio::task::spawn_blocking(move || batch.commit().into_diagnostic())
                    .await
                    .into_diagnostic()
                    .flatten();
                if let Err(e) = res {
                    error!("failed to commit reactivation for {did}: {e}");
                    return;
                }

                if self
                    .state
                    .backfill_tx
                    .send(did.clone().into_static())
                    .is_err()
                {
                    error!("failed to queue backfill for {did}, backfill worker crashed?");
                }
            }
            _ => {}
        }
    }

    async fn process_commit(
        &self,
        commit: &jacquard::api::com_atproto::sync::subscribe_repos::Commit<'_>,
    ) -> Result<()> {
        let db = &self.state.db;
        let did = &commit.repo;

        let mut should_process = self.full_network;
        let did_key = keys::repo_key(&did);

        // check repo state
        let repo_state_bytes = Db::get(db.repos.clone(), did_key).await?;

        if !should_process && repo_state_bytes.is_some() {
            should_process = true;
        }

        if !should_process {
            return Ok(());
        }

        let repo_state = if let Some(bytes) = repo_state_bytes {
            deser_repo_state(bytes).ok()
        } else {
            None
        };

        let status = repo_state
            .as_ref()
            .map(|s| s.status.clone())
            .unwrap_or(RepoStatus::New);

        match status {
            RepoStatus::New
            | RepoStatus::Deactivated
            | RepoStatus::Takendown
            | RepoStatus::Suspended => {
                if matches!(
                    status,
                    RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended
                ) {
                    info!("reactivating repo {did} due to new commit");
                } else {
                    debug!("new repo detected: {did}");
                }

                // 1. save state as backfilling
                tokio::task::spawn_blocking({
                    let state = self.state.clone();
                    let did = did.clone().into_static();
                    let did_key = did_key.to_vec();
                    move || {
                        let mut new_state = RepoState::new(did);
                        new_state.status = RepoStatus::Backfilling;

                        let mut batch = state.db.inner.batch();
                        batch.insert(&state.db.repos, &did_key, ser_repo_state(&new_state)?);
                        batch.insert(&state.db.pending, &did_key, Vec::new());
                        batch.commit().into_diagnostic()
                    }
                })
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
                if let Some(mut state) = repo_state {
                    if !state.rev.is_empty() && commit.rev.as_str() <= state.rev.as_str() {
                        debug!(
                            "skipping replayed event for {}: {} <= {}",
                            did, commit.rev, state.rev
                        );
                        return Ok(());
                    }

                    // check gap
                    if let Some(prev) = &commit.prev_data {
                        if !state.data.is_empty() && prev.as_str() != state.data.as_str() {
                            tracing::warn!(
                                "gap detected for {}: prev {} != stored {}. triggering backfill",
                                did,
                                prev,
                                state.data
                            );

                            // 1. update status to Backfilling
                            state.status = RepoStatus::Backfilling;
                            tokio::task::spawn_blocking({
                                let app_state = self.state.clone();
                                let did_key = did_key.to_vec();
                                move || {
                                    let mut batch = app_state.db.inner.batch();
                                    batch.insert(
                                        &app_state.db.repos,
                                        &did_key,
                                        ser_repo_state(&state)?,
                                    );
                                    batch.insert(&app_state.db.pending, &did_key, Vec::new()); // ensure it's in pending for recovery
                                    batch.commit().into_diagnostic()
                                }
                            })
                            .await
                            .into_diagnostic()??;

                            // 2. queue backfill
                            if let Err(e) = self.state.backfill_tx.send(did.clone().into_static()) {
                                error!("failed to queue backfill for {}: {}", did, e);
                            }

                            // 3. buffer this event
                            self.buffer_event(commit).await?;
                            return Ok(());
                        }
                    }
                }

                let res = tokio::task::spawn_blocking({
                    let app_state = self.state.clone();
                    let commit_static = commit.clone().into_static();
                    move || ops::apply_commit(&app_state.db, &commit_static, true)
                })
                .await
                .into_diagnostic()?;

                if let Err(e) = res {
                    error!("failed to apply live commit for {did}: {e}");
                    Db::check_poisoned_report(&e);
                    self.buffer_event(commit).await?;
                } else {
                    debug!("synced event for {did}, {} ops", commit.ops.len());
                }
            }
            RepoStatus::Error(_) => {
                // maybe retry? for now ignore.
            }
        }
        Ok(())
    }

    async fn buffer_event(
        &self,
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
            error!("failed to buffer event (channel closed): {e}");
        }

        Ok(())
    }
}
