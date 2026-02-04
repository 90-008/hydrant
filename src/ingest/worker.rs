use crate::db::{self, keys};
use crate::ingest::BufferedMessage;
use crate::ops::{self, send_backfill_req};
use crate::resolver::NoSigningKeyError;
use crate::state::AppState;
use crate::types::{AccountEvt, IdentityEvt, RepoState, RepoStatus};
use jacquard::api::com_atproto::sync::subscribe_repos::SubscribeReposMessage;

use fjall::OwnedWriteBatch;
use futures::future::join_all;
use jacquard::cowstr::ToCowStr;
use jacquard::types::did::Did;
use jacquard_common::IntoStatic;
use jacquard_common::types::crypto::PublicKey;
use jacquard_repo::error::CommitError;
use miette::{Diagnostic, IntoDiagnostic, Result};
use smol_str::ToSmolStr;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};

#[derive(Debug)]
struct KeyFetchError(miette::Report);

impl std::fmt::Display for KeyFetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for KeyFetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl Diagnostic for KeyFetchError {
    fn code<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        self.0.code()
    }

    fn help<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        self.0.help()
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        self.0.labels()
    }

    fn diagnostic_source(&self) -> Option<&dyn Diagnostic> {
        self.0.diagnostic_source()
    }

    fn related<'a>(&'a self) -> Option<Box<dyn Iterator<Item = &'a dyn Diagnostic> + 'a>> {
        self.0.related()
    }

    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        self.0.source_code()
    }

    fn severity(&self) -> Option<miette::Severity> {
        self.0.severity()
    }

    fn url<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        self.0.url()
    }
}

#[derive(Debug, Clone, Copy)]
enum ProcessResult {
    Deleted,
    Ok,
}

enum RepoCheckResult {
    Syncing,
    Ok(RepoState<'static>),
}

pub struct FirehoseWorker {
    state: Arc<AppState>,
    rx: mpsc::UnboundedReceiver<BufferedMessage>,
    verify_signatures: bool,
}

impl FirehoseWorker {
    pub fn new(
        state: Arc<AppState>,
        rx: mpsc::UnboundedReceiver<BufferedMessage>,
        verify_signatures: bool,
    ) -> Self {
        Self {
            state,
            rx,
            verify_signatures,
        }
    }

    pub fn run(mut self, handle: tokio::runtime::Handle) -> Result<()> {
        const BUF_SIZE: usize = 500;
        let mut buf = Vec::<BufferedMessage>::with_capacity(BUF_SIZE);
        let mut failed = Vec::<BufferedMessage>::new();

        loop {
            let mut batch = self.state.db.inner.batch();
            let mut deleted = HashSet::new();

            // resolve signing keys for commits and syncs if verification is enabled
            let keys = if self.verify_signatures {
                let dids: HashSet<Did> = buf
                    .iter()
                    .filter_map(|msg| match msg {
                        SubscribeReposMessage::Commit(c) => Some(c.repo.clone()),
                        SubscribeReposMessage::Sync(s) => Some(s.did.clone()),
                        _ => None,
                    })
                    .collect();

                let futures = dids.into_iter().map(|did| async {
                    let res = self.state.resolver.resolve_signing_key(&did).await;
                    (did, res)
                });

                handle.block_on(join_all(futures)).into_iter().collect()
            } else {
                HashMap::new()
            };

            for msg in buf.drain(..) {
                let (did, seq) = match &msg {
                    SubscribeReposMessage::Commit(c) => (&c.repo, c.seq),
                    SubscribeReposMessage::Identity(i) => (&i.did, i.seq),
                    SubscribeReposMessage::Account(a) => (&a.did, a.seq),
                    SubscribeReposMessage::Sync(s) => (&s.did, s.seq),
                    _ => continue,
                };

                if self.state.blocked_dids.contains_sync(did) {
                    failed.push(msg);
                    continue;
                }
                if deleted.contains(did) {
                    continue;
                }

                match self.process_message(&mut batch, &msg, did, &keys) {
                    Ok(ProcessResult::Ok) => {}
                    Ok(ProcessResult::Deleted) => {
                        deleted.insert(did.clone());
                    }
                    Err(e) => {
                        error!("error processing message for {did}: {e}");
                        db::check_poisoned_report(&e);
                        // dont retry commit or sync on key fetch errors
                        // since we'll just try again later if we get commit or sync again
                        if e.downcast_ref::<KeyFetchError>().is_none()
                            && e.downcast_ref::<CommitError>().is_none()
                            && e.downcast_ref::<NoSigningKeyError>().is_none()
                        {
                            failed.push(msg);
                        }
                    }
                }

                self.state
                    .cur_firehose
                    .store(seq, std::sync::atomic::Ordering::SeqCst);
            }

            // commit all changes to db
            batch.commit().into_diagnostic()?;
            self.state
                .db
                .inner
                .persist(fjall::PersistMode::Buffer)
                .into_diagnostic()?;

            // add failed back to buf here so the ordering is preserved
            if !failed.is_empty() {
                buf.append(&mut failed);
            }

            // wait until we receive some messages
            // this does mean we will have an up to 1 second delay, before we send events to consumers
            // but thats reasonable imo, could also be configured of course
            let _ = handle.block_on(tokio::time::timeout(
                Duration::from_secs(1),
                self.rx.recv_many(&mut buf, BUF_SIZE),
            ));
            if buf.is_empty() {
                if self.rx.is_closed() {
                    error!("ingestor crashed? shutting down buffer processor");
                    break;
                }
                continue;
            }
        }

        Ok(())
    }

    fn process_message(
        &self,
        batch: &mut OwnedWriteBatch,
        msg: &BufferedMessage,
        did: &Did,
        keys: &HashMap<Did<'static>, Result<PublicKey<'static>>>,
    ) -> Result<ProcessResult> {
        let state = &self.state;
        let verify_signatures = self.verify_signatures;

        let RepoCheckResult::Ok(repo_state) = Self::check_repo_state(batch, state, did)? else {
            return Ok(ProcessResult::Ok);
        };

        let get_key = || {
            if verify_signatures {
                let key = keys.get(did).ok_or_else(|| {
                    KeyFetchError(miette::miette!(
                        "!!! THIS IS A BUG !!! missing pubkey for {did}"
                    ))
                })?;
                match key {
                    Ok(key) => Ok(Some(key)),
                    Err(e) => {
                        return Err(KeyFetchError(miette::miette!(
                            "failed to get pubkey for {did}: {e}"
                        )));
                    }
                }
            } else {
                Ok(None)
            }
        };

        match msg {
            SubscribeReposMessage::Commit(commit) => {
                trace!("processing buffered commit for {did}");

                if matches!(repo_state.rev, Some(ref rev) if commit.rev.as_str() <= rev.as_str()) {
                    debug!(
                        "skipping replayed event for {}: {} <= {}",
                        did,
                        commit.rev,
                        repo_state.rev.as_ref().expect("we checked in if")
                    );
                    return Ok(ProcessResult::Ok);
                }

                if let (Some(prev_repo), Some(prev_commit)) = (&repo_state.data, &commit.prev_data)
                    && prev_repo != &prev_commit.0
                {
                    warn!(
                        "gap detected for {}: prev {} != stored {}. triggering backfill",
                        did, prev_repo, prev_commit.0
                    );

                    let mut batch = state.db.inner.batch();
                    ops::update_repo_status(
                        &mut batch,
                        &state.db,
                        did,
                        repo_state,
                        RepoStatus::Backfilling,
                    )?;
                    batch.commit().into_diagnostic()?;
                    send_backfill_req(state, did.clone().into_static())?;

                    return Ok(ProcessResult::Ok);
                }

                ops::apply_commit(batch, &state.db, repo_state, &commit, get_key()?)?();
            }
            SubscribeReposMessage::Sync(sync) => {
                debug!("processing buffered sync for {did}");

                match ops::verify_sync_event(sync.blocks.as_ref(), get_key()?) {
                    Ok((root, rev)) => {
                        if let Some(current_data) = &repo_state.data {
                            if current_data == &root {
                                debug!("skipping noop sync for {did}");
                                return Ok(ProcessResult::Ok);
                            }
                        }

                        if let Some(current_rev) = &repo_state.rev {
                            if rev.as_str() <= current_rev.as_str() {
                                debug!("skipping replayed sync for {did}");
                                return Ok(ProcessResult::Ok);
                            }
                        }

                        warn!("sync event for {did}: triggering backfill");
                        let mut batch = state.db.inner.batch();
                        ops::update_repo_status(
                            &mut batch,
                            &state.db,
                            did,
                            repo_state,
                            RepoStatus::Backfilling,
                        )?;
                        batch.commit().into_diagnostic()?;

                        send_backfill_req(state, did.clone().into_static())?;
                        return Ok(ProcessResult::Ok);
                    }
                    Err(e) => {
                        error!("failed to process sync event for {did}: {e}");
                    }
                }
            }
            SubscribeReposMessage::Identity(identity) => {
                debug!("processing buffered identity for {did}");
                let handle = identity
                    .handle
                    .as_ref()
                    .map(|h| h.to_cowstr().into_static());

                let evt = IdentityEvt {
                    did: did.clone().into_static(),
                    handle,
                };
                ops::emit_identity_event(&state.db, evt);
            }
            SubscribeReposMessage::Account(account) => {
                debug!("processing buffered account for {did}");
                let evt = AccountEvt {
                    did: did.clone().into_static(),
                    active: account.active,
                    status: account.status.as_ref().map(|s| s.to_cowstr().into_static()),
                };

                if !account.active {
                    use jacquard::api::com_atproto::sync::subscribe_repos::AccountStatus;
                    match &account.status {
                        Some(AccountStatus::Deleted) => {
                            debug!("account {did} deleted, wiping data");
                            ops::delete_repo(batch, &state.db, did)?;
                            return Ok(ProcessResult::Deleted);
                        }
                        status => {
                            let status = match status {
                                Some(status) => match status {
                                    AccountStatus::Deleted => {
                                        unreachable!("deleted account status is handled before")
                                    }
                                    AccountStatus::Takendown => RepoStatus::Takendown,
                                    AccountStatus::Suspended => RepoStatus::Suspended,
                                    AccountStatus::Deactivated => RepoStatus::Deactivated,
                                    AccountStatus::Throttled => {
                                        RepoStatus::Error("throttled".into())
                                    }
                                    AccountStatus::Desynchronized => {
                                        RepoStatus::Error("desynchronized".into())
                                    }
                                    AccountStatus::Other(s) => {
                                        warn!(
                                            "unknown account status for {did}, will put in error state: {s}"
                                        );
                                        RepoStatus::Error(s.to_smolstr())
                                    }
                                },
                                None => {
                                    warn!("account {did} inactive but no status provided");
                                    RepoStatus::Error("unknown".into())
                                }
                            };
                            ops::update_repo_status(batch, &state.db, did, repo_state, status)?;
                        }
                    }
                } else {
                    // normally we would initiate backfill here
                    // but we don't have to do anything because:
                    // 1. we handle changing repo status to Synced before this (in check repo state)
                    // 2. initiating backfilling is also handled there
                }

                ops::emit_account_event(&state.db, evt);
            }
            _ => {
                warn!("unknown message type in buffer for {did}");
            }
        }

        Ok(ProcessResult::Ok)
    }

    fn check_repo_state(
        batch: &mut OwnedWriteBatch,
        state: &AppState,
        did: &Did<'_>,
    ) -> Result<RepoCheckResult> {
        // check if we have this repo
        let repo_key = keys::repo_key(&did);
        let Some(state_bytes) = state.db.repos.get(&repo_key).into_diagnostic()? else {
            // we don't know this repo, but we are receiving events for it
            // this means we should backfill it before processing its events
            debug!("discovered new account {did} from firehose, queueing backfill");

            let new_state = RepoState::backfilling(did);
            // using a separate batch here since we want to make it known its being backfilled
            // immediately. we could use the batch for the unit of work we are doing but
            // then we wouldn't be able to start backfilling until the unit of work is done
            let mut batch = state.db.inner.batch();

            batch.insert(
                &state.db.repos,
                &repo_key,
                crate::db::ser_repo_state(&new_state)?,
            );
            batch.insert(&state.db.pending, &repo_key, &[]);
            batch.commit().into_diagnostic()?;

            send_backfill_req(state, did.clone().into_static())?;

            return Ok(RepoCheckResult::Syncing);
        };
        let mut repo_state = crate::db::deser_repo_state(&state_bytes)?.into_static();

        // if we are backfilling or it is new, DON'T mark it as synced yet
        // the backfill worker will do that when it finishes
        match &repo_state.status {
            RepoStatus::Synced => Ok(RepoCheckResult::Ok(repo_state)),
            RepoStatus::Backfilling | RepoStatus::Error(_) => {
                // repo is being backfilled or is in error state
                // we dont touch the state because the backfill worker will do that
                // we should not really get here because the backfill worker should have marked it as
                // being worked on (blocked repos) meaning we would have returned earlier
                debug!(
                    "ignoring active status for {did} as it is {:?}",
                    repo_state.status
                );
                Ok(RepoCheckResult::Syncing)
            }
            RepoStatus::Deactivated | RepoStatus::Suspended | RepoStatus::Takendown => {
                // if it was in deactivated/takendown/suspended state, we can mark it as synced
                // because we are receiving live events now
                repo_state = ops::update_repo_status(
                    batch,
                    &state.db,
                    &did,
                    repo_state,
                    RepoStatus::Synced,
                )?;
                Ok(RepoCheckResult::Ok(repo_state))
            }
        }
    }
}
