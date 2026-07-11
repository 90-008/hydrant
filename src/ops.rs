use fjall::OwnedWriteBatch;

use jacquard_common::IntoStatic;
use jacquard_common::types::did::Did;
use miette::{Context, IntoDiagnostic, Result};
use tracing::debug;

use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use crate::db::{self, Db, keys};
use crate::filter::FilterConfig;
use crate::ingest::stream::Commit;
use crate::ingest::validation::ValidatedCommit;
use crate::state::AppState;
use crate::types::{GaugeState, RepoState, RepoStatus, ResyncErrorKind, ResyncState};

#[cfg(feature = "indexer_stream")]
use {
    crate::types::{AccountEvt, BroadcastEvent, IdentityEvt, MarshallableEvt},
    std::sync::atomic::Ordering,
};
pub(crate) mod backlink_ops;
use record_events::RecordEvents;

pub mod record_events;

pub fn persist_to_resync_buffer(db: &Db, did: &Did, commit: &Commit) -> Result<()> {
    let key = keys::resync_buffer_key(did, DbTid::from(&commit.rev));
    let value = rmp_serde::to_vec_named(commit).into_diagnostic()?;
    db.indexer
        .resync_buffer
        .insert(key, value)
        .into_diagnostic()?;
    debug!(
        did = %did,
        seq = commit.seq,
        "buffered commit to resync_buffer"
    );
    Ok(())
}

// emitting identity is ephemeral
// we dont replay these, consumers can just fetch identity themselves if they need it
#[cfg(feature = "indexer_stream")]
pub fn make_identity_event(db: &Db, evt: IdentityEvt<'static>) -> BroadcastEvent {
    let event_id = db.stream.next_event_id.fetch_add(1, Ordering::SeqCst);
    let marshallable = MarshallableEvt {
        id: event_id,
        kind: crate::types::EventType::Identity,
        record: None,
        identity: Some(evt),
        account: None,
    };
    BroadcastEvent::Ephemeral(Box::new(marshallable))
}

#[cfg(feature = "indexer_stream")]
pub fn make_account_event(db: &Db, evt: AccountEvt<'static>) -> BroadcastEvent {
    let event_id = db.stream.next_event_id.fetch_add(1, Ordering::SeqCst);
    let marshallable = MarshallableEvt {
        id: event_id,
        kind: crate::types::EventType::Account,
        record: None,
        identity: None,
        account: Some(evt),
    };
    BroadcastEvent::Ephemeral(Box::new(marshallable))
}

pub fn delete_repo(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    did: &Did,
    _repo_state: &RepoState,
) -> Result<()> {
    debug!(did = %did, "deleting repo");

    let repo_key = keys::repo_key(did);
    let metadata_key = keys::repo_metadata_key(did);

    let metadata_bytes = db.repo_metadata.get(&metadata_key).into_diagnostic()?;
    if let Some(metadata_bytes) = metadata_bytes {
        let metadata = db::deser_repo_meta(&metadata_bytes)?;
        batch.remove(&db.indexer.pending, keys::pending_key(metadata.index_id));
    }

    // we don't delete from repos, relay uses it as a tombstone
    // todo: we should still delete it after some time
    batch.remove(&db.indexer.resync, &repo_key);
    batch.remove(&db.repo_metadata, &metadata_key);

    // 2. delete from resync buffer
    let resync_prefix = keys::resync_buffer_prefix(did);
    for guard in db.indexer.resync_buffer.prefix(&resync_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.indexer.resync_buffer, k);
    }

    // 3. delete from records
    // todo: figure out how we want to handle the blocks associated with these records
    //       without delving too much into gc madness we had before
    db::delete_repo_records(batch, db, did)?;

    // 4. reset collection counts
    let mut count_prefix = Vec::new();
    count_prefix.push(b'r');
    count_prefix.push(keys::SEP);
    TrimmedDid::from(did).write_to_vec(&mut count_prefix);
    count_prefix.push(keys::SEP);

    for guard in db.counts.prefix(&count_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.counts, k);
    }

    // 5. remove backlinks for all records in this repo
    backlink_ops::delete_repo(batch, db, did)?;

    Ok(())
}

pub fn transition_repo<'s>(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    lifecycle_transitions: &mut Vec<(Did<'static>, GaugeState)>,
    did: &Did,
    mut repo_state: RepoState<'s>,
    new_status: RepoStatus,
) -> Result<RepoState<'s>> {
    debug!(did = %did, status = ?new_status, "updating repo status");

    let repo_key = keys::repo_key(did);
    let metadata_key = keys::repo_metadata_key(did);

    let metadata_bytes = db.repo_metadata.get(&metadata_key).into_diagnostic()?;
    if let Some(metadata_bytes) = metadata_bytes {
        let metadata = db::deser_repo_meta(&metadata_bytes)?;
        let pending_key = keys::pending_key(metadata.index_id);

        // manage queues
        match &new_status {
            RepoStatus::Synced => {
                lifecycle_transitions.push((did.clone().into_static(), GaugeState::Synced));
                batch.remove(&db.indexer.pending, pending_key.as_slice());
                // we dont have to remove from resync here because it has to transition resync -> pending first
            }
            RepoStatus::Error(msg) => {
                tracing::warn!("transitioning to error: {msg}");
                lifecycle_transitions.push((
                    did.clone().into_static(),
                    GaugeState::Resync(Some(ResyncErrorKind::Generic)),
                ));
                batch.remove(&db.indexer.pending, pending_key.as_slice());
                // TODO: we need to make errors have kind instead of "message" in repo status
                // and then pass it to resync error kind
                let resync_state = crate::types::ResyncState::Error {
                    kind: crate::types::ResyncErrorKind::Generic,
                    retry_count: 0,
                    next_retry: chrono::Utc::now().timestamp(),
                };
                batch.insert(
                    &db.indexer.resync,
                    &repo_key,
                    rmp_serde::to_vec(&resync_state).into_diagnostic()?,
                );
            }
            RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended => {
                lifecycle_transitions.push((did.clone().into_static(), GaugeState::Resync(None)));
                // this shouldnt be needed since a repo wont be in a pending state when it gets to any of these states
                // batch.remove(&db.pending, &pending_key);
                let resync_state = ResyncState::Gone {
                    status: new_status.clone(),
                };
                batch.insert(
                    &db.indexer.resync,
                    &repo_key,
                    rmp_serde::to_vec(&resync_state).into_diagnostic()?,
                );
            }
            RepoStatus::Deleted => {
                lifecycle_transitions.push((did.clone().into_static(), GaugeState::Synced));
                // terminal state: remove from queues, no resync entry needed
                batch.remove(&db.indexer.pending, pending_key.as_slice());
                batch.remove(&db.indexer.resync, &repo_key);
            }
            RepoStatus::Desynchronized | RepoStatus::Throttled => {
                lifecycle_transitions.push((
                    did.clone().into_static(),
                    GaugeState::Resync(Some(ResyncErrorKind::Generic)),
                ));
                // like an error: remove from pending and schedule a resync attempt
                batch.remove(&db.indexer.pending, pending_key.as_slice());
                let resync_state = crate::types::ResyncState::Error {
                    kind: crate::types::ResyncErrorKind::Generic,
                    retry_count: 0,
                    next_retry: chrono::Utc::now().timestamp(),
                };
                batch.insert(
                    &db.indexer.resync,
                    &repo_key,
                    rmp_serde::to_vec(&resync_state).into_diagnostic()?,
                );
            }
        }
    }

    repo_state.active = matches!(new_status, RepoStatus::Synced | RepoStatus::Error(_));
    repo_state.status = new_status;
    repo_state.touch();

    Ok(repo_state)
}

pub struct ApplyCommitResults<'s> {
    pub repo_state: RepoState<'s>,
    #[cfg_attr(not(feature = "indexer_stream"), allow(dead_code))]
    pub events: RecordEvents,
}

pub fn apply_commit<'s>(
    txn: &mut crate::db::Txn<'_>,
    state: &AppState,
    mut repo_state: RepoState<'s>,
    validated: ValidatedCommit<'_>,
    filter: &FilterConfig,
) -> Result<ApplyCommitResults<'s>> {
    let commit = validated.commit;
    let parsed = validated.parsed_blocks;
    let did = &commit.repo;
    debug!(did = %did, commit = %commit.commit, "applying commit");

    repo_state.root = Some(validated.commit_obj.into());
    repo_state.touch();

    let mut record_txn = txn.records(state, &DbTid::from(&commit.rev), did);

    for op in &commit.ops {
        let (collection, rkey) = parse_path(&op.path)?;

        if !filter.matches_collection(collection) {
            continue;
        }

        let rkey = DbRkey::new(rkey);
        let action = DbAction::try_from(op.action.as_str())?;

        match action {
            DbAction::Create | DbAction::Update => {
                let Some(cid) = &op.cid else {
                    continue;
                };
                let cid_ipld = cid
                    .to_ipld()
                    .into_diagnostic()
                    .wrap_err("expected valid cid from relay")?;
                let Some(bytes) = parsed.blocks.get(&cid_ipld) else {
                    return Err(miette::miette!(
                        "block {cid} not found in CAR for record {did}/{collection}/{rkey}"
                    ));
                };
                record_txn.put_record(collection, &rkey, cid_ipld, bytes, action)?;
            }
            DbAction::Delete => {
                record_txn.delete_record(collection, &rkey)?;
            }
        }
    }

    record_txn.update_repo_state(&repo_state)?;
    let events = record_txn.finish()?;

    Ok(ApplyCommitResults { repo_state, events })
}

pub fn parse_path(path: &str) -> Result<(&str, &str)> {
    let mut parts = path.splitn(2, '/');
    let collection = parts.next().wrap_err("missing collection")?;
    let rkey = parts.next().wrap_err("missing rkey")?;

    if collection.is_empty() || jacquard_common::types::nsid::Nsid::new(collection).is_err() {
        miette::bail!("invalid collection NSID: {collection}");
    }
    if rkey.is_empty() || jacquard_common::types::string::Rkey::new(rkey).is_err() {
        miette::bail!("invalid record key (rkey): {rkey}");
    }

    Ok((collection, rkey))
}
