use fjall::OwnedWriteBatch;
use fjall::Slice;

use jacquard_common::CowStr;
#[cfg(feature = "backlinks")]
use jacquard_common::Data;
use jacquard_common::types::did::Did;
use miette::{Context, IntoDiagnostic, Result};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tracing::debug;

use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use crate::db::{self, Db, keys};
use crate::filter::FilterConfig;
use crate::ingest::stream::Commit;
use crate::ingest::validation::ValidatedCommit;
use crate::types::StoredData;
use crate::types::{
    AccountEvt, BroadcastEvent, IdentityEvt, MarshallableEvt, RepoState, RepoStatus, ResyncState,
    StoredEvent,
};

pub fn persist_to_resync_buffer(db: &Db, did: &Did, commit: &Commit) -> Result<()> {
    let key = keys::resync_buffer_key(did, DbTid::from(&commit.rev));
    let value = rmp_serde::to_vec(commit).into_diagnostic()?;
    db.resync_buffer.insert(key, value).into_diagnostic()?;
    debug!(
        did = %did,
        seq = commit.seq,
        "buffered commit to resync_buffer"
    );
    Ok(())
}

// emitting identity is ephemeral
// we dont replay these, consumers can just fetch identity themselves if they need it
pub fn make_identity_event(db: &Db, evt: IdentityEvt<'static>) -> BroadcastEvent {
    let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);
    let marshallable = MarshallableEvt {
        id: event_id,
        kind: crate::types::EventType::Identity,
        record: None,
        identity: Some(evt),
        account: None,
    };
    BroadcastEvent::Ephemeral(Box::new(marshallable))
}

pub fn make_account_event(db: &Db, evt: AccountEvt<'static>) -> BroadcastEvent {
    let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);
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
        let metadata = db::deser_repo_metadata(&metadata_bytes)?;
        batch.remove(&db.pending, keys::pending_key(metadata.index_id));
    }

    // 1. delete from resync, and metadata
    // we don't delete from repos, relay uses it as a tombstone
    // todo: we should still delete it after some time
    batch.remove(&db.resync, &repo_key);
    batch.remove(&db.repo_metadata, &metadata_key);

    // 2. delete from resync buffer
    let resync_prefix = keys::resync_buffer_prefix(did);
    for guard in db.resync_buffer.prefix(&resync_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.resync_buffer, k);
    }

    // 3. delete from records
    // todo: figure out how we want to handle the blocks associated with these records
    //       without delving too much into gc madness we had before
    let records_prefix = keys::record_prefix_did(did);
    for guard in db.records.prefix(&records_prefix) {
        let (k, _cid_bytes) = guard.into_inner().into_diagnostic()?;
        batch.remove(&db.records, k);
    }

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
    #[cfg(feature = "backlinks")]
    crate::backlinks::store::delete_repo(batch, &db.backlinks, did)?;

    Ok(())
}

pub fn transition_repo<'batch, 's>(
    batch: &'batch mut OwnedWriteBatch,
    db: &Db,
    did: &Did,
    mut repo_state: RepoState<'s>,
    new_status: RepoStatus,
) -> Result<RepoState<'s>> {
    debug!(did = %did, status = ?new_status, "updating repo status");

    let repo_key = keys::repo_key(did);
    let metadata_key = keys::repo_metadata_key(did);

    let metadata_bytes = db.repo_metadata.get(&metadata_key).into_diagnostic()?;
    if let Some(metadata_bytes) = metadata_bytes {
        let metadata = db::deser_repo_metadata(&metadata_bytes)?;
        let pending_key = keys::pending_key(metadata.index_id);

        // manage queues
        match &new_status {
            RepoStatus::Synced => {
                batch.remove(&db.pending, &pending_key);
                // we dont have to remove from resync here because it has to transition resync -> pending first
            }
            RepoStatus::Error(msg) => {
                tracing::warn!("transitioning to error: {msg}");
                batch.remove(&db.pending, &pending_key);
                // TODO: we need to make errors have kind instead of "message" in repo status
                // and then pass it to resync error kind
                let resync_state = crate::types::ResyncState::Error {
                    kind: crate::types::ResyncErrorKind::Generic,
                    retry_count: 0,
                    next_retry: chrono::Utc::now().timestamp(),
                };
                batch.insert(
                    &db.resync,
                    &repo_key,
                    rmp_serde::to_vec(&resync_state).into_diagnostic()?,
                );
            }
            RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended => {
                // this shouldnt be needed since a repo wont be in a pending state when it gets to any of these states
                // batch.remove(&db.pending, &pending_key);
                let resync_state = ResyncState::Gone {
                    status: new_status.clone(),
                };
                batch.insert(
                    &db.resync,
                    &repo_key,
                    rmp_serde::to_vec(&resync_state).into_diagnostic()?,
                );
            }
            RepoStatus::Deleted => {
                // terminal state: remove from queues, no resync entry needed
                batch.remove(&db.pending, &pending_key);
                batch.remove(&db.resync, &repo_key);
            }
            RepoStatus::Desynchronized | RepoStatus::Throttled => {
                // like an error: remove from pending and schedule a resync attempt
                batch.remove(&db.pending, &pending_key);
                let resync_state = crate::types::ResyncState::Error {
                    kind: crate::types::ResyncErrorKind::Generic,
                    retry_count: 0,
                    next_retry: chrono::Utc::now().timestamp(),
                };
                batch.insert(
                    &db.resync,
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
    pub records_delta: i64,
    pub blocks_count: i64,
}

pub fn apply_commit<'s>(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    mut repo_state: RepoState<'s>,
    validated: ValidatedCommit<'_>,
    filter: &FilterConfig,
    ephemeral: bool,
) -> Result<ApplyCommitResults<'s>> {
    let commit = validated.commit;
    let parsed = validated.parsed_blocks;
    let did = &commit.repo;
    debug!(did = %did, commit = %commit.commit, "applying commit");

    repo_state.root = Some(validated.commit_obj.into());
    repo_state.touch();

    // 2. iterate ops and update records index
    let mut records_delta = 0;
    let mut blocks_count = 0;
    let mut collection_deltas: HashMap<&str, i64> = HashMap::new();

    for op in &commit.ops {
        let (collection, rkey) = parse_path(&op.path)?;

        if !filter.matches_collection(collection) {
            continue;
        }

        let rkey = DbRkey::new(rkey);
        let db_key = keys::record_key(did, collection, &rkey);

        let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);

        let action = DbAction::try_from(op.action.as_str())?;
        let block = match action {
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
                let cid_raw = cid_ipld.to_bytes();
                let block_key = Slice::from(keys::block_key(collection, &cid_raw));

                blocks_count += 1;
                if !ephemeral {
                    batch.insert(&db.blocks, block_key.clone(), bytes.as_ref());
                    batch.insert(&db.records, db_key.clone(), cid_raw);
                    // accumulate counts
                    if action == DbAction::Create {
                        records_delta += 1;
                        *collection_deltas.entry(collection).or_default() += 1;
                    }
                    #[cfg(feature = "backlinks")]
                    if let Ok(value) = serde_ipld_dagcbor::from_slice::<Data>(bytes.as_ref()) {
                        crate::backlinks::store::index_record(
                            batch,
                            &db.backlinks,
                            did.as_str(),
                            collection,
                            &rkey.to_smolstr(),
                            &value,
                        )?;
                    }
                    None
                } else if action == DbAction::Create || action == DbAction::Update {
                    Some(bytes.clone())
                } else {
                    unreachable!("we tested if we are in create or update action")
                }
            }
            DbAction::Delete => {
                if !ephemeral {
                    batch.remove(&db.records, db_key);

                    // accumulate counts
                    records_delta -= 1;
                    *collection_deltas.entry(collection).or_default() -= 1;

                    #[cfg(feature = "backlinks")]
                    crate::backlinks::store::delete_record(
                        batch,
                        &db.backlinks,
                        did.as_str(),
                        collection,
                        &rkey.to_smolstr(),
                    )?;
                }

                None
            }
        };

        let evt = StoredEvent {
            live: true,
            did: TrimmedDid::from(did),
            rev: DbTid::from(&commit.rev),
            collection: CowStr::Borrowed(collection),
            rkey,
            action,
            data: block
                .map(StoredData::Block)
                .or_else(|| {
                    op.cid
                        .as_ref()
                        .map(|c| c.to_ipld().expect("valid cid"))
                        .map(StoredData::Ptr)
                })
                .unwrap_or(StoredData::Nothing),
        };

        let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
        batch.insert(&db.events, keys::event_key(event_id), bytes);
    }

    // update counts
    if !ephemeral {
        for (col, delta) in collection_deltas {
            db::update_record_count(batch, db, did, col, delta)?;
        }
    }

    Ok(ApplyCommitResults {
        repo_state,
        records_delta,
        blocks_count,
    })
}

pub fn parse_path(path: &str) -> Result<(&str, &str)> {
    let mut parts = path.splitn(2, '/');
    let collection = parts.next().wrap_err("missing collection")?;
    let rkey = parts.next().wrap_err("missing rkey")?;
    Ok((collection, rkey))
}
