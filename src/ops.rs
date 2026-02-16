use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use crate::db::{self, Db, keys, ser_repo_state};
use crate::types::{
    AccountEvt, BroadcastEvent, IdentityEvt, MarshallableEvt, RepoState, RepoStatus, ResyncState,
    StoredEvent,
};
use fjall::OwnedWriteBatch;
use jacquard::CowStr;
use jacquard::IntoStatic;

use jacquard::types::cid::Cid;
use jacquard::types::did::Did;
use jacquard_api::com_atproto::sync::subscribe_repos::Commit;
use jacquard_common::types::crypto::PublicKey;
use jacquard_repo::car::reader::parse_car_bytes;
use miette::{Context, IntoDiagnostic, Result};
use rand::{Rng, rng};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{debug, trace};

pub fn persist_to_resync_buffer(db: &Db, did: &Did, commit: &Commit) -> Result<()> {
    let key = keys::resync_buffer_key(did, DbTid::from(&commit.rev));
    let value = rmp_serde::to_vec(commit).into_diagnostic()?;
    db.resync_buffer.insert(key, value).into_diagnostic()?;
    debug!(
        "buffered commit seq {} for {did} to resync_buffer",
        commit.seq
    );
    Ok(())
}

pub fn has_buffered_commits(db: &Db, did: &Did) -> bool {
    let prefix = keys::resync_buffer_prefix(did);
    db.resync_buffer.prefix(&prefix).next().is_some()
}

// emitting identity is ephemeral
// we dont replay these, consumers can just fetch identity themselves if they need it
pub fn make_identity_event(db: &Db, evt: IdentityEvt<'static>) -> BroadcastEvent {
    let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);
    let marshallable = MarshallableEvt {
        id: event_id,
        event_type: "identity".into(),
        record: None,
        identity: Some(evt),
        account: None,
    };
    BroadcastEvent::Ephemeral(marshallable)
}

pub fn make_account_event(db: &Db, evt: AccountEvt<'static>) -> BroadcastEvent {
    let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);
    let marshallable = MarshallableEvt {
        id: event_id,
        event_type: "account".into(),
        record: None,
        identity: None,
        account: Some(evt),
    };
    BroadcastEvent::Ephemeral(marshallable)
}

pub fn delete_repo<'batch>(
    batch: &'batch mut OwnedWriteBatch,
    db: &Db,
    did: &jacquard::types::did::Did,
    repo_state: RepoState,
) -> Result<()> {
    debug!("deleting repo {did}");

    let repo_key = keys::repo_key(did);
    let pending_key = keys::pending_key(repo_state.index_id);

    // 1. delete from repos, pending, resync
    batch.remove(&db.repos, &repo_key);
    match repo_state.status {
        RepoStatus::Synced => {}
        RepoStatus::Backfilling => {
            batch.remove(&db.pending, &pending_key);
        }
        _ => {
            batch.remove(&db.resync, &repo_key);
        }
    }

    // 2. delete from resync buffer
    let resync_prefix = keys::resync_buffer_prefix(did);
    for guard in db.resync_buffer.prefix(&resync_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.resync_buffer, k);
    }

    // 3. delete from records
    let records_prefix = keys::record_prefix_did(did);
    for guard in db.records.prefix(&records_prefix) {
        let k = guard.key().into_diagnostic()?;
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

    Ok(())
}

pub fn update_repo_status<'batch, 's>(
    batch: &'batch mut OwnedWriteBatch,
    db: &Db,
    did: &jacquard::types::did::Did,
    mut repo_state: RepoState<'s>,
    new_status: RepoStatus,
) -> Result<RepoState<'s>> {
    debug!("updating repo status for {did} to {new_status:?}");

    let repo_key = keys::repo_key(did);
    let pending_key = keys::pending_key(repo_state.index_id);

    // manage queues
    match &new_status {
        RepoStatus::Synced => {
            batch.remove(&db.pending, &pending_key);
            // we dont have to remove from resync here because it has to transition resync -> pending first
        }
        RepoStatus::Backfilling => {
            // if we are coming from an error state, remove from resync
            if !matches!(repo_state.status, RepoStatus::Synced) {
                batch.remove(&db.resync, &repo_key);
            }
            // remove the old entry
            batch.remove(&db.pending, &pending_key);
            // add as new entry
            repo_state.index_id = rng().next_u64();
            batch.insert(
                &db.pending,
                keys::pending_key(repo_state.index_id),
                &repo_key,
            );
        }
        RepoStatus::Error(_msg) => {
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
    }

    repo_state.status = new_status;
    repo_state.last_updated_at = chrono::Utc::now().timestamp();

    batch.insert(&db.repos, &repo_key, ser_repo_state(&repo_state)?);

    Ok(repo_state)
}

pub fn verify_sync_event(blocks: &[u8], key: Option<&PublicKey>) -> Result<(Cid<'static>, String)> {
    let parsed = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(parse_car_bytes(blocks))
            .into_diagnostic()
    })?;

    let root_bytes = parsed
        .blocks
        .get(&parsed.root)
        .ok_or_else(|| miette::miette!("root block missing from CAR"))?;

    let repo_commit = jacquard_repo::commit::Commit::from_cbor(root_bytes).into_diagnostic()?;

    if let Some(key) = key {
        repo_commit
            .verify(key)
            .map_err(|e| miette::miette!("signature verification failed: {e}"))?;
    }

    Ok((
        Cid::ipld(repo_commit.data).into_static(),
        repo_commit.rev.to_string(),
    ))
}

pub struct ApplyCommitResults<'s> {
    pub repo_state: RepoState<'s>,
    pub records_delta: i64,
    pub blocks_count: i64,
}

pub fn apply_commit<'batch, 'db, 'commit, 's>(
    batch: &'batch mut OwnedWriteBatch,
    db: &'db Db,
    mut repo_state: RepoState<'s>,
    commit: &'commit Commit<'commit>,
    signing_key: Option<&PublicKey>,
) -> Result<ApplyCommitResults<'s>> {
    let did = &commit.repo;
    debug!("applying commit {} for {did}", &commit.commit);

    // 1. parse CAR blocks and store them in CAS
    let start = Instant::now();
    let parsed = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(parse_car_bytes(commit.blocks.as_ref()))
            .into_diagnostic()
    })?;

    trace!("parsed car for {did} in {:?}", start.elapsed());

    let root_bytes = parsed
        .blocks
        .get(&parsed.root)
        .ok_or_else(|| miette::miette!("root block missing from CAR"))?;

    let repo_commit = jacquard_repo::commit::Commit::from_cbor(root_bytes).into_diagnostic()?;

    if let Some(key) = signing_key {
        repo_commit
            .verify(key)
            .map_err(|e| miette::miette!("signature verification failed for {did}: {e}"))?;
        trace!("signature verified for {did}");
    }

    repo_state.rev = Some((&commit.rev).into());
    repo_state.data = Some(repo_commit.data);
    repo_state.last_updated_at = chrono::Utc::now().timestamp();

    batch.insert(&db.repos, keys::repo_key(did), ser_repo_state(&repo_state)?);

    // store all blocks in the CAS
    for (cid, bytes) in &parsed.blocks {
        batch.insert(&db.blocks, cid.to_bytes(), bytes.to_vec());
    }

    // 2. iterate ops and update records index
    let mut records_delta = 0;
    let mut collection_deltas: HashMap<&str, i64> = HashMap::new();

    for op in &commit.ops {
        let (collection, rkey) = parse_path(&op.path)?;
        let rkey = DbRkey::new(rkey);
        let db_key = keys::record_key(did, collection, &rkey);

        let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);

        let action = DbAction::try_from(op.action.as_str())?;
        match action {
            DbAction::Create | DbAction::Update => {
                let Some(cid) = &op.cid else {
                    continue;
                };
                batch.insert(
                    &db.records,
                    db_key.clone(),
                    cid.to_ipld()
                        .into_diagnostic()
                        .wrap_err("expected valid cid from relay")?
                        .to_bytes(),
                );

                // accumulate counts
                if action == DbAction::Create {
                    records_delta += 1;
                    *collection_deltas.entry(collection).or_default() += 1;
                }
            }
            DbAction::Delete => {
                batch.remove(&db.records, db_key);

                // accumulate counts
                records_delta -= 1;
                *collection_deltas.entry(collection).or_default() -= 1;
            }
        }

        let evt = StoredEvent {
            live: true,
            did: TrimmedDid::from(did),
            rev: DbTid::from(&commit.rev),
            collection: CowStr::Borrowed(collection),
            rkey,
            action,
            cid: op.cid.as_ref().map(|c| c.to_ipld().expect("valid cid")),
        };

        let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
        batch.insert(&db.events, keys::event_key(event_id), bytes);
    }

    // update counts
    let blocks_count = parsed.blocks.len() as i64;
    for (col, delta) in collection_deltas {
        db::update_record_count(batch, db, did, col, delta)?;
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
