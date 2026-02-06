use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
use crate::db::{self, Db, keys, ser_repo_state};
use crate::state::AppState;
use crate::types::{
    AccountEvt, BroadcastEvent, IdentityEvt, MarshallableEvt, RepoState, RepoStatus, ResyncState,
    StoredEvent,
};
use fjall::OwnedWriteBatch;
use jacquard::CowStr;
use jacquard::IntoStatic;
use jacquard::cowstr::ToCowStr;
use jacquard::types::cid::Cid;
use jacquard_api::com_atproto::sync::subscribe_repos::Commit;
use jacquard_common::types::crypto::PublicKey;
use jacquard_repo::car::reader::parse_car_bytes;
use miette::{Context, IntoDiagnostic, Result};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{debug, trace, warn};

pub fn send_backfill_req(state: &AppState, did: jacquard::types::did::Did<'static>) -> Result<()> {
    state
        .backfill_tx
        .send(did.clone())
        .map_err(|_| miette::miette!("failed to send backfill request for {did}"))?;
    let _ = state.blocked_dids.insert_sync(did);
    Ok(())
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
) -> Result<()> {
    debug!("deleting repo {did}");
    let repo_key = keys::repo_key(did);

    // 1. delete from repos, pending, resync
    batch.remove(&db.repos, &repo_key);
    batch.remove(&db.pending, &repo_key);
    batch.remove(&db.resync, &repo_key);

    // 2. delete from records (prefix: repo_key + SEP)
    let mut records_prefix = repo_key.clone();
    records_prefix.push(keys::SEP);
    for guard in db.records.prefix(&records_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.records, k);
    }

    // 3. reset collection counts
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

    let key = keys::repo_key(did);

    // manage queues
    match &new_status {
        RepoStatus::Synced => {
            batch.remove(&db.pending, &key);
            batch.remove(&db.resync, &key);
        }
        RepoStatus::Backfilling => {
            batch.insert(&db.pending, &key, &[]);
            batch.remove(&db.resync, &key);
        }
        RepoStatus::Error(msg) => {
            batch.remove(&db.pending, &key);
            let resync_state = ResyncState::Error {
                message: msg.clone(),
                retry_count: 0,
                next_retry: chrono::Utc::now().timestamp(),
            };
            batch.insert(
                &db.resync,
                &key,
                rmp_serde::to_vec(&resync_state).into_diagnostic()?,
            );
        }
        RepoStatus::Deactivated | RepoStatus::Takendown | RepoStatus::Suspended => {
            batch.remove(&db.pending, &key);
            let resync_state = ResyncState::Gone {
                status: new_status.clone(),
            };
            batch.insert(
                &db.resync,
                &key,
                rmp_serde::to_vec(&resync_state).into_diagnostic()?,
            );
        }
    }

    repo_state.status = new_status;
    repo_state.last_updated_at = chrono::Utc::now().timestamp();

    batch.insert(&db.repos, &key, ser_repo_state(&repo_state)?);

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

    repo_state.rev = Some(commit.rev.clone().into());
    repo_state.data = Some(repo_commit.data);
    repo_state.last_updated_at = chrono::Utc::now().timestamp();

    batch.insert(&db.repos, keys::repo_key(did), ser_repo_state(&repo_state)?);

    // store all blocks in the CAS
    for (cid, bytes) in &parsed.blocks {
        batch.insert(
            &db.blocks,
            keys::block_key(&cid.to_cowstr()),
            bytes.to_vec(),
        );
    }

    // 2. iterate ops and update records index
    let mut records_delta = 0;
    let mut collection_deltas: HashMap<&str, i64> = HashMap::new();

    for op in &commit.ops {
        let (collection, rkey) = parse_path(&op.path)?;
        let db_key = keys::record_key(did, collection, rkey);

        let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);

        match op.action.as_str() {
            "create" | "update" => {
                let Some(cid) = &op.cid else {
                    continue;
                };
                let s = smol_str::SmolStr::from(cid.as_str());
                batch.insert(&db.records, db_key, s.as_bytes().to_vec());

                // accumulate counts
                if op.action.as_str() == "create" {
                    records_delta += 1;
                    *collection_deltas.entry(collection).or_default() += 1;
                }
            }
            "delete" => {
                batch.remove(&db.records, db_key);

                // accumulate counts
                records_delta -= 1;
                *collection_deltas.entry(collection).or_default() -= 1;
            }
            _ => {
                warn!("{did}/{}: unknown op action '{}'", op.path, op.action);
            }
        }

        let evt = StoredEvent {
            live: true,
            did: TrimmedDid::from(did),
            rev: DbTid::from(&commit.rev),
            collection: CowStr::Borrowed(collection),
            rkey: DbRkey::new(rkey),
            action: DbAction::from(op.action.as_str()),
            cid: op.cid.as_ref().map(|c| c.0.to_ipld().expect("valid cid")),
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
