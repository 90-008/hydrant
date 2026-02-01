use crate::db::{keys, Db};
use crate::types::{BroadcastEvent, IdentityEvt, MarshallableEvt, StoredEvent};
use jacquard::api::com_atproto::sync::subscribe_repos::Commit;
use jacquard::cowstr::ToCowStr;
use jacquard_repo::car::reader::parse_car_bytes;
use miette::{IntoDiagnostic, Result};
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{debug, trace};

// emitting identity is ephemeral
// we dont replay these, consumers can just fetch identity themselves if they need it
pub fn emit_identity_event(db: &Db, evt: IdentityEvt) {
    let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);
    let marshallable = MarshallableEvt {
        id: event_id,
        event_type: "identity".into(),
        record: None,
        identity: Some(evt),
    };
    let _ = db.event_tx.send(BroadcastEvent::Ephemeral(marshallable));
}

pub fn delete_repo(db: &Db, did: &jacquard::types::did::Did) -> Result<()> {
    debug!("deleting repo {did}");
    let mut batch = db.inner.batch();
    let repo_key = keys::repo_key(did);

    // 1. delete from repos, pending, errors
    batch.remove(&db.repos, repo_key);
    batch.remove(&db.pending, repo_key);
    batch.remove(&db.errors, repo_key);

    // 2. delete from buffer (prefix: repo_key + SEP)
    let mut buffer_prefix = repo_key.to_vec();
    buffer_prefix.push(keys::SEP);
    for guard in db.buffer.prefix(&buffer_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.buffer, k);
    }

    // 3. delete from records (prefix: repo_key + SEP)
    let mut records_prefix = repo_key.to_vec();
    records_prefix.push(keys::SEP);
    let mut deleted_count = 0;

    for guard in db.records.prefix(&records_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.records, k);
        deleted_count += 1;
    }

    // 4. reset collection counts
    let mut count_prefix = Vec::new();
    count_prefix.push(b'r');
    count_prefix.push(keys::SEP);
    count_prefix.extend_from_slice(keys::did_prefix(did).as_bytes());
    count_prefix.push(keys::SEP);

    for guard in db.counts.prefix(&count_prefix) {
        let k = guard.key().into_diagnostic()?;
        batch.remove(&db.counts, k);
    }

    batch.commit().into_diagnostic()?;

    // update global record count
    if deleted_count > 0 {
        tokio::spawn(db.increment_count(keys::count_keyspace_key("records"), -deleted_count));
    }

    Ok(())
}

pub fn apply_commit(db: &Db, commit: &Commit<'_>, live: bool) -> Result<()> {
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

    let (_, mut batch) = Db::update_repo_state(db.inner.batch(), &db.repos, did, |state, _| {
        state.rev = commit.rev.as_str().into();
        state.data = parsed.root.to_smolstr();
        state.last_updated_at = chrono::Utc::now().timestamp();
        Ok((true, ()))
    })?;

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
    let mut events_count = 0;
    let mut collection_deltas: HashMap<SmolStr, i64> = HashMap::new();

    for op in &commit.ops {
        let parts: Vec<&str> = op.path.splitn(2, '/').collect();
        if parts.len() != 2 {
            continue;
        }
        let collection = parts[0];
        let rkey = parts[1];

        let db_key = keys::record_key(did, collection, rkey);

        let event_id = db.next_event_id.fetch_add(1, Ordering::SeqCst);

        let mut cid_str = None;

        match op.action.as_str() {
            "create" | "update" => {
                let Some(cid) = &op.cid else {
                    continue;
                };
                let s = smol_str::SmolStr::from(cid.as_str());
                batch.insert(&db.records, db_key, s.as_bytes().to_vec());
                cid_str = Some(s);

                // accumulate counts
                if op.action.as_str() == "create" {
                    records_delta += 1;
                    *collection_deltas
                        .entry(collection.to_smolstr())
                        .or_default() += 1;
                }
            }
            "delete" => {
                batch.remove(&db.records, db_key);

                // accumulate counts
                records_delta -= 1;
                *collection_deltas
                    .entry(collection.to_smolstr())
                    .or_default() -= 1;
            }
            _ => {}
        }

        let evt = StoredEvent::Record {
            live,
            did: did.as_str().into(),
            rev: commit.rev.as_str().into(),
            collection: collection.into(),
            rkey: rkey.into(),
            action: op.action.as_str().into(),
            cid: cid_str,
        };

        let bytes = rmp_serde::to_vec(&evt).into_diagnostic()?;
        batch.insert(&db.events, keys::event_key(event_id as i64), bytes);
        events_count += 1;
    }

    let start = Instant::now();

    batch.commit().into_diagnostic()?;
    trace!("committed sync batch for {did} in {:?}", start.elapsed());

    let blocks_count = parsed.blocks.len() as i64;
    tokio::spawn({
        let blocks_fut = (blocks_count > 0)
            .then(|| db.increment_count(keys::count_keyspace_key("blocks"), blocks_count));
        let records_fut = (records_delta != 0)
            .then(|| db.increment_count(keys::count_keyspace_key("records"), records_delta));
        let events_fut = (events_count > 0)
            .then(|| db.increment_count(keys::count_keyspace_key("events"), events_count));
        let collections_fut = collection_deltas
            .into_iter()
            .map(|(col, delta)| db.increment_count(keys::count_collection_key(&did, &col), delta))
            .collect::<Vec<_>>();
        futures::future::join_all(
            blocks_fut
                .into_iter()
                .chain(records_fut)
                .chain(events_fut)
                .chain(collections_fut),
        )
    });

    let _ = db.event_tx.send(BroadcastEvent::Persisted(
        db.next_event_id.load(Ordering::SeqCst) - 1,
    ));

    Ok(())
}
