use jacquard_common::types::string::Did;

use crate::db::types::{DbTid, TrimmedDid};

/// separator used for composite keys
pub const SEP: u8 = b'|';

pub const CURSOR_KEY: &[u8] = b"firehose_cursor";

// Key format: {DID} (trimmed)
pub fn repo_key<'a>(did: &'a Did) -> Vec<u8> {
    let mut vec = Vec::with_capacity(32);
    TrimmedDid::from(did).write_to_vec(&mut vec);
    vec
}

// key format: {DID}\x00{RKey} (DID trimmed)
pub fn record_key(did: &Did, rkey: &str) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key = Vec::with_capacity(repo.len() + rkey.len() + 1);
    repo.write_to_vec(&mut key);
    key.push(SEP);
    key.extend_from_slice(rkey.as_bytes());
    key
}

// prefix format: {DID}\x00 (DID trimmed) - for scanning all records of a DID within a collection
pub fn record_prefix(did: &Did) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut prefix = Vec::with_capacity(repo.len() + 1);
    repo.write_to_vec(&mut prefix);
    prefix.push(SEP);
    prefix
}

// key format: {DID}

// key format: {SEQ}
pub fn event_key(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

pub const COUNT_KS_PREFIX: &[u8] = &[b'k', SEP];

// count keys for the counts keyspace
// key format: k\x00{keyspace_name}
pub fn count_keyspace_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(COUNT_KS_PREFIX.len() + name.len());
    key.extend_from_slice(COUNT_KS_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key
}

pub const COUNT_COLLECTION_PREFIX: &[u8] = &[b'r', SEP];

// key format: r|{DID}|{collection} (DID trimmed)
pub fn count_collection_key(did: &Did, collection: &str) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key =
        Vec::with_capacity(COUNT_COLLECTION_PREFIX.len() + repo.len() + 1 + collection.len());
    key.extend_from_slice(COUNT_COLLECTION_PREFIX);
    repo.write_to_vec(&mut key);
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key
}

// key format: {DID}|{rev}
pub fn resync_buffer_key(did: &Did, rev: DbTid) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key = Vec::with_capacity(repo.len() + 1 + 8);
    repo.write_to_vec(&mut key);
    key.push(SEP);
    key.extend_from_slice(&rev.as_u64().to_be_bytes());
    key
}

// prefix format: {DID}| (DID trimmed)
pub fn resync_buffer_prefix(did: &Did) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut prefix = Vec::with_capacity(repo.len() + 1);
    repo.write_to_vec(&mut prefix);
    prefix.push(SEP);
    prefix
}
