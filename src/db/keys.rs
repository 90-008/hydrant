use jacquard_common::types::string::Did;

use crate::db::types::TrimmedDid;

/// separator used for composite keys
pub const SEP: u8 = 0x00;

pub const CURSOR_KEY: &[u8] = b"firehose_cursor";

// Key format: {DID} (trimmed)
pub fn repo_key<'a>(did: &'a Did) -> TrimmedDid<'a> {
    TrimmedDid::from(did)
}

// key format: {DID}\x00{Collection}\x00{RKey} (DID trimmed)
pub fn record_key(did: &Did, collection: &str, rkey: &str) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key = Vec::with_capacity(repo.len() + collection.len() + rkey.len() + 2);
    key.extend_from_slice(repo.as_bytes());
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key.push(SEP);
    key.extend_from_slice(rkey.as_bytes());
    key
}

// prefix format: {DID}\x00 (DID trimmed) - for scanning all records of a DID
pub fn record_prefix(did: &Did) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut prefix = Vec::with_capacity(repo.len() + 1);
    prefix.extend_from_slice(repo.as_bytes());
    prefix.push(SEP);
    prefix
}

// key format: {DID}

// key format: {SEQ}
pub fn event_key(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

// key format: {CID}
pub fn block_key(cid: &str) -> &[u8] {
    cid.as_bytes()
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

// key format: r\x00{DID}\x00{collection} (DID trimmed)
pub fn count_collection_key(did: &Did, collection: &str) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key =
        Vec::with_capacity(COUNT_COLLECTION_PREFIX.len() + repo.len() + 1 + collection.len());
    key.extend_from_slice(COUNT_COLLECTION_PREFIX);
    key.extend_from_slice(repo.as_bytes());
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key
}
