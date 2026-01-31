use jacquard_common::types::string::Did;
use miette::{Context, IntoDiagnostic};

/// separator used for composite keys
pub const SEP: u8 = 0x00;

pub fn did_prefix<'a>(did: &'a Did<'a>) -> &'a str {
    did.as_str().trim_start_matches("did:")
}

pub fn reconstruct_did<'a>(trimmed_did: &'a str) -> Result<Did<'static>, miette::Error> {
    Did::new_owned(format!("did:{trimmed_did}"))
        .into_diagnostic()
        .wrap_err("expected did to be trimmed")
}

// Key format: {DID} (trimmed)
pub fn repo_key<'a>(did: &'a Did) -> &'a [u8] {
    did_prefix(did).as_bytes()
}

// key format: {DID}\x00{Collection}\x00{RKey} (DID trimmed)
pub fn record_key(did: &Did, collection: &str, rkey: &str) -> Vec<u8> {
    let prefix = did_prefix(did);
    let mut key = Vec::with_capacity(prefix.len() + collection.len() + rkey.len() + 2);
    key.extend_from_slice(prefix.as_bytes());
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key.push(SEP);
    key.extend_from_slice(rkey.as_bytes());
    key
}

// key format: {DID}
pub fn buffer_prefix<'a>(did: &'a Did) -> &'a [u8] {
    repo_key(did)
}

// key format: {SEQ}
pub fn event_key(seq: i64) -> [u8; 8] {
    seq.to_be_bytes()
}

// key format: {CID}
pub fn block_key(cid: &str) -> &[u8] {
    cid.as_bytes()
}

// count keys for the counts keyspace
// key format: k\x00{keyspace_name}
pub fn count_keyspace_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + name.len());
    key.push(b'k');
    key.push(SEP);
    key.extend_from_slice(name.as_bytes());
    key
}

// key format: r\x00{DID}\x00{collection} (DID trimmed)
pub fn count_collection_key(did: &Did, collection: &str) -> Vec<u8> {
    let prefix = did_prefix(did);
    let mut key = Vec::with_capacity(2 + prefix.len() + 1 + collection.len());
    key.push(b'r');
    key.push(SEP);
    key.extend_from_slice(prefix.as_bytes());
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key
}
