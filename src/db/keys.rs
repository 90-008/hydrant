use jacquard_common::types::string::Did;
use miette::{Context, IntoDiagnostic, Result};

/// separator used for composite keys
pub const SEP: u8 = 0x00;

pub fn did_prefix<'a>(did: &'a Did<'a>) -> &'a str {
    did.as_str().trim_start_matches("did:")
}

pub fn reconstruct_did<'a>(trimmed_did: &'a str) -> Result<Did<'static>> {
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

// key format: {DID}\x00{timestamp} (for buffer entries)
pub fn buffer_key(did: &Did, timestamp: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(did.len() + 1 + 8);
    key.extend_from_slice(did_prefix(did).as_bytes());
    key.push(SEP);
    key.extend_from_slice(&timestamp.to_be_bytes());
    key
}

pub fn parse_buffer_key(key: &[u8]) -> Result<(Did<'static>, i64)> {
    let pos = key
        .iter()
        .rposition(|&b| b == SEP)
        .ok_or_else(|| miette::miette!("buffer key invalid, no seperator found"))?;
    let did_bytes = &key[..pos];
    let ts_bytes = &key[pos + 1..];
    let timestamp = i64::from_be_bytes(
        ts_bytes
            .try_into()
            .map_err(|e| miette::miette!("buffer key invalid, {e}"))?,
    );
    let did = reconstruct_did(&String::from_utf8_lossy(did_bytes))?;
    Ok((did, timestamp))
}
