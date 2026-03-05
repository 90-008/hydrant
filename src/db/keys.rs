use jacquard_common::types::string::Did;
use smol_str::SmolStr;

use crate::db::types::{DbRkey, DbTid, TrimmedDid};

/// separator used for composite keys
pub const SEP: u8 = b'|';

pub const CURSOR_KEY: &[u8] = b"firehose_cursor";

// Key format: {DID}
pub fn repo_key<'a>(did: &'a Did) -> Vec<u8> {
    let mut vec = Vec::with_capacity(32);
    TrimmedDid::from(did).write_to_vec(&mut vec);
    vec
}

pub fn pending_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// prefix format: {DID}| (DID trimmed)
pub fn record_prefix_did(did: &Did) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut prefix = Vec::with_capacity(repo.len() + 1);
    repo.write_to_vec(&mut prefix);
    prefix.push(SEP);
    prefix
}

// prefix format: {DID}|{collection}|
pub fn record_prefix_collection(did: &Did, collection: &str) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut prefix = Vec::with_capacity(repo.len() + 1 + collection.len() + 1);
    repo.write_to_vec(&mut prefix);
    prefix.push(SEP);
    prefix.extend_from_slice(collection.as_bytes());
    prefix.push(SEP);
    prefix
}

// key format: {DID}|{collection}|{rkey}
pub fn record_key(did: &Did, collection: &str, rkey: &DbRkey) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key = Vec::with_capacity(repo.len() + 1 + collection.len() + 1 + rkey.len() + 1);
    repo.write_to_vec(&mut key);
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key.push(SEP);
    write_rkey(&mut key, rkey);
    key
}

pub fn write_rkey(buf: &mut Vec<u8>, rkey: &DbRkey) {
    match rkey {
        DbRkey::Tid(tid) => {
            buf.push(b't');
            buf.extend_from_slice(tid.as_bytes());
        }
        DbRkey::Str(s) => {
            buf.push(b's');
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

pub fn parse_rkey(raw: &[u8]) -> miette::Result<DbRkey> {
    let Some(kind) = raw.first() else {
        miette::bail!("record key is empty");
    };
    let rkey = match kind {
        b't' => {
            DbRkey::Tid(DbTid::new_from_bytes(raw[1..].try_into().map_err(|e| {
                miette::miette!("record key '{raw:?}' is invalid: {e}")
            })?))
        }
        b's' => DbRkey::Str(SmolStr::new(
            std::str::from_utf8(&raw[1..])
                .map_err(|e| miette::miette!("record key '{raw:?}' is invalid: {e}"))?,
        )),
        _ => miette::bail!("invalid record key kind: {}", *kind as char),
    };
    Ok(rkey)
}

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
    key.extend_from_slice(&rev.as_bytes());
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

/// key format: `ret|<did bytes>`
pub const CRAWLER_RETRY_PREFIX: &[u8] = b"ret|";

pub fn crawler_retry_key(did: &Did) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key = Vec::with_capacity(CRAWLER_RETRY_PREFIX.len() + repo.len());
    key.extend_from_slice(CRAWLER_RETRY_PREFIX);
    repo.write_to_vec(&mut key);
    key
}

/// value format: `<retry_after: i64 BE><status: u16 BE>`
pub fn crawler_retry_value(retry_after: i64, status: u16) -> [u8; 10] {
    let mut buf = [0u8; 10];
    buf[..8].copy_from_slice(&retry_after.to_be_bytes());
    buf[8..].copy_from_slice(&status.to_be_bytes());
    buf
}

pub fn crawler_retry_parse_value(val: &[u8]) -> miette::Result<(i64, u16)> {
    miette::ensure!(val.len() >= 10, "crawler retry value too short");
    let retry_after = i64::from_be_bytes(val[..8].try_into().unwrap());
    let status = u16::from_be_bytes(val[8..10].try_into().unwrap());
    Ok((retry_after, status))
}

pub fn crawler_retry_parse_key(key: &[u8]) -> miette::Result<TrimmedDid<'_>> {
    TrimmedDid::try_from(&key[CRAWLER_RETRY_PREFIX.len()..])
}
