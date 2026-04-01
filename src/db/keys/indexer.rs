use jacquard_common::types::string::Did;
use smol_str::SmolStr;

use super::SEP;
use crate::db::types::{DbRkey, DbTid, TrimmedDid};

pub const EVENT_WATERMARK_PREFIX: &[u8] = b"ewm|";

pub fn pending_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

pub fn event_watermark_key(timestamp_secs: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(EVENT_WATERMARK_PREFIX.len() + 8);
    key.extend_from_slice(EVENT_WATERMARK_PREFIX);
    key.extend_from_slice(&timestamp_secs.to_be_bytes());
    key
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

// key format: r|{DID}|{collection} (DID trimmed)
pub fn count_collection_key(did: &Did, collection: &str) -> Vec<u8> {
    let mut key = super::did_collection_prefix(did);
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

pub fn crawler_retry_parse_key(key: &[u8]) -> miette::Result<TrimmedDid<'_>> {
    TrimmedDid::try_from(&key[CRAWLER_RETRY_PREFIX.len()..])
}

pub const CRAWLER_CURSOR_PREFIX: &[u8] = b"crawler_cursor|";

pub fn crawler_cursor_key(relay: &str) -> Vec<u8> {
    let mut key = CRAWLER_CURSOR_PREFIX.to_vec();
    key.extend_from_slice(relay.as_bytes());
    key
}

pub const BY_COLLECTION_CURSOR_PREFIX: &[u8] = b"by_collection_cursor|";

/// prefix for all by-collection cursors belonging to a given index URL.
pub fn by_collection_cursor_prefix(url: &str) -> Vec<u8> {
    let mut prefix = BY_COLLECTION_CURSOR_PREFIX.to_vec();
    prefix.extend_from_slice(url.as_bytes());
    prefix.push(SEP);
    prefix
}

pub fn by_collection_cursor_key(url: &str, collection: &str) -> Vec<u8> {
    let mut key = by_collection_cursor_prefix(url);
    key.extend_from_slice(collection.as_bytes());
    key
}

pub const CRAWLER_SOURCE_PREFIX: &[u8] = b"src|";

pub fn crawler_source_key(url: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(CRAWLER_SOURCE_PREFIX.len() + url.len());
    key.extend_from_slice(CRAWLER_SOURCE_PREFIX);
    key.extend_from_slice(url.as_bytes());
    key
}

// key format: {collection}|{cid_bytes}
pub fn block_key(collection: &str, cid: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + 1 + cid.len());
    key.extend_from_slice(collection.as_bytes());
    key.push(SEP);
    key.extend_from_slice(cid);
    key
}
