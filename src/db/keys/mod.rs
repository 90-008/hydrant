use jacquard_common::types::string::Did;

use crate::db::types::TrimmedDid;

pub mod v1;

pub use v1::{firehose_cursor_key, firehose_cursor_key_from_url};

/// separator used for composite keys
pub const SEP: u8 = b'|';

#[cfg(feature = "indexer")]
pub mod indexer;
#[cfg(feature = "indexer")]
pub use indexer::*;

#[cfg(feature = "relay")]
pub const RELAY_EVENT_WATERMARK_PREFIX: &[u8] = b"rwm|";

/// THIS SHOULD ALWAYS BE STABLE. DO NOT CHANGE
pub const VERSIONING_KEY: &[u8] = b"db_version";

// key format: {DID}
pub fn repo_key<'a>(did: &'a Did) -> Vec<u8> {
    let mut vec = Vec::with_capacity(32);
    TrimmedDid::from(did).write_to_vec(&mut vec);
    vec
}

pub const REPO_METADATA_PREFIX: &[u8] = b"rm|";

pub fn repo_metadata_key<'a>(did: &'a Did) -> Vec<u8> {
    let mut vec = Vec::with_capacity(REPO_METADATA_PREFIX.len() + 32);
    vec.extend_from_slice(REPO_METADATA_PREFIX);
    TrimmedDid::from(did).write_to_vec(&mut vec);
    vec
}

#[cfg(feature = "relay")]
/// key format: {SEQ} (u64 big-endian), mirroring event_key
pub fn relay_event_key(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

#[cfg(feature = "relay")]
pub fn relay_event_watermark_key(timestamp_secs: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RELAY_EVENT_WATERMARK_PREFIX.len() + 8);
    key.extend_from_slice(RELAY_EVENT_WATERMARK_PREFIX);
    key.extend_from_slice(&timestamp_secs.to_be_bytes());
    key
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

pub fn did_collection_prefix(did: &Did) -> Vec<u8> {
    let repo = TrimmedDid::from(did);
    let mut key = Vec::with_capacity(COUNT_COLLECTION_PREFIX.len() + repo.len() + 1);
    key.extend_from_slice(COUNT_COLLECTION_PREFIX);
    repo.write_to_vec(&mut key);
    key.push(SEP);
    key
}

pub const SEED_CURSOR_PREFIX: &[u8] = b"seed_cursor|";

pub fn seed_cursor_key(url: &str) -> Vec<u8> {
    let mut key = SEED_CURSOR_PREFIX.to_vec();
    key.extend_from_slice(url.as_bytes());
    key
}

pub const FIREHOSE_CURSOR_PREFIX: &[u8] = b"firehose_cursor|";

pub const FIREHOSE_SOURCE_PREFIX: &[u8] = b"firehose|";

pub fn firehose_source_key(url: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIREHOSE_SOURCE_PREFIX.len() + url.len());
    key.extend_from_slice(FIREHOSE_SOURCE_PREFIX);
    key.extend_from_slice(url.as_bytes());
    key
}

pub fn pds_account_count_key(host: &str) -> String {
    format!("p|{host}")
}
