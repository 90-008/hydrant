use fjall::{Keyspace, OwnedWriteBatch};
use jacquard_common::types::string::Did;
use jacquard_common::{Data, types::cid::Cid};
use miette::{IntoDiagnostic, Result};

use crate::db::keys::{self, SEP};
use crate::db::types::DbRkey;

/// key format: `f|{did}|{collection}|{rkey}`
///
/// value: msgpack-encoded `Vec<(String, String)>`, list of (path, target) pairs found in this record.
pub fn forward_key(did: &str, collection: &str, rkey: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + did.len() + 1 + collection.len() + 1 + rkey.len());
    key.push(b'f');
    key.push(SEP);
    key.extend_from_slice(did.as_bytes());
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key.push(SEP);
    key.extend_from_slice(rkey.as_bytes());
    key
}

/// key format: `r|{target}|{collection}|{path}|{did}|{rkey}`
///
/// value: empty — the key itself carries all information.
///
/// `|` separators are unambiguous because targets (AT URIs, DIDs), collection
/// NSIDs, field paths, DIDs, and rkeys cannot contain `|`.
/// prefix scans:
/// - all backlinks to a target: `r|{target}|`
/// - backlinks from a specific collection: `r|{target}|{collection}|`
/// - backlinks from a specific collection+field path: `r|{target}|{collection}|{path}|`
pub fn reverse_key(target: &str, collection: &str, path: &str, did: &str, rkey: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        2 + target.len() + 1 + collection.len() + 1 + path.len() + 1 + did.len() + 1 + rkey.len(),
    );
    key.push(b'r');
    key.push(SEP);
    key.extend_from_slice(target.as_bytes());
    key.push(SEP);
    key.extend_from_slice(collection.as_bytes());
    key.push(SEP);
    key.extend_from_slice(path.as_bytes());
    key.push(SEP);
    key.extend_from_slice(did.as_bytes());
    key.push(SEP);
    key.extend_from_slice(rkey.as_bytes());
    key
}

/// scan prefix for all reverse entries pointing to `target`.
///
/// - no collection: `r|{target}|`
/// - collection only: `r|{target}|{collection}|`  (matches all paths for that collection)
/// - collection + path: `r|{target}|{collection}|{path}|`  (exact path match)
///
/// path should include the leading `.` (e.g. `.subject.uri`). always ends with `|`.
pub fn reverse_scan_prefix(target: &str, collection: Option<&str>, path: Option<&str>) -> Vec<u8> {
    let col_len = collection.map_or(0, |c| c.len() + 1);
    let path_len = if collection.is_some() {
        path.map_or(0, |p| p.len() + 1)
    } else {
        0
    };
    let mut prefix = Vec::with_capacity(2 + target.len() + 1 + col_len + path_len);
    prefix.push(b'r');
    prefix.push(SEP);
    prefix.extend_from_slice(target.as_bytes());
    prefix.push(SEP);
    if let Some(col) = collection {
        prefix.extend_from_slice(col.as_bytes());
        prefix.push(SEP);
        if let Some(p) = path {
            prefix.extend_from_slice(p.as_bytes());
            prefix.push(SEP);
        }
    }
    prefix
}

/// scan prefix for all forward entries from the given DID.
pub fn forward_did_prefix(did: &Did) -> Vec<u8> {
    let did_str = did.as_str();
    let mut prefix = Vec::with_capacity(2 + did_str.len() + 1);
    prefix.push(b'f');
    prefix.push(SEP);
    prefix.extend_from_slice(did_str.as_bytes());
    prefix.push(SEP);
    prefix
}

/// extract (collection, path, did, rkey) from a reverse key.
///
/// reverse key format: `r|{target}|{collection}|{path}|{did}|{rkey}`
/// parses the last four `|`-separated segments from the right.
pub fn source_from_reverse_key(key: &[u8]) -> Option<(&str, &str, &str, &str)> {
    let rkey_sep = key.iter().rposition(|&b| b == SEP)?;
    let rkey = std::str::from_utf8(&key[rkey_sep + 1..]).ok()?;
    let did_sep = key[..rkey_sep].iter().rposition(|&b| b == SEP)?;
    let did = std::str::from_utf8(&key[did_sep + 1..rkey_sep]).ok()?;
    let path_sep = key[..did_sep].iter().rposition(|&b| b == SEP)?;
    let path = std::str::from_utf8(&key[path_sep + 1..did_sep]).ok()?;
    let col_sep = key[..path_sep].iter().rposition(|&b| b == SEP)?;
    let col = std::str::from_utf8(&key[col_sep + 1..path_sep]).ok()?;
    Some((col, path, did, rkey))
}

/// index all links found in a record.
///
/// on update, stale reverse entries from the previous version are removed first.
/// on create with no links, no entries are written.
pub fn index_record(
    batch: &mut OwnedWriteBatch,
    backlinks_ks: &Keyspace,
    did: &str,
    collection: &str,
    rkey: &str,
    value: &Data,
) -> Result<()> {
    let fwd_key = forward_key(did, collection, rkey);

    // remove reverse entries from the previous version of this record if any
    if let Some(old_bytes) = backlinks_ks.get(&fwd_key).into_diagnostic()? {
        let old_links: Vec<(String, String)> =
            rmp_serde::from_slice(&old_bytes).into_diagnostic()?;
        for (path, target) in &old_links {
            batch.remove(
                backlinks_ks,
                reverse_key(target, collection, path, did, rkey),
            );
        }
    }

    let new_links = crate::backlinks::links::extract_links(value);
    if new_links.is_empty() {
        batch.remove(backlinks_ks, fwd_key);
    } else {
        for link in &new_links {
            batch.insert(
                backlinks_ks,
                reverse_key(&link.target, collection, &link.path, did, rkey),
                &[] as &[u8],
            );
        }
        let fwd_pairs: Vec<(&str, &str)> = new_links
            .iter()
            .map(|l| (l.path.as_str(), l.target.as_str()))
            .collect();
        let fwd_val = rmp_serde::to_vec(&fwd_pairs).into_diagnostic()?;
        batch.insert(backlinks_ks, fwd_key, fwd_val);
    }

    Ok(())
}

/// remove all backlink index entries for a deleted record.
pub fn delete_record(
    batch: &mut OwnedWriteBatch,
    backlinks_ks: &Keyspace,
    did: &str,
    collection: &str,
    rkey: &str,
) -> Result<()> {
    let fwd_key = forward_key(did, collection, rkey);
    let Some(old_bytes) = backlinks_ks.get(&fwd_key).into_diagnostic()? else {
        return Ok(());
    };
    let links: Vec<(String, String)> = rmp_serde::from_slice(&old_bytes).into_diagnostic()?;
    for (path, target) in &links {
        batch.remove(
            backlinks_ks,
            reverse_key(target, collection, path, did, rkey),
        );
    }
    batch.remove(backlinks_ks, fwd_key);
    Ok(())
}

/// remove all backlink index entries for every record in a deleted repo.
///
/// scans the forward index by the `f|{did}|` prefix and removes both forward
/// and reverse entries for each record found.
pub fn delete_repo(batch: &mut OwnedWriteBatch, backlinks_ks: &Keyspace, did: &Did) -> Result<()> {
    let prefix = forward_did_prefix(did);
    let did_str = did.as_str();

    for guard in backlinks_ks.prefix(&prefix) {
        let (fwd_key, fwd_val) = guard.into_inner().into_diagnostic()?;
        // fwd_key suffix after f|{did}| is {collection}|{rkey}
        let suffix = &fwd_key[prefix.len()..];
        let Some(sep) = suffix.iter().position(|&b| b == SEP) else {
            tracing::warn!("backlinks: malformed forward key during delete_repo");
            continue;
        };
        let (Ok(col), Ok(rkey)) = (
            std::str::from_utf8(&suffix[..sep]),
            std::str::from_utf8(&suffix[sep + 1..]),
        ) else {
            tracing::warn!("backlinks: invalid utf8 in forward key during delete_repo");
            continue;
        };

        let links: Vec<(String, String)> = match rmp_serde::from_slice(&fwd_val) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "backlinks: failed to decode forward value for {did_str}/{col}/{rkey}: {e}"
                );
                continue;
            }
        };
        for (path, target) in &links {
            batch.remove(backlinks_ks, reverse_key(target, col, path, did_str, rkey));
        }
        batch.remove(backlinks_ks, fwd_key);
    }

    Ok(())
}

/// look up the CID string for a record from the records keyspace.
/// returns `None` if the record is not found (e.g. deleted after backlink was written).
pub fn lookup_cid_from_ks(
    records: &Keyspace,
    did: &str,
    collection: &str,
    rkey: &str,
) -> Option<String> {
    let did = Did::new(did).ok()?;
    let db_rkey = DbRkey::new(rkey);
    let record_key = keys::record_key(&did, collection, &db_rkey);
    let cid_bytes = records.get(record_key).ok()??;
    Cid::new(&cid_bytes).ok().map(|c| c.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: &str = "at://did:plc:abc/app.bsky.feed.post/tid";
    const COLLECTION: &str = "app.bsky.feed.like";
    const PATH: &str = ".subject.uri";
    const SRC_DID: &str = "did:plc:xyz";
    const SRC_RKEY: &str = "rkey1";

    #[test]
    fn reverse_key_scan_prefix_no_collection() {
        let key = reverse_key(TARGET, COLLECTION, PATH, SRC_DID, SRC_RKEY);
        let prefix = reverse_scan_prefix(TARGET, None, None);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn reverse_key_scan_prefix_with_collection() {
        let key = reverse_key(TARGET, COLLECTION, PATH, SRC_DID, SRC_RKEY);
        let prefix = reverse_scan_prefix(TARGET, Some(COLLECTION), None);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn reverse_key_scan_prefix_with_collection_and_path() {
        let key = reverse_key(TARGET, COLLECTION, PATH, SRC_DID, SRC_RKEY);
        let prefix = reverse_scan_prefix(TARGET, Some(COLLECTION), Some(PATH));
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn reverse_key_collection_prefix_does_not_match_other_collection() {
        let collection2 = "app.bsky.feed.like2";
        let key2 = reverse_key(TARGET, collection2, PATH, SRC_DID, SRC_RKEY);
        let prefix1 = reverse_scan_prefix(TARGET, Some(COLLECTION), None);
        assert!(!key2.starts_with(&prefix1));
    }

    #[test]
    fn reverse_key_path_prefix_does_not_match_other_path() {
        let path2 = ".subject.uri2";
        let key2 = reverse_key(TARGET, COLLECTION, path2, SRC_DID, SRC_RKEY);
        let prefix = reverse_scan_prefix(TARGET, Some(COLLECTION), Some(PATH));
        assert!(!key2.starts_with(&prefix));
    }

    #[test]
    fn collection_only_prefix_matches_all_paths() {
        let path2 = ".embed.uri";
        let key2 = reverse_key(TARGET, COLLECTION, path2, SRC_DID, SRC_RKEY);
        let col_prefix = reverse_scan_prefix(TARGET, Some(COLLECTION), None);
        assert!(key2.starts_with(&col_prefix));
    }

    #[test]
    fn source_from_reverse_key_roundtrip() {
        let key = reverse_key(TARGET, COLLECTION, PATH, SRC_DID, SRC_RKEY);
        let (col, path, did, rkey) = source_from_reverse_key(&key).unwrap();
        assert_eq!(col, COLLECTION);
        assert_eq!(path, PATH);
        assert_eq!(did, SRC_DID);
        assert_eq!(rkey, SRC_RKEY);
    }

    #[test]
    fn forward_did_prefix_matches_key() {
        let did = jacquard_common::types::string::Did::new("did:plc:abc123").unwrap();
        let prefix = forward_did_prefix(&did);
        let key = forward_key("did:plc:abc123", "app.bsky.feed.post", "tid123");
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn forward_did_prefix_does_not_match_other_did() {
        let did = jacquard_common::types::string::Did::new("did:plc:abc123").unwrap();
        let prefix = forward_did_prefix(&did);
        let other_key = forward_key("did:plc:xyz999", "app.bsky.feed.post", "tid123");
        assert!(!other_key.starts_with(&prefix));
    }
}
