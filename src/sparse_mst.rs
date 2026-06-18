use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use cid::Cid as IpldCid;
use jacquard_repo::mst::NodeData;
use miette::{IntoDiagnostic, Result, WrapErr};
use smol_str::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyRange {
    start: SmolStr,
    end: Option<SmolStr>,
}

impl KeyRange {
    pub(crate) fn contains(&self, key: &str) -> bool {
        key >= self.start.as_str() && self.end.as_deref().map(|end| key < end).unwrap_or(true)
    }

    fn intersects_subtree(
        &self,
        lower_exclusive: Option<&str>,
        upper_exclusive: Option<&str>,
    ) -> bool {
        if let Some(upper) = upper_exclusive {
            if self.start.as_str() >= upper {
                return false;
            }
        }

        if let (Some(end), Some(lower)) = (self.end.as_deref(), lower_exclusive) {
            if end <= lower {
                return false;
            }
        }

        true
    }
}

pub(crate) fn sparse_ranges(patterns: &[SmolStr]) -> Vec<KeyRange> {
    patterns
        .iter()
        .filter_map(|pattern| {
            let prefix = pattern
                .strip_suffix(".*")
                .map(|prefix| SmolStr::new(format!("{prefix}.")))
                .unwrap_or_else(|| SmolStr::new(format!("{pattern}/")));
            Some(KeyRange {
                end: prefix_upper_bound(prefix.as_str()),
                start: prefix,
            })
        })
        .collect()
}

pub(crate) fn sparse_probe_collection(patterns: &[SmolStr]) -> Option<SmolStr> {
    patterns.iter().find_map(|pattern| {
        pattern
            .strip_suffix(".*")
            .map(|prefix| format!("{prefix}.probe"))
            .unwrap_or_else(|| pattern.to_string())
            .parse::<jacquard_common::types::string::Nsid>()
            .ok()
            .map(|_| {
                pattern
                    .strip_suffix(".*")
                    .map(|prefix| SmolStr::new(format!("{prefix}.probe")))
                    .unwrap_or_else(|| pattern.clone())
            })
    })
}

fn prefix_upper_bound(prefix: &str) -> Option<SmolStr> {
    let mut bytes = prefix.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] < u8::MAX {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return String::from_utf8(bytes).ok().map(SmolStr::new);
        }
    }
    None
}

#[derive(Debug, Clone)]
pub(crate) struct SparseScanOutput {
    pub(crate) leaves: Vec<(SmolStr, IpldCid)>,
    pub(crate) node_blocks_seen: usize,
    pub(crate) node_bytes_seen: usize,
}

pub(crate) struct SparseScanner {
    ranges: Vec<KeyRange>,
    blocks: BTreeMap<IpldCid, Bytes>,
}

impl SparseScanner {
    pub(crate) fn new(ranges: Vec<KeyRange>, blocks: BTreeMap<IpldCid, Bytes>) -> Self {
        Self { ranges, blocks }
    }

    pub(crate) fn insert_blocks(&mut self, blocks: BTreeMap<IpldCid, Bytes>) {
        self.blocks.extend(blocks);
    }

    pub(crate) fn scan(&self, root: IpldCid) -> Result<Result<SparseScanOutput, Vec<IpldCid>>> {
        let mut visited = BTreeSet::new();
        let mut missing = BTreeSet::new();
        let mut leaves = Vec::new();
        let mut stats = ScanStats::default();

        self.scan_node(root, &mut visited, &mut missing, &mut leaves, &mut stats)?;

        if missing.is_empty() {
            Ok(Ok(SparseScanOutput {
                leaves,
                node_blocks_seen: stats.node_blocks_seen,
                node_bytes_seen: stats.node_bytes_seen,
            }))
        } else {
            Ok(Err(missing.into_iter().collect()))
        }
    }

    pub(crate) fn take_blocks(self) -> BTreeMap<IpldCid, Bytes> {
        self.blocks
    }

    fn scan_node(
        &self,
        cid: IpldCid,
        visited: &mut BTreeSet<IpldCid>,
        missing: &mut BTreeSet<IpldCid>,
        leaves: &mut Vec<(SmolStr, IpldCid)>,
        stats: &mut ScanStats,
    ) -> Result<()> {
        if !visited.insert(cid) {
            return Ok(());
        }

        let Some(bytes) = self.blocks.get(&cid) else {
            missing.insert(cid);
            return Ok(());
        };

        stats.node_blocks_seen += 1;
        stats.node_bytes_seen += bytes.len();

        let node: NodeData = serde_ipld_dagcbor::from_slice(bytes)
            .into_diagnostic()
            .wrap_err_with(|| format!("failed to decode mst node {cid}"))?;
        let entries = decode_node_entries(&node)?;

        for idx in 0..entries.len() {
            match &entries[idx] {
                FlatEntry::Leaf { key, cid } => {
                    if self.ranges.iter().any(|range| range.contains(key)) {
                        leaves.push((key.clone(), *cid));
                    }
                }
                FlatEntry::Tree { cid } => {
                    let lower = previous_leaf(&entries, idx);
                    let upper = next_leaf(&entries, idx);
                    if self
                        .ranges
                        .iter()
                        .any(|range| range.intersects_subtree(lower, upper))
                    {
                        self.scan_node(*cid, visited, missing, leaves, stats)?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Default)]
struct ScanStats {
    node_blocks_seen: usize,
    node_bytes_seen: usize,
}

#[derive(Debug, Clone)]
enum FlatEntry {
    Tree { cid: IpldCid },
    Leaf { key: SmolStr, cid: IpldCid },
}

fn decode_node_entries(node: &NodeData) -> Result<Vec<FlatEntry>> {
    let mut entries = Vec::new();
    if let Some(cid) = node.left {
        entries.push(FlatEntry::Tree { cid });
    }

    let mut last_key = String::new();
    for entry in &node.entries {
        let suffix = std::str::from_utf8(&entry.key_suffix)
            .into_diagnostic()
            .wrap_err("invalid utf8 in mst key suffix")?;
        let prefix_len = entry.prefix_len as usize;
        let prefix = last_key
            .get(..prefix_len)
            .ok_or_else(|| miette::miette!("invalid mst key prefix length {prefix_len}"))?;
        let key = SmolStr::new(format!("{prefix}{suffix}"));

        entries.push(FlatEntry::Leaf {
            key: key.clone(),
            cid: entry.value,
        });
        last_key = key.to_string();

        if let Some(cid) = entry.tree {
            entries.push(FlatEntry::Tree { cid });
        }
    }

    Ok(entries)
}

fn previous_leaf(entries: &[FlatEntry], idx: usize) -> Option<&str> {
    entries[..idx].iter().rev().find_map(|entry| match entry {
        FlatEntry::Leaf { key, .. } => Some(key.as_str()),
        FlatEntry::Tree { .. } => None,
    })
}

fn next_leaf(entries: &[FlatEntry], idx: usize) -> Option<&str> {
    entries[idx + 1..].iter().find_map(|entry| match entry {
        FlatEntry::Leaf { key, .. } => Some(key.as_str()),
        FlatEntry::Tree { .. } => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::Cid;
    use cid::multihash::Multihash;
    use jacquard_common::types::crypto::{DAG_CBOR, SHA2_256};
    use jacquard_repo::mst::TreeEntry;

    fn cid(byte: u8) -> IpldCid {
        let hash = [byte; 32];
        let mh = Multihash::<64>::wrap(SHA2_256, &hash).unwrap();
        Cid::new_v1(DAG_CBOR, mh)
    }

    fn node(entries: Vec<(&str, IpldCid, Option<IpldCid>)>, left: Option<IpldCid>) -> NodeData {
        let mut last = String::new();
        let entries = entries
            .into_iter()
            .map(|(key, value, tree)| {
                let prefix_len = common_prefix_len(&last, key);
                let suffix = key[prefix_len..].as_bytes().to_vec();
                last = key.to_string();
                TreeEntry {
                    key_suffix: suffix.into(),
                    prefix_len: prefix_len as u8,
                    tree,
                    value,
                }
            })
            .collect();
        NodeData { left, entries }
    }

    fn common_prefix_len(a: &str, b: &str) -> usize {
        a.chars().zip(b.chars()).take_while(|(a, b)| a == b).count()
    }

    #[test]
    fn builds_exact_and_wildcard_ranges() {
        let ranges = sparse_ranges(&[SmolStr::new("sh.tangled.repo"), SmolStr::new("app.bsky.*")]);

        assert!(ranges[0].contains("sh.tangled.repo/abc"));
        assert!(!ranges[0].contains("sh.tangled.repo.comment/abc"));
        assert!(ranges[1].contains("app.bsky.feed.post/abc"));
        assert!(!ranges[1].contains("app.bskyfoo.feed.post/abc"));
        assert!(!ranges[1].contains("app.csky.feed.post/abc"));
    }

    #[test]
    fn synthesizes_probe_collection_for_wildcard() {
        assert_eq!(
            sparse_probe_collection(&[SmolStr::new("sh.tangled.*")]).as_deref(),
            Some("sh.tangled.probe")
        );
        assert_eq!(
            sparse_probe_collection(&[SmolStr::new("sh.tangled.repo")]).as_deref(),
            Some("sh.tangled.repo")
        );
    }

    #[test]
    fn reports_missing_intersecting_subtrees_only() {
        let wanted = sparse_ranges(&[SmolStr::new("sh.tangled.*")]);
        let left = cid(1);
        let middle = cid(2);
        let right = cid(3);
        let root = cid(4);
        let root_node = node(
            vec![
                ("app.bsky.feed.post/1", cid(5), Some(middle)),
                ("zz.example.record/1", cid(6), Some(right)),
            ],
            Some(left),
        );
        let root_bytes = serde_ipld_dagcbor::to_vec(&root_node).unwrap();
        let scanner = SparseScanner::new(wanted, BTreeMap::from([(root, root_bytes.into())]));

        let missing = scanner.scan(root).unwrap().unwrap_err();

        assert_eq!(missing, vec![middle]);
    }
}
