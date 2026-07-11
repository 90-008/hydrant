use std::collections::BTreeMap;

use bytes::Bytes;
use cid::Cid as IpldCid;
use jacquard_repo::mst::NodeData;
use jacquard_repo::mst::util::layer_for_key;
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

pub(crate) fn mst_node_layer(bytes: &[u8]) -> Result<Option<usize>> {
    let node: NodeData = serde_ipld_dagcbor::from_slice(bytes)
        .into_diagnostic()
        .wrap_err("failed to decode mst node for layer estimate")?;
    decode_node_entries(&node).map(|entries| {
        entries.into_iter().find_map(|entry| match entry {
            FlatEntry::Leaf { key, .. } => Some(layer_for_key(key.as_str())),
            FlatEntry::Tree { .. } => None,
        })
    })
}

#[derive(Debug, Clone)]
pub(crate) struct SparseScanOutput {
    pub(crate) leaves: Vec<(SmolStr, IpldCid)>,
    pub(crate) node_blocks_seen: usize,
    pub(crate) node_bytes_seen: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeBounds {
    lower: Option<SmolStr>,
    upper: Option<SmolStr>,
}

impl NodeBounds {
    fn unbounded() -> Self {
        Self {
            lower: None,
            upper: None,
        }
    }

    fn covers(&self, other: &Self) -> bool {
        let lower_ok = match (&self.lower, &other.lower) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a <= b,
        };
        let upper_ok = match (&self.upper, &other.upper) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a >= b,
        };
        lower_ok && upper_ok
    }

    fn child(&self, lower_leaf: Option<&str>, upper_leaf: Option<&str>) -> Self {
        let lower = match (self.lower.as_deref(), lower_leaf) {
            (Some(a), Some(b)) => Some(SmolStr::new(a.max(b))),
            (a, b) => a.or(b).map(SmolStr::new),
        };
        let upper = match (self.upper.as_deref(), upper_leaf) {
            (Some(a), Some(b)) => Some(SmolStr::new(a.min(b))),
            (a, b) => a.or(b).map(SmolStr::new),
        };
        Self { lower, upper }
    }
}

pub(crate) struct SparseScanner {
    ranges: Vec<KeyRange>,
    blocks: BTreeMap<IpldCid, Bytes>,
    root: Option<IpldCid>,
    pending_missing: BTreeMap<IpldCid, Vec<NodeBounds>>,
    visited: BTreeMap<IpldCid, Vec<NodeBounds>>,
    leaves: BTreeMap<SmolStr, IpldCid>,
    stats: ScanStats,
}

impl SparseScanner {
    pub(crate) fn new(ranges: Vec<KeyRange>, blocks: BTreeMap<IpldCid, Bytes>) -> Self {
        Self {
            ranges,
            blocks,
            root: None,
            pending_missing: BTreeMap::new(),
            visited: BTreeMap::new(),
            leaves: BTreeMap::new(),
            stats: ScanStats::default(),
        }
    }

    pub(crate) fn insert_blocks(&mut self, blocks: BTreeMap<IpldCid, Bytes>) {
        self.blocks.extend(blocks);
    }

    pub(crate) fn scan(&mut self, root: IpldCid) -> Result<Result<SparseScanOutput, Vec<IpldCid>>> {
        if let Some(existing_root) = self.root {
            if existing_root != root {
                return Err(miette::miette!(
                    "sparse scanner root changed from {existing_root} to {root}"
                ));
            }
        } else {
            self.root = Some(root);
            self.scan_node(root, NodeBounds::unbounded())?;
        }

        let ready = self
            .pending_missing
            .iter()
            .filter(|(cid, _)| self.blocks.contains_key(cid))
            .map(|(cid, bounds)| (*cid, bounds.clone()))
            .collect::<Vec<_>>();
        for (cid, bounds_list) in ready {
            self.pending_missing.remove(&cid);
            for bounds in bounds_list {
                self.scan_node(cid, bounds)?;
            }
        }

        if self.pending_missing.is_empty() {
            Ok(Ok(SparseScanOutput {
                leaves: self
                    .leaves
                    .iter()
                    .map(|(key, cid)| (key.clone(), *cid))
                    .collect(),
                node_blocks_seen: self.stats.node_blocks_seen,
                node_bytes_seen: self.stats.node_bytes_seen,
            }))
        } else {
            Ok(Err(self.pending_missing.keys().copied().collect()))
        }
    }

    pub(crate) fn take_blocks(self) -> BTreeMap<IpldCid, Bytes> {
        self.blocks
    }

    fn scan_node(&mut self, cid: IpldCid, bounds: NodeBounds) -> Result<()> {
        if self
            .visited
            .get(&cid)
            .is_some_and(|seen| seen.iter().any(|prior| prior.covers(&bounds)))
        {
            return Ok(());
        }

        let Some(bytes) = self.blocks.get(&cid) else {
            let pending = self.pending_missing.entry(cid).or_default();
            if !pending.iter().any(|prior| prior.covers(&bounds)) {
                pending.push(bounds);
            }
            return Ok(());
        };

        let seen = self.visited.entry(cid).or_default();
        if seen.is_empty() {
            self.stats.node_blocks_seen += 1;
            self.stats.node_bytes_seen += bytes.len();
        }
        seen.push(bounds.clone());

        let node: NodeData = serde_ipld_dagcbor::from_slice(bytes)
            .into_diagnostic()
            .wrap_err_with(|| format!("failed to decode mst node {cid}"))?;
        let entries = decode_node_entries(&node)?;

        for idx in 0..entries.len() {
            match &entries[idx] {
                FlatEntry::Leaf { key, cid } => {
                    if self.ranges.iter().any(|range| range.contains(key)) {
                        self.leaves.insert(key.clone(), *cid);
                    }
                }
                FlatEntry::Tree { cid } => {
                    let child =
                        bounds.child(previous_leaf(&entries, idx), next_leaf(&entries, idx));
                    if self.ranges.iter().any(|range| {
                        range.intersects_subtree(child.lower.as_deref(), child.upper.as_deref())
                    }) {
                        self.scan_node(*cid, child)?;
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
        let mut scanner = SparseScanner::new(wanted, BTreeMap::from([(root, root_bytes.into())]));

        let missing = scanner.scan(root).unwrap().unwrap_err();

        assert_eq!(missing, vec![middle]);
    }

    #[test]
    fn resumes_without_rescanning_known_nodes() {
        let wanted = sparse_ranges(&[SmolStr::new("sh.tangled.*")]);
        let middle = cid(1);
        let root = cid(2);
        let record = cid(3);
        let root_node = node(
            vec![
                ("app.bsky.feed.post/1", cid(4), Some(middle)),
                ("zz.example.record/1", cid(5), None),
            ],
            None,
        );
        let middle_node = node(vec![("sh.tangled.repo/1", record, None)], None);
        let mut scanner = SparseScanner::new(
            wanted,
            BTreeMap::from([(root, serde_ipld_dagcbor::to_vec(&root_node).unwrap().into())]),
        );

        assert_eq!(scanner.scan(root).unwrap().unwrap_err(), vec![middle]);

        scanner.insert_blocks(BTreeMap::from([(
            middle,
            serde_ipld_dagcbor::to_vec(&middle_node).unwrap().into(),
        )]));
        let scan = scanner.scan(root).unwrap().unwrap();

        assert_eq!(
            scan.leaves,
            vec![(SmolStr::new("sh.tangled.repo/1"), record)]
        );
        assert_eq!(scan.node_blocks_seen, 2);
    }

    #[test]
    fn estimates_node_layer_from_first_leaf() {
        let root_node = node(vec![("sh.tangled.repo/1", cid(1), None)], None);
        let bytes = serde_ipld_dagcbor::to_vec(&root_node).unwrap();

        assert_eq!(
            mst_node_layer(&bytes).unwrap(),
            Some(layer_for_key("sh.tangled.repo/1"))
        );
    }

    fn car_bytes(node: &NodeData) -> Bytes {
        serde_ipld_dagcbor::to_vec(node).unwrap().into()
    }

    #[test]
    fn ancestor_bounds_prune_out_of_range_boundary_subtrees() {
        // two disjoint ranges; the fetched middle subtree's leftmost child sits
        // entirely between them once the ancestor lower bound is applied.
        let wanted = sparse_ranges(&[SmolStr::new("aa.x"), SmolStr::new("zz.y")]);
        let boundary_child = cid(1);
        let middle = cid(2);
        let root = cid(3);
        let root_node = node(
            vec![
                ("aa.x/1", cid(4), None),
                ("bb.q/1", cid(5), Some(middle)),
                ("zz.y/5", cid(6), None),
            ],
            None,
        );
        // boundary_child spans ("bb.q/1", "mm.a/1") given the ancestor bound,
        // which intersects neither range; without the bound its interval is
        // (-inf, "mm.a/1"), which falsely intersects the aa.x range.
        let middle_node = node(
            vec![("mm.a/1", cid(7), None), ("zz.y/1", cid(8), None)],
            Some(boundary_child),
        );
        let mut scanner =
            SparseScanner::new(wanted, BTreeMap::from([(root, car_bytes(&root_node))]));

        assert_eq!(scanner.scan(root).unwrap().unwrap_err(), vec![middle]);

        scanner.insert_blocks(BTreeMap::from([(middle, car_bytes(&middle_node))]));
        let scan = scanner.scan(root).unwrap().unwrap();

        assert_eq!(
            scan.leaves,
            vec![
                (SmolStr::new("aa.x/1"), cid(4)),
                (SmolStr::new("zz.y/1"), cid(8)),
                (SmolStr::new("zz.y/5"), cid(6)),
            ]
        );
    }

    #[test]
    fn duplicate_pending_cids_fetch_once_and_scan_deterministically() {
        // the same missing child CID is referenced from two positions with
        // different ancestor bounds; it must appear once in the missing list
        // and both contexts must be scanned after a single insert.
        let wanted = sparse_ranges(&[SmolStr::new("aa.x"), SmolStr::new("zz.y")]);
        let shared = cid(1);
        let root = cid(2);
        let root_node = node(
            vec![
                ("aa.w/1", cid(3), Some(shared)),
                ("bb.q/1", cid(4), None),
                ("zz.x/1", cid(5), Some(shared)),
            ],
            None,
        );
        let shared_node = node(
            vec![("aa.x/1", cid(6), None), ("zz.y/1", cid(7), None)],
            None,
        );
        let mut scanner =
            SparseScanner::new(wanted, BTreeMap::from([(root, car_bytes(&root_node))]));

        assert_eq!(scanner.scan(root).unwrap().unwrap_err(), vec![shared]);

        scanner.insert_blocks(BTreeMap::from([(shared, car_bytes(&shared_node))]));
        let scan = scanner.scan(root).unwrap().unwrap();

        assert_eq!(
            scan.leaves,
            vec![
                (SmolStr::new("aa.x/1"), cid(6)),
                (SmolStr::new("zz.y/1"), cid(7)),
            ]
        );
        assert_eq!(scan.node_blocks_seen, 2);
    }
}
