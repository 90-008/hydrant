use jacquard_common::Data;
use jacquard_common::types::string::AtprotoStr;
use jacquard_common::types::uri::Uri;
use smol_str::{SmolStr, format_smolstr};

pub struct Link {
    /// dotted field path within the record, e.g. `.subject.uri` or `.facets[].features[app.bsky.richtext.facet#link].uri`
    pub path: SmolStr,
    pub target: SmolStr,
}

/// extract all link targets from a record, with their field paths.
///
/// paths follow the same convention as constellation:
/// - start with `.` (root-level field `subject` -> `.subject`)
/// - object keys: `{parent}.{key}`
/// - plain array elements: `{parent}[]`
/// - typed array elements (objects with `$type`): `{parent}[{$type}]`
///
/// results are sorted by (path, target) and deduplicated.
pub fn extract_links(value: &Data) -> Vec<Link> {
    let mut links = Vec::new();
    walk(value, "", &mut links);
    links.sort_unstable_by(|a, b| a.path.cmp(&b.path).then(a.target.cmp(&b.target)));
    links.dedup_by(|a, b| a.path == b.path && a.target == b.target);
    links
}

fn walk(value: &Data<'_>, path: &str, links: &mut Vec<Link>) {
    match value {
        Data::String(s) => {
            let target = match s {
                AtprotoStr::AtUri(uri) => uri.as_str(),
                AtprotoStr::Did(did) => did.as_str(),
                AtprotoStr::Uri(Uri::At(uri)) => uri.as_str(),
                AtprotoStr::Uri(Uri::Did(did)) => did.as_str(),
                _ => return,
            };
            links.push(Link {
                path: SmolStr::new(path),
                target: SmolStr::new(target),
            });
        }
        Data::Array(arr) => {
            for item in &arr.0 {
                let child_path = match item {
                    Data::Object(obj) => {
                        let type_tag = obj.0.get("$type").and_then(|v| match v {
                            Data::String(s) => Some(s.as_str()),
                            _ => None,
                        });
                        match type_tag {
                            Some(t) => format_smolstr!("{path}[{t}]"),
                            None => format_smolstr!("{path}[]"),
                        }
                    }
                    _ => format_smolstr!("{path}[]"),
                };
                walk(item, &child_path, links);
            }
        }
        Data::Object(obj) => {
            for (k, v) in &obj.0 {
                walk(v, &format_smolstr!("{path}.{k}"), links);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(s: &str) -> Data<'static> {
        Data::from_json_owned(serde_json::Value::String(s.to_string())).unwrap()
    }

    fn targets(links: &[Link]) -> Vec<&str> {
        links.iter().map(|l| l.target.as_str()).collect()
    }

    fn paths(links: &[Link]) -> Vec<&str> {
        links.iter().map(|l| l.path.as_str()).collect()
    }

    #[test]
    fn extracts_at_uri() {
        let val = make("at://did:plc:abc123/app.bsky.feed.post/tid123");
        let links = extract_links(&val);
        assert_eq!(
            targets(&links),
            vec!["at://did:plc:abc123/app.bsky.feed.post/tid123"]
        );
        assert_eq!(paths(&links), vec![""]);
    }

    #[test]
    fn extracts_did() {
        let val = make("did:plc:abc123");
        let links = extract_links(&val);
        assert_eq!(targets(&links), vec!["did:plc:abc123"]);
    }

    #[test]
    fn skips_plain_string() {
        assert!(extract_links(&make("hello world")).is_empty());
    }

    #[test]
    fn skips_handle() {
        assert!(extract_links(&make("alice.bsky.social")).is_empty());
    }

    #[test]
    fn object_paths() {
        let json = serde_json::json!({
            "subject": {
                "uri": "at://did:plc:abc/app.bsky.feed.post/tid",
                "cid": "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq"
            }
        });
        let val = Data::from_json_owned(json).unwrap();
        let links = extract_links(&val);
        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0].target.as_str(),
            "at://did:plc:abc/app.bsky.feed.post/tid"
        );
        assert_eq!(links[0].path.as_str(), ".subject.uri");
    }

    #[test]
    fn array_paths_plain() {
        let json = serde_json::json!([
            "at://did:plc:a/app.bsky.feed.post/tid1",
            "at://did:plc:b/app.bsky.feed.post/tid2"
        ]);
        let val = Data::from_json_owned(json).unwrap();
        let links = extract_links(&val);
        assert_eq!(links.len(), 2);
        assert!(links.iter().all(|l| l.path.as_str() == "[]"));
    }

    #[test]
    fn array_paths_typed_object() {
        let json = serde_json::json!({
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": "at://did:plc:abc/app.bsky.feed.post/tid"
                }
            ]
        });
        let val = Data::from_json_owned(json).unwrap();
        let links = extract_links(&val);
        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0].path.as_str(),
            ".features[app.bsky.richtext.facet#link].uri"
        );
    }

    #[test]
    fn array_paths_untyped_object() {
        let json = serde_json::json!({
            "items": [
                { "subject": "did:plc:abc" }
            ]
        });
        let val = Data::from_json_owned(json).unwrap();
        let links = extract_links(&val);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].path.as_str(), ".items[].subject");
    }

    #[test]
    fn deduplicates_same_path_and_target() {
        let uri = "at://did:plc:abc123/app.bsky.feed.post/tid123";
        let json = serde_json::json!([uri, uri]);
        let val = Data::from_json_owned(json).unwrap();
        assert_eq!(extract_links(&val).len(), 1);
    }

    #[test]
    fn keeps_same_target_at_different_paths() {
        let uri = "at://did:plc:abc/app.bsky.feed.post/tid";
        let json = serde_json::json!({
            "root": { "uri": uri },
            "parent": { "uri": uri }
        });
        let val = Data::from_json_owned(json).unwrap();
        let links = extract_links(&val);
        assert_eq!(links.len(), 2);
        let ps: Vec<_> = links.iter().map(|l| l.path.as_str()).collect();
        assert!(ps.contains(&".parent.uri"));
        assert!(ps.contains(&".root.uri"));
    }

    #[test]
    fn skips_cid_link() {
        let json = serde_json::json!({"$link": "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq"});
        let val = Data::from_json_owned(json).unwrap();
        assert!(extract_links(&val).is_empty());
    }
}
