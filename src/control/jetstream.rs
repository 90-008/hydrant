use super::*;
use crate::db::types::TrimmedDid;
use crate::types::StoredJetstreamEvent;
use jacquard_common::IntoStatic;
use jacquard_common::types::nsid::Nsid;
use jacquard_common::types::string::Did;
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashSet;
use std::sync::Arc;

pub struct JetstreamEventStream(mpsc::Receiver<Result<bytes::Bytes, JetstreamStreamError>>);

#[derive(Debug, Clone, thiserror::Error)]
pub enum JetstreamStreamError {
    #[error("jetstream consumer too slow: {reason}")]
    ConsumerTooSlow { reason: String },
}

impl JetstreamStreamError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ConsumerTooSlow { .. } => "ConsumerTooSlow",
        }
    }
}

impl Stream for JetstreamEventStream {
    type Item = Result<bytes::Bytes, JetstreamStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

#[derive(Clone, Default)]
pub struct JetstreamFilter {
    inner: Arc<arc_swap::ArcSwap<JetstreamSubscriberOptions>>,
}

impl JetstreamFilter {
    pub fn new(options: JetstreamSubscriberOptions) -> Self {
        Self {
            inner: Arc::new(arc_swap::ArcSwap::from_pointee(options)),
        }
    }

    pub fn update(&self, options: JetstreamSubscriberOptions) {
        self.inner.store(Arc::new(options));
    }

    pub fn max_message_size_bytes(&self) -> u32 {
        self.inner.load().max_message_size_bytes()
    }

    pub(crate) fn wants(&self, event: &StoredJetstreamEvent<'_>) -> bool {
        self.inner.load().wants(event)
    }
}

#[derive(Clone, Default)]
pub struct JetstreamSubscriberOptions {
    wanted_collections: Option<WantedCollections>,
    wanted_dids: Arc<HashSet<Vec<u8>>>,
    max_message_size_bytes: u32,
    wanted_event_types: Option<WantedEventTypes>,
}

#[derive(Clone)]
struct WantedCollections {
    prefixes: Vec<SmolStr>,
    full_paths: HashSet<SmolStr>,
}

#[derive(Clone)]
struct WantedEventTypes {
    live: bool,
    historical: bool,
}

impl JetstreamSubscriberOptions {
    pub fn parse(
        wanted_collections: &[String],
        wanted_dids: &[String],
        max_message_size_bytes: u32,
        wanted_event_types: &[String],
    ) -> std::result::Result<Self, String> {
        let wanted_collections = parse_wanted_collections(wanted_collections)?;
        let wanted_dids = parse_wanted_dids(wanted_dids)?;
        let wanted_event_types = parse_wanted_event_types(wanted_event_types)?;
        Ok(Self {
            wanted_collections,
            wanted_dids: Arc::new(wanted_dids),
            max_message_size_bytes,
            wanted_event_types,
        })
    }

    pub fn max_message_size_bytes(&self) -> u32 {
        self.max_message_size_bytes
    }

    pub(crate) fn wants(&self, event: &StoredJetstreamEvent<'_>) -> bool {
        if let Some(wanted) = &self.wanted_event_types {
            let is_live = event.is_live();
            if is_live && !wanted.live {
                return false;
            }
            if !is_live && !wanted.historical {
                return false;
            }
        }

        if !self.wanted_dids.is_empty() {
            let mut did = Vec::with_capacity(event.did().len());
            event.did().write_to_vec(&mut did);
            if !self.wanted_dids.contains(&did) {
                return false;
            }
        }

        let Some(wanted_collections) = &self.wanted_collections else {
            return true;
        };
        let Some(collection) = event.collection() else {
            return true;
        };

        wanted_collections.full_paths.contains(collection)
            || wanted_collections
                .prefixes
                .iter()
                .any(|prefix| collection.starts_with(prefix.as_str()))
    }
}

impl Hydrant {
    pub fn subscribe_jetstream(
        &self,
        cursor: Option<i64>,
        filter: JetstreamFilter,
    ) -> JetstreamEventStream {
        let (tx, rx) = mpsc::channel(500);
        let state = self.state.clone();
        let runtime = tokio::runtime::Handle::current();
        let opts = stream::StreamOptions::from_config(&self.config);

        std::thread::Builder::new()
            .name("hydrant-jetstream".into())
            .spawn(move || {
                let _g = runtime.enter();
                stream::jetstream_stream_thread(state, tx, cursor, filter, opts);
            })
            .expect("failed to spawn Jetstream thread");

        JetstreamEventStream(rx)
    }
}

fn parse_wanted_collections(
    provided: &[String],
) -> std::result::Result<Option<WantedCollections>, String> {
    if provided.is_empty() {
        return Ok(None);
    }
    if provided.len() > 100 {
        return Err("too many wanted collections".into());
    }

    let mut prefixes = Vec::new();
    let mut full_paths = HashSet::new();
    for collection_raw in provided {
        for collection in collection_raw.split(',') {
            if collection.is_empty() {
                continue;
            }
            if let Some(prefix) = collection.strip_suffix(".*") {
                validate_collection_prefix(prefix)
                    .map_err(|_| format!("invalid collection prefix: {collection}"))?;
                prefixes.push(format!("{prefix}.").to_smolstr());
                continue;
            }

            let nsid = Nsid::new(collection)
                .map_err(|_| format!("invalid collection: {collection}"))?
                .into_static();
            full_paths.insert(nsid.as_str().to_smolstr());
        }
    }

    Ok(Some(WantedCollections {
        prefixes,
        full_paths,
    }))
}

fn validate_collection_prefix(prefix: &str) -> std::result::Result<(), String> {
    let synthetic = format!("{prefix}.x");
    Nsid::new(synthetic.as_str())
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn parse_wanted_event_types(
    provided: &[String],
) -> std::result::Result<Option<WantedEventTypes>, String> {
    if provided.is_empty() {
        return Ok(None);
    }
    let mut live = false;
    let mut historical = false;
    for ev in provided {
        match ev.as_str() {
            "live" => live = true,
            "historical" => historical = true,
            _ => return Err(format!("unknown wantedEventTypes value: {}", ev)),
        }
    }
    Ok(Some(WantedEventTypes { live, historical }))
}

fn parse_wanted_dids(provided: &[String]) -> std::result::Result<HashSet<Vec<u8>>, String> {
    let mut wanted = HashSet::new();
    for did_raw in provided {
        for did_str in did_raw.split(',') {
            if did_str.is_empty() {
                continue;
            }
            let did = Did::new(did_str).map_err(|_| "invalid wanted DID".to_string())?;
            let trimmed = TrimmedDid::from(&did);
            let mut buf = Vec::with_capacity(trimmed.len());
            trimmed.write_to_vec(&mut buf);
            wanted.insert(buf);
        }
    }
    if wanted.len() > 10_000 {
        return Err("too many wanted DIDs".into());
    }
    Ok(wanted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_prefixes_accept_domain_prefixes() {
        let opts = JetstreamSubscriberOptions::parse(&["app.bsky.*".into()], &[], 0, &[]).unwrap();
        let wanted = opts.wanted_collections.unwrap();

        assert!(wanted.prefixes.iter().any(|prefix| prefix == "app.bsky."));
        assert!(
            JetstreamSubscriberOptions::parse(&["app.bsky.feed.po*".into()], &[], 0, &[]).is_err()
        );
    }

    #[test]
    fn full_path_filter_matches_commit_collection() {
        use crate::db::types::TrimmedDid;
        use crate::types::StoredJetstreamEvent;
        use jacquard_common::{CowStr, IntoStatic};
        use smol_str::ToSmolStr;

        let opts = JetstreamSubscriberOptions::parse(&["app.bsky.feed.post".into()], &[], 0, &[])
            .expect("app.bsky.feed.post must be a valid collection");

        let did =
            TrimmedDid::from(&jacquard_common::types::string::Did::new("did:plc:abc123").unwrap())
                .into_static();

        let matching = StoredJetstreamEvent::Commit {
            did: did.clone(),
            collection: CowStr::Owned("app.bsky.feed.post".to_smolstr()),
            event_id: 1,
            live: true,
        };
        let non_matching = StoredJetstreamEvent::Commit {
            did: did.clone(),
            collection: CowStr::Owned("app.bsky.actor.profile".to_smolstr()),
            event_id: 2,
            live: true,
        };
        let account = StoredJetstreamEvent::Account {
            did,
            active: true,
            status: None,
            seq: 1,
            time: crate::ingest::stream::Datetime(chrono::Utc::now().into()),
        };

        assert!(
            opts.wants(&matching),
            "app.bsky.feed.post commit should match"
        );
        assert!(
            !opts.wants(&non_matching),
            "other collection must not match"
        );
        assert!(
            opts.wants(&account),
            "account events must pass through collection filter"
        );
    }

    #[test]
    fn wanted_did_limit_counts_unique_dids() {
        let dids = vec!["did:plc:abc123".to_string(); 10_001];

        let opts = JetstreamSubscriberOptions::parse(&[], &dids, 0, &[]).unwrap();

        assert_eq!(opts.wanted_dids.len(), 1);
    }
}
