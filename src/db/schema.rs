//! keyspace schemas: the single table of truth for every keyspace.
//!
//! each keyspace is declared as one type implementing [`Schema`], carrying its
//! on-disk name, open-time tuning, optional zstd dictionary training config,
//! and debug key/value rendering. the registry (`db::registry`) derives the
//! by-name lookup, `/stats` enumeration, `/debug/*` dispatch, and dictionary
//! training from these types, so a keyspace cannot be half-registered.

use std::marker::PhantomData;

use fjall::{
    CompressionType, Keyspace, KeyspaceCreateOptions,
    config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy},
};
use miette::Result;
use serde_json::Value;

use super::keyspaces::OpenCx;

pub(super) const fn kb(v: u32) -> u32 {
    v * 1024
}

pub(super) const fn mb(v: u64) -> u64 {
    v * 1024 * 1024
}

/// zstd dictionary training configuration for a keyspace.
#[derive(Debug, Clone, Copy)]
pub struct TrainCfg {
    /// maximum dictionary size in bytes.
    pub dict_size: u32,
    /// number of data blocks to sample.
    pub samples: usize,
    /// how sampled blocks are filtered.
    pub sampler: Sampler,
}

#[derive(Debug, Clone, Copy)]
pub enum Sampler {
    /// dedup samples by their (first, last) key pair.
    DedupKeys,
    /// cap samples per collection prefix (up to the first `keys::SEP`).
    PerCollectionCap(usize),
}

/// a keyspace schema: name, tuning, training, and debug rendering.
pub trait Schema {
    /// on-disk keyspace name. THIS SHOULD ALWAYS BE STABLE. DO NOT CHANGE.
    const NAME: &'static str;

    /// zstd dictionary training config; `None` disables training and
    /// dictionary loading for this keyspace.
    const TRAIN: Option<TrainCfg> = None;

    /// per-keyspace tuning applied at open.
    fn options(cx: &OpenCx) -> KeyspaceCreateOptions;

    /// render a raw value for `/debug/*` endpoints.
    fn render_value(value: &[u8]) -> Value {
        Value::String(hex::encode(value))
    }

    /// parse a user-supplied `/debug/*` key string into key bytes.
    /// `None` means the string is not a valid key for this keyspace.
    fn parse_debug_key(key: &str) -> Option<Vec<u8>> {
        Some(key.as_bytes().to_vec())
    }

    /// render raw key bytes for `/debug/iter` output.
    fn render_debug_key(key: &[u8]) -> String {
        String::from_utf8_lossy(key).into_owned()
    }
}

/// typed handle over a [`Keyspace`], parameterized by its [`Schema`].
///
/// zero-cost: one `Keyspace` handle plus a marker. cloning is as cheap as
/// cloning the underlying handle.
pub struct Ks<S: Schema> {
    inner: Keyspace,
    _s: PhantomData<fn() -> S>,
}

impl<S: Schema> Clone for Ks<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _s: PhantomData,
        }
    }
}

impl<S: Schema> Ks<S> {
    /// open this keyspace with its schema tuning, recording the name for the
    /// open-time registry completeness check.
    pub(super) fn open(cx: &OpenCx) -> Result<Self> {
        let inner = cx.open_ks(S::NAME, S::options(cx))?;
        cx.record_opened(S::NAME);
        Ok(Self {
            inner,
            _s: PhantomData,
        })
    }

    /// an owned clone of the underlying keyspace handle.
    pub fn keyspace(&self) -> Keyspace {
        self.inner.clone()
    }

    /// the raw keyspace handle. escape hatch for migrations and debug paths.
    pub fn raw(&self) -> &Keyspace {
        &self.inner
    }

    /// point reads/writes mirror [`Keyspace`]'s signatures exactly, adding
    /// the fatal poison check every callsite previously had to remember.
    /// inherent methods win over the deref bridge, so all `db.<ks>.<op>()`
    /// callsites route through these.
    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> fjall::Result<Option<fjall::Slice>> {
        self.inner.get(key).inspect_err(super::check_poisoned)
    }

    #[inline]
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> fjall::Result<bool> {
        self.inner
            .contains_key(key)
            .inspect_err(super::check_poisoned)
    }

    #[inline]
    pub fn insert<K: Into<fjall::UserKey>, V: Into<fjall::UserValue>>(
        &self,
        key: K,
        value: V,
    ) -> fjall::Result<()> {
        self.inner
            .insert(key, value)
            .inspect_err(super::check_poisoned)
    }

    #[inline]
    pub fn remove<K: Into<fjall::UserKey>>(&self, key: K) -> fjall::Result<()> {
        self.inner.remove(key).inspect_err(super::check_poisoned)
    }
}

/// phase-1 migration bridge: existing callsites use raw `Keyspace` methods
/// directly. removed once callsites are ported to typed accessors.
impl<S: Schema> std::ops::Deref for Ks<S> {
    type Target = Keyspace;

    fn deref(&self) -> &Keyspace {
        &self.inner
    }
}

fn msgpack_or_hex<T: serde::de::DeserializeOwned + serde::Serialize>(value: &[u8]) -> Value {
    rmp_serde::from_slice::<T>(value)
        .ok()
        .and_then(|v| serde_json::to_value(v).ok())
        .unwrap_or_else(|| Value::String(hex::encode(value)))
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn u64_be_key_render(key: &[u8]) -> String {
    key.try_into()
        .map(|arr| u64::from_be_bytes(arr).to_string())
        .unwrap_or_else(|_| "invalid_u64".to_string())
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
fn u64_be_key_parse(key: &str) -> Option<Vec<u8>> {
    key.parse::<u64>().ok().map(|id| id.to_be_bytes().to_vec())
}

/// maps `{DID}` -> `RepoState` (msgpack)
pub struct Repos;

impl Schema for Repos {
    const NAME: &'static str = "repos";
    const TRAIN: Option<TrainCfg> = Some(TrainCfg {
        dict_size: kb(64),
        samples: 1000,
        sampler: Sampler::DedupKeys,
    });

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // crawler checks if a repo doesn't exist
            .expect_point_read_hits(false)
            .max_memtable_size(mb(cx.cfg.db_repos_memtable_size_mb))
            // did -> repo state, not gonna be compressable well because dids are random
            // and repo state doesnt have repeats really..
            // these block sizes work fine since we insert into repos constantly anyway
            // whenever we update anything related to a repo, so the repos that arent
            // being updated will be compacted away!
            .data_block_size_policy(BlockSizePolicy::new([kb(4), kb(4), kb(16), kb(64)]))
            .data_block_compression_policy(CompressionPolicy::new([
                CompressionType::None,
                CompressionType::None,
                (cx.compression)("repos", 3),
                (cx.compression)("repos", 5),
            ]))
            // did plc are random so the interval wont rlly matter
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([2, 4]))
    }

    fn render_value(value: &[u8]) -> Value {
        // RepoState borrows from the input, so it can't go through the
        // DeserializeOwned helper
        rmp_serde::from_slice::<crate::types::RepoState>(value)
            .ok()
            .and_then(|v| serde_json::to_value(&v).ok())
            .unwrap_or_else(|| Value::String(hex::encode(value)))
    }
}

/// maps `rm|{DID}` -> `RepoMetadata` (msgpack)
pub struct RepoMetadata;

impl Schema for RepoMetadata {
    const NAME: &'static str = "repo_metadata";

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            .expect_point_read_hits(true)
            .max_memtable_size(mb(cx.cfg.db_repos_memtable_size_mb / 2))
            // its did -> random u64 id + bool, not much to compress, very small
            .data_block_size_policy(BlockSizePolicy::new([kb(2), kb(4), kb(8)]))
            .data_block_compression_policy(CompressionPolicy::new([
                CompressionType::None,
                CompressionType::None,
                (cx.compression)("repos", 3),
            ]))
            // did plc are random so the interval wont rlly matter
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([2, 4]))
    }

    fn render_value(value: &[u8]) -> Value {
        msgpack_or_hex::<crate::types::RepoMetadata>(value)
    }
}

/// per-relay cursor keys -> u64/i64 BE bytes
pub struct Cursors;

impl Schema for Cursors {
    const NAME: &'static str = "cursors";

    fn options(_cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // cursor point reads hit almost 100% of the time
            .expect_point_read_hits(true)
            .max_memtable_size(mb(4))
            // its just cursors...
            .data_block_size_policy(BlockSizePolicy::all(kb(1)))
            .data_block_compression_policy(CompressionPolicy::disabled())
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(1))
    }

    fn render_value(value: &[u8]) -> Value {
        render_counter(value)
    }
}

/// maps `k|{NAME}` or `r|{DID}|{COL}` -> count (u64 BE), plus delta log
pub struct Counts;

impl Schema for Counts {
    const NAME: &'static str = "counts";

    fn options(_cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // count increments hit because counters are mostly pre-initialized
            .expect_point_read_hits(true)
            .max_memtable_size(mb(16))
            // the data is very small
            // this is at worst did|col -> u64, so its tiny (40 bytes)
            .data_block_size_policy(BlockSizePolicy::all(kb(2)))
            .data_block_compression_policy(CompressionPolicy::disabled())
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(7))
    }

    fn render_value(value: &[u8]) -> Value {
        render_counter(value)
    }
}

fn render_counter(value: &[u8]) -> Value {
    if let Ok(arr) = value.try_into() {
        return Value::Number(u64::from_be_bytes(arr).into());
    }
    std::str::from_utf8(value)
        .map(|s| Value::String(s.to_owned()))
        .unwrap_or_else(|_| Value::String(hex::encode(value)))
}

/// filter config: mode key, signal/collection/exclude sets, pds meta
pub struct Filter;

impl Schema for Filter {
    const NAME: &'static str = "filter";

    fn options(_cx: &OpenCx) -> KeyspaceCreateOptions {
        // filter handles high-volume point reads (repo excludes) so it needs the bloom filter
        KeyspaceCreateOptions::default()
            .max_memtable_size(mb(16))
            // dids arent compressable so this is fine, and we have nothing as the value
            .data_block_size_policy(BlockSizePolicy::all(kb(1)))
            .data_block_compression_policy(CompressionPolicy::disabled())
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(2))
    }
}

/// crawler + firehose source urls and retry entries
pub struct Crawler;

impl Schema for Crawler {
    const NAME: &'static str = "crawler";

    fn options(_cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // only iterators are used here
            .expect_point_read_hits(true)
            .max_memtable_size(mb(8))
            // did -> failed state, not very compressable
            .data_block_size_policy(BlockSizePolicy::all(kb(2)))
            .data_block_compression_policy(CompressionPolicy::disabled())
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(2))
    }
}

/// maps `{DID}|{COL}|{RKey}` -> record CID
#[cfg(feature = "indexer")]
pub struct Records;

#[cfg(feature = "indexer")]
impl Schema for Records {
    const NAME: &'static str = "records";

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // point reads might miss when using getRecord
            // but we assume thats not going to happen often...
            // since this keyspace is big, turning off bloom filters will help a lot with memory/disk space,
            // but leaves point reads vulnerable to disk I/O misses under public query load
            .expect_point_read_hits(!cx.cfg.db_records_bloom_filters)
            .max_memtable_size(mb(cx.cfg.db_records_memtable_size_mb))
            // its just did|col|rkey -> cid, very small (84 bytes for bsky post)
            .data_block_size_policy(BlockSizePolicy::new([kb(8), kb(16)]))
            // cids arent compressable, most rkeys are TIDs so they will get compressed
            // by prefix truncation anyway
            .data_block_compression_policy(CompressionPolicy::disabled())
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([16, 32]))
    }

    fn render_value(value: &[u8]) -> Value {
        use std::str::FromStr;
        std::str::from_utf8(value).map_or_else(
            |_| Value::String(hex::encode(value)),
            |s| {
                jacquard_common::types::cid::Cid::from_str(s)
                    .ok()
                    .and_then(|cid| serde_json::to_value(cid).ok())
                    .unwrap_or_else(|| Value::String(s.to_owned()))
            },
        )
    }
}

/// content-addressable storage of raw DAG-CBOR blocks, keyed `{COL}|{CID bytes}`
#[cfg(feature = "indexer")]
pub struct Blocks;

#[cfg(feature = "indexer")]
impl Schema for Blocks {
    const NAME: &'static str = "blocks";
    const TRAIN: Option<TrainCfg> = Some(TrainCfg {
        dict_size: kb(128),
        samples: 5000,
        sampler: Sampler::PerCollectionCap(200),
    });

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        // this is used in non-ephemeral mode
        KeyspaceCreateOptions::default()
            // point reads are used a lot by stream, we know the blocks exist though
            .expect_point_read_hits(true)
            .max_memtable_size(mb(cx.cfg.db_blocks_memtable_size_mb))
            // 16 - 128 kb, as the newer blocks will be in the first level (or memtable)
            // and any consumers will probably be streaming the newer events...
            // and blocks are pretty big-ish like around 5kb...
            // replaying will hit later levels so it will be slower but thats honestly
            // an acceptable tradeoff to save more space...
            // todo: we can probably decrease these when we get zstd dict compression?
            .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(64), kb(128)]))
            // lets not compress first level so the reads for new blocks are faster
            // since we will be streaming them to consumers
            .data_block_compression_policy(CompressionPolicy::new([
                CompressionType::None,
                (cx.compression)("blocks", 3),
                (cx.compression)("blocks", 3),
                (cx.compression)("blocks", 5),
            ]))
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([8, 16, 32]))
    }

    fn render_value(value: &[u8]) -> Value {
        serde_ipld_dagcbor::from_slice::<Value>(value)
            .unwrap_or_else(|_| Value::String(hex::encode(value)))
    }

    fn render_debug_key(key: &[u8]) -> String {
        // key is col|cid_bytes, show as "col|<cid_str>"
        let Some(sep) = key.iter().position(|&b| b == super::keys::SEP) else {
            return String::from_utf8_lossy(key).into_owned();
        };
        let col = String::from_utf8_lossy(&key[..sep]);
        match cid::Cid::read_bytes(&key[sep + 1..]) {
            Ok(cid) => format!("{col}|{cid}"),
            Err(_) => String::from_utf8_lossy(key).into_owned(),
        }
    }
}

/// backfill queue of `{ID}` (u64 BE) -> empty
#[cfg(feature = "indexer")]
pub struct Pending;

#[cfg(feature = "indexer")]
impl Schema for Pending {
    const NAME: &'static str = "pending";

    fn options(_cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // iterated over as a queue, no point reads are used so bloom filters are disabled
            .expect_point_read_hits(true)
            .max_memtable_size(mb(8))
            // its just index of id (int) -> did, and dids arent compressable (especially with the ids being random)
            .data_block_size_policy(BlockSizePolicy::all(kb(8)))
            // and we'll transition from pending to synced anyway, no point trying to compress
            .data_block_compression_policy(CompressionPolicy::disabled())
            // ids are sequential and share prefix so we can use large interval to save space
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(64))
    }

    fn render_value(_value: &[u8]) -> Value {
        Value::Null
    }
}

/// per-repo resync/retry state, `{DID}` -> `ResyncState` (msgpack)
#[cfg(feature = "indexer")]
pub struct Resync;

#[cfg(feature = "indexer")]
impl Schema for Resync {
    const NAME: &'static str = "resync";

    fn options(_cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // we only point read in backfill when we check for existing resync state
            // ...and also in repos api. so we can disable bloom filters
            .expect_point_read_hits(true)
            .max_memtable_size(mb(8))
            // did -> error state, so its gonna be basically random, cant compress well
            .data_block_size_policy(BlockSizePolicy::all(kb(4)))
            // and we arent going to have many of these anyway, no point trying
            .data_block_compression_policy(CompressionPolicy::disabled())
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(4))
    }

    fn render_value(value: &[u8]) -> Value {
        msgpack_or_hex::<crate::types::ResyncState>(value)
    }
}

/// live events buffered during backfill, `{DID}|{Rev}` -> commit (msgpack)
#[cfg(feature = "indexer")]
pub struct ResyncBuffer;

#[cfg(feature = "indexer")]
impl Schema for ResyncBuffer {
    const NAME: &'static str = "resync_buffer";

    fn options(_cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // iterated during backfill, no point reads
            .expect_point_read_hits(true)
            .max_memtable_size(mb(16))
            .data_block_size_policy(BlockSizePolicy::all(kb(32)))
            // dont have to compress here since resync buffer will be emptied at some point anyway
            .data_block_compression_policy(CompressionPolicy::disabled())
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(16))
    }
}

/// maps `{ID}` (u64 BE) -> `StoredEvent`, the source for the json stream api
#[cfg(feature = "indexer_stream")]
pub struct Events;

#[cfg(feature = "indexer_stream")]
impl Schema for Events {
    const NAME: &'static str = "events";
    const TRAIN: Option<TrainCfg> = Some(TrainCfg {
        dict_size: kb(64),
        samples: 1000,
        sampler: Sampler::DedupKeys,
    });

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // only iterators are used here, no point reads
            .expect_point_read_hits(true)
            .max_memtable_size(mb(cx.cfg.db_events_memtable_size_mb))
            // the compression here wont be quite as good since events are quite random
            // eg. by many different repos and different records etc.
            // since its sequential we should still go with bigger block size though
            // backfills will be sequential though...
            .data_block_size_policy(
                cx.cfg
                    .ephemeral
                    .then(|| BlockSizePolicy::new([kb(64), kb(128), kb(256)]))
                    .unwrap_or_else(|| BlockSizePolicy::new([kb(128), kb(256)])),
            )
            // monotonic {ID} keys are trivial-moved down (never rewritten), so the L0
            // policy is permanent; the live tail is served uncompressed from the memtable
            .data_block_compression_policy(
                cx.cfg
                    .ephemeral
                    .then(|| {
                        CompressionPolicy::new([
                            CompressionType::None,
                            (cx.compression)("events", 3),
                        ])
                    })
                    .unwrap_or_else(|| {
                        CompressionPolicy::new([
                            (cx.compression)("events", 3),
                            (cx.compression)("events", 3),
                            (cx.compression)("events", 3),
                            (cx.compression)("events", 5),
                        ])
                    }),
            )
            // ids are int, we can prefix truncate a lot
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128]))
    }

    fn render_value(value: &[u8]) -> Value {
        // StoredEvent borrows from the input, so it can't go through the
        // DeserializeOwned helper
        rmp_serde::from_slice::<crate::types::StoredEvent>(value)
            .ok()
            .and_then(|v| serde_json::to_value(&v).ok())
            .unwrap_or_else(|| Value::String(hex::encode(value)))
    }

    fn parse_debug_key(key: &str) -> Option<Vec<u8>> {
        u64_be_key_parse(key)
    }

    fn render_debug_key(key: &[u8]) -> String {
        u64_be_key_render(key)
    }
}

/// maps `{time_us}|{ID}` (16 bytes) -> jetstream event data
#[cfg(feature = "jetstream")]
pub struct JetstreamEvents;

#[cfg(feature = "jetstream")]
impl Schema for JetstreamEvents {
    const NAME: &'static str = "jetstream_events";
    const TRAIN: Option<TrainCfg> = Some(TrainCfg {
        dict_size: kb(64),
        samples: 1000,
        sampler: Sampler::DedupKeys,
    });

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // time-ordered append-only stream metadata, only iterated for replay.
            .expect_point_read_hits(true)
            .max_memtable_size(mb(cx.cfg.db_events_memtable_size_mb))
            .data_block_size_policy(BlockSizePolicy::new([kb(128), kb(256)]))
            .data_block_compression_policy(CompressionPolicy::new([
                (cx.compression)("jetstream_events", 3),
                (cx.compression)("jetstream_events", 3),
                (cx.compression)("jetstream_events", 5),
            ]))
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128]))
    }

    fn render_value(value: &[u8]) -> Value {
        // StoredJetstreamEvent borrows from the input, so it can't go through
        // the DeserializeOwned helper
        rmp_serde::from_slice::<crate::types::StoredJetstreamEvent>(value)
            .ok()
            .and_then(|v| serde_json::to_value(&v).ok())
            .unwrap_or_else(|| Value::String(hex::encode(value)))
    }
}

/// maps `{SEQ}` (u64 BE) -> re-encoded relay frame
#[cfg(feature = "relay")]
pub struct RelayEvents;

#[cfg(feature = "relay")]
impl Schema for RelayEvents {
    const NAME: &'static str = "relay_events";

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // only iterated for cursor replay
            .expect_point_read_hits(true)
            .max_memtable_size(mb(cx.cfg.db_events_memtable_size_mb))
            .data_block_size_policy(BlockSizePolicy::new([kb(128), kb(256)]))
            // monotonic {SEQ} keys are trivial-moved down (never rewritten), so L0
            // compression is permanent; the live tail is served from the memtable
            .data_block_compression_policy(CompressionPolicy::new([
                (cx.compression)("relay_events", 3),
                (cx.compression)("relay_events", 3),
                (cx.compression)("relay_events", 3),
                (cx.compression)("relay_events", 5),
            ]))
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([64, 128]))
    }

    fn parse_debug_key(key: &str) -> Option<Vec<u8>> {
        u64_be_key_parse(key)
    }

    fn render_debug_key(key: &[u8]) -> String {
        u64_be_key_render(key)
    }
}

/// reverse index of record references
#[cfg(feature = "backlinks")]
pub struct Backlinks;

#[cfg(feature = "backlinks")]
impl Schema for Backlinks {
    const NAME: &'static str = "backlinks";
    const TRAIN: Option<TrainCfg> = Some(TrainCfg {
        dict_size: kb(64),
        samples: 1000,
        sampler: Sampler::DedupKeys,
    });

    fn options(cx: &OpenCx) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default()
            // lets assume we hit backlinks, getBacklinks will use iterator anyway
            // so we can disable bloom filter okay
            .expect_point_read_hits(true)
            .max_memtable_size(mb(cx.cfg.db_records_memtable_size_mb))
            // same as records basically
            .data_block_size_policy(BlockSizePolicy::new([kb(16), kb(32)]))
            .data_block_compression_policy(CompressionPolicy::new([
                CompressionType::None,
                (cx.compression)("backlinks", 3),
            ]))
            .data_block_restart_interval_policy(RestartIntervalPolicy::new([16, 32]))
    }
}
