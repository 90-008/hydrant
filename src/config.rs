use smol_str::SmolStr;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

use crate::pds_meta::TierPolicy;

pub mod env;
pub mod types;

pub use types::{
    BackfillStrategy, Compression, CrawlerMode, CrawlerSource, FirehoseSource, RateTier,
    SignatureVerification,
};

#[derive(Debug, Clone)]
pub struct Config {
    /// path to the database folder. set via `HYDRANT_DATABASE_PATH`.
    pub database_path: PathBuf,
    /// if `true`, discovers and indexes all repositories in the network.
    /// set via `HYDRANT_FULL_NETWORK`.
    pub full_network: bool,
    /// if `true`, no records are stored; events are deleted after `ephemeral_ttl`.
    /// set via `HYDRANT_EPHEMERAL`.
    pub ephemeral: bool,
    /// how long events are retained in ephemeral mode before deletion.
    /// set via `HYDRANT_EPHEMERAL_TTL` (humantime duration, e.g. `60min`).
    pub ephemeral_ttl: Duration,

    /// firehose sources for ingestion. set via `HYDRANT_RELAY_HOST` (single)
    /// or `HYDRANT_RELAY_HOSTS` (comma-separated; takes precedence).
    /// prefix a URL with `pds::` to mark it as a direct PDS connection.
    pub relays: Vec<FirehoseSource>,
    /// base URL(s) of the PLC directory (comma-separated for multiple).
    /// defaults to `https://plc.wtf`, or `https://plc.directory` in full-network mode.
    /// set via `HYDRANT_PLC_URL`.
    pub plc_urls: Vec<Url>,
    /// whether to ingest events from relay firehose subscriptions.
    /// set via `HYDRANT_ENABLE_FIREHOSE`.
    pub enable_firehose: bool,
    /// whether to process queued repository backfills.
    /// set via `HYDRANT_ENABLE_BACKFILL`.
    pub enable_backfill: bool,
    /// number of concurrent workers processing firehose events.
    /// set via `HYDRANT_FIREHOSE_WORKERS`.
    pub firehose_workers: usize,
    /// number of consecutive firehose connection failures before a PDS is marked offline.
    /// set via `HYDRANT_FIREHOSE_MAX_FAILURES`.
    pub firehose_max_failures: usize,
    /// how often the firehose cursor is persisted to disk.
    /// set via `HYDRANT_CURSOR_SAVE_INTERVAL` (humantime duration, e.g. `3sec`).
    pub cursor_save_interval: Duration,
    /// timeout for fetching a full repository CAR during backfill.
    /// set via `HYDRANT_REPO_FETCH_TIMEOUT` (humantime duration, e.g. `5min`).
    pub repo_fetch_timeout: Duration,
    /// maximum number of concurrent backfill tasks.
    /// set via `HYDRANT_BACKFILL_CONCURRENCY_LIMIT`.
    pub backfill_concurrency_limit: usize,
    /// backfill strategy. `full` preserves existing full-repo backfill behavior.
    /// `sparse-filter` attempts authenticated sparse collection backfill first and falls back
    /// to full repo backfill. `auto` probes filtered repos and falls back to full repo backfill
    /// for tiny MST roots.
    /// set via `HYDRANT_BACKFILL_STRATEGY`.
    pub backfill_strategy: BackfillStrategy,

    /// whether to run the network crawler. `None` defers to the default for the current mode.
    /// set via `HYDRANT_ENABLE_CRAWLER`.
    pub enable_crawler: Option<bool>,
    /// maximum number of repos allowed in the backfill pending queue before the crawler pauses.
    /// set via `HYDRANT_CRAWLER_MAX_PENDING_REPOS`.
    pub crawler_max_pending_repos: usize,
    /// pending queue size at which the crawler resumes after being paused.
    /// set via `HYDRANT_CRAWLER_RESUME_PENDING_REPOS`.
    pub crawler_resume_pending_repos: usize,
    /// crawler sources: each entry pairs a URL with a discovery mode.
    ///
    /// set via `HYDRANT_CRAWLER_URLS` as a comma-separated list of `[mode::]url` entries,
    /// e.g. `relay::wss://bsky.network,by_collection::https://lightrail.microcosm.blue`.
    /// a bare URL without a `mode::` prefix uses the default mode (`relay` for full-network,
    /// `by_collection` otherwise). defaults to the relay hosts with the default mode.
    /// set to an empty string to disable crawling entirely.
    pub crawler_sources: Vec<CrawlerSource>,

    /// signature verification level for incoming commits.
    /// set via `HYDRANT_VERIFY_SIGNATURES` (`full`, `backfill-only`, or `none`).
    pub verify_signatures: SignatureVerification,
    /// number of resolved identities to keep in the in-memory LRU cache.
    /// set via `HYDRANT_IDENTITY_CACHE_SIZE`.
    pub identity_cache_size: u64,
    /// enable MST inversion validation on incoming commits (expensive).
    /// set via `HYDRANT_VERIFY_MST`.
    pub verify_mst: bool,
    /// clock drift window for future-rev rejection, in seconds.
    /// commits with a rev timestamp more than this many seconds in the future are rejected.
    /// set via `HYDRANT_REV_CLOCK_SKEW`. default: 300 (5 minutes).
    pub rev_clock_skew_secs: i64,

    /// NSID patterns that trigger auto-discovery in filter mode (e.g. `app.bsky.feed.post`).
    /// set via `HYDRANT_FILTER_SIGNALS` as a comma-separated list.
    pub filter_signals: Option<Vec<String>>,
    /// NSID patterns used to filter which record collections are stored.
    /// if `None`, all collections are stored. set via `HYDRANT_FILTER_COLLECTIONS`.
    pub filter_collections: Option<Vec<String>>,
    /// DIDs that are always skipped, regardless of mode.
    /// set via `HYDRANT_FILTER_EXCLUDES` as a comma-separated list.
    pub filter_excludes: Option<Vec<String>>,

    /// enable backlinks indexing (only meaningful in non-ephemeral mode).
    /// set via `HYDRANT_ENABLE_BACKLINKS=true`.
    pub enable_backlinks: bool,

    /// if `true`, record blocks are not stored; only the index (records, counts, events) is kept.
    /// `getRecord`, `listRecords`, and `getRepo` will return errors when this is enabled.
    /// event stream still functions but create/update events will not include record values.
    /// only valid in indexer mode (not relay).
    /// set via `HYDRANT_ONLY_INDEX_LINKS=true`.
    pub only_index_links: bool,

    /// maximum number of new PDS sources that may be added (via seeding or API) in a single
    /// UTC calendar day. `None` means unlimited.
    /// set via `HYDRANT_NEW_HOST_LIMIT`.
    pub new_host_limit: Option<u64>,

    /// how often offline firehose sources are automatically retried.
    /// set via `HYDRANT_OFFLINE_HOST_RETRY_INTERVAL` (humantime duration, e.g. `30min`).
    /// set to `none` to disable automatic retries.
    pub offline_host_retry_interval: Option<Duration>,

    /// base URL(s) of relay or aggregator services to seed firehose PDS sources from at startup.
    ///
    /// hydrant calls `com.atproto.sync.listHosts` on each URL and adds the returned PDSes
    /// as firehose sources (with `is_pds = true`). account counts from the response are
    /// applied to newly-seen hosts to initialise rate-limiting immediately.
    ///
    /// set via `HYDRANT_SEED_HOSTS` as a comma-separated list of base URLs.
    pub seed_hosts: Vec<Url>,
    /// named rate tier definitions for PDS rate limiting.
    ///
    /// built-in tiers ("default" and "trusted") are always present and may be overridden.
    /// set via `HYDRANT_RATE_TIERS` as a comma-separated list of `name:base/mul/hourly/daily` entries,
    /// e.g. `trusted:5000/10.0/18000000/432000000,custom:100/1.0/7200000/172800000`.
    ///
    /// built from `HYDRANT_TIER_RULES` and `HYDRANT_RATE_TIERS` at startup.
    pub tier_policy: TierPolicy,

    /// glob rules mapping host patterns to named rate tiers.
    ///
    /// set via `HYDRANT_TIER_RULES` as a comma-separated list of `pattern:tiername` entries,
    /// e.g. `*.bsky.network:trusted,pds.example.com:custom`. rules are evaluated in order;
    /// api-assigned per-host overrides always take priority over these rules.
    pub tier_rules: Vec<(String, String)>,

    /// db internals, tune only if you know what you're doing.
    ///
    /// size of the fjall block cache in MB. set via `HYDRANT_CACHE_SIZE`.
    pub cache_size: u64,
    /// db internals, tune only if you know what you're doing.
    ///
    /// compression algorithm for data keyspaces (blocks, records, repos, events).
    /// set via `HYDRANT_DATA_COMPRESSION` (`lz4`, `zstd`, or `none`).
    pub data_compression: Compression,
    /// db internals, tune only if you know what you're doing.
    ///
    /// compression algorithm for the fjall journal.
    /// set via `HYDRANT_JOURNAL_COMPRESSION` (`lz4`, `zstd`, or `none`).
    pub journal_compression: Compression,
    /// db internals, tune only if you know what you're doing.
    ///
    /// number of background threads used by the fjall storage engine.
    /// set via `HYDRANT_DB_WORKER_THREADS`.
    pub db_worker_threads: usize,
    /// db internals, tune only if you know what you're doing.
    ///
    /// maximum total size of the fjall journal in MB before a flush is forced.
    /// set via `HYDRANT_DB_MAX_JOURNALING_SIZE_MB`.
    pub db_max_journaling_size_mb: u64,
    /// db internals, tune only if you know what you're doing.
    ///
    /// in-memory write buffer (memtable) size for the blocks keyspace in MB.
    /// set via `HYDRANT_DB_BLOCKS_MEMTABLE_SIZE_MB`.
    pub db_blocks_memtable_size_mb: u64,
    /// db internals, tune only if you know what you're doing.
    ///
    /// in-memory write buffer (memtable) size for the repos keyspace in MB.
    /// set via `HYDRANT_DB_REPOS_MEMTABLE_SIZE_MB`.
    pub db_repos_memtable_size_mb: u64,
    /// db internals, tune only if you know what you're doing.
    ///
    /// in-memory write buffer (memtable) size for the events keyspace in MB.
    /// set via `HYDRANT_DB_EVENTS_MEMTABLE_SIZE_MB`.
    pub db_events_memtable_size_mb: u64,
    /// db internals, tune only if you know what you're doing.
    ///
    /// in-memory write buffer (memtable) size for the records keyspace in MB.
    /// set via `HYDRANT_DB_RECORDS_MEMTABLE_SIZE_MB`.
    pub db_records_memtable_size_mb: u64,

    /// replay batch size.
    ///
    /// `0` (the default) means auto: use about half the subscriber channel capacity
    /// (`STREAM_CHANNEL_CAPACITY / 2`), capped to the channel's current available
    /// capacity. this prevents DB reads when the output buffer is already saturated.
    ///
    /// set to a non-zero value to fix the batch size manually.
    ///
    /// set via `HYDRANT_STREAM_REPLAY_CHUNK_SIZE`.
    pub stream_replay_chunk_size: usize,
    /// optional pause between replay batches.
    ///
    /// normally zero. slow consumers are handled by bounded-channel backpressure
    /// and `HYDRANT_STREAM_SEND_TIMEOUT`. only set this as an emergency knob if
    /// you need to artificially throttle replay throughput.
    ///
    /// set via `HYDRANT_STREAM_REPLAY_CHUNK_PAUSE` (humantime duration, e.g. `2ms`).
    pub stream_replay_chunk_pause: Duration,
    /// maximum number of live in-memory stream events buffered per subscriber while it catches up.
    /// set via `HYDRANT_STREAM_PENDING_EVENT_LIMIT`.
    pub stream_pending_event_limit: usize,
    /// maximum time a subscriber may block stream delivery before being disconnected.
    /// set via `HYDRANT_STREAM_SEND_TIMEOUT` (humantime duration, e.g. `30sec`).
    pub stream_send_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        const BASE_MEMTABLE_MB: u64 = 32;
        Self {
            database_path: PathBuf::from("./hydrant.db"),
            #[cfg(feature = "indexer")]
            ephemeral: false,
            #[cfg(feature = "relay")]
            ephemeral: true,
            #[cfg(feature = "indexer")]
            ephemeral_ttl: Duration::from_secs(3600), // 1 hour
            #[cfg(feature = "relay")]
            ephemeral_ttl: Duration::from_secs(3600 * 24 * 3), // 3 days
            #[cfg(not(feature = "relay"))]
            full_network: false,
            #[cfg(feature = "relay")]
            full_network: true,
            #[cfg(not(feature = "relay"))]
            relays: vec![FirehoseSource {
                url: Url::parse("wss://relay.fire.hose.cam/").unwrap(),
                is_pds: false,
            }],
            #[cfg(feature = "relay")]
            relays: vec![],
            #[cfg(not(feature = "relay"))]
            seed_hosts: vec![],
            #[cfg(feature = "relay")]
            seed_hosts: vec![Url::parse("https://bsky.network").unwrap()],
            plc_urls: vec![Url::parse("https://plc.wtf").unwrap()],
            enable_firehose: true,
            enable_backfill: true,
            firehose_workers: 8,
            firehose_max_failures: 15,
            cursor_save_interval: Duration::from_secs(3),
            repo_fetch_timeout: Duration::from_secs(300),
            backfill_concurrency_limit: 16,
            backfill_strategy: BackfillStrategy::Auto,
            enable_crawler: None,
            crawler_max_pending_repos: 2000,
            crawler_resume_pending_repos: 1000,
            crawler_sources: vec![CrawlerSource {
                url: Url::parse("https://lightrail.microcosm.blue").unwrap(),
                mode: CrawlerMode::ByCollection,
            }],
            verify_signatures: SignatureVerification::Full,
            identity_cache_size: 1_000_000,
            verify_mst: false,
            rev_clock_skew_secs: 300,
            filter_signals: None,
            filter_collections: None,
            filter_excludes: None,
            enable_backlinks: false,
            only_index_links: false,
            new_host_limit: Some(50),
            offline_host_retry_interval: Some(Duration::from_secs(30 * 60)),
            tier_rules: vec![],
            tier_policy: {
                let mut tiers = HashMap::new();
                tiers.insert(SmolStr::new("default"), RateTier::default_tier());
                tiers.insert(SmolStr::new("trusted"), RateTier::trusted());
                TierPolicy {
                    tiers,
                    rules: vec![],
                }
            },
            cache_size: 256,
            data_compression: Compression::Zstd,
            journal_compression: Compression::Lz4,
            db_worker_threads: 4,
            db_max_journaling_size_mb: 400,
            db_blocks_memtable_size_mb: BASE_MEMTABLE_MB,
            db_repos_memtable_size_mb: BASE_MEMTABLE_MB / 2,
            db_events_memtable_size_mb: BASE_MEMTABLE_MB,
            db_records_memtable_size_mb: BASE_MEMTABLE_MB / 3 * 2,
            stream_replay_chunk_size: 0,
            stream_replay_chunk_pause: Duration::ZERO,
            stream_pending_event_limit: 4096,
            stream_send_timeout: Duration::from_secs(30),
        }
    }
}

impl Config {
    /// returns the default config for full network usage.
    pub fn full_network() -> Self {
        const BASE_MEMTABLE_MB: u64 = 192;
        Self {
            full_network: true,
            plc_urls: vec![Url::parse("https://plc.directory").unwrap()],
            firehose_workers: 24,
            backfill_concurrency_limit: 64,
            crawler_sources: vec![CrawlerSource {
                url: Url::parse("wss://relay.fire.hose.cam/").unwrap(),
                mode: CrawlerMode::ListRepos,
            }],
            db_worker_threads: 8,
            db_max_journaling_size_mb: 1024,
            db_blocks_memtable_size_mb: BASE_MEMTABLE_MB,
            db_repos_memtable_size_mb: BASE_MEMTABLE_MB / 4,
            db_events_memtable_size_mb: BASE_MEMTABLE_MB,
            db_records_memtable_size_mb: BASE_MEMTABLE_MB / 3 * 2,
            ..Self::default()
        }
    }
}

macro_rules! config_line {
    ($f:expr, $label:expr, $value:expr) => {
        writeln!($f, "  {:<width$}{}", $label, $value, width = LABEL_WIDTH)
    };
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const LABEL_WIDTH: usize = 27;

        writeln!(f, "hydrant configuration:")?;
        config_line!(
            f,
            "relay hosts",
            format_args!(
                "{:?}",
                self.relays
                    .iter()
                    .map(|s| if s.is_pds {
                        format!("pds::{}", s.url)
                    } else {
                        s.url.to_string()
                    })
                    .collect::<Vec<_>>()
            )
        )?;
        config_line!(f, "plc urls", format_args!("{:?}", self.plc_urls))?;
        config_line!(f, "full network indexing", self.full_network)?;
        config_line!(f, "verify signatures", self.verify_signatures)?;
        config_line!(f, "backfill enabled", self.enable_backfill)?;
        config_line!(f, "backfill concurrency", self.backfill_concurrency_limit)?;
        config_line!(f, "backfill strategy", self.backfill_strategy)?;
        config_line!(f, "identity cache size", self.identity_cache_size)?;
        config_line!(
            f,
            "cursor save interval",
            format_args!("{}sec", self.cursor_save_interval.as_secs())
        )?;
        config_line!(
            f,
            "repo fetch timeout",
            format_args!("{}sec", self.repo_fetch_timeout.as_secs())
        )?;
        config_line!(f, "ephemeral", self.ephemeral)?;
        config_line!(f, "database path", self.database_path.to_string_lossy())?;
        config_line!(f, "cache size", format_args!("{} mb", self.cache_size))?;
        config_line!(f, "data compression", self.data_compression)?;
        config_line!(f, "journal compression", self.journal_compression)?;
        config_line!(f, "firehose workers", self.firehose_workers)?;
        config_line!(f, "firehose max failures", self.firehose_max_failures)?;
        config_line!(f, "db worker threads", self.db_worker_threads)?;
        config_line!(
            f,
            "db journal size",
            format_args!("{} mb", self.db_max_journaling_size_mb)
        )?;
        config_line!(
            f,
            "db blocks memtable",
            format_args!("{} mb", self.db_blocks_memtable_size_mb)
        )?;
        config_line!(
            f,
            "db repos memtable",
            format_args!("{} mb", self.db_repos_memtable_size_mb)
        )?;
        config_line!(
            f,
            "db events memtable",
            format_args!("{} mb", self.db_events_memtable_size_mb)
        )?;
        config_line!(
            f,
            "db records memtable",
            format_args!("{} mb", self.db_records_memtable_size_mb)
        )?;
        let replay_chunk = if self.stream_replay_chunk_size == 0 {
            "auto".to_owned()
        } else {
            self.stream_replay_chunk_size.to_string()
        };
        config_line!(f, "stream replay chunk", replay_chunk)?;
        config_line!(
            f,
            "stream replay pause",
            format_args!("{}ms", self.stream_replay_chunk_pause.as_millis())
        )?;
        config_line!(f, "stream pending limit", self.stream_pending_event_limit)?;
        config_line!(
            f,
            "stream send timeout",
            format_args!("{}sec", self.stream_send_timeout.as_secs())
        )?;
        config_line!(f, "crawler max pending", self.crawler_max_pending_repos)?;
        config_line!(
            f,
            "crawler resume pending",
            self.crawler_resume_pending_repos
        )?;
        if !self.crawler_sources.is_empty() {
            let sources: Vec<_> = self
                .crawler_sources
                .iter()
                .map(|s| format!("{}::{}", s.mode, s.url))
                .collect();
            config_line!(f, "crawler sources", sources.join(", "))?;
        }
        if let Some(signals) = &self.filter_signals {
            config_line!(f, "filter signals", format_args!("{:?}", signals))?;
        }
        if let Some(collections) = &self.filter_collections {
            config_line!(f, "filter collections", format_args!("{:?}", collections))?;
        }
        if let Some(excludes) = &self.filter_excludes {
            config_line!(f, "filter excludes", format_args!("{:?}", excludes))?;
        }
        if self.enable_backlinks {
            config_line!(f, "backlinks", "enabled")?;
        }
        if self.only_index_links {
            config_line!(f, "only index links", "true")?;
        }
        if !self.seed_hosts.is_empty() {
            config_line!(
                f,
                "seed hosts",
                format_args!(
                    "{:?}",
                    self.seed_hosts
                        .iter()
                        .map(|u| u.as_str())
                        .collect::<Vec<_>>()
                )
            )?;
        }
        if let Some(limit) = self.new_host_limit {
            config_line!(f, "max pds/day", limit)?;
        }
        match self.offline_host_retry_interval {
            Some(d) => config_line!(
                f,
                "offline retry interval",
                format_args!("{}sec", d.as_secs())
            )?,
            None => config_line!(f, "offline retry interval", "disabled")?,
        }
        Ok(())
    }
}
