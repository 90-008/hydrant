use miette::Result;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

/// loads `.env` from the current directory, setting any variables not already in the environment.
fn load_dotenv() {
    let Ok(contents) = std::fs::read_to_string(".env") else {
        return;
    };
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let val = val.trim();
        let val = val
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .or_else(|| val.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(val);
        if std::env::var(key).is_err() {
            // SAFETY: single-threaded at startup; no other threads are reading env yet.
            unsafe { std::env::set_var(key, val) };
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrawlerMode {
    /// enumerate via `com.atproto.sync.listRepos`, then check signals with `describeRepo`.
    Relay,
    /// enumerate via `com.atproto.sync.listReposByCollection` for each configured signal.
    ByCollection,
}

impl CrawlerMode {
    fn default_for(full_network: bool) -> Self {
        if full_network {
            Self::Relay
        } else {
            Self::ByCollection
        }
    }
}

impl FromStr for CrawlerMode {
    type Err = miette::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "relay" => Ok(Self::Relay),
            "by_collection" | "by-collection" => Ok(Self::ByCollection),
            _ => Err(miette::miette!(
                "invalid crawler mode: expected 'relay' or 'by_collection'"
            )),
        }
    }
}

impl fmt::Display for CrawlerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Relay => write!(f, "relay"),
            Self::ByCollection => write!(f, "by_collection"),
        }
    }
}

/// a single crawler source: a URL and the mode used to enumerate it.
#[derive(Debug, Clone)]
pub struct CrawlerSource {
    pub url: Url,
    pub mode: CrawlerMode,
}

impl CrawlerSource {
    /// parse `[mode::]url` — mode prefix is optional, falls back to `default_mode`.
    fn parse(s: &str, default_mode: CrawlerMode) -> Option<Self> {
        if let Some((prefix, rest)) = s.split_once("::") {
            let mode = prefix.parse().ok()?;
            let url = Url::parse(rest).ok()?;
            Some(Self { url, mode })
        } else {
            let url = Url::parse(s).ok()?;
            Some(Self {
                url,
                mode: default_mode,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Lz4,
    Zstd,
    None,
}

impl FromStr for Compression {
    type Err = miette::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            "none" => Ok(Self::None),
            _ => Err(miette::miette!("invalid compression type")),
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
            Self::None => write!(f, "none"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SignatureVerification {
    /// verify all commits, from the firehose and when backfilling a repo from a PDS.
    Full,
    /// only verify commits when backfilling a repo from a PDS.
    BackfillOnly,
    /// don't verify anything.
    None,
}

impl FromStr for SignatureVerification {
    type Err = miette::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "full" => Ok(Self::Full),
            "backfill-only" => Ok(Self::BackfillOnly),
            "none" => Ok(Self::None),
            _ => Err(miette::miette!("invalid signature verification level")),
        }
    }
}

impl fmt::Display for SignatureVerification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::BackfillOnly => write!(f, "backfill-only"),
            Self::None => write!(f, "none"),
        }
    }
}

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

    /// relay URLs used for firehose ingestion. set via `HYDRANT_RELAY_HOST` (single)
    /// or `HYDRANT_RELAY_HOSTS` (comma-separated; takes precedence).
    pub relays: Vec<Url>,
    /// base URL(s) of the PLC directory (comma-separated for multiple).
    /// defaults to `https://plc.wtf`, or `https://plc.directory` in full-network mode.
    /// set via `HYDRANT_PLC_URL`.
    pub plc_urls: Vec<Url>,
    /// whether to ingest events from relay firehose subscriptions.
    /// set via `HYDRANT_ENABLE_FIREHOSE`.
    pub enable_firehose: bool,
    /// number of concurrent workers processing firehose events.
    /// set via `HYDRANT_FIREHOSE_WORKERS`.
    pub firehose_workers: usize,
    /// how often the firehose cursor is persisted to disk.
    /// set via `HYDRANT_CURSOR_SAVE_INTERVAL` (humantime duration, e.g. `3sec`).
    pub cursor_save_interval: Duration,
    /// timeout for fetching a full repository CAR during backfill.
    /// set via `HYDRANT_REPO_FETCH_TIMEOUT` (humantime duration, e.g. `5min`).
    pub repo_fetch_timeout: Duration,
    /// maximum number of concurrent backfill tasks.
    /// set via `HYDRANT_BACKFILL_CONCURRENCY_LIMIT`.
    pub backfill_concurrency_limit: usize,

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
}

impl Default for Config {
    fn default() -> Self {
        const BASE_MEMTABLE_MB: u64 = 32;
        Self {
            database_path: PathBuf::from("./hydrant.db"),
            full_network: false,
            ephemeral: false,
            ephemeral_ttl: Duration::from_secs(3600),
            relays: vec![Url::parse("wss://relay.fire.hose.cam/").unwrap()],
            plc_urls: vec![Url::parse("https://plc.wtf").unwrap()],
            enable_firehose: true,
            firehose_workers: 8,
            cursor_save_interval: Duration::from_secs(3),
            repo_fetch_timeout: Duration::from_secs(300),
            backfill_concurrency_limit: 16,
            enable_crawler: None,
            crawler_max_pending_repos: 2000,
            crawler_resume_pending_repos: 1000,
            crawler_sources: vec![CrawlerSource {
                url: Url::parse("https://lightrail.microcosm.blue").unwrap(),
                mode: CrawlerMode::ByCollection,
            }],
            verify_signatures: SignatureVerification::Full,
            identity_cache_size: 1_000_000,
            filter_signals: None,
            filter_collections: None,
            filter_excludes: None,
            enable_backlinks: false,
            cache_size: 256,
            data_compression: Compression::Lz4,
            journal_compression: Compression::Lz4,
            db_worker_threads: 4,
            db_max_journaling_size_mb: 400,
            db_blocks_memtable_size_mb: BASE_MEMTABLE_MB,
            db_repos_memtable_size_mb: BASE_MEMTABLE_MB / 2,
            db_events_memtable_size_mb: BASE_MEMTABLE_MB,
            db_records_memtable_size_mb: BASE_MEMTABLE_MB / 3 * 2,
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
                mode: CrawlerMode::Relay,
            }],
            db_worker_threads: 8,
            db_max_journaling_size_mb: 1024,
            db_blocks_memtable_size_mb: BASE_MEMTABLE_MB,
            db_repos_memtable_size_mb: BASE_MEMTABLE_MB / 2,
            db_events_memtable_size_mb: BASE_MEMTABLE_MB,
            db_records_memtable_size_mb: BASE_MEMTABLE_MB / 3 * 2,
            ..Self::default()
        }
    }

    /// reads and builds the config from environment variables, loading `.env` first if present.
    pub fn from_env() -> Result<Self> {
        load_dotenv();

        macro_rules! cfg {
            (@val $key:expr) => {
                std::env::var(concat!("HYDRANT_", $key))
            };
            ($key:expr, $default:expr, sec) => {
                cfg!(@val $key)
                    .ok()
                    .and_then(|s| humantime::parse_duration(&s).ok())
                    .unwrap_or($default)
            };
            ($key:expr, $default:expr) => {
                cfg!(@val $key)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or($default.to_owned())
                    .into()
            };
        }

        // full_network is read first since it determines which defaults to use.
        let full_network: bool = cfg!("FULL_NETWORK", false);
        let defaults = full_network
            .then(Self::full_network)
            .unwrap_or_else(Self::default);

        let relay_host: Url = cfg!("RELAY_HOST", defaults.relays[0].clone());
        let relay_hosts = std::env::var("HYDRANT_RELAY_HOSTS")
            .ok()
            .and_then(|hosts| {
                hosts
                    .split(',')
                    .map(|s| Url::parse(s.trim()))
                    .collect::<Result<Vec<_>, _>>()
                    .inspect_err(|e| tracing::warn!("invalid relay host URL: {e}"))
                    .ok()
            })
            .unwrap_or_default();
        let relay_hosts = relay_hosts
            .is_empty()
            .then(|| vec![relay_host])
            .unwrap_or(relay_hosts);

        let plc_urls: Vec<Url> = std::env::var("HYDRANT_PLC_URL")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|s| Url::parse(s.trim()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| miette::miette!("invalid PLC URL: {e}"))
            })
            .unwrap_or_else(|| Ok(defaults.plc_urls.clone()))?;

        let cursor_save_interval = cfg!("CURSOR_SAVE_INTERVAL", defaults.cursor_save_interval, sec);
        let repo_fetch_timeout = cfg!("REPO_FETCH_TIMEOUT", defaults.repo_fetch_timeout, sec);

        let ephemeral: bool = cfg!("EPHEMERAL", defaults.ephemeral);
        let ephemeral_ttl = cfg!("EPHEMERAL_TTL", defaults.ephemeral_ttl, sec);
        let database_path = cfg!("DATABASE_PATH", defaults.database_path);
        let cache_size = cfg!("CACHE_SIZE", defaults.cache_size);
        let data_compression = cfg!("DATA_COMPRESSION", defaults.data_compression);
        let journal_compression = cfg!("JOURNAL_COMPRESSION", defaults.journal_compression);

        let verify_signatures = cfg!("VERIFY_SIGNATURES", defaults.verify_signatures);
        let identity_cache_size = cfg!("IDENTITY_CACHE_SIZE", defaults.identity_cache_size);
        let enable_firehose = cfg!("ENABLE_FIREHOSE", defaults.enable_firehose);
        let enable_crawler = std::env::var("HYDRANT_ENABLE_CRAWLER")
            .ok()
            .and_then(|s| s.parse().ok());

        let backfill_concurrency_limit = cfg!(
            "BACKFILL_CONCURRENCY_LIMIT",
            defaults.backfill_concurrency_limit
        );
        let firehose_workers = cfg!("FIREHOSE_WORKERS", defaults.firehose_workers);

        let db_worker_threads = cfg!("DB_WORKER_THREADS", defaults.db_worker_threads);
        let db_max_journaling_size_mb = cfg!(
            "DB_MAX_JOURNALING_SIZE_MB",
            defaults.db_max_journaling_size_mb
        );
        let db_blocks_memtable_size_mb = cfg!(
            "DB_BLOCKS_MEMTABLE_SIZE_MB",
            defaults.db_blocks_memtable_size_mb
        );
        let db_events_memtable_size_mb = cfg!(
            "DB_EVENTS_MEMTABLE_SIZE_MB",
            defaults.db_events_memtable_size_mb
        );
        let db_records_memtable_size_mb = cfg!(
            "DB_RECORDS_MEMTABLE_SIZE_MB",
            defaults.db_records_memtable_size_mb
        );
        let db_repos_memtable_size_mb = cfg!(
            "DB_REPOS_MEMTABLE_SIZE_MB",
            defaults.db_repos_memtable_size_mb
        );

        let crawler_max_pending_repos = cfg!(
            "CRAWLER_MAX_PENDING_REPOS",
            defaults.crawler_max_pending_repos
        );
        let crawler_resume_pending_repos = cfg!(
            "CRAWLER_RESUME_PENDING_REPOS",
            defaults.crawler_resume_pending_repos
        );

        let filter_signals = std::env::var("HYDRANT_FILTER_SIGNALS").ok().map(|s| {
            s.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

        let filter_collections = std::env::var("HYDRANT_FILTER_COLLECTIONS").ok().map(|s| {
            s.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

        let filter_excludes = std::env::var("HYDRANT_FILTER_EXCLUDES").ok().map(|s| {
            s.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

        let enable_backlinks: bool = cfg!("ENABLE_BACKLINKS", defaults.enable_backlinks);

        let default_mode = CrawlerMode::default_for(full_network);
        let crawler_sources = match std::env::var("HYDRANT_CRAWLER_URLS") {
            Ok(s) => s
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .filter_map(|s| CrawlerSource::parse(s, default_mode))
                .collect(),
            Err(_) => match default_mode {
                CrawlerMode::Relay => relay_hosts
                    .iter()
                    .map(|url| CrawlerSource {
                        url: url.clone(),
                        mode: CrawlerMode::Relay,
                    })
                    .collect(),
                CrawlerMode::ByCollection => defaults.crawler_sources.clone(),
            },
        };

        Ok(Self {
            database_path,
            full_network,
            ephemeral,
            ephemeral_ttl,
            relays: relay_hosts,
            plc_urls,
            enable_firehose,
            firehose_workers,
            cursor_save_interval,
            repo_fetch_timeout,
            backfill_concurrency_limit,
            enable_crawler,
            crawler_max_pending_repos,
            crawler_resume_pending_repos,
            crawler_sources,
            verify_signatures,
            identity_cache_size,
            filter_signals,
            filter_collections,
            filter_excludes,
            enable_backlinks,
            cache_size,
            data_compression,
            journal_compression,
            db_worker_threads,
            db_max_journaling_size_mb,
            db_blocks_memtable_size_mb,
            db_repos_memtable_size_mb,
            db_events_memtable_size_mb,
            db_records_memtable_size_mb,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub api_port: u16,
    pub enable_debug: bool,
    pub debug_port: u16,
}

impl AppConfig {
    pub fn from_env() -> Self {
        macro_rules! cfg {
            ($key:expr, $default:expr) => {
                std::env::var(concat!("HYDRANT_", $key))
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or($default)
            };
        }
        let api_port = cfg!("API_PORT", 3000u16);
        let enable_debug = cfg!("ENABLE_DEBUG", false);
        let debug_port = cfg!("DEBUG_PORT", api_port + 1);
        Self {
            api_port,
            enable_debug,
            debug_port,
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
        config_line!(f, "relay hosts", format_args!("{:?}", self.relays))?;
        config_line!(f, "plc urls", format_args!("{:?}", self.plc_urls))?;
        config_line!(f, "full network indexing", self.full_network)?;
        config_line!(f, "verify signatures", self.verify_signatures)?;
        config_line!(f, "backfill concurrency", self.backfill_concurrency_limit)?;
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
        Ok(())
    }
}
