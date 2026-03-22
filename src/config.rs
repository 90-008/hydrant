use miette::Result;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

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
    Full,
    BackfillOnly,
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
    pub database_path: PathBuf,
    pub relays: Vec<Url>,
    pub plc_urls: Vec<Url>,
    pub full_network: bool,
    pub ephemeral: bool,
    pub ephemeral_ttl: Duration,
    pub cursor_save_interval: Duration,
    pub repo_fetch_timeout: Duration,
    pub cache_size: u64,
    pub backfill_concurrency_limit: usize,
    pub data_compression: Compression,
    pub journal_compression: Compression,
    pub verify_signatures: SignatureVerification,
    pub identity_cache_size: u64,
    pub enable_firehose: bool,
    pub enable_crawler: Option<bool>,
    pub firehose_workers: usize,
    pub db_worker_threads: usize,
    pub db_max_journaling_size_mb: u64,
    pub db_blocks_memtable_size_mb: u64,
    pub db_repos_memtable_size_mb: u64,
    pub db_events_memtable_size_mb: u64,
    pub db_records_memtable_size_mb: u64,
    pub crawler_max_pending_repos: usize,
    pub crawler_resume_pending_repos: usize,
    pub filter_signals: Option<Vec<String>>,
    pub filter_collections: Option<Vec<String>>,
    pub filter_excludes: Option<Vec<String>>,
    /// enable backlinks indexing (only meaningful in non-ephemeral mode).
    /// set via `HYDRANT_ENABLE_BACKLINKS=true`.
    pub enable_backlinks: bool,
    /// crawler sources: each entry pairs a URL with a discovery mode.
    ///
    /// set via `HYDRANT_CRAWLER_URLS` as a comma-separated list of `[mode::]url` entries,
    /// e.g. `relay::wss://bsky.network,by_collection::https://lightrail.microcosm.blue`.
    /// a bare URL without a `mode::` prefix uses the default mode (`relay` for full-network,
    /// `by_collection` otherwise). defaults to the relay hosts with the default mode.
    /// set to an empty string to disable crawling entirely.
    pub crawler_sources: Vec<CrawlerSource>,
}

impl Default for Config {
    fn default() -> Self {
        const BASE_MEMTABLE_MB: u64 = 32;
        Self {
            database_path: PathBuf::from("./hydrant.db"),
            relays: vec![Url::parse("wss://relay.fire.hose.cam/").unwrap()],
            plc_urls: vec![Url::parse("https://plc.wtf").unwrap()],
            full_network: false,
            ephemeral: false,
            ephemeral_ttl: Duration::from_secs(3600),
            cursor_save_interval: Duration::from_secs(3),
            repo_fetch_timeout: Duration::from_secs(300),
            cache_size: 256,
            backfill_concurrency_limit: 16,
            data_compression: Compression::Lz4,
            journal_compression: Compression::Lz4,
            verify_signatures: SignatureVerification::Full,
            identity_cache_size: 1_000_000,
            enable_firehose: true,
            enable_crawler: None,
            firehose_workers: 8,
            db_worker_threads: 4,
            db_max_journaling_size_mb: 400,
            db_blocks_memtable_size_mb: BASE_MEMTABLE_MB,
            db_repos_memtable_size_mb: BASE_MEMTABLE_MB / 2,
            db_events_memtable_size_mb: BASE_MEMTABLE_MB,
            db_records_memtable_size_mb: BASE_MEMTABLE_MB / 3 * 2,
            crawler_max_pending_repos: 2000,
            crawler_resume_pending_repos: 1000,
            filter_signals: None,
            filter_collections: None,
            filter_excludes: None,
            enable_backlinks: false,
            crawler_sources: vec![CrawlerSource {
                url: Url::parse("https://lightrail.microcosm.blue").unwrap(),
                mode: CrawlerMode::ByCollection,
            }],
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
            backfill_concurrency_limit: 64,
            firehose_workers: 24,
            db_worker_threads: 8,
            db_max_journaling_size_mb: 1024,
            db_blocks_memtable_size_mb: BASE_MEMTABLE_MB,
            db_events_memtable_size_mb: BASE_MEMTABLE_MB,
            db_repos_memtable_size_mb: BASE_MEMTABLE_MB / 2,
            db_records_memtable_size_mb: BASE_MEMTABLE_MB / 3 * 2,
            crawler_sources: vec![CrawlerSource {
                url: Url::parse("wss://relay.fire.hose.cam/").unwrap(),
                mode: CrawlerMode::Relay,
            }],
            ..Self::default()
        }
    }

    /// reads and builds the config from environment variables.
    pub fn from_env() -> Result<Self> {
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
            relays: relay_hosts,
            plc_urls,
            ephemeral,
            ephemeral_ttl,
            full_network,
            cursor_save_interval,
            repo_fetch_timeout,
            cache_size,
            backfill_concurrency_limit,
            data_compression,
            journal_compression,
            verify_signatures,
            identity_cache_size,
            enable_firehose,
            enable_crawler,
            firehose_workers,
            db_worker_threads,
            db_max_journaling_size_mb,
            db_blocks_memtable_size_mb,
            db_repos_memtable_size_mb,
            db_events_memtable_size_mb,
            db_records_memtable_size_mb,
            crawler_max_pending_repos,
            crawler_resume_pending_repos,
            filter_signals,
            filter_collections,
            filter_excludes,
            enable_backlinks,
            crawler_sources,
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
