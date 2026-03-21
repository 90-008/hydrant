use miette::Result;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

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
    pub api_port: u16,
    pub cache_size: u64,
    pub backfill_concurrency_limit: usize,
    pub data_compression: Compression,
    pub journal_compression: Compression,
    pub debug_port: u16,
    pub enable_debug: bool,
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
}

impl Config {
    pub fn from_env() -> Result<Self> {
        macro_rules! cfg {
            (@val $key:expr) => {
                std::env::var(concat!("HYDRANT_", $key))
            };
            ($key:expr, $default:expr, sec) => {
                cfg!(@val $key)
                    .ok()
                    .and_then(|s| humantime::parse_duration(&s).ok())
                    .unwrap_or(Duration::from_secs($default))
            };
            ($key:expr, $default:expr) => {
                cfg!(@val $key)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or($default.to_owned())
                    .into()
            };
        }

        let relay_host: Url = cfg!(
            "RELAY_HOST",
            Url::parse("wss://relay.fire.hose.cam/").unwrap()
        );
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

        let full_network: bool = cfg!("FULL_NETWORK", false);

        let plc_urls: Vec<Url> = std::env::var("HYDRANT_PLC_URL")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|s| Url::parse(s.trim()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| miette::miette!("invalid PLC URL: {e}"))
            })
            .unwrap_or_else(|| {
                Ok(vec![
                    full_network
                        .then_some(Url::parse("https://plc.directory").unwrap())
                        .unwrap_or(Url::parse("https://plc.wtf").unwrap()),
                ])
            })?;

        let cursor_save_interval = cfg!("CURSOR_SAVE_INTERVAL", 3, sec);
        let repo_fetch_timeout = cfg!("REPO_FETCH_TIMEOUT", 300, sec);

        let ephemeral: bool = cfg!("EPHEMERAL", false);
        let ephemeral_ttl = cfg!("EPHEMERAL_TTL", 60 * 60, sec);
        let database_path = cfg!("DATABASE_PATH", "./hydrant.db");
        let cache_size = cfg!("CACHE_SIZE", 256u64);
        let data_compression = cfg!("DATA_COMPRESSION", Compression::Lz4);
        let journal_compression = cfg!("JOURNAL_COMPRESSION", Compression::Lz4);

        let api_port = cfg!("API_PORT", 3000u16);
        let enable_debug = cfg!("ENABLE_DEBUG", false);
        let debug_port: u16 = api_port + 1;
        let debug_port = cfg!("DEBUG_PORT", debug_port);
        let verify_signatures = cfg!("VERIFY_SIGNATURES", SignatureVerification::Full);
        let identity_cache_size = cfg!("IDENTITY_CACHE_SIZE", 1_000_000u64);
        let enable_firehose = cfg!("ENABLE_FIREHOSE", true);
        let enable_crawler = std::env::var("HYDRANT_ENABLE_CRAWLER")
            .ok()
            .and_then(|s| s.parse().ok());

        let backfill_concurrency_limit = cfg!(
            "BACKFILL_CONCURRENCY_LIMIT",
            full_network.then_some(64usize).unwrap_or(16usize)
        );
        let firehose_workers = cfg!(
            "FIREHOSE_WORKERS",
            full_network.then_some(24usize).unwrap_or(8usize)
        );

        let (
            default_db_worker_threads,
            default_db_max_journaling_size_mb,
            default_db_memtable_size_mb,
        ): (usize, u64, u64) = full_network
            .then_some((8usize, 1024u64, 192u64))
            .unwrap_or((4usize, 400u64, 32u64));

        let db_worker_threads = cfg!("DB_WORKER_THREADS", default_db_worker_threads);
        let db_max_journaling_size_mb = cfg!(
            "DB_MAX_JOURNALING_SIZE_MB",
            default_db_max_journaling_size_mb
        );
        let db_blocks_memtable_size_mb =
            cfg!("DB_BLOCKS_MEMTABLE_SIZE_MB", default_db_memtable_size_mb);
        let db_events_memtable_size_mb =
            cfg!("DB_EVENTS_MEMTABLE_SIZE_MB", default_db_memtable_size_mb);
        let db_records_memtable_size_mb = cfg!(
            "DB_RECORDS_MEMTABLE_SIZE_MB",
            // records is did + col + rkey -> CID so its pretty cheap
            default_db_memtable_size_mb / 3 * 2
        );
        let db_repos_memtable_size_mb =
            cfg!("DB_REPOS_MEMTABLE_SIZE_MB", default_db_memtable_size_mb / 2);

        let crawler_max_pending_repos = cfg!("CRAWLER_MAX_PENDING_REPOS", 2000usize);
        let crawler_resume_pending_repos = cfg!("CRAWLER_RESUME_PENDING_REPOS", 1000usize);

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

        Ok(Self {
            database_path,
            relays: relay_hosts,
            plc_urls,
            ephemeral,
            ephemeral_ttl,
            full_network,
            cursor_save_interval,
            repo_fetch_timeout,
            api_port,
            cache_size,
            backfill_concurrency_limit,
            data_compression,
            journal_compression,
            debug_port,
            enable_debug,
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
        })
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
        config_line!(f, "api port", self.api_port)?;
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
        if let Some(signals) = &self.filter_signals {
            config_line!(f, "filter signals", format_args!("{:?}", signals))?;
        }
        if let Some(collections) = &self.filter_collections {
            config_line!(f, "filter collections", format_args!("{:?}", collections))?;
        }
        if let Some(excludes) = &self.filter_excludes {
            config_line!(f, "filter excludes", format_args!("{:?}", excludes))?;
        }
        config_line!(f, "enable debug", self.enable_debug)?;
        if self.enable_debug {
            config_line!(f, "debug port", self.debug_port)?;
        }
        Ok(())
    }
}
