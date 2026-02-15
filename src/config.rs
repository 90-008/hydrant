use miette::{IntoDiagnostic, Result};
use smol_str::SmolStr;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

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
    pub relay_host: Url,
    pub plc_urls: Vec<Url>,
    pub full_network: bool,
    pub cursor_save_interval: Duration,
    pub repo_fetch_timeout: Duration,
    pub log_level: SmolStr,
    pub api_port: u16,
    pub cache_size: u64,
    pub backfill_concurrency_limit: usize,
    pub disable_lz4_compression: bool,
    pub debug_port: u16,
    pub enable_debug: bool,
    pub verify_signatures: SignatureVerification,
    pub identity_cache_size: u64,
    pub disable_firehose: bool,
    pub disable_backfill: bool,
    pub firehose_workers: usize,
    pub db_worker_threads: usize,
    pub db_max_journaling_size_mb: u64,
    pub db_pending_memtable_size_mb: u64,
    pub db_blocks_memtable_size_mb: u64,
    pub db_repos_memtable_size_mb: u64,
    pub db_events_memtable_size_mb: u64,
    pub db_records_default_memtable_size_mb: u64,
    pub db_records_partition_overrides: Vec<(glob::Pattern, u64)>,
    pub crawler_max_pending_repos: usize,
    pub crawler_resume_pending_repos: usize,
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

        let log_level = cfg!("LOG_LEVEL", "info");

        let relay_host = cfg!(
            "RELAY_HOST",
            Url::parse("wss://relay.fire.hose.cam").unwrap()
        );
        let plc_urls: Vec<Url> = std::env::var("HYDRANT_PLC_URL")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|s| Url::parse(s.trim()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| miette::miette!("invalid PLC URL: {}", e))
            })
            .unwrap_or_else(|| Ok(vec![Url::parse("https://plc.wtf").unwrap()]))?;

        let full_network: bool = cfg!("FULL_NETWORK", false);
        let backfill_concurrency_limit = cfg!("BACKFILL_CONCURRENCY_LIMIT", 128usize);
        let cursor_save_interval = cfg!("CURSOR_SAVE_INTERVAL", 5, sec);
        let repo_fetch_timeout = cfg!("REPO_FETCH_TIMEOUT", 300, sec);

        let database_path = cfg!("DATABASE_PATH", "./hydrant.db");
        let cache_size = cfg!("CACHE_SIZE", 256u64);
        let disable_lz4_compression = cfg!("NO_LZ4_COMPRESSION", false);

        let api_port = cfg!("API_PORT", 3000u16);
        let enable_debug = cfg!("ENABLE_DEBUG", false);
        let debug_port = cfg!("DEBUG_PORT", 3001u16);
        let verify_signatures = cfg!("VERIFY_SIGNATURES", SignatureVerification::Full);
        let identity_cache_size = cfg!("IDENTITY_CACHE_SIZE", 1_000_000u64);
        let disable_firehose = cfg!("DISABLE_FIREHOSE", false);
        let disable_backfill = cfg!("DISABLE_BACKFILL", false);
        let firehose_workers = cfg!("FIREHOSE_WORKERS", 32usize);

        let (
            default_db_worker_threads,
            default_db_max_journaling_size_mb,
            default_db_memtable_size_mb,
            default_records_memtable_size_mb,
            default_partition_overrides,
        ): (usize, u64, u64, u64, &str) = full_network
            .then_some((8usize, 1024u64, 192u64, 8u64, "app.bsky.*=64"))
            .unwrap_or((4usize, 512u64, 64u64, 16u64, ""));

        let db_worker_threads = cfg!("DB_WORKER_THREADS", default_db_worker_threads);
        let db_max_journaling_size_mb = cfg!(
            "DB_MAX_JOURNALING_SIZE_MB",
            default_db_max_journaling_size_mb
        );
        let db_pending_memtable_size_mb =
            cfg!("DB_PENDING_MEMTABLE_SIZE_MB", default_db_memtable_size_mb);
        let db_blocks_memtable_size_mb =
            cfg!("DB_BLOCKS_MEMTABLE_SIZE_MB", default_db_memtable_size_mb);
        let db_repos_memtable_size_mb =
            cfg!("DB_REPOS_MEMTABLE_SIZE_MB", default_db_memtable_size_mb);
        let db_events_memtable_size_mb =
            cfg!("DB_EVENTS_MEMTABLE_SIZE_MB", default_db_memtable_size_mb);
        let db_records_default_memtable_size_mb = cfg!(
            "DB_RECORDS_DEFAULT_MEMTABLE_SIZE_MB",
            default_records_memtable_size_mb
        );

        let crawler_max_pending_repos = cfg!("CRAWLER_MAX_PENDING_REPOS", 2000usize);
        let crawler_resume_pending_repos = cfg!("CRAWLER_RESUME_PENDING_REPOS", 1000usize);

        let db_records_partition_overrides: Vec<(glob::Pattern, u64)> =
            std::env::var("HYDRANT_DB_RECORDS_PARTITION_OVERRIDES")
                .unwrap_or_else(|_| default_partition_overrides.to_string())
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let mut parts = s.split('=');
                    let pattern = parts
                        .next()
                        .ok_or_else(|| miette::miette!("invalid partition override format"))?;
                    let size = parts
                        .next()
                        .ok_or_else(|| miette::miette!("invalid partition override format"))?
                        .parse::<u64>()
                        .into_diagnostic()?;
                    Ok((glob::Pattern::new(pattern).into_diagnostic()?, size))
                })
                .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            database_path,
            relay_host,
            plc_urls,
            full_network,
            cursor_save_interval,
            repo_fetch_timeout,
            log_level,
            api_port,
            cache_size,
            backfill_concurrency_limit,
            disable_lz4_compression,
            debug_port,
            enable_debug,
            verify_signatures,
            identity_cache_size,
            disable_firehose,
            disable_backfill,
            firehose_workers,
            db_worker_threads,
            db_max_journaling_size_mb,
            db_pending_memtable_size_mb,
            db_blocks_memtable_size_mb,
            db_repos_memtable_size_mb,
            db_events_memtable_size_mb,
            db_records_default_memtable_size_mb,
            db_records_partition_overrides: db_records_partition_overrides,
            crawler_max_pending_repos,
            crawler_resume_pending_repos,
        })
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "hydrant configuration:")?;
        writeln!(f, "  log level:                {}", self.log_level)?;
        writeln!(f, "  relay host:               {}", self.relay_host)?;
        writeln!(f, "  plc urls:                 {:?}", self.plc_urls)?;
        writeln!(f, "  full network indexing:    {}", self.full_network)?;
        writeln!(f, "  verify signatures:        {}", self.verify_signatures)?;
        writeln!(
            f,
            "  backfill concurrency:     {}",
            self.backfill_concurrency_limit
        )?;
        writeln!(
            f,
            "  identity cache size:      {}",
            self.identity_cache_size
        )?;
        writeln!(
            f,
            "  cursor save interval:     {}sec",
            self.cursor_save_interval.as_secs()
        )?;
        writeln!(
            f,
            "  repo fetch timeout:       {}sec",
            self.repo_fetch_timeout.as_secs()
        )?;
        writeln!(
            f,
            "  database path:            {}",
            self.database_path.to_string_lossy()
        )?;
        writeln!(f, "  cache size:               {} mb", self.cache_size)?;
        writeln!(
            f,
            "  disable lz4 compression:  {}",
            self.disable_lz4_compression
        )?;
        writeln!(f, "  api port:                 {}", self.api_port)?;
        writeln!(f, "  firehose workers:         {}", self.firehose_workers)?;
        writeln!(f, "  db worker threads:        {}", self.db_worker_threads)?;
        writeln!(
            f,
            "  db journal size:          {} mb",
            self.db_max_journaling_size_mb
        )?;
        writeln!(
            f,
            "  db pending memtable:      {} mb",
            self.db_pending_memtable_size_mb
        )?;
        writeln!(
            f,
            "  db blocks memtable:       {} mb",
            self.db_blocks_memtable_size_mb
        )?;
        writeln!(
            f,
            "  db repos memtable:        {} mb",
            self.db_repos_memtable_size_mb
        )?;
        writeln!(
            f,
            "  db events memtable:       {} mb",
            self.db_events_memtable_size_mb
        )?;
        writeln!(
            f,
            "  db records def memtable:  {} mb",
            self.db_records_default_memtable_size_mb
        )?;

        writeln!(
            f,
            "  crawler max pending:      {}",
            self.crawler_max_pending_repos
        )?;
        writeln!(
            f,
            "  crawler resume pending:   {}",
            self.crawler_resume_pending_repos
        )?;
        writeln!(f, "  enable debug:             {}", self.enable_debug)?;
        if self.enable_debug {
            writeln!(f, "  debug port:               {}", self.debug_port)?;
        }
        Ok(())
    }
}
