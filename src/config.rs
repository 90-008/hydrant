use miette::Result;
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
    pub plc_url: Url,
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
        let plc_url = cfg!("PLC_URL", Url::parse("https://plc.wtf").unwrap());

        let full_network = cfg!("FULL_NETWORK", false);
        let backfill_concurrency_limit = cfg!("BACKFILL_CONCURRENCY_LIMIT", 32usize);
        let cursor_save_interval = cfg!("CURSOR_SAVE_INTERVAL", 10, sec);
        let repo_fetch_timeout = cfg!("REPO_FETCH_TIMEOUT", 300, sec);

        let database_path = cfg!("DATABASE_PATH", "./hydrant.db");
        let cache_size = cfg!("CACHE_SIZE", 256u64);
        let disable_lz4_compression = cfg!("NO_LZ4_COMPRESSION", false);

        let api_port = cfg!("API_PORT", 3000u16);
        let enable_debug = cfg!("ENABLE_DEBUG", false);
        let debug_port = cfg!("DEBUG_PORT", 3001u16);
        let verify_signatures = cfg!("VERIFY_SIGNATURES", SignatureVerification::Full);
        let identity_cache_size = cfg!("IDENTITY_CACHE_SIZE", 100_000u64);

        Ok(Self {
            database_path,
            relay_host,
            plc_url,
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
        })
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "hydrant configuration:")?;
        writeln!(f, "  log level:                {}", self.log_level)?;
        writeln!(f, "  relay host:               {}", self.relay_host)?;
        writeln!(f, "  plc url:                  {}", self.plc_url)?;
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
        writeln!(f, "  enable debug:             {}", self.enable_debug)?;
        if self.enable_debug {
            writeln!(f, "  debug port:               {}", self.debug_port)?;
        }
        Ok(())
    }
}
