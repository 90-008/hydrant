use miette::{IntoDiagnostic, Result};
use smol_str::SmolStr;
use std::env;
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_path: PathBuf,
    pub relay_host: SmolStr,
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
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let database_path = env::var("HYDRANT_DATABASE_PATH")
            .unwrap_or_else(|_| "./hydrant.db".to_string())
            .into();

        let relay_host = env::var("HYDRANT_RELAY_HOST")
            .unwrap_or_else(|_| "wss://relay.fire.hose.cam".to_string())
            .into();

        let plc_url = env::var("HYDRANT_PLC_URL")
            .unwrap_or_else(|_| "https://plc.wtf".to_string())
            .parse()
            .into_diagnostic()?;

        let full_network = env::var("HYDRANT_FULL_NETWORK")
            .map(|v| v == "true")
            .unwrap_or(false);

        let cursor_save_interval = env::var("HYDRANT_CURSOR_SAVE_INTERVAL")
            .ok()
            .and_then(|s| humantime::parse_duration(&s).ok())
            .unwrap_or(Duration::from_secs(10));

        let repo_fetch_timeout = env::var("HYDRANT_REPO_FETCH_TIMEOUT")
            .ok()
            .and_then(|s| humantime::parse_duration(&s).ok())
            .unwrap_or(Duration::from_secs(300));

        let log_level = env::var("HYDRANT_LOG_LEVEL")
            .unwrap_or_else(|_| "info".to_string())
            .into();

        let api_port = env::var("HYDRANT_API_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3000);

        let cache_size = env::var("HYDRANT_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256);

        let backfill_concurrency_limit = env::var("HYDRANT_BACKFILL_CONCURRENCY_LIMIT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32);

        let disable_lz4_compression = env::var("HYDRANT_NO_LZ4_COMPRESSION")
            .map(|v| v == "true")
            .unwrap_or(false);

        let debug_port = env::var("HYDRANT_DEBUG_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3001);

        let enable_debug = env::var("HYDRANT_ENABLE_DEBUG")
            .map(|v| v == "true")
            .unwrap_or(false);

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
        })
    }
}
