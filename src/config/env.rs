use miette::Result;
use smol_str::SmolStr;
use std::time::Duration;
use url::Url;

use super::Config;
use super::types::{CrawlerMode, CrawlerSource, FirehoseSource, RateTier};
use crate::pds_meta::{TierPolicy, TierRule};

/// this is for internal use only, please don't use this macro.
#[doc(hidden)]
#[macro_export]
macro_rules! __cfg {
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
use crate::__cfg as cfg;

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

fn parse_new_host_limit(default: Option<u64>) -> Option<u64> {
    match std::env::var("HYDRANT_NEW_HOST_LIMIT") {
        Ok(s) if s.trim().eq_ignore_ascii_case("none") => None,
        Ok(s) => s.trim().parse().ok().or(default),
        Err(_) => default,
    }
}

impl Config {
    /// reads and builds the config from environment variables, loading `.env` first if present.
    pub fn from_env() -> Result<Self> {
        load_dotenv();

        // full_network is read first since it determines which defaults to use.
        // relay mode defaults to true so that the network is indexed by default.
        #[cfg(feature = "relay")]
        let default_full_network = true;
        #[cfg(not(feature = "relay"))]
        let default_full_network = false;
        let full_network: bool = cfg!("FULL_NETWORK", default_full_network);
        let defaults = full_network
            .then(Self::full_network)
            .unwrap_or_else(Self::default);

        let relay_hosts = match std::env::var("HYDRANT_RELAY_HOSTS") {
            Ok(hosts) if !hosts.trim().is_empty() => hosts
                .split(',')
                .filter_map(|s| {
                    let s = s.trim();
                    (!s.is_empty())
                        .then(|| {
                            FirehoseSource::parse(s).or_else(|| {
                                tracing::warn!("invalid relay host URL: {s}");
                                None
                            })
                        })
                        .flatten()
                })
                .collect(),
            // HYDRANT_RELAY_HOSTS explicitly set to ""
            Ok(_) => vec![],
            // not set at all, fall back to RELAY_HOST (bare URL, no pds:: prefix support here)
            Err(_) => match std::env::var("HYDRANT_RELAY_HOST") {
                Ok(s) if !s.trim().is_empty() => {
                    FirehoseSource::parse(s.trim()).into_iter().collect()
                }
                _ => defaults.relays.clone(),
            },
        };

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
        let max_car_body_bytes = cfg!("MAX_CAR_BODY_BYTES", defaults.max_car_body_bytes);

        let ephemeral: bool = cfg!("EPHEMERAL", defaults.ephemeral);
        let ephemeral_ttl = cfg!("EPHEMERAL_TTL", defaults.ephemeral_ttl, sec);
        let database_path = cfg!("DATABASE_PATH", defaults.database_path);
        let cache_size = cfg!("CACHE_SIZE", defaults.cache_size);
        let data_compression = cfg!("DATA_COMPRESSION", defaults.data_compression);
        let journal_compression = cfg!("JOURNAL_COMPRESSION", defaults.journal_compression);

        let verify_signatures = cfg!("VERIFY_SIGNATURES", defaults.verify_signatures);
        let identity_cache_size = cfg!("IDENTITY_CACHE_SIZE", defaults.identity_cache_size);
        let verify_mst: bool = cfg!("VERIFY_MST", defaults.verify_mst);
        let rev_clock_skew_secs: i64 = cfg!("REV_CLOCK_SKEW", defaults.rev_clock_skew_secs);
        let enable_firehose = cfg!("ENABLE_FIREHOSE", defaults.enable_firehose);
        let enable_backfill = cfg!("ENABLE_BACKFILL", defaults.enable_backfill);
        let enable_crawler = std::env::var("HYDRANT_ENABLE_CRAWLER")
            .ok()
            .and_then(|s| s.parse().ok());

        let backfill_concurrency_limit = cfg!(
            "BACKFILL_CONCURRENCY_LIMIT",
            defaults.backfill_concurrency_limit
        );
        let backfill_strategy = cfg!("BACKFILL_STRATEGY", defaults.backfill_strategy);

        // comma-separated proxy URLs to spread backfill fetches across egress IPs.
        let backfill_proxies: Vec<Url> = std::env::var("HYDRANT_BACKFILL_PROXIES")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|u| {
                        let u = u.trim();
                        if u.is_empty() {
                            return None;
                        }
                        Url::parse(u)
                            .or_else(|_| Url::parse(&format!("http://{u}")))
                            .ok()
                            .or_else(|| {
                                tracing::warn!("invalid backfill proxy URL: {u}");
                                None
                            })
                    })
                    .collect()
            })
            .unwrap_or_else(|| defaults.backfill_proxies.clone());
        let per_pds_concurrency = cfg!("PER_PDS_CONCURRENCY", defaults.per_pds_concurrency);
        let firehose_workers = cfg!("FIREHOSE_WORKERS", defaults.firehose_workers);
        let firehose_max_failures = cfg!("FIREHOSE_MAX_FAILURES", defaults.firehose_max_failures);

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
        let db_records_bloom_filters: bool = cfg!(
            "DB_RECORDS_BLOOM_FILTERS",
            defaults.db_records_bloom_filters
        );
        let db_repos_memtable_size_mb = cfg!(
            "DB_REPOS_MEMTABLE_SIZE_MB",
            defaults.db_repos_memtable_size_mb
        );
        let stream_replay_chunk_size = cfg!(
            "STREAM_REPLAY_CHUNK_SIZE",
            defaults.stream_replay_chunk_size
        );
        let stream_replay_chunk_pause = cfg!(
            "STREAM_REPLAY_CHUNK_PAUSE",
            defaults.stream_replay_chunk_pause,
            sec
        );
        let stream_pending_event_limit = cfg!(
            "STREAM_PENDING_EVENT_LIMIT",
            defaults.stream_pending_event_limit
        );
        let stream_send_timeout = cfg!("STREAM_SEND_TIMEOUT", defaults.stream_send_timeout, sec);

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
        let get_repo_concurrency_limit = cfg!(
            "GET_REPO_CONCURRENCY_LIMIT",
            defaults.get_repo_concurrency_limit
        );
        let verify_cids: bool = cfg!("VERIFY_CIDS", defaults.verify_cids);
        let only_index_links: bool = cfg!("ONLY_INDEX_LINKS", defaults.only_index_links);
        let max_pds_added_per_day = parse_new_host_limit(defaults.new_host_limit);

        let offline_retry_interval: Option<Duration> =
            match std::env::var("HYDRANT_OFFLINE_HOST_RETRY_INTERVAL")
                .ok()
                .as_deref()
            {
                None => defaults.offline_host_retry_interval,
                Some("none") => None,
                Some(s) => humantime::parse_duration(s)
                    .ok()
                    .or(defaults.offline_host_retry_interval),
            };

        // start with built-in tier definitions, then layer in any env-defined overrides.
        // format: HYDRANT_RATE_TIERS=name:base/mul/hourly/daily,...
        let mut tiers = defaults.tier_policy.tiers.clone();
        if let Ok(s) = std::env::var("HYDRANT_RATE_TIERS") {
            for entry in s.split(',') {
                let entry = entry.trim();
                if let Some((name, spec)) = entry.split_once(':') {
                    match RateTier::parse(spec) {
                        Some(tier) => {
                            tiers.insert(SmolStr::new(name.trim()), tier);
                        }
                        None => tracing::warn!(
                            "ignoring invalid rate rate tier '{name}': expected base/mul/hourly/daily format"
                        ),
                    }
                }
            }
        }

        let seed_hosts: Vec<Url> = std::env::var("HYDRANT_SEED_HOSTS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|u| {
                        let u = u.trim();
                        if u.is_empty() {
                            return None;
                        }
                        Url::parse(u).ok().or_else(|| {
                            tracing::warn!("invalid seed host URL: {u}");
                            None
                        })
                    })
                    .collect()
            })
            .unwrap_or_else(|| defaults.seed_hosts.clone());

        // build ordered glob rules from HYDRANT_TIER_RULES
        let mut rules: Vec<TierRule> = vec![];
        let mut tier_rules: Vec<(String, String)> = vec![];
        if let Ok(s) = std::env::var("HYDRANT_TIER_RULES") {
            for entry in s.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                if let Some((pattern_str, tier_name)) = entry.split_once(':') {
                    let pattern_str = pattern_str.trim();
                    let tier_name = tier_name.trim();
                    match glob::Pattern::new(pattern_str) {
                        Ok(pattern) => {
                            rules.push(TierRule {
                                pattern,
                                tier_name: SmolStr::new(tier_name),
                            });
                            tier_rules.push((pattern_str.to_string(), tier_name.to_string()));
                        }
                        Err(e) => tracing::warn!(
                            "ignoring invalid tier rule pattern '{pattern_str}': {e}"
                        ),
                    }
                }
            }
        }

        let tier_policy = TierPolicy { tiers, rules };

        let default_mode = CrawlerMode::default_for(full_network);
        let crawler_sources = match std::env::var("HYDRANT_CRAWLER_URLS") {
            Ok(s) => s
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .filter_map(|s| CrawlerSource::parse(s, default_mode))
                .collect(),
            Err(_) => match default_mode {
                CrawlerMode::ListRepos => relay_hosts
                    .iter()
                    .map(|source| CrawlerSource {
                        url: source.url.clone(),
                        mode: CrawlerMode::ListRepos,
                    })
                    .collect(),
                CrawlerMode::ByCollection => defaults.crawler_sources.clone(),
            },
        };

        Ok(Self {
            database_path,
            full_network,
            ephemeral,
            seed_hosts,
            ephemeral_ttl,
            relays: relay_hosts,
            plc_urls,
            enable_firehose,
            firehose_workers,
            firehose_max_failures,
            cursor_save_interval,
            enable_backfill,
            repo_fetch_timeout,
            max_car_body_bytes,
            backfill_concurrency_limit,
            backfill_strategy,
            backfill_proxies,
            per_pds_concurrency,
            enable_crawler,
            crawler_max_pending_repos,
            crawler_resume_pending_repos,
            crawler_sources,
            verify_signatures,
            identity_cache_size,
            verify_mst,
            rev_clock_skew_secs,
            filter_signals,
            filter_collections,
            filter_excludes,
            enable_backlinks,
            get_repo_concurrency_limit,
            verify_cids,
            only_index_links,
            new_host_limit: max_pds_added_per_day,
            offline_host_retry_interval: offline_retry_interval,
            tier_policy,
            tier_rules,
            cache_size,
            data_compression,
            journal_compression,
            db_worker_threads,
            db_max_journaling_size_mb,
            db_blocks_memtable_size_mb,
            db_repos_memtable_size_mb,
            db_events_memtable_size_mb,
            db_records_memtable_size_mb,
            db_records_bloom_filters,
            stream_replay_chunk_size,
            stream_replay_chunk_pause,
            stream_pending_event_limit,
            stream_send_timeout,
        })
    }
}
