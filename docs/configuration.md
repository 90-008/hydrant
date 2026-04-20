# configuration

hydrant is configured via environment variables, all prefixed with `HYDRANT_` (except `RUST_LOG`). a `.env` file in the working directory is loaded automatically.

## core

| variable | default | description |
| :--- | :--- | :--- |
| `DATABASE_PATH` | `./hydrant.db` | path to the database folder |
| `RUST_LOG` | `info` | log filter ([tracing env-filter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)) |
| `API_PORT` | `3000` | port for the API server |
| `ENABLE_DEBUG` | `false` | enable debug endpoints |
| `DEBUG_PORT` | `API_PORT + 1` | port for debug endpoints |

## indexing mode

| variable | default | description |
| :--- | :--- | :--- |
| `FULL_NETWORK` | `false` (indexer), `true` (relay) | if `true`, discover and index all repos in the network |
| `EPHEMERAL` | `false` (indexer), `true` (relay) | if enabled, no records are stored; events are deleted after `EPHEMERAL_TTL` |
| `EPHEMERAL_TTL` | `60min`, `3d` (relay) | how long to keep events before deletion |
| `ONLY_INDEX_LINKS` | `false` | don't store record blocks, only the index. `getRecord`, `listRecords`, and `getRepo` will fail; the event stream still works but create/update events won't include record values |

## filter

| variable | default | description |
| :--- | :--- | :--- |
| `FILTER_SIGNALS` | | comma-separated NSID patterns triggering auto-discovery in filter mode (e.g. `app.bsky.feed.post,app.bsky.graph.*`) |
| `FILTER_COLLECTIONS` | | comma-separated NSID patterns limiting which records are stored. empty = store all |
| `FILTER_EXCLUDES` | | comma-separated DIDs to always skip |

## firehose

| variable | default | description |
| :--- | :--- | :--- |
| `RELAY_HOST` | `wss://relay.fire.hose.cam/` (indexer), empty (relay) | single firehose source URL |
| `RELAY_HOSTS` | | comma-separated firehose sources. prefix with `pds::` for direct PDS connections. overrides `RELAY_HOST` |
| `SEED_HOSTS` | `https://bsky.network` (relay) | relay URLs to call `com.atproto.sync.listHosts` on at startup, adding every non-banned PDS as a firehose source |
| `ENABLE_FIREHOSE` | `true` | whether to ingest relay subscriptions |
| `FIREHOSE_WORKERS` | `8` (`24` full network) | concurrent workers for firehose events |
| `CURSOR_SAVE_INTERVAL` | `3sec` | how often to persist the firehose cursor |

## crawler

| variable | default | description |
| :--- | :--- | :--- |
| `CRAWLER_URLS` | relay hosts (full network), `https://lightrail.microcosm.blue` (filter) | comma-separated `[mode::]url` crawler sources |
| `ENABLE_CRAWLER` | `true` if full network or sources configured | whether to actively query the network |
| `CRAWLER_MAX_PENDING_REPOS` | `2000` | max pending repos before the crawler pauses |
| `CRAWLER_RESUME_PENDING_REPOS` | `1000` | pending-repo count at which the crawler resumes |

## backfill & identity

| variable | default | description |
| :--- | :--- | :--- |
| `BACKFILL_CONCURRENCY_LIMIT` | `16` (`64` full network) | max concurrent backfill tasks |
| `REPO_FETCH_TIMEOUT` | `5min` | timeout for fetching a repository |
| `VERIFY_SIGNATURES` | `full` | signature verification: `full`, `backfill-only`, or `none` |
| `PLC_URL` | `https://plc.wtf`, `https://plc.directory` (full network) | PLC directory base URL(s), comma-separated |
| `IDENTITY_CACHE_SIZE` | `100000` | number of identity entries to cache in memory |

## performance

| variable | default | description |
| :--- | :--- | :--- |
| `CACHE_SIZE` | `256` | database cache size in MB |

## rate limiting (relay mode)

| variable | default | description |
| :--- | :--- | :--- |
| `NEW_HOST_LIMIT` | `50` | max new hosts addable via `com.atproto.sync.requestCrawl` per day |
| `RATE_TIERS` | | comma-separated tier definitions in `name:base/mul/hourly/daily[/account_limit]` format |
| `TIER_RULES` | | comma-separated ordered glob rules in `pattern:tier_name` format; first match wins |
