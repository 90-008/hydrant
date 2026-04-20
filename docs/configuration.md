# configuration

hydrant is configured via environment variables, all prefixed with `HYDRANT_` (except `RUST_LOG`). a `.env` file in the working directory is loaded automatically.

## core

| variable | default | description |
| :--- | :--- | :--- |
| `DATABASE_PATH` | `./hydrant.db` | path to the database folder |
| `RUST_LOG` | `info` | log filter directives (e.g., `debug`, `hydrant=trace`). [tracing env-filter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) |
| `API_PORT` | `3000` | port for the API server |
| `ENABLE_DEBUG` | `false` | enable debug endpoints |
| `DEBUG_PORT` | `API_PORT + 1` | port for debug endpoints (if enabled) |

## indexing mode

| variable | default | description |
| :--- | :--- | :--- |
| `FULL_NETWORK` | `false` (indexer), `true` (relay) | if `true`, discover and index all repos in the network |
| `EPHEMERAL` | `false` (indexer), `true` (relay) | if enabled, no records are stored (in indexer mode). events are deleted after a certain duration (`EPHEMERAL_TTL`) |
| `EPHEMERAL_TTL` | `60min`, `3d` (relay) | how long to keep events before deletion |
| `ONLY_INDEX_LINKS` | `false` | indexer only. if enabled, record blocks are not stored, only the index (records, counts, events) is kept. `getRecord`, `listRecords`, and `getRepo` will return errors. the event stream still works but create/update events will not include record values |

## filter

| variable | default | description |
| :--- | :--- | :--- |
| `FILTER_SIGNALS` | | comma-separated list of NSID patterns to use for the filter (e.g. `app.bsky.feed.post,app.bsky.graph.*`) |
| `FILTER_COLLECTIONS` | | comma-separated list of NSID patterns to use for the collections filter. empty = store all |
| `FILTER_EXCLUDES` | | comma-separated list of DIDs to exclude from indexing |

## firehose

| variable | default | description |
| :--- | :--- | :--- |
| `RELAY_HOST` | `wss://relay.fire.hose.cam/` (indexer), empty (relay) | URL of a single firehose source |
| `RELAY_HOSTS` | | comma-separated list of firehose sources. if unset, falls back to `RELAY_HOST`. prefix a URL with `pds::` to mark it as a direct PDS connection (e.g. `pds::wss://pds.example.com`). bare URLs are treated as relays. defaults to empty in relay mode; PDS' are expected to be seeded via `SEED_HOSTS` or the firehose management API |
| `SEED_HOSTS` | `https://bsky.network` (relay) | comma-separated list of base URLs to call `com.atproto.sync.listHosts` on at startup. hydrant adds every non-banned host as a PDS firehose source |
| `ENABLE_FIREHOSE` | `true` | whether to ingest relay subscriptions |
| `FIREHOSE_WORKERS` | `8` (`24` full network) | number of concurrent workers for firehose events |
| `CURSOR_SAVE_INTERVAL` | `3sec` | how often to persist the firehose cursor |

## crawler

| variable | default | description |
| :--- | :--- | :--- |
| `CRAWLER_URLS` | relay hosts (full network), `https://lightrail.microcosm.blue` (filter) | comma-separated list of `[mode::]url` crawler sources. mode is `relay` or `by_collection`; bare URLs use the default mode. set to empty string to disable crawling |
| `ENABLE_CRAWLER` | `true` if full network or sources configured | whether to actively query the network for unknown repositories |
| `CRAWLER_MAX_PENDING_REPOS` | `2000` | max pending repos before the crawler pauses |
| `CRAWLER_RESUME_PENDING_REPOS` | `1000` | pending-repo count at which the crawler resumes |

## backfill & identity

| variable | default | description |
| :--- | :--- | :--- |
| `BACKFILL_CONCURRENCY_LIMIT` | `16` (`64` full network) | maximum number of concurrent backfill tasks |
| `REPO_FETCH_TIMEOUT` | `5min` | timeout for fetching a repository |
| `VERIFY_SIGNATURES` | `full` | signature verification level: `full`, `backfill-only`, or `none` |
| `PLC_URL` | `https://plc.wtf`, `https://plc.directory` (full network) | base URL(s) of the PLC directory, comma-separated |
| `IDENTITY_CACHE_SIZE` | `100000` | number of identity entries to cache in memory |

## performance

| variable | default | description |
| :--- | :--- | :--- |
| `CACHE_SIZE` | `256` | size of the database cache in MB |

## rate limiting (relay mode)

| variable | default | description |
| :--- | :--- | :--- |
| `NEW_HOST_LIMIT` | `50` | in relay mode, how many new hosts can be added via `com.atproto.sync.requestCrawl` per day |
| `RATE_TIERS` | | comma-separated list of named rate tier definitions in `name:base/mul/hourly/daily[/account_limit]` format (e.g. `trusted:5000/10.0/18000000/432000000/10000000`). the optional account limit prevents new accounts from being created on a PDS once reached. built-in tiers (`default`, `trusted`) are always present and can be overridden |
| `TIER_RULES` | | comma-separated ordered list of glob rules in `pattern:tier_name` format (e.g. `*.bsky.network:trusted`). rules are evaluated in order; first match wins. explicit API assignments via `PUT /pds/tiers` take precedence; the `default` tier is the final fallback. uses standard glob wildcards (`*`, `?`) matched against the PDS hostname |
