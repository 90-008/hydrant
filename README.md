# hydrant

`hydrant` is an AT Protocol indexer built on the `fjall` database that handles sync for you. it's flexible, supporting both full-network indexing and filtered indexing (e.g., by DID), also allowing querying with XRPCs and providing an ordered event stream with cursor support.

you can see [random.wisp.place](https://tangled.org/did:plc:dfl62fgb7wtjj3fcbb72naae/random.wisp.place) for an example on how to use hydrant.

**WARNING: *the db format is not stable yet.*** it's in active development so if you are going to rely on the db format being stable, don't (eg. for query features, if you are using ephemeral mode this doesn't matter for example, or you dont mind losing your existing backfilled data in hydrant if you already processed them.).

## vs `tap`

while [`tap`](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) is designed as a firehose consumer and simply just propagates events while handling sync, `hydrant` is flexible, it allows you to directly query the database for records, and it also provides an ordered view of events, allowing the use of a cursor to fetch events from a specific point in time.

### stream behavior

the `WS /stream` (hydrant) and `WS /channel` (tap) endpoints have different designs:

| aspect | `tap` (`/channel`) | `hydrant` (`/stream`) |
| :--- | :--- | :--- |
| distribution | sharded work queue: events are load-balanced across connected clients. If 5 clients connect, each receives ~20% of events. | broadcast: every connected client receives a full copy of the event stream. if 5 clients connect, all 5 receive 100% of events. |
| cursors | server-managed: clients ACK messages. the server tracks progress and redelivers unacked messages. | client-managed: client provides `?cursor=123`. the server streams from that point. |
| persistence | events are stored in an outbox and sent to the consumer, removing them, so they can't be replayed once they are acked. | `record` events are replayable. `identity`/`account` are ephemeral. use `GET /repos/:did` to query identity / account info (handle, pds, signing key, etc.). |
| backfill | backfill events are mixed into the live queue and prioritized (per-repo, acting as synchronization barrier) by the server. | backfill simply inserts historical events (`live: false`) into the global event log. streaming is just reading this log sequentially. synchronization is the same as tap, `live: true` vs `live: false`. |
| event types | `record`, `identity` (includes status) | `record`, `identity` (handle), `account` (status) |

### multiple relay support

`hydrant` supports connecting to multiple relays simultaneously for both firehose ingestion and crawling. when `RELAY_HOSTS` is configured with multiple URLs:

- one independent firehose stream loop is spawned per relay
- one independent crawling loop is spawned per relay
- each relay maintains its own firehose / crawler cursor state
- all ingestion loops and crawlers share the same worker pool and database
- all crawlers share the same pending queue for backfill

## configuration

`hydrant` is configured via environment variables. all variables are prefixed with `HYDRANT_` (except `RUST_LOG`).

| variable | default | description |
| :--- | :--- | :--- |
| `DATABASE_PATH` | `./hydrant.db` | path to the database folder. |
| `RUST_LOG` | `info` | log filter directives (e.g., `debug`, `hydrant=trace`). [`tracing` env-filter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html). |
| `RELAY_HOST` | `wss://relay.fire.hose.cam/` | URL of the relay. |
| `RELAY_HOSTS` | | comma-separated list of relay URLs. if unset, falls back to `RELAY_HOST`. |
| `PLC_URL` | `https://plc.wtf`, `https://plc.directory` if full network | base URL(s) of the PLC directory (comma-separated for multiple). |
| `EPHEMERAL` | `false` | if enabled, no records are stored. events are only stored up to 10 minutes for playback. |
| `FULL_NETWORK` | `false` | if `true`, discovers and indexes all repositories in the network. |
| `FILTER_SIGNALS` | | comma-separated list of NSID patterns to use for the filter (e.g. `app.bsky.feed.post,app.bsky.graph.*`). |
| `FILTER_COLLECTIONS` | | comma-separated list of NSID patterns to use for the collections filter. |
| `FILTER_EXCLUDES` | | comma-separated list of DIDs to exclude from indexing. |
| `FIREHOSE_WORKERS` | `8` (`24` if full network) | number of concurrent workers for firehose events. |
| `BACKFILL_CONCURRENCY_LIMIT` | `16` (`64` if full network) | maximum number of concurrent backfill tasks. |
| `VERIFY_SIGNATURES` | `full` | signature verification level: `full`, `backfill-only`, or `none`. |
| `CURSOR_SAVE_INTERVAL` | `3` | interval (in seconds) to save the firehose cursor. |
| `REPO_FETCH_TIMEOUT` | `300` | timeout (in seconds) for fetching repositories. |
| `CACHE_SIZE` | `256` | size of the database cache in MB. |
| `IDENTITY_CACHE_SIZE` | `100000` | number of identity entries to cache. |
| `API_PORT` | `3000` | port for the API server. |
| `ENABLE_DEBUG` | `false` | enable debug endpoints. |
| `DEBUG_PORT` | `API_PORT + 1` | port for debug endpoints (if enabled). |
| `NO_LZ4_COMPRESSION` | `false` | disable lz4 compression for storage. |
| `ENABLE_FIREHOSE` | `true` | whether to ingest relay subscriptions. |
| `ENABLE_BACKFILL` | `true` | whether to backfill from PDS instances. |
| `ENABLE_CRAWLER` | `false` (if Filter), `true` (if Full) | whether to actively query the network for unknown repositories. |
| `CRAWLER_MAX_PENDING_REPOS` | `2000` | max pending repos for crawler. |
| `CRAWLER_RESUME_PENDING_REPOS` | `1000` | resume threshold for crawler pending repos. |

## api

### management

- `GET /filter`: get the current filter configuration.
- `PATCH /filter`: update the filter configuration.

#### filter mode

the `mode` field controls what gets indexed:

| mode | behaviour |
| :--- | :--- |
| `filter` | auto-discovers and backfills any account whose firehose commit touches a collection matching one of the `signals` patterns. you can also explicitly track individual repositories via the `/repos` endpoint regardless of matching signals. |
| `full` | index the entire network. `signals` are ignored for discovery, but `excludes` and `collections` still apply. |

#### fields

| field | type | description |
| :--- | :--- | :--- |
| `mode` | `"filter"` \| `"full"` | indexing mode (see above). |
| `signals` | set update | NSID patterns (e.g. `app.bsky.feed.post` or `app.bsky.*`) that trigger auto-discovery in `filter` mode. |
| `collections` | set update | NSID patterns used to filter which records are stored. if empty, all collections are stored. applies in all modes. |
| `excludes` | set update | set of DIDs to always skip, regardless of mode. checked before any other filter logic. |

#### set updates

each set field accepts one of two forms:

- **replace**: an array replaces the entire set — `["did:plc:abc", "did:web:example.org"]`
- **patch**: an object maps items to `true` (add) or `false` (remove) — `{"did:plc:abc": true, "did:web:example.org": false}`

#### NSID patterns

`signals` and `collections` support an optional `.*` suffix to match an entire namespace:

- `app.bsky.feed.post` — exact match only
- `app.bsky.feed.*` — matches any collection under `app.bsky.feed`

### repository management

- `GET /repos`: get an NDJSON stream of repositories and their sync status. supports pagination and filtering:
    - `limit`: max results (default 100, max 1000)
    - `cursor`: opaque key for paginating.
    - `partition`: `all` (default), `pending` (backfill queue), or `resync` (retries)
- `GET /repos/{did}`: get the sync status and metadata of a specific repository. also returns the handle, PDS URL and the atproto signing key (these won't be available before the repo has been backfilled once at least).
- `PUT /repos`: explicitly track repositories. accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same).
- `DELETE /repos`: untrack repositories. accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same).

### data access (xrpc)

`hydrant` implements the following XRPC endpoints under `/xrpc/`:

#### com.atproto.*

the following are implemented currently:
- `com.atproto.repo.getRecord`
- `com.atproto.repo.listRecords`

#### systems.gaze.hydrant.*

these are some non-standard XRPCs that might be useful.

##### systems.gaze.hydrant.countRecords

return the total number of stored records in a collection.

| param | required | description |
| :--- | :--- | :--- |
| `identifier` | yes | DID or handle of the repository. |
| `collection` | yes | NSID of the collection. |

returns `{ count }`.

### event stream

- `GET /stream`: subscribe to the event stream.
  - query parameters:
    - `cursor` (optional): start streaming from a specific event ID.

### stats

- `GET /stats`: get stats about the database:
  - `counts`: counts of repos, records, events, and errors, etc.
  - `sizes`: sizes of the database keyspaces on disk, in bytes.
