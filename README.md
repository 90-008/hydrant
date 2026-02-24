# hydrant

`hydrant` is an AT Protocol indexer built on the `fjall` database. it's meant to be a flexible indexer, supporting both full-network indexing and filtered indexing (e.g., by DID), also allowing querying with XRPCs and providing an ordered event stream with cursor support.

## configuration

`hydrant` is configured via environment variables. all variables are prefixed with `HYDRANT_`.

| variable | default | description |
| :--- | :--- | :--- |
| `DATABASE_PATH` | `./hydrant.db` | path to the database folder. |
| `LOG_LEVEL` | `info` | log level (e.g., `debug`, `info`, `warn`, `error`). |
| `RELAY_HOST` | `wss://relay.fire.hose.cam` | websocket URL of the upstream firehose relay. |
| `PLC_URL` | `https://plc.wtf` | base URL(s) of the PLC directory (comma-separated for multiple). |
| `FULL_NETWORK` | `false` | if `true`, discovers and indexes all repositories in the network. |
| `FIREHOSE_WORKERS` | `8` (`32` if full network) | number of concurrent workers for firehose events. |
| `BACKFILL_CONCURRENCY_LIMIT` | `128` | maximum number of concurrent backfill tasks. |
| `VERIFY_SIGNATURES` | `full` | signature verification level: `full`, `backfill-only`, or `none`. |
| `CURSOR_SAVE_INTERVAL` | `5` | interval (in seconds) to save the firehose cursor. |
| `REPO_FETCH_TIMEOUT` | `300` | timeout (in seconds) for fetching repositories. |
| `CACHE_SIZE` | `256` | size of the database cache in MB. |
| `IDENTITY_CACHE_SIZE` | `1000000` | number of identity entries to cache. |
| `API_PORT` | `3000` | port for the API server. |
| `ENABLE_DEBUG` | `false` | enable debug endpoints. |
| `DEBUG_PORT` | `3001` | port for debug endpoints (if enabled). |
| `NO_LZ4_COMPRESSION` | `false` | disable lz4 compression for storage. |
| `DISABLE_FIREHOSE` | `false` | disable firehose ingestion. |
| `DISABLE_BACKFILL` | `false` | disable backfill processing. |
| `DB_WORKER_THREADS` | `4` (`8` if full network) | database worker threads. |
| `DB_MAX_JOURNALING_SIZE_MB` | `512` (`1024` if full network) | max database journaling size in MB. |
| `DB_PENDING_MEMTABLE_SIZE_MB` | `64` (`192` if full network) | pending memtable size in MB. |
| `DB_BLOCKS_MEMTABLE_SIZE_MB` | `64` (`192` if full network) | blocks memtable size in MB. |
| `DB_REPOS_MEMTABLE_SIZE_MB` | `64` (`192` if full network) | repos memtable size in MB. |
| `DB_EVENTS_MEMTABLE_SIZE_MB` | `64` (`192` if full network) | events memtable size in MB. |
| `DB_RECORDS_MEMTABLE_SIZE_MB` | `64` (`192` if full network) | records memtable size in MB. |
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
| `dids` | only index repositories explicitly listed in `dids`. new accounts seen on the firehose are ignored unless they are in the list. |
| `signal` | like `dids`, but also auto-discovers and backfills any account whose firehose commit touches a collection matching one of the `signals` patterns. |
| `full` | index the entire network. `dids` and `signals` are ignored for discovery, but `excludes` and `collections` still apply. |

#### fields

| field | type | description |
| :--- | :--- | :--- |
| `mode` | `"dids"` \| `"signal"` \| `"full"` | indexing mode (see above). |
| `dids` | set update | set of DIDs to explicitly track. in `dids` and `signal` modes, always processed regardless of signal matching. adding an untracked DID enqueues a backfill. |
| `signals` | set update | NSID patterns (e.g. `app.bsky.feed.post` or `app.bsky.*`) that trigger auto-discovery in `signal` mode. |
| `collections` | set update | NSID patterns used to filter which records are stored. if empty, all collections are stored. applies in all modes. |
| `excludes` | set update | set of DIDs to always skip, regardless of mode. checked before any other filter logic. |

#### set updates

each set field accepts one of two forms:

- **replace**: an array replaces the entire set — `["did:plc:abc", "did:plc:xyz"]`
- **patch**: an object maps items to `true` (add) or `false` (remove) — `{"did:plc:abc": true, "did:plc:xyz": false}`

#### NSID patterns

`signals` and `collections` support an optional `.*` suffix to match an entire namespace:

- `app.bsky.feed.post` — exact match only
- `app.bsky.feed.*` — matches any collection under `app.bsky.feed`

### data access (xrpc)

`hydrant` implements the following XRPC endpoints under `/xrpc/`:

#### `com.atproto.repo.getRecord`

retrieve a single record by its AT-URI components.

| param | required | description |
| :--- | :--- | :--- |
| `repo` | yes | DID or handle of the repository. |
| `collection` | yes | NSID of the collection. |
| `rkey` | yes | record key. |

returns the record value, its CID, and its AT-URI. responds with `RecordNotFound` if not present.

#### `com.atproto.repo.listRecords`

list records in a collection, newest-first by default.

| param | required | description |
| :--- | :--- | :--- |
| `repo` | yes | DID or handle of the repository. |
| `collection` | yes | NSID of the collection. |
| `limit` | no | max records to return (default `50`, max `100`). |
| `cursor` | no | opaque cursor for pagination (from a previous response). |
| `reverse` | no | if `true`, iterates oldest-first. |

returns `{ records, cursor }`. if `cursor` is present there are more results.

#### `systems.gaze.hydrant.countRecords`

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

- `GET /stats`: get aggregate counts of repos, records, events, and errors.

## vs `tap`

while [`tap`](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) is designed primarily as a firehose consumer and relay, `hydrant` is flexible, it allows you to directly query the database for records, and it also provides an ordered view of events, allowing the use of a cursor to fetch events from a specific point in time.

### stream behavior

the `WS /stream` (hydrant) and `WS /channel` (tap) endpoints have different designs:

| aspect | `tap` (`/channel`) | `hydrant` (`/stream`) |
| :--- | :--- | :--- |
| distribution | sharded work queue: events are load-balanced across connected clients. If 5 clients connect, each receives ~20% of events. | broadcast: every connected client receives a full copy of the event stream. If 5 clients connect, all 5 receive 100% of events. |
| cursors | server-managed: clients ACK messages. The server tracks progress and redelivers unacked messages. | client-managed: client provides `?cursor=123`. The server streams from that point. |
| backfill | integrated queue: backfill events are mixed into the live queue and prioritized by the server. | unified log: backfill simply inserts "historical" events (`live: false`) into the global event log. streaming is just reading this log sequentially. |
| event types | `record`, `identity` (includes status) | `record`, `identity` (handle), `account` (status) |
| persistence | **full**: all events are stored and replayable. | **hybrid**: `record` events are persisted/replayable. `identity`/`account` are ephemeral/live-only. |