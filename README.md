# hydrant

`hydrant` is an AT Protocol indexer built on the `fjall` database. it's meant to be a flexible indexer, supporting both full-network indexing and filtered indexing (e.g., by DID), also allowing querying with XRPC's like `com.atproto.sync.getRepo`, `com.atproto.repo.listRecords`, and so on, which should allow many more usecases compared to just providing an event stream.

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

- `POST /repo/add`: register a DID, start backfilling, and subscribe to updates.
  - body: `{ "dids": ["did:plc:..."] }`
- `POST /repo/remove`: unregister a DID and delete all associated data.
  - body: `{ "dids": ["did:plc:..."] }`

### data access (xrpc)

`hydrant` implements some AT Protocol XRPC endpoints for reading data:

- `com.atproto.repo.getRecord`: retrieve a single record by collection and rkey.
- `com.atproto.repo.listRecords`: list records in a collection, with pagination.
- `systems.gaze.hydrant.countRecords`: count records in a collection.

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