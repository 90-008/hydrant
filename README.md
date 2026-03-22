# hydrant

`hydrant` is an AT Protocol indexer built on the `fjall` database that handles sync for you. it's flexible, supporting both full-network indexing and filtered indexing (e.g., by DID), also allowing querying with XRPCs and providing an ordered event stream with cursor support.

you can see [random.wisp.place](https://tangled.org/did:plc:dfl62fgb7wtjj3fcbb72naae/random.wisp.place) (standalone binary using http API) or the [statusphere example](./examples/statusphere.rs) (hydrant-as-library) for examples on how to use hydrant.

**WARNING: *the db format is not stable yet.*** it's in active development so if you are going to rely on the db format being stable, don't (eg. for query features, if you are using ephemeral mode this doesn't matter for example, or you dont mind losing your existing backfilled data in hydrant if you already processed them.).

## vs `tap`

while [`tap`](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) is designed as a firehose consumer and simply just propagates events while handling sync, `hydrant` is flexible, it allows you to directly query the database for records, and it also provides an ordered view of events, allowing the use of a cursor to fetch events from a specific point. it can act as both an indexer or an ephemeral view of some window of events.

### stream behavior

the `WS /stream` (hydrant) and `WS /channel` (tap) endpoints have different designs:

| aspect | `tap` (`/channel`) | `hydrant` (`/stream`) |
| :--- | :--- | :--- |
| distribution | sharded work queue: events are load-balanced across connected clients. If 5 clients connect, each receives ~20% of events. | broadcast: every connected client receives a full copy of the event stream. if 5 clients connect, all 5 receive 100% of events. |
| cursors | server-managed: clients ACK messages. the server tracks progress and redelivers unacked messages. | client-managed: client provides `?cursor=123`. the server streams from that point. |
| persistence | events are stored in an outbox and sent to the consumer, and removed from the outbox when acked. nothing is replayable. | `record` events are replayable. `identity`/`account` are ephemeral. use `GET /repos/:did` to query identity / account info (handle, pds, signing key, etc.). |
| backfill | backfill events are mixed into the live queue and prioritized (per-repo, acting as synchronization barrier) by the server. | backfill simply inserts historical events (`live: false`) into the global event log. streaming is just reading this log sequentially. synchronization is the same as tap, `live: true` vs `live: false`. |
| event types | `record`, `identity` (includes status) | `record`, `identity` (handle, cache-buster), `account` (status) |

### multiple relay support

`hydrant` supports connecting to multiple relays simultaneously for firehose ingestion. when `RELAY_HOSTS` is configured with multiple URLs:

- one independent firehose stream loop is spawned per relay
- each relay maintains its own firehose cursor state
- all ingestion loops share the same worker pool and database

commit events are de-duplicated according to the repo `rev`. account / identity events are de-duplicated using the `time` field.
todo: decide what to do on relay-side account takedowns or if relays set the `time` field.

### crawler sources

the crawler is configured separately from the firehose via `CRAWLER_URLS`. each source is a `[mode::]url` entry where the mode prefix is optional and defaults to `by_collection` in filter mode or `relay` in full-network mode.

- `relay`: enumerates the network via `com.atproto.sync.listRepos`, then checks each repo's collections via `describeRepo`. used for full-network discovery.
- `by_collection`: queries `com.atproto.sync.listReposByCollection` for each configured signal. more efficient for filtered indexing since it only surfaces repos that have matching records.
cursors are stored per collection.

```
CRAWLER_URLS=by_collection::https://lightrail.microcosm.blue,relay::wss://bsky.network
```

each source maintains its own cursor so restarts resume mid-pass.

## configuration

`hydrant` is configured via environment variables. all variables are prefixed with `HYDRANT_` (except `RUST_LOG`).

| variable | default | description |
| :--- | :--- | :--- |
| `DATABASE_PATH` | `./hydrant.db` | path to the database folder. |
| `RUST_LOG` | `info` | log filter directives (e.g., `debug`, `hydrant=trace`). [`tracing` env-filter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html). |
| `RELAY_HOST` | `wss://relay.fire.hose.cam/` | URL of the relay (firehose only). |
| `RELAY_HOSTS` | | comma-separated list of relay URLs (firehose only). if unset, falls back to `RELAY_HOST`. |
| `CRAWLER_URLS` | relay hosts in full-network mode, `https://lightrail.microcosm.blue` in filter mode | comma-separated list of `[mode::]url` crawler sources. mode is `relay` or `by_collection`; bare URLs use the default mode. set to empty string to disable crawling. |
| `PLC_URL` | `https://plc.wtf`, `https://plc.directory` if full network | base URL(s) of the PLC directory (comma-separated for multiple). |
| `EPHEMERAL` | `false` | if enabled, no records are stored. events are deleted after a certain duration (`EPHEMERAL_TTL`). |
| `EPHEMERAL_TTL` | `60min` | decides after how long events should be deleted. |
| `FULL_NETWORK` | `false` | if `true`, discovers and indexes all repositories in the network. |
| `FILTER_SIGNALS` | | comma-separated list of NSID patterns to use for the filter (e.g. `app.bsky.feed.post,app.bsky.graph.*`). |
| `FILTER_COLLECTIONS` | | comma-separated list of NSID patterns to use for the collections filter. |
| `FILTER_EXCLUDES` | | comma-separated list of DIDs to exclude from indexing. |
| `FIREHOSE_WORKERS` | `8` (`24` if full network) | number of concurrent workers for firehose events. |
| `BACKFILL_CONCURRENCY_LIMIT` | `16` (`64` if full network) | maximum number of concurrent backfill tasks. |
| `VERIFY_SIGNATURES` | `full` | signature verification level: `full`, `backfill-only`, or `none`. |
| `CURSOR_SAVE_INTERVAL` | `3sec` | interval (in seconds) to save the firehose cursor. |
| `REPO_FETCH_TIMEOUT` | `5min` | timeout (in seconds) for fetching repositories. |
| `CACHE_SIZE` | `256` | size of the database cache in MB. |
| `IDENTITY_CACHE_SIZE` | `100000` | number of identity entries to cache. |
| `API_PORT` | `3000` | port for the API server. |
| `ENABLE_DEBUG` | `false` | enable debug endpoints. |
| `DEBUG_PORT` | `API_PORT + 1` | port for debug endpoints (if enabled). |
| `ENABLE_FIREHOSE` | `true` | whether to ingest relay subscriptions. |
| `ENABLE_CRAWLER` | `true` if full network or crawler sources are configured, `false` otherwise | whether to actively query the network for unknown repositories. |
| `CRAWLER_MAX_PENDING_REPOS` | `2000` | max pending repos for crawler. |
| `CRAWLER_RESUME_PENDING_REPOS` | `1000` | resume threshold for crawler pending repos. |

## api

### management

- `GET /filter`: get the current filter configuration.
- `PATCH /filter`: update the filter configuration.

#### ingestion control

- `GET /ingestion`: get the current ingestion status.
  - returns `{ "crawler": bool, "firehose": bool, "backfill": bool }`.
- `PATCH /ingestion`: enable or disable ingestion components at runtime without restarting.
  - body: `{ "crawler"?: bool, "firehose"?: bool, "backfill"?: bool }` — only provided fields are updated.
  - when disabled, each component finishes its current task before pausing (e.g. the backfill worker completes any in-flight repo syncs, the firehose finishes processing the current message). they resume immediately when re-enabled.

#### database operations

- `POST /db/train`: train zstd compression dictionaries for the `repos`, `blocks`, and `events` keyspaces. dictionaries are written to disk; a restart is required to apply them. the crawler, firehose, and backfill worker are paused for the duration and restored on completion.
- `POST /db/compact`: trigger a full major compaction of all database keyspaces in parallel. the crawler, firehose, and backfill worker are paused for the duration and restored on completion.
- `DELETE /cursors`: reset all stored cursors for a given URL. body: `{ "key": "..." }` where key is a URL. clears the relay crawler cursor, and any by-collection cursors associated with that URL. causes the next crawler pass to restart from the beginning.

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

#### blue.microcosm.links.*

hydrant implements a subset of [microcosm constellation](https://constellation.microcosm.blue/) when it's built with the `backlinks` cargo feature (`cargo build --features backlinks`).

when enabled, hydrant indexes all AT URI and DID references found inside stored records into a reverse index. this lets you efficiently answer "what records link to this subject?".

##### blue.microcosm.links.getBacklinks

return records that link to a given subject.

| param | required | description |
| :--- | :--- | :--- |
| `subject` | yes | AT URI or DID to look up backlinks for. |
| `source` | no | filter by source collection, e.g. `app.bsky.feed.like`. also accepts `collection:path` form to further filter by field path, e.g. `app.bsky.feed.like:subject.uri`. the path is matched against the dotted field path within the record (`.` is prepended automatically). |
| `limit` | no | max results to return (default 50, max 100). |
| `cursor` | no | opaque pagination cursor from a previous response. |
| `reverse` | no | if `true`, return results in reverse order (default `false`). |

returns `{ backlinks: [{ uri, cid }], cursor? }`.

results are ordered by source record rkey (ascending by default, descending when `reverse=true`). the cursor is stable across new insertions for TID rkey records.

##### blue.microcosm.links.getBacklinksCount

return the number of records that link to a given subject.

| param | required | description |
| :--- | :--- | :--- |
| `subject` | yes | AT URI or DID to count backlinks for. |
| `source` | no | filter by source collection (same format as `getBacklinks`). |

returns `{ count }`.

### event stream

- `GET /stream`: subscribe to the event stream.
  - query parameters:
    - `cursor` (optional): start streaming from a specific event ID.

### stats

- `GET /stats`: get stats about the database:
  - `counts`: counts of repos, records, events, and errors, etc.
  - `sizes`: sizes of the database keyspaces on disk, in bytes.
