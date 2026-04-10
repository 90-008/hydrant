#### table-of-contents

-> [hydrant](#hydrant)</br>
-> [vs tap](#vs-tap) | [stream](#stream-behavior) | [multi-relay](#multiple-relay-support) | [seeding](#firehose-seeding) | [crawler sources](#crawler-sources)</br>
-> [configuration](#configuration) | [build features](#build-features)</br>
-> [rest api](#rest-api) | [filter](#filter-management) | [ingestion](#ingestion-control) | [crawler](#crawler-management) | [firehose](#firehose-management) | [pds](#pds-management) | [repos](#repository-management)</br>
-> [xrpc api](#data-access-xrpc) | [atproto](#comatproto) | [backlinks](#bluemicrocosmlinks) | [identity](#bluemicrocosmidentity) | [custom](#systemsgazehydrant)

# hydrant

`hydrant` is an AT Protocol indexer built on the `fjall` database. it's built to
be flexible, supporting both full-network indexing and filtered indexing (e.g.,
by DID), allowing querying with XRPCs (not only `com.atproto.*`!), providing an
ordered event stream, etc. oh and it can also act as a relay!

you can see
[random.wisp.place](https://tangled.org/did:plc:dfl62fgb7wtjj3fcbb72naae/random.wisp.place)
(standalone binary using http API) or the [statusphere
example](./examples/statusphere.rs) (hydrant-as-library) for examples on how to
use hydrant (for rust docs look at https://hydrant.klbr.net/ for now).

**WARNING: *the db format is only partially stable.*** we provide migrations in hydrant
itself, so nothing should go wrong! you should still probably keep backups just in case!

## vs tap

<small>[<- back to toc](#table-of-contents)</small>

you can read [this blogpost](https://90008.leaflet.pub/3mhp3t4kuw22e) or read on below.

while [`tap`](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) is
designed as a firehose consumer and simply just propagates events while handling
sync, `hydrant` is flexible, it allows you to directly query the database for
records, and it also provides an ordered view of events, allowing the use of a
cursor to fetch events from a specific point. it can act as both an indexer or
an ephemeral view of some window of events.

### stream behavior

<small>[<- back to toc](#table-of-contents)</small>

the `WS /stream` (hydrant) and `WS /channel` (tap) endpoints have different designs:

| aspect | `tap` (`/channel`) | `hydrant` (`/stream`) |
| :--- | :--- | :--- |
| distribution | sharded work queue: events are load-balanced across connected clients. If 5 clients connect, each receives ~20% of events. | broadcast: every connected client receives a full copy of the event stream. if 5 clients connect, all 5 receive 100% of events. |
| cursors | server-managed: clients ACK messages. the server tracks progress and redelivers unacked messages. | client-managed: client provides `?cursor=123`. the server streams from that point. |
| persistence | events are stored in an outbox and sent to the consumer, and removed from the outbox when acked. nothing is replayable. | `record` events are replayable. `identity`/`account` are ephemeral. use `GET /repos/:did` to query identity / account info (handle, pds, signing key, etc.). |
| backfill | backfill events are mixed into the live queue and prioritized (per-repo, acting as synchronization barrier) by the server. | backfill simply inserts historical events (`live: false`) into the global event log. streaming is just reading this log sequentially. synchronization is the same as tap, `live: true` vs `live: false`. |
| event types | `record`, `identity` (includes status) | `record`, `identity` (handle, cache-buster), `account` (status) |

### multiple relay support

<small>[<- back to toc](#table-of-contents)</small>

`hydrant` supports connecting to multiple relays simultaneously for firehose
ingestion. when `RELAY_HOSTS` is configured with multiple URLs:

- one independent firehose stream loop is spawned per relay
- each relay maintains its own firehose cursor state
- all ingestion loops share the same worker pool and database

commit events are de-duplicated according to the repo `rev`. account / identity
events are de-duplicated using the `time` field. todo: decide what to do on
relay-side account takedowns or if relays set the `time` field.

#### direct PDS connections

a firehose source can also be a direct connection to a PDS rather than a relay.
prefix the URL with `pds::` to mark it as such:

```
HYDRANT_RELAY_HOSTS=wss://bsky.network,pds::wss://pds.example.com
```

only when a source is marked as a direct PDS (`is_pds: true`), hydrant enforces
host authority. relays (`is_pds: false`, the default) are exempt from this check,
since they forward commits from many PDSes by design. this means you will trust
the relay on this though.

#### firehose seeding

<small>[<- back to toc](#table-of-contents)</small>

in relay mode, `RELAY_HOSTS` defaults to empty. set `SEED_HOSTS` to one or more
relay base URLs and hydrant will call `com.atproto.sync.listHosts` on each at
startup, adding every returned PDS as a firehose source:

```
HYDRANT_SEED_HOSTS=https://bsky.network
```

seeding runs as a background task so the main firehose loop is not blocked. seed
URLs are fetched concurrently (up to four at a time) and the full `listHosts`
pagination is consumed for each. if a request fails partway through, the hosts
collected so far are still added and the failure is logged.

each discovered host is added as a persistent PDS firehose source (`is_pds: true`), 
equivalent to calling `POST /firehose/sources`.

banned hosts (`status: "banned"`) are skipped. all other statuses are included
since the firehose ingestor retries on disconnect and transiently-unavailable
hosts will reconnect on their own.

seeding runs from latest cursor on restart so new PDS' added to the upstream relay
since the last start are picked up automatically (if they haven't through firehose).
sources that are already running are detected and skipped, so re-seeding is idempotent.

### crawler sources

<small>[<- back to toc](#table-of-contents)</small>

the crawler is configured separately from the firehose via `CRAWLER_URLS`. each
source is a `[mode::]url` entry where the mode prefix is optional and defaults
to `by_collection` in filter mode or `list_repos` in full-network mode.

- `list_repos`: enumerates the network via `com.atproto.sync.listRepos`, checks
  each repo's collections via `describeRepo`.
- `by_collection`: queries `com.atproto.sync.listReposByCollection` for each
  configured signal. more efficient for filtered indexing since it only surfaces
  repos that have matching records. cursors are stored per collection. note that
  it won't crawl anything if no signals are specified.

```
CRAWLER_URLS=by_collection::https://lightrail.microcosm.blue,list_repos::wss://bsky.network
```

each source maintains its own cursor so restarts resume mid-pass.

sources can also be added and removed at runtime via the `/crawler/sources` API
(see [here](#crawler-management)). dynamically added sources are persisted to the
database and survive restarts. `CRAWLER_URLS` sources are startup-only: they are
not written to the database and will always reappear after a restart regardless of
runtime changes (unless you change the config of course).

## configuration

<small>[<- back to toc](#table-of-contents)</small>

`hydrant` is configured via environment variables. all variables are prefixed
with `HYDRANT_` (except `RUST_LOG`). if a `.env` file exists in the working
directory, it will also be loaded automatically.

| variable | default | description |
| :--- | :--- | :--- |
| `DATABASE_PATH` | `./hydrant.db` | path to the database folder. |
| `RUST_LOG` | `info` | log filter directives (e.g., `debug`, `hydrant=trace`). [`tracing` env-filter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html). |
| `RELAY_HOST` | `wss://relay.fire.hose.cam/` (indexer), empty (relay) | URL of a firehose source. |
| `RELAY_HOSTS` | | comma-separated list of firehose sources. if unset, falls back to `RELAY_HOST`. prefix a URL with `pds::` to mark it as a direct PDS connection (e.g. `pds::wss://pds.example.com`). bare URLs are treated as relays. defaults to empty in relay mode, PDS' are expected to be seeded via `SEED_HOSTS` or the firehose management API. |
| `SEED_HOSTS` | `https://bsky.network` (relay) | comma-separated list of base URLs to call `com.atproto.sync.listHosts` on at startup. hydrant adds every non-banned host as a PDS firehose source. see [firehose seeding](#firehose-seeding). |
| `CRAWLER_URLS` | relay hosts in full-network mode, `https://lightrail.microcosm.blue` in filter mode | comma-separated list of `[mode::]url` crawler sources. mode is `relay` or `by_collection`; bare URLs use the default mode. set to empty string to disable crawling. |
| `PLC_URL` | `https://plc.wtf`, `https://plc.directory` if full network | base URL(s) of the PLC directory (comma-separated for multiple). |
| `EPHEMERAL` | `false` | if enabled, no records are stored. events are deleted after a certain duration (`EPHEMERAL_TTL`). |
| `EPHEMERAL_TTL` | `60min`, `3d` in relay mode | decides after how long events should be deleted. |
| `FULL_NETWORK` | `false` (indexer), `true` (relay) | if `true`, discovers and indexes all repositories in the network. |
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
| `TRUSTED_HOSTS` | | comma-separated list of PDS hostnames to pre-assign to the `trusted` rate tier at startup. hosts not listed here use the `default` tier unless assigned via the API. |
| `RATE_TIERS` | | comma-separated list of named rate tier definitions in `name:base/mul/hourly/daily[/account_limit]` format (e.g. `trusted:5000/10.0/18000000/432000000/10000000`). the optional account limit prevents new accounts from being created on this PDS once reached. built-in tiers (`default`, `trusted`) are always present and can be overridden. |

## build features

<small>[<- back to toc](#table-of-contents)</small>

`hydrant` has several optional compile-time features:

| feature | default | description |
| :--- | :--- | :--- |
| `indexer` | yes | makes hydrant act as an indexer. incompatible with the relay feature. |
| `relay` | no | makes hydrant act as a relay. incompatible with the indexer feature. |
| `backlinks` | no | enables the backlinks indexer and XRPC endpoints (`blue.microcosm.links.*`). requires indexer feature. |

## REST api

<small>[<- back to toc](#table-of-contents)</small>

### event stream

- `GET /stream`: subscribe to the event stream.
  - query parameters:
    - `cursor` (optional): start streaming from a specific event ID.

### stats

- `GET /stats`: get stats about the database:
  - `counts`: counts of repos, records, events, and errors, etc.
  - `sizes`: sizes of the database keyspaces on disk, in bytes.

### filter management

<small>[<- back to toc](#table-of-contents)</small>

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

- **replace**: an array replaces the entire set, eg. `["did:plc:abc", "did:web:example.org"]`
- **patch**: an object maps items to `true` (add) or `false` (remove), eg. `{"did:plc:abc": true, "did:web:example.org": false}`

#### NSID patterns

`signals` and `collections` support an optional `.*` suffix to match an entire namespace:

- `app.bsky.feed.post`: exact match only
- `app.bsky.feed.*`: matches any collection under `app.bsky.feed`

### ingestion control

<small>[<- back to toc](#table-of-contents)</small>

- `GET /ingestion`: get the current ingestion status.
  - returns `{ "crawler": bool, "firehose": bool, "backfill": bool }`.
- `PATCH /ingestion`: enable or disable ingestion components at runtime without
  restarting.
  - body: `{ "crawler"?: bool, "firehose"?: bool, "backfill"?: bool }`. only provided fields are updated.
  - when disabled, each component finishes its current task before pausing (e.g.
    the backfill worker completes any in-flight repo syncs, the firehose
    finishes processing the current message). they resume immediately when
    re-enabled.

### crawler management

<small>[<- back to toc](#table-of-contents)</small>

- `GET /crawler/sources`: list all currently active crawler sources.
  - returns a JSON array of `{ "url": string, "mode": "relay" | "by_collection", "persisted": bool }`.
  - `persisted: true` means the source was added via the API and is stored in the
    database, it will survive a restart. `persisted: false` means the source
    came from `CRAWLER_URLS` and is not written to the database.
- `POST /crawler/sources`: add a crawler source at runtime.
  - body: `{ "url": string, "mode": "relay" | "by_collection" }`.
  - the source is written to the database before the producer task is started, so
    it is safe to add sources and then immediately restart without losing them.
  - if a source with the same URL already exists (whether from `CRAWLER_URLS` or
    a previous `POST`), it is replaced: the running task is stopped and a new one
    is started with the new mode. any cursor state for that URL is preserved.
  - returns `201 Created` on success.
- `DELETE /crawler/sources`: remove a crawler source at runtime.
  - body: `{ "url": string }`.
  - the producer task is stopped immediately.
  - if the source was added via the API (`persisted: true`), it is removed from
    the database and will not reappear on restart. if it came from `CRAWLER_URLS`
    (`persisted: false`), only the running task is stopped, the source will
    reappear on the next restart since `CRAWLER_URLS` is re-applied at startup.
    (unless you remove it manually from your configuration of course).
  - cursor state is not cleared. use `DELETE /crawler/cursors` separately if you want
    the source to restart from the beginning when re-added.
  - returns `200 OK` if the source was found and removed, `404 Not Found` otherwise.
- `DELETE /crawler/cursors`: reset stored cursors for a given crawler URL. body: `{ "key": "..." }`
  where key is a URL. clears the list-repos crawler cursor as well as any by-collection
  cursors associated with that URL. causes the next crawler pass to restart from the beginning.

### firehose management

<small>[<- back to toc](#table-of-contents)</small>

- `GET /firehose/sources`: list all currently active firehose sources.
  - returns a JSON array of `{ "url": string, "persisted": bool, "is_pds": bool }`.
  - `persisted: true` means the source was added via the API and is stored in the
    database, it will survive a restart. `persisted: false` means the source
    came from `RELAY_HOSTS` and is not written to the database.
  - `is_pds: true` means the source is a direct PDS connection with host authority enforcement enabled.
- `POST /firehose/sources`: add a firehose source at runtime.
  - body: `{ "url": string, "is_pds": bool }`. `is_pds` defaults to `false`.
  - the source is persisted to the database before the ingestor task is started.
  - if a source with the same URL already exists, it is replaced: the running
    task is stopped and a new one is started. any existing cursor state for that
    URL is preserved.
  - returns `201 Created` on success.
- `DELETE /firehose/sources`: remove a firehose relay at runtime.
  - body: `{ "url": string }`.
  - the ingestor task is stopped immediately.
  - if the source was added via the API (`persisted: true`), it is removed from
    the database and will not reappear on restart. if it came from `RELAY_HOSTS`
    (`persisted: false`), only the running task is stopped; the source reappears
    on the next restart.
  - cursor state is not cleared. use `DELETE /firehose/cursors` separately if you want
    the relay to restart from the beginning when re-added.
  - returns `200 OK` if the relay was found and removed, `404 Not Found` otherwise.
- `DELETE /firehose/cursors`: reset the stored cursor for a given firehose relay URL. body: `{ "key": "..." }`
  where key is a URL. causes the next firehose connection to restart from the beginning.

### PDS management

<small>[<- back to toc](#table-of-contents)</small>

hydrant rate-limits firehose events per PDS. each PDS is assigned to a named
rate tier that controls how aggressively hydrant limits events from it. two
built-in tiers are always present: `default` (conservative limits for unknown
operators) and `trusted` (higher limits for well-behaved operators). additional
tiers can be defined via `RATE_TIERS`.

the per-second limit scales with the number of active accounts on the PDS:
`max(per_second_base, accounts × per_second_account_mul)`.

you can also define an optional `account_limit` for a rate tier. if a PDS
exceeds this number of active accounts, hydrant will reject any new account
creation events from it.

the built-in tiers are defined as follows:
- `default`: `50` per sec (floor), `+0.5` per account. max `3_600_000`/hr, `86_400_000`/day. `100` account limit.
- `trusted`: `5000` per sec (floor), `+10.0` per account. max `18_000_000`/hr, `432_000_000`/day. `10_000_000` account limit.

- `GET /pds/tiers`: list all current tier assignments alongside the available
  tier definitions.
  - returns `{ "assignments": [{ "host": string, "tier": string }], "rate_tiers": { <name>: { "per_second_base": int, "per_second_account_mul": float, "per_hour": int, "per_day": int } } }`.
  - `assignments` only contains PDSes with an explicit assignment; any PDS not
    listed uses the `default` tier.
- `PUT /pds/tiers`: assign a PDS to a named rate tier.
  - body: `{ "host": string, "tier": string }`.
  - `host` is the PDS hostname (e.g. `pds.example.com`).
  - `tier` must be one of the configured tier names. returns `400` if unknown.
  - assignments are persisted to the database and survive restarts.
  - re-assigning the same host updates the tier in place without creating a duplicate.
- `DELETE /pds/tiers`: remove an explicit tier assignment for a PDS, reverting
  it to the `default` tier.
  - query parameter: `?host=<hostname>` (e.g. `?host=pds.example.com`).
  - returns `200` even if no assignment existed.
- `GET /pds/rate-tiers`: list the available rate tier definitions.
  - returns a map of tier name to `{ "per_second_base", "per_second_account_mul", "per_hour", "per_day", "account_limit" }`.

hosts listed in `TRUSTED_HOSTS` are seeded as `trusted` at startup, but only
when no database assignment already exists for that host — DB entries always win.
the seed is not written to the database, so it is re-applied on every restart.
consequences: if you remove a host from `TRUSTED_HOSTS` and it has no DB entry,
it will revert to `default` on the next restart. if you explicitly assign a host
via the API (which writes to the DB), that assignment persists regardless of
`TRUSTED_HOSTS`. if you delete a host's DB assignment via the API while it is
still listed in `TRUSTED_HOSTS`, it will be re-seeded as `trusted` on the next
restart.

### repository management

<small>[<- back to toc](#table-of-contents)</small>

all `/repos` endpoints that return lists respond with NDJSON by default. send `Accept: application/json` or `Content-Type: application/json` to get a JSON array instead.

- `GET /repos`: get a list of repositories and their sync status. supports pagination and filtering:
    - `limit`: max results (default 100, max 1000)
    - `cursor`: did key for paginating.
- `GET /repos/{did}`: get the sync status and metadata of a specific repository.
  also returns the handle, PDS URL and the atproto signing key (these won't be
  available before the repo has been backfilled once at least).
- `PUT /repos`: explicitly track repositories. accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same).
  only affects repositories that are not known or are untracked.
  returns a list of the DIDs that were queued for backfill.
- `DELETE /repos`: untrack repositories.
  accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same).
  only affects repositories that are currently tracked.
  returns a list of the DIDs that were untracked.
- `POST /repos/resync`: force a new backfill for one or more repositories.
  accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same).
  only affects repositories hydrant already knows about.
  returns a list of the DIDs that were queued.

### database operations

- `POST /db/train`: train zstd compression dictionaries for the `repos`,
  `blocks`, and `events` keyspaces. dictionaries are written to disk; a restart
  is required to apply them. the crawler, firehose, and backfill worker are
  paused for the duration and restored on completion.
- `POST /db/compact`: trigger a full major compaction of all database keyspaces
  in parallel. the crawler, firehose, and backfill worker are paused for the
  duration and restored on completion.

## data access (xrpc)

<small>[<- back to toc](#table-of-contents)</small>

`hydrant` implements the following XRPC endpoints under `/xrpc/`:

### com.atproto.*

<small>[<- back to toc](#table-of-contents)</small>

these are standard atproto endpoints. you can look at [the atproto api reference](https://docs.bsky.app/docs/category/http-reference) for more info.

the following are implemented currently:
- `com.atproto.repo.getRecord`
- `com.atproto.repo.listRecords`
- `com.atproto.repo.describeRepo` (also see `systems.gaze.hydrant.describeRepo`)
- `com.atproto.sync.getRepo` (`since` parameter not implemented!)
- `com.atproto.sync.getHostStatus`
- `com.atproto.sync.listHosts`
- `com.atproto.sync.getRepoStatus`
- `com.atproto.sync.listRepos`
- `com.atproto.sync.getLatestCommit`
- `com.atproto.sync.requestCrawl` (adds the host to firehose sources in relay mode)
- `com.atproto.sync.subscribeRepos` (WebSocket firehose stream, requires `relay` feature)

### systems.gaze.hydrant.*

<small>[<- back to toc](#table-of-contents)</small>

these are some non-standard XRPCs that might be useful.

#### systems.gaze.hydrant.countRecords

return the total number of stored records in a collection.

| param | required | description |
| :--- | :--- | :--- |
| `identifier` | yes | DID or handle of the repository. |
| `collection` | yes | NSID of the collection. |

returns `{ count }`.

#### systems.gaze.hydrant.describeRepo

return account and identity information about this repo.
this is equal to `com.atproto.repo.describeRepo`, except we don't return the full DID document.
the handle is bi-directionally verified, if its invalid or the handle does not exist we return
"handle.invalid".

| param | required | description |
| :--- | :--- | :--- |
| `identifier` | yes | DID or handle of the repository. |

returns `{ did, handle, pds, collections }`.

### blue.microcosm.links.*

<small>[<- back to toc](#table-of-contents)</small>

hydrant implements a subset of [microcosm constellation](https://constellation.microcosm.blue/) 
when it's built with the `backlinks` cargo feature (`cargo build --features backlinks`).

when enabled, hydrant indexes all AT URI and DID references found inside stored records into a 
reverse index. this lets you efficiently answer "what records link to this subject?".

#### blue.microcosm.links.getBacklinks

return records that link to a given subject.

| param | required | description |
| :--- | :--- | :--- |
| `subject` | yes | AT URI or DID to look up backlinks for. |
| `source` | no | filter by source collection, e.g. `app.bsky.feed.like`. also accepts `collection:path` form to further filter by field path, e.g. `app.bsky.feed.like:subject.uri`. the path is matched against the dotted field path within the record (`.` is prepended automatically). |
| `limit` | no | max results to return (default 50, max 100). |
| `cursor` | no | opaque pagination cursor from a previous response. |
| `reverse` | no | if `true`, return results in reverse order (default `false`). |

returns `{ backlinks: [{ uri, cid }], cursor? }`.

results are ordered by source record rkey (ascending by default, descending when `reverse=true`). 
the cursor is stable across new insertions for TID rkey records.

#### blue.microcosm.links.getBacklinksCount

return the number of records that link to a given subject.

| param | required | description |
| :--- | :--- | :--- |
| `subject` | yes | AT URI or DID to count backlinks for. |
| `source` | no | filter by source collection (same format as `getBacklinks`). |

returns `{ count }`.
