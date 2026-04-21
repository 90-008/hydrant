---
title: relay, seeding & crawler sources
---

## multiple relay support

`hydrant` supports connecting to multiple relays simultaneously for firehose ingestion. when `RELAY_HOSTS` is configured with multiple URLs:

- one independent firehose stream loop is spawned per relay
- each relay maintains its own firehose cursor state
- all ingestion loops share the same worker pool and database

commit events are de-duplicated according to the repo `rev`. account / identity events are de-duplicated using the `time` field.

## direct PDS connections

a firehose source can also be a direct connection to a PDS rather than a relay. prefix the URL with `pds::` to mark it as such:

```
HYDRANT_RELAY_HOSTS=wss://bsky.network,pds::wss://pds.example.com
```

only when a source is marked as a direct PDS (`is_pds: true`), hydrant enforces host authority. relays (`is_pds: false`, the default) are exempt from this check, since they forward commits from many PDSes by design.

## firehose seeding

in relay mode, `RELAY_HOSTS` defaults to empty. set `SEED_HOSTS` to one or more relay base URLs and hydrant will call `com.atproto.sync.listHosts` on each at startup, adding every returned PDS as a firehose source:

```
HYDRANT_SEED_HOSTS=https://bsky.network
```

seeding runs as a background task so the main firehose loop is not blocked. seed URLs are fetched concurrently (up to four at a time) and the full `listHosts` pagination is consumed for each. if a request fails partway through, the hosts collected so far are still added and the failure is logged.

each discovered host is added as a persistent PDS firehose source (`is_pds: true`), equivalent to calling `POST /firehose/sources`.

banned hosts (`status: "banned"`) are skipped. all other statuses are included since the firehose ingestor retries on disconnect and transiently-unavailable hosts will reconnect on their own.

seeding runs from latest cursor on restart so new PDS' added to the upstream relay since the last start are picked up automatically (if they haven't through firehose). sources that are already running are detected and skipped, so re-seeding is idempotent.

## crawler sources

the crawler is configured separately from the firehose via `CRAWLER_URLS`. each source is a `[mode::]url` entry where the mode prefix is optional and defaults to `by_collection` in filter mode or `list_repos` in full-network mode.

- `list_repos`: enumerates the network via `com.atproto.sync.listRepos`, checks each repo's collections via `describeRepo`.
- `by_collection`: queries `com.atproto.sync.listReposByCollection` for each configured signal. more efficient for filtered indexing since it only surfaces repos that have matching records. cursors are stored per collection. note that it won't crawl anything if no signals are specified.

```
CRAWLER_URLS=by_collection::https://lightrail.microcosm.blue,list_repos::wss://bsky.network
```

each source maintains its own cursor so restarts resume mid-pass.

sources can also be added and removed at runtime via the `/crawler/sources` API (see [crawler management](../api/crawler.md)). dynamically added sources are persisted to the database and survive restarts. `CRAWLER_URLS` sources are startup-only: they are not written to the database and will always reappear after a restart regardless of runtime changes (unless you change the config of course).
