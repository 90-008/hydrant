# filter management

- `GET /filter`: get the current filter configuration.
- `PATCH /filter`: update the filter configuration.

## filter mode

the `mode` field controls what gets indexed:

| mode | behaviour |
| :--- | :--- |
| `filter` | auto-discovers and backfills any account whose firehose commit touches a collection matching one of the `signals` patterns. you can also explicitly track individual repositories via the `/repos` endpoint regardless of matching signals. |
| `full` | index the entire network. `signals` are ignored for discovery, but `excludes` and `collections` still apply. |

## fields

| field | type | description |
| :--- | :--- | :--- |
| `mode` | `"filter"` \| `"full"` | indexing mode (see above). |
| `signals` | set update | NSID patterns (e.g. `app.bsky.feed.post` or `app.bsky.*`) that trigger auto-discovery in `filter` mode. |
| `collections` | set update | NSID patterns used to filter which records are stored. if empty, all collections are stored. applies in all modes. |
| `excludes` | set update | set of DIDs to always skip, regardless of mode. checked before any other filter logic. |

## set updates

each set field accepts one of two forms:

- **replace**: an array replaces the entire set, eg. `["app.bsky.feed.post", "app.bsky.graph.*"]`
- **patch**: an object maps items to `true` (add) or `false` (remove), eg. `{"app.bsky.feed.post": true, "app.bsky.graph.*": false}`

## NSID patterns

`signals` and `collections` support an optional `.*` suffix to match an entire namespace:

- `app.bsky.feed.post`: exact match only
- `app.bsky.feed.*`: matches any collection under `app.bsky.feed`
