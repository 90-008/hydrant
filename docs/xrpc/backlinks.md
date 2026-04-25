---
title: blue.microcosm.links.*
---

hydrant implements a subset of [microcosm constellation](https://constellation.microcosm.blue/) when it's built with the `backlinks` cargo feature (`cargo build --features backlinks`).

when enabled, hydrant indexes all AT URI and DID references found inside stored records into a reverse index. this lets you efficiently answer "what records link to this subject?".

## blue.microcosm.links.getBacklinks

return records that link to a given subject.

| param | required | description |
| :--- | :--- | :--- |
| `subject` | yes | AT URI or DID to look up backlinks for. |
| `source` | no | filter by source collection, e.g. `app.bsky.feed.like`. also accepts `collection:path` form to further filter by field path, e.g. `app.bsky.feed.like:subject.uri`. the path is matched against the dotted field path within the record (`.` is prepended automatically). |
| `did` | no | filter links to those from specific users. |
| `limit` | no | max results to return (default 50, max 100). |
| `cursor` | no | opaque pagination cursor from a previous response. |
| `reverse` | no | if `true`, return results in reverse order (default `false`). |

returns `{ backlinks: [{ uri, cid }], cursor? }`.

results are ordered by source record rkey (ascending by default, descending when `reverse=true`). the cursor is stable across new insertions for TID rkey records.

## blue.microcosm.links.getBacklinksCount

return the number of records that link to a given subject.

| param | required | description |
| :--- | :--- | :--- |
| `subject` | yes | AT URI or DID to count backlinks for. |
| `source` | no | filter by source collection (same format as `getBacklinks`). |

returns `{ count }`.
