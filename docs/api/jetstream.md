---
title: jetstream stream
---

hydrant implements a jetstream-compatible websocket subscription endpoint when compiled with the `jetstream` feature. it allows clients to consume atproto repository updates (commits, identity updates, and account state transitions) via a simplified, JSON-based format.

## GET /subscribe

subscribe to the jetstream websocket stream.

### query parameters

| param | type | description |
| :--- | :--- | :--- |
| `wantedCollections` | seq<string> | list of collection NSIDs to receive (e.g. `app.bsky.feed.post`). supports namespace wildcards (e.g. `app.bsky.feed.*`). |
| `wantedDids` | seq<string> | list of DIDs to receive (e.g. `did:plc:abc123xyz`). |
| `wantedEventTypes` | seq<string> | list of event types to receive: `live` or `historical`. if not specified, both are returned. |
| `maxMessageSizeBytes` | integer | filters out events whose serialized JSON size exceeds this value. |
| `cursor` | integer | unix microseconds timestamp (`time_us`) to replay historical events from. |
| `compress` | boolean | if `true` (or if header `Socket-Encoding` contains `zstd`), compresses frames using zstd and sends them as binary websocket frames. |
| `requireHello` | boolean | if `true`, the socket will not stream any events until the client sends a `hello` / `options_update` message first. |

### in-stream options update

clients can dynamically modify filtering criteria (`wantedCollections`, `wantedDids`, `maxMessageSizeBytes`, and `wantedEventTypes`) without reconnecting by sending a text frame with the following JSON format:

```json
{
  "type": "options_update",
  "payload": {
    "wantedCollections": ["app.bsky.feed.post", "app.bsky.like.*"],
    "wantedDids": ["did:plc:abc123xyz"],
    "maxMessageSizeBytes": 5000000,
    "wantedEventTypes": ["live"]
  }
}
```

### event payload formats

all jetstream events are serialized as JSON objects containing:
* `did`: string (did of the repository)
* `time_us`: integer (unix microseconds timestamp)
* `kind`: string (`"commit"`, `"identity"`, or `"account"`)

#### commit

fired when a record is created, updated, or deleted:

```json
{
  "did": "did:plc:abc123xyz",
  "time_us": 1716823456789012,
  "kind": "commit",
  "commit": {
    "rev": "3kpjxabc123",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3kpjxabc123",
    "record": {
      "$type": "app.bsky.feed.post",
      "text": "hello, world!",
      "createdAt": "2026-05-27T12:00:00.000Z"
    },
    "cid": "bafyreihy..."
  }
}
```

if hydrant is running in indexer mode with `HYDRANT_ONLY_INDEX_LINKS=true`, record content blocks are not persisted. consequently, `record` and `cid` may be omitted from `commit` payloads.

#### identity

fired when a did document or handle updates:

```json
{
  "did": "did:plc:abc123xyz",
  "time_us": 1716823456789012,
  "kind": "identity",
  "identity": {
    "did": "did:plc:abc123xyz",
    "seq": 1234567,
    "time": "2026-05-27T12:00:00Z",
    "handle": "user.bsky.social"
  }
}
```

#### account

fired when a repository's status changes (active, deactivated, or deleted):

```json
{
  "did": "did:plc:abc123xyz",
  "time_us": 1716823456789012,
  "kind": "account",
  "account": {
    "active": false,
    "did": "did:plc:abc123xyz",
    "seq": 1234567,
    "time": "2026-05-27T12:00:00Z",
    "status": "deactivated"
  }
}
```

### additional details

- **live stream scope**: jetstream subscribers receive both live firehose events (`live: true`) and historical backfill / sync replay events (`live: false`) by default. be aware that historical backfills will interleave with live events unless filtered using the `wantedEventTypes` query parameter.
- **slow consumers**: if a socket's send buffer remains full for longer than the configured timeout, the server sends a `{"type":"error","error":"ConsumerTooSlow"}` message and drops the connection.
