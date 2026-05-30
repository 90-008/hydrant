---
title: build features
---

`hydrant` has several optional compile-time features:

| feature | default | description |
| :--- | :--- | :--- |
| `indexer` | yes | makes hydrant act as an indexer. incompatible with the relay feature. |
| `indexer_stream` | yes | enables the event stream for the indexer. requires indexer feature. |
| `relay` | no | makes hydrant act as a relay. incompatible with the indexer feature. |
| `backlinks` | no | enables the backlinks indexer and XRPC endpoints (`blue.microcosm.links.*`). requires indexer feature. |
| `jetstream` | no | enables the jetstream-compatible `GET /subscribe` websocket stream for indexer, ephemeral indexer, and relay builds. requires `indexer_stream` or `relay`. hydrant stores live-event metadata only; replay is bounded by normal event ttl in ephemeral/relay mode. |
| `user-keyspace` | no | enables library API for creating keyspaces in the hydrant database. the API of the keyspace is exactly the same as the fjall version hydrant uses, no abstraction efforts are made. |

to build with a specific feature:

```bash
cargo build --release --features backlinks
```
