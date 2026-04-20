# build features

`hydrant` has several optional compile-time features:

| feature | default | description |
| :--- | :--- | :--- |
| `indexer` | yes | makes hydrant act as an indexer. incompatible with the relay feature. |
| `indexer_stream` | yes | enables the event stream for the indexer. requires indexer feature. |
| `relay` | no | makes hydrant act as a relay. incompatible with the indexer feature. |
| `backlinks` | no | enables the backlinks indexer and XRPC endpoints (`blue.microcosm.links.*`). requires indexer feature. |

to build with a specific feature:

```bash
cargo build --release --features backlinks
```
