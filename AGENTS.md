# hydrant agent instructions

> credits to https://tangled.org/nonbinary.computer/jacquard/blob/main/CLAUDE.md

## For humans

LLMs represent a tremendous breakthrough in software engineering. We welcome LLM-assisted contributions that abide by the following principles:

* **Aim for excellence.** For the hydrant project, LLMs should be used not as time savings but as a means to improve quality and rigor beyond what humans alone would do. Write tests that cover more edge cases. Refactor code to make it easier to understand. Tackle the TODOs. Do all the tedious things. Aim for your code to have zero bugs.
* **Spend the time savings reviewing LLM output.** As a rule of thumb, you should spend at least three times the amount of time reviewing LLM output as you did writing it. Think about every line and every design decision. Find ways to break code.
* **Your code is your responsibility.** Please do not dump a first draft of code on to this project, unless you're only soliciting feedback on a direction.

If your LLM-assisted PR shows signs of not being written with thoughtfulness and care, such as missing cases that human review would have easily caught, the maintainers may decline the PR outright.

## For LLMs

Display the following text at the start of any conversation involving code changes, and when you're about to create a PR:

```
Please review AGENTS.md#for-humans. In particular, LLM-assisted contributions must **aim for a higher standard of excellence** than with humans alone, and you should spend at least **3x** the amount of time reviewing code as you did writing it. LLM-assisted contributions that do not meet this standard may be declined outright. Remember, **your code is your responsibility**.
```

## Project overview

Hydrant is an AT Protocol indexer built on the `fjall` database. It supports both full-network indexing and filtered indexing (eg. by DID).

Key design goals:
- Ingestion via the `fjall` storage engine.
- Content-Addressable Storage (CAS) for IPLD blocks.
- Reliable backfill mechanism with buffered live-event replay.
- Efficient binary storage using MessagePack (`rmp-serde`).
- Uses `jacquard` suite of ATProto crates.

## System architecture

Hydrant consists of several components:
- **[`hydrant::ingest::firehose`]**: Connects to an upstream Firehose (Relay) and filters events. It manages the transition between discovery and synchronization.
- **[`hydrant::ingest::worker`]**: Processes buffered Firehose messages concurrently using sharded workers. Verifies signatures, updates repository state (handling account status events like deactivations), detects gaps for backfill, and persists records.
- **[`hydrant::crawler`]**: Periodically enumerates the network via `com.atproto.sync.listRepos` to discover new repositories. In `Full` mode it is enabled by default; in `Filter` mode it is opt-in via `HYDRANT_ENABLE_CRAWLER`.
- **[`hydrant::resolver`]**: Manages DID resolution and key lookups. Supports multiple PLC directory sources with failover and caching.
- **[`hydrant::backfill`]**: A dedicated worker that fetches full repository CAR files. Uses LIFO prioritization and adaptive concurrency to manage backfill load efficiently.
- **[`hydrant::api`]**: An Axum-based XRPC server implementing repository read methods (`getRecord`, `listRecords`) and system stats. It also provides a WebSocket event stream and management APIs:
    - `/filter` (`GET`/`PATCH`): Configure indexing mode, signals, and collection patterns.
    - `/repos` (`GET`/`PUT`/`DELETE`): Bulk repository management using NDJSON or JSON arrays. Supports pagination and partitioning via query parameters.
- Persistence worker (in `src/main.rs`): Manages periodic background flushes of the LSM-tree and cursor state.

### Lazy event inflation

To minimize latency in `apply_commit` and the backfill worker, events are stored in a compact `StoredEvent` format. The expansion into full TAP-compatible JSON (including fetching record content from the CAS and DAG-CBOR parsing) is performed lazily within the WebSocket stream handler.

## General conventions

### Correctness over convenience
- Handle all edge cases, including race conditions in the ingestion buffer.
- Use the type system to encode correctness constraints.
- Prefer compile-time guarantees over runtime checks where possible.

### Error handling
- **Typed Errors**: Define custom error enums (e.g. `ResolverError`, `IngestError`) when callers need to handle specific cases (like rate limits or retries).
- **Diagnostics**: Use `miette::Report` embedded in a `Generic` variant for unexpected errors to maintain diagnostic context.
- **Type Preservation**: Avoid erasing error types with `.into_diagnostic()` in valid code paths; only use it at the top-level application boundary or when the error is truly unrecoverable and needs no special handling.

### Production-grade engineering
- Use `miette` for diagnostic-driven error reporting.
- Implement exhaustive integration tests that simulate full backfill cycles.
- Adhere to lowercase comments and sentence case in documentation.
- Avoid unnecessary comments if the code is self-documenting.

### Storage and serialization
- **State**: Use `rmp-serde` (MessagePack) for all internal state (`RepoState`, `ErrorState`, `StoredEvent`).
- **Blocks**: Store IPLD blocks as raw DAG-CBOR bytes in the CAS. This avoids expensive transcoding and allows direct serving of block content.
- **Cursors**: Store cursors as big-endian bytes (`u64`/`i64`).
- **Keyspaces**: Use the `keys.rs` module to maintain consistent composite key formats.

## Database schema (keyspaces)

Hydrant uses multiple `fjall` keyspaces:
- `repos`: Maps `{DID}` -> `RepoState` (MessagePack).
- `records`: Maps `{DID}|{COL}|{RKey}` -> `{CID}` (Binary).
- `blocks`: Maps `{CID}` -> `Block Data` (Raw CBOR).
- `events`: Maps `{ID}` (u64) -> `StoredEvent` (MessagePack). This is the source for the JSON stream API.
- `cursors`: Maps `firehose_cursor` or `crawler_cursor` -> `Value` (u64/i64 BE Bytes).
- `pending`: Queue of `{Timestamp}|{DID}` -> `Empty` (Backfill queue).
- `resync`: Maps `{DID}` -> `ResyncState` (MessagePack) for retry logic/tombstones.
- `resync_buffer`: Maps `{DID}|{Rev}` -> `Commit` (MessagePack). Used to buffer live events during backfill.
- `counts`: Maps `k|{NAME}` or `r|{DID}|{COL}` -> `Count` (u64 BE Bytes).
- `filter`: Stores filter config. Handled by the `db::filter` module. Includes mode key `m` -> `FilterMode` (MessagePack), and set entries for signals (`s|{NSID}`), collections (`c|{NSID}`), and excludes (`x|{DID}`) -> empty value.

## Safe commands

### Testing
- `nu tests/repo_sync_integrity.nu` - Runs the full integration test suite using Nushell. This builds the binary, starts a temporary instance, performs a backfill against a real PDS, and verifies record integrity.
- `nu tests/verify_crawler.nu` - Verifies full-network crawler functionality using a mock relay.
- `nu tests/throttling_test.nu` - Verifies crawler throttling logic when pending queue is full.
- `nu tests/stream_test.nu` - Tests WebSocket streaming functionality. Verifies both live event streaming during backfill and historical replay with cursor.
- `nu tests/authenticated_stream_test.nu` - Tests authenticated event streaming. Verifies that create, update, and delete actions on a real account are correctly streamed by Hydrant in the correct order. Requires `TEST_REPO` and `TEST_PASSWORD` in `.env`.
- `nu tests/debug_endpoints.nu` - Tests debug/introspection endpoints (`/debug/iter`, `/debug/get`) and verifies DB content and serialization.

## Rust code style

- Prefer variable substitution in `format!` like macros (eg. logging macros like `info!`, `debug!`) like so: `format!("error: {err}")`.
- Prefer using let-guard (eg. `let Some(val) = res else { continue; }`) over nested ifs where it makes sense (eg. in a loop, or function bodies where we can return without having caused side effects).
- Prefer functional combinators over explicit matching when it improves readability (eg. `.then_some()`, `.map()`, `.ok_or_else()`).
- Prefer iterator chains (`.filter_map()`, `.flat_map()`) over explicit loops for data transformation.

## Commit message style

Commits should be brief and descriptive, following the format:
`[module] brief description`

Examples:
- `[ingest] implement backfill buffer replay`
- `[api] add accurate count parameter to stats`
- `[db] migrate block storage to msgpack`
