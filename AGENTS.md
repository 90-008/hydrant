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
- **[`hydrant::ingest::firehose`]**: Connects to one or more upstream Firehose relays and filters events. It manages the transition between discovery and synchronization. Multiple relay sources are supported and can be managed at runtime via the API.
- **[`hydrant::ingest::worker`]**: Processes buffered Firehose messages concurrently using sharded workers. Verifies signatures, updates repository state (handling account status events like deactivations), detects gaps for backfill, and persists records.
- **[`hydrant::crawler`]**: Enumerates the network to discover repositories. Supports two modes: `Relay` (via `com.atproto.sync.listRepos`) and `ByCollection` (via `com.atproto.sync.listReposByCollection`). Multiple sources can be configured, each with their own mode and cursor. In `Full` mode the relay crawler is enabled by default; `Filter` mode requires opt-in via `HYDRANT_ENABLE_CRAWLER`.
- **[`hydrant::resolver`]**: Manages DID resolution and key lookups. Supports multiple PLC directory sources with failover and caching.
- **[`hydrant::backfill`]**: A dedicated worker that fetches full repository CAR files. Uses LIFO prioritization and adaptive concurrency to manage backfill load efficiently.
- **[`hydrant::backlinks`]** (feature-gated): Maintains a reverse index of record references. Exposes `blue.microcosm.links.getBacklinks` and `blue.microcosm.links.getBacklinksCount` XRPC endpoints.
- **[`hydrant::api`]**: An Axum-based XRPC server implementing repository read methods (`getRecord`, `listRecords`, `countRecords`) and system stats. It also provides a WebSocket event stream and management APIs:
    - `/filter` (`GET`/`PATCH`): Configure indexing mode, signals, and collection patterns.
    - `/repos` (`GET`/`PUT`/`DELETE`): Repository management (supports pagination).
    - `/ingestion` (`GET`/`PATCH`): Pause/resume crawler, firehose, and backfill components at runtime.
    - `/crawler/sources` (`GET`/`POST`/`DELETE`): Manage crawler relay sources at runtime.
    - `/firehose/sources` (`GET`/`POST`/`DELETE`): Manage firehose relay sources at runtime.
    - `/db/train` (`POST`): Train per-keyspace zstd dictionaries.
    - `/db/compact` (`POST`): Trigger manual database compaction.
    - `/cursors` (`DELETE`): Reset cursors.
- Persistence worker (in `src/main.rs`): Manages periodic background flushes of the LSM-tree and cursor state.

### Lazy event inflation

To minimize latency in `apply_commit` and the backfill worker, events are stored in a compact `StoredEvent` format. The expansion into full TAP-compatible JSON (including fetching record content from the CAS and DAG-CBOR parsing) is performed lazily within the WebSocket stream handler.

### Library API

Hydrant can be used as an embedded library. The public surface is exposed via `src/lib.rs`:
- `hydrant::config` — configuration structs and builder helpers
- `hydrant::control` — high-level `Hydrant` handle, `RepoHandle`, `RepoManager`
- `hydrant::filter` — filter types and operations
- `hydrant::types` — shared data types (`RepoState`, etc.)

See `examples/statusphere.rs` for a usage example.

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
- **Blocks**: Store IPLD blocks as raw DAG-CBOR bytes in the CAS, keyed by `{collection}|{CID bytes}`. The collection prefix enables per-collection zstd dictionary training for better compression ratios.
- **Cursors**: Store cursors as big-endian bytes (`u64`/`i64`).
- **Compression**: Configurable via `HYDRANT_DATA_COMPRESSION` (`lz4`, `zstd`, `none`). Per-keyspace zstd dictionaries can be trained via `POST /db/train` and are stored as `dict_{keyspace}.bin` in the database directory.
- **Keyspaces**: Use the `keys.rs` module to maintain consistent composite key formats.

## Database schema (keyspaces)

Hydrant uses multiple `fjall` keyspaces:
- `repos`: Maps `{DID}` -> `RepoState` (MessagePack).
- `records`: Maps `{DID}|{COL}|{RKey}` -> `{CID}` (Binary).
- `blocks`: Maps `{collection}|{CID bytes}` -> `Block Data` (Raw CBOR). The collection prefix enables per-collection zstd dictionary training.
- `events`: Maps `{ID}` (u64 BE) -> `StoredEvent` (MessagePack). This is the source for the JSON stream API.
- `cursors`: Maps per-relay cursor keys -> `Value` (u64/i64 BE Bytes). Keys: `firehose_cursor|{relay}`, `crawler_cursor|{relay}`, `by_collection_cursor|{url}|{collection}`.
- `pending`: Queue of `{ID}` (u64 BE) -> `Empty` (Backfill queue).
- `resync`: Maps `{DID}` -> `ResyncState` (MessagePack) for retry logic/tombstones.
- `resync_buffer`: Maps `{DID}|{Rev}` -> `Commit` (MessagePack). Used to buffer live events during backfill.
- `counts`: Maps `k|{NAME}` or `r|{DID}|{COL}` -> `Count` (u64 BE Bytes).
- `filter`: Stores filter config. Handled by the `db::filter` module. Includes mode key `m` -> `FilterMode` (MessagePack), and set entries for signals (`s|{NSID}`), collections (`c|{NSID}`), and excludes (`x|{DID}`) -> empty value.
- `crawler`: Stores crawler and firehose source URLs with prefixed keys. Retry entries use `ret|{DID}` -> empty value. Crawler sources use `src|{URL}` -> empty value. Firehose sources use `firehose|{URL}` -> empty value.
- `backlinks` (feature-gated): Reverse index of record references for the backlinks feature.

## Safe commands

### Testing
- `nu tests/run_all.nu` - Runs all tests in parallel with automatically assigned free ports. Pass `--skip-creds` to skip tests requiring `.env` credentials, or `--only=[<name>...]` to run a subset.
- `nu tests/repo_sync_integrity.nu` - Performs a backfill against a real PDS, and verifies record integrity compared to hydrant.
- `nu tests/verify_crawler.nu` - Verifies full-network crawler functionality using a mock relay.
- `nu tests/throttling.nu` - Verifies crawler throttling logic when pending queue is full.
- `nu tests/stream.nu` - Tests WebSocket streaming functionality. Verifies both live event streaming during backfill and historical replay with cursor.
- `nu tests/authenticated_stream.nu` - Tests authenticated event streaming. Verifies that create, update, and delete actions on a real account are correctly streamed by Hydrant in the correct order. Requires `TEST_REPO` and `TEST_PASSWORD` in `.env`.
- `nu tests/debug_endpoints.nu` - Tests debug/introspection endpoints (`/debug/iter`, `/debug/get`) and verifies DB content and serialization.
- `nu tests/api.nu` - Tests management API endpoints (filter, repos, ingestion, sources).
- `nu tests/repos_api.nu` - Tests the `/repos` API endpoints including pagination and single-repo lookup.
- `nu tests/signal_filter.nu` - Verifies signal-based filtered indexing.
- `nu tests/by_collection.nu` - Tests by-collection crawling via `listReposByCollection`.
- `nu tests/backlinks.nu` - Tests backlinks indexing and XRPC query endpoints (requires `backlinks` feature).
- `nu tests/ephemeral_gc.nu` - Tests ephemeral mode TTL expiration and event watermark cleanup.

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

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **hydrant** (1339 symbols, 3645 relationships, 113 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/hydrant/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/hydrant/context` | Codebase overview, check index freshness |
| `gitnexus://repo/hydrant/clusters` | All functional areas |
| `gitnexus://repo/hydrant/processes` | All execution flows |
| `gitnexus://repo/hydrant/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## CLI

- Re-index: `npx gitnexus analyze`
- Check freshness: `npx gitnexus status`
- Generate docs: `npx gitnexus wiki`

<!-- gitnexus:end -->
