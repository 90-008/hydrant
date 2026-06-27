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
    - `/pds/tiers` (`GET`/`PUT`/`DELETE`): PDS custom rate limit tiers.
    - `/pds/rate-tiers` (`GET`): Predefined PDS rate tiers.
    - `/pds/banned` (`GET`/`PUT`/`DELETE`): Block/unblock indexing of specific PDS hosts.
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
- **Schema evolution**: Treat versioned wire types in `src/types.rs` as frozen snapshots once shipped. If a stored type changes shape, add a new versioned type, export the newest version for live code, and add a forward migration that explicitly deserializes the previous version and writes the new one. Do not modify older migration input/output types in place. For example, if `RepoState` changes after `v7`, add `v8::RepoState` and a `v7 -> v8` migration instead of mutating `v7`.

### Rate limiting & XRPC Client integration
- **Header preservation**: The `jacquard-common` client statelessly discards HTTP response headers on non-200 responses. If rate-limiting headers (like `Retry-After` or `ratelimit-reset`) need to be processed for error responses (e.g. 429 Too Many Requests), wrap the client in a custom transport-level `HttpClient` wrapper (like `ThrottledHttpClient`) to intercept the response and update the throttler state before the headers are discarded.
- **Pacing**: Use token bucket or leaky bucket pacing (`wait_for_allow`) in addition to task concurrency limits. Simply limiting task concurrency is insufficient to prevent 429 cascades when retrying many repositories on a single PDS.
- **Preemptive Throttling**: Distinguish preemptive skips (e.g., when a PDS is known to be throttled) from direct rate limit failures. Avoid incrementing repository error retry counts on preemptive skips to prevent compounding backoffs unnecessarily.

## Database schema (keyspaces)

Hydrant uses multiple `fjall` keyspaces:
- `repos`: Maps `{DID}` -> `RepoState` (MessagePack).
- `repo_metadata`: Maps `rm|{DID}` -> `RepoMetadata` (MessagePack).
- `records`: Maps `{DID}|{COL}|{RKey}` -> `{CID}` (Binary).
- `blocks`: Maps `{collection}|{CID bytes}` -> `Block Data` (Raw CBOR). The collection prefix enables per-collection zstd dictionary training.
- `events`: Maps `{ID}` (u64 BE) -> `StoredEvent` (MessagePack). This is the source for the JSON stream API.
- `cursors`: Maps per-relay cursor keys -> `Value` (u64/i64 BE Bytes). Keys: `firehose_cursor|{relay}`, `crawler_cursor|{relay}`, `by_collection_cursor|{url}|{collection}`.
- `pending`: Queue of `{ID}` (u64 BE) -> `Empty` (Backfill queue).
- `resync`: Maps `{DID}` -> `ResyncState` (MessagePack) for retry logic/tombstones.
- `resync_buffer`: Maps `{DID}|{Rev}` -> `Commit` (MessagePack). Used to buffer live events during backfill.
- `counts`: Maps `k|{NAME}` or `r|{DID}|{COL}` -> `Count` (u64 BE Bytes).
- `filter`: Stores filter config. Handled by the `db::filter` and `db::pds_meta` modules. Includes:
    - Mode key `m` -> `FilterMode` (MessagePack).
    - Set entries for signals (`s|{NSID}`), collections (`c|{NSID}`), and excludes (`x|{DID}`) -> empty value.
    - PDS rate tiers: `{host}|tier` -> tier name (UTF-8 string).
    - PDS host statuses: `{host}|status` -> `HostStatus` (MessagePack).
- `crawler`: Stores crawler and firehose source URLs with prefixed keys. Retry entries use `ret|{DID}` -> empty value. Crawler sources use `src|{URL}` -> empty value. Firehose sources use `firehose|{URL}` -> empty value.
- `relay_events` (feature-gated, relay mode): Maps `{SEQ}` (u64 BE) -> Relay event data.
- `jetstream_events` (feature-gated, jetstream mode): Maps `{time_us}|{ID}` (16 bytes) -> Jetstream event data.
- `backlinks` (feature-gated): Reverse index of record references for the backlinks feature.

## Safe commands

### Testing
- `nu tests/run_all.nu` - Runs all tests in parallel with automatically assigned free ports. Pass `--skip-creds` to skip tests requiring `.env` credentials or external account fixtures, or `--only=[<name>...]` to run a subset.
- `nu tests/api_crawler_sources.nu` - Tests `/crawler/sources` CRUD plus dynamic/configured source restart behavior.
- `nu tests/api_firehose_sources.nu` - Tests `/firehose/sources` CRUD behavior.
- `nu tests/api_pds_tiers.nu` - Tests PDS tier APIs, tier persistence, and custom rate tiers.
- `nu tests/api_pds_banned.nu` - Tests PDS ban APIs and ban persistence.
- `nu tests/api_repos.nu` - Tests the `/repos` API endpoints including pagination and validation errors.
- `nu tests/crawler_full_network.nu` - Verifies full-network crawler discovery and cursor persistence using a mock relay.
- `nu tests/crawler_pending_throttling.nu` - Verifies crawler throttling and resumption when pending queue limits are hit.
- `nu tests/crawler_by_collection.nu` - Tests by-collection crawling via `listReposByCollection`.
- `nu tests/stream_live_backfill.nu` - Tests live `/stream` events while a repo backfill is running.
- `nu tests/stream_cursor_replay.nu` - Tests `/stream?cursor=0` historical event replay.
- `nu tests/stream_ping.nu` - Tests ping/pong handling on `/stream`.
- `nu tests/authenticated_stream_single_relay.nu` - Tests authenticated event streaming through one relay. Requires `TEST_REPO` and `TEST_PASSWORD` in `.env`.
- `nu tests/authenticated_stream_multi_relay.nu` - Tests authenticated event streaming through multiple relays. Requires `TEST_REPO` and `TEST_PASSWORD` in `.env`.
- `nu tests/relay_subscribe_repos_ping.nu` - Tests ping/pong handling on relay-mode `subscribeRepos`.
- `nu tests/repo_count_resync.nu` - Verifies count repair across forced resync after stale create/delete events. Requires `TEST_REPO` and `TEST_PASSWORD` in `.env`.
- `nu tests/repo_sync_integrity.nu` - Performs a backfill against a real PDS, and verifies record integrity compared to hydrant. Defaults to `atproto.com`'s public repo; set `REPO_SYNC_INTEGRITY_DID` and optionally `REPO_SYNC_INTEGRITY_PDS` to test another repo.
- `nu tests/debug_endpoints.nu` - Tests debug/introspection endpoints (`/debug/iter`, `/debug/get`) and verifies DB content and serialization.
- `nu tests/signal_filter.nu` - Verifies signal-based filtered indexing.
- `nu tests/backlinks.nu` - Tests backlinks indexing and XRPC query endpoints (requires `backlinks` feature). Defaults to `support.bsky.team`'s public repo; set `BACKLINKS_TEST_DID` to test another repo.
- `nu tests/ephemeral_ttl_noop.nu` - Tests that a TTL tick leaves recent index events intact.
- `nu tests/ephemeral_ttl_prunes_events.nu` - Tests TTL pruning for ephemeral index events.
- `nu tests/relay_ttl_prunes_events.nu` - Tests TTL pruning for relay-mode events.
- `nu tests/pds_host_status_transitions.nu` - Tests PDS host status transitions through offline, active, throttled, and active.
- `nu tests/pds_tier_rule_status.nu` - Tests glob tier rules in PDS host status calculation.

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


<!-- headroom:rtk-instructions -->
# RTK (Rust Token Killer) - Token-Optimized Commands

When running shell commands, **always prefix with `rtk`**. This reduces context
usage by 60-90% with zero behavior change. If rtk has no filter for a command,
it passes through unchanged — so it is always safe to use.

## Key Commands
```bash
# Git (59-80% savings)
rtk git status          rtk git diff            rtk git log

# Files & Search (60-75% savings)
rtk ls <path>           rtk read <file>         rtk grep <pattern>
rtk find <pattern>      rtk diff <file>

# Test (90-99% savings) — shows failures only
rtk pytest tests/       rtk cargo test          rtk test <cmd>

# Build & Lint (80-90% savings) — shows errors only
rtk tsc                 rtk lint                rtk cargo build
rtk prettier --check    rtk mypy                rtk ruff check

# Analysis (70-90% savings)
rtk err <cmd>           rtk log <file>          rtk json <file>
rtk summary <cmd>       rtk deps                rtk env

# GitHub (26-87% savings)
rtk gh pr view <n>      rtk gh run list         rtk gh issue list

# Infrastructure (85% savings)
rtk docker ps           rtk kubectl get         rtk docker logs <c>

# Package managers (70-90% savings)
rtk pip list            rtk pnpm install        rtk npm run <script>
```

## Rules
- In command chains, prefix each segment: `rtk git add . && rtk git commit -m "msg"`
- For debugging, use raw command without rtk prefix
- `rtk proxy <cmd>` runs command without filtering but tracks usage
<!-- /headroom:rtk-instructions -->

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:7510c1e2 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
