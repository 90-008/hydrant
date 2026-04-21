---
title: getting started
---

## requirements

hydrant is written in rust and requires the rust toolchain (including `cargo`), `make`, `cmake` for some dependencies. you will also need the clang toolchain and the [wild linker](https://github.com/wild-linker/wild).

## building from source

```bash
cargo build --release
```

the binary will be at `target/release/hydrant`.

to build with optional features (e.g. `backlinks`):

```bash
cargo build --release --features backlinks
```

see [build features](build-features.md) for the full list.

## running

set the required environment variables and run the binary:

```bash
export HYDRANT_DATABASE_PATH=./hydrant.db
./target/release/hydrant
```

see [configuration](configuration.md) for all available variables. if a `.env` file exists in the working directory it will be loaded automatically.

## reverse proxying

it is **highly recommended** to run hydrant behind a reverse proxy (like nginx or caddy) if you intend to expose the XRPC or event stream APIs to the public. hydrant's API includes several management endpoints that do not require or support authentication. **you MUST NOT expose these management endpoints to the public internet.**

### public endpoints (safe to proxy)

- `/xrpc/*`: XRPC endpoints.
- `/stream`: hydrant's ordered event stream.
- `/stats`: general database statistics.
- `/health` / `/_health`: health check.

### management endpoints (keep private)

- `/repos`: explicit repository tracking/resyncing/untracking.
- `/filter`: management of NSID filter patterns.
- `/ingestion`: manual control over component lifecycle (crawler, firehose, etc.).
- `/crawler/sources`: management of crawler relays.
- `/firehose/sources`: management of firehose relays.
- `/pds/tiers`: rate-limit tier assignments.
- `/db/train` / `/db/compact`: database maintenance tasks.
- `*/cursors`: cursor management.
- `/debug/*`: introspection and testing endpoints.
