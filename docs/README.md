---
title: hydrant
---

`hydrant` is an AT Protocol indexer built on the `fjall` database. it's built to be flexible, supporting both full-network indexing and filtered indexing (e.g., by DID), allowing querying with XRPCs (not only `com.atproto.*`!), providing an ordered event stream, etc. oh and it can also act as a relay!

you can see [random.wisp.place](https://tangled.org/did:plc:dfl62fgb7wtjj3fcbb72naae/random.wisp.place) (standalone binary using http API) or the [statusphere example](https://tangled.org/did:plc:dfl62fgb7wtjj3fcbb72naae/hydrant/blob/main/examples/statusphere.rs) (hydrant-as-library) for examples. for rust docs look at https://hydrant.klbr.net/ for now.

**WARNING: *the db format is only partially stable.*** we provide migrations in hydrant itself, so nothing should go wrong! you should still probably keep backups just in case!

## what's here

- [vs tap](concepts/vs-tap.md): comparison against tap, the go sync utility
- [getting started](getting-started.md): building, running, reverse proxying
- [configuration](configuration.md): all environment variables
- [build features](build-features.md): optional cargo features (`relay`, `backlinks`, etc.)
- [concepts](concepts/README.md): how the stream works, relay comparison, multi-relay support
- [rest api](api/README.md): management API reference
- [xrpc](xrpc/README.md): data access via XRPC

## quick start

```bash
cargo build --release
export HYDRANT_DATABASE_PATH=./hydrant.db
./target/release/hydrant
```
