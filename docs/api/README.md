# rest api

hydrant's REST API is split into public endpoints (safe to expose) and management endpoints (keep private). see [getting started](../getting-started.md#reverse-proxying) for guidance on what to expose.

## public

- `GET /stream`: subscribe to the event stream. query params: `cursor` (optional, start from a specific event ID).
- `GET /stats`: get stats about the database (counts of repos, records, events; sizes of keyspaces on disk).
- `GET /health` / `GET /_health`: health check.

## management

- [filter](filter.md): NSID filter configuration
- [ingestion](ingestion.md): enable/disable crawler, firehose, backfill at runtime
- [crawler](crawler.md): crawler source management
- [firehose](firehose.md): firehose source management
- [pds](pds.md): rate-limit tier assignments
- [repos](repos.md): explicit repository tracking, resyncing, untracking
- [database](database.md): compression training, compaction
