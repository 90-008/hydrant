# database operations

- `POST /db/train`: train zstd compression dictionaries for the `repos`, `blocks`, and `events` keyspaces. dictionaries are written to disk; a restart is required to apply them. the crawler, firehose, and backfill worker are paused for the duration and restored on completion.
- `POST /db/compact`: trigger a full major compaction of all database keyspaces in parallel. the crawler, firehose, and backfill worker are paused for the duration and restored on completion.
