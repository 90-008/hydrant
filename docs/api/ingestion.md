---
title: ingestion control
---

## GET /ingestion

get the current ingestion status. returns `{ "crawler": bool, "firehose": bool, "backfill": bool }`.

## PATCH /ingestion

enable or disable ingestion components at runtime without restarting. only provided fields are updated.

| field | description |
| :--- | :--- |
| `crawler` | enable or disable the crawler |
| `firehose` | enable or disable the firehose |
| `backfill` | enable or disable the backfill worker |

when disabled, each component finishes its current task before pausing (e.g. the backfill worker completes any in-flight repo syncs, the firehose finishes processing the current message). they resume immediately when re-enabled.
