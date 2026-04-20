# ingestion control

## GET /ingestion

get the current ingestion status. returns `{ "crawler": bool, "firehose": bool, "backfill": bool }`.

## PATCH /ingestion

enable or disable ingestion components at runtime without restarting. body: `{ "crawler"?: bool, "firehose"?: bool, "backfill"?: bool }`. only provided fields are updated.

when disabled, each component finishes its current task before pausing (e.g. the backfill worker completes any in-flight repo syncs, the firehose finishes processing the current message). they resume immediately when re-enabled.
