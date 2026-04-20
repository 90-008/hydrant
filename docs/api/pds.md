# PDS management

hydrant rate-limits firehose events per PDS. each PDS is assigned to a named rate tier that controls how aggressively hydrant limits events from it. two built-in tiers are always present: `default` (conservative limits for unknown operators) and `trusted` (higher limits for well-behaved operators). additional tiers can be defined via `RATE_TIERS`.

the per-second limit scales with the number of active accounts on the PDS: `max(per_second_base, accounts × per_second_account_mul)`.

you can also define an optional `account_limit` for a rate tier. if a PDS exceeds this number of active accounts, hydrant will reject any new account creation events from it.

the built-in tiers are:

| tier | per_second_base | per_second_account_mul | per_hour | per_day | account_limit |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `default` | 50 | +0.5 | 3,600,000 | 86,400,000 | 100 |
| `trusted` | 5000 | +10.0 | 18,000,000 | 432,000,000 | 10,000,000 |

tiers are resolved in this order: explicit API assignment (set via `PUT /pds/tiers`, stored in the database, survives restarts), then glob rules (from `TIER_RULES`, evaluated in order; first match wins), then the `default` tier (applied if nothing else matches).

deleting an API assignment reverts the host to glob-rule resolution, not necessarily back to `default`. if a rule like `*.bsky.network:trusted` matches the host, it will become trusted again without any further action.

## GET /pds/tiers

list all current tier assignments alongside the available tier definitions. returns `{ "assignments": [{ "host": string, "tier": string }], "rate_tiers": { <name>: { "per_second_base": int, "per_second_account_mul": float, "per_hour": int, "per_day": int } } }`.

`assignments` only contains PDSes with an explicit API assignment. hosts without one resolve via glob rules or fall back to `default`.

## PUT /pds/tiers

assign a PDS to a named rate tier. body: `{ "host": string, "tier": string }`.

`host` is the PDS hostname (e.g. `pds.example.com`). `tier` must be one of the configured tier names; returns `400` if unknown.

assignments are persisted to the database and survive restarts. re-assigning the same host updates the tier in place without creating a duplicate.

## DELETE /pds/tiers

remove an explicit tier assignment for a PDS. query parameter: `?host=<hostname>` (e.g. `?host=pds.example.com`).

reverts the host to glob-rule resolution (not necessarily `default`; a matching `TIER_RULES` pattern still applies).

returns `200` even if no assignment existed.

## GET /pds/rate-tiers

list the available rate tier definitions. returns a map of tier name to `{ "per_second_base", "per_second_account_mul", "per_hour", "per_day", "account_limit" }`.
