# PDS management

hydrant rate-limits firehose events per PDS. each PDS is assigned to a named rate tier that controls how aggressively hydrant limits events from it. two built-in tiers are always present: `default` (conservative limits for unknown operators) and `trusted` (higher limits for well-behaved operators). additional tiers can be defined via `RATE_TIERS`.

the per-second limit scales with the number of active accounts on the PDS: `max(per_second_base, accounts × per_second_account_mul)`.

you can also define an optional `account_limit` for a rate tier. if a PDS exceeds this number of active accounts, hydrant will reject any new account creation events from it.

the built-in tiers are defined as follows:
- `default`: `50` per sec (floor), `+0.5` per account. max `3_600_000`/hr, `86_400_000`/day. `100` account limit.
- `trusted`: `5000` per sec (floor), `+10.0` per account. max `18_000_000`/hr, `432_000_000`/day. `10_000_000` account limit.

tiers are resolved in this order:

1. **explicit API assignment**, set via `PUT /pds/tiers`, stored in the database, survives restarts.
2. **glob rules**, from `TIER_RULES`, evaluated in order; first match wins.
3. **`default` tier**, applied if no rule or explicit assignment matches.

deleting an API assignment reverts the host to glob-rule resolution, not necessarily back to `default`. if a rule like `*.bsky.network:trusted` matches the host, it will become trusted again without any further action.

- `GET /pds/tiers`: list all current tier assignments alongside the available tier definitions.
  - returns `{ "assignments": [{ "host": string, "tier": string }], "rate_tiers": { <name>: { "per_second_base": int, "per_second_account_mul": float, "per_hour": int, "per_day": int } } }`.
  - `assignments` only contains PDSes with an explicit API assignment. hosts without one resolve via glob rules or fall back to `default`.
- `PUT /pds/tiers`: assign a PDS to a named rate tier.
  - body: `{ "host": string, "tier": string }`.
  - `host` is the PDS hostname (e.g. `pds.example.com`).
  - `tier` must be one of the configured tier names. returns `400` if unknown.
  - assignments are persisted to the database and survive restarts.
  - re-assigning the same host updates the tier in place without creating a duplicate.
- `DELETE /pds/tiers`: remove an explicit tier assignment for a PDS.
  - query parameter: `?host=<hostname>` (e.g. `?host=pds.example.com`).
  - reverts the host to glob-rule resolution (not necessarily `default`, a matching `TIER_RULES` pattern still applies).
  - returns `200` even if no assignment existed.
- `GET /pds/rate-tiers`: list the available rate tier definitions.
  - returns a map of tier name to `{ "per_second_base", "per_second_account_mul", "per_hour", "per_day", "account_limit" }`.
