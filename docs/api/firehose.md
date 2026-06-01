---
title: firehose management
---

## GET /firehose/sources

list all known firehose sources, including offline ones waiting for the retry loop. returns a JSON array of:

```json
{
  "url": "ws://127.0.0.1:9000/",
  "is_pds": true,
  "running": false,
  "failing": true,
  "throttled": true,
  "consecutive_failures": 3,
  "throttled_until": 1717240000,
  "retry_in_secs": 42,
  "host_status": "offline",
  "pds": {
    "host": "127.0.0.1",
    "seq": 0,
    "account_count": 0,
    "status": "offline"
  }
}
```

`is_pds: true` means the source is a direct PDS connection with host authority enforcement enabled. `host_status` and `pds` are only present for PDS sources.

### query parameters

all filters are exact-match and optional. multiple filters are combined with logical `AND`.

| param | description |
| :--- | :--- |
| `host` | exact hostname from the source URL (e.g. `?host=localhost`) |
| `url` | exact source URL |
| `is_pds` | filter by direct-PDS vs relay source (`true` / `false`) |
| `running` | filter by whether the ingestor task is currently running |
| `failing` | filter by whether hydrant has recorded failures/backoff state for the source |
| `throttled` | filter by whether the source is currently inside a retry backoff window |

## GET /firehose/source

fetch a single firehose source with the same runtime fields as `GET /firehose/sources`.

### query parameters

provide exactly one of:

| param | description |
| :--- | :--- |
| `url` | exact source URL |
| `host` | exact hostname from the source URL |

returns `404` if no source matches. if `host` matches more than one source, returns `409 Conflict` and asks the caller to query by exact `url`.

## POST /firehose/sources

add a firehose source at runtime.

| field | description |
| :--- | :--- |
| `url` | URL of the firehose source |
| `is_pds` | whether the source is a direct PDS connection (default `false`) |

the source is persisted to the database before the ingestor task is started.

if a source with the same URL already exists, it is replaced: the running task is stopped and a new one is started. any existing cursor state for that URL is preserved.

returns `201 Created` on success.

## DELETE /firehose/sources

remove a firehose relay at runtime.

| field | description |
| :--- | :--- |
| `url` | URL of the source to remove |

the ingestor task is stopped immediately.

if the source was added via the API (`persisted: true`), it is removed from the database and will not reappear on restart. if it came from `RELAY_HOSTS` (`persisted: false`), only the running task is stopped; the source reappears on the next restart.

cursor state is not cleared. use `DELETE /firehose/cursors` separately if you want the relay to restart from the beginning when re-added.

returns `200 OK` if the relay was found and removed, `404 Not Found` otherwise.

## DELETE /firehose/cursors

reset the stored cursor for a given firehose relay URL.

| field | description |
| :--- | :--- |
| `key` | URL of the firehose source to reset |

causes the next firehose connection to restart from the beginning.
