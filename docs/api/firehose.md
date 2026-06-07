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
  "last_failure": {
    "at": 1717239958,
    "kind": "tcp_refused",
    "detail": "connection refused"
  },
  "stats": {
    "connection_attempts": 2,
    "successful_connections": 1,
    "connect_errors": 1,
    "stream_errors": 0,
    "frames_read": 1200,
    "bytes_read": 22000000,
    "messages_decoded": 1200,
    "messages_forwarded": 1198,
    "messages_skipped": 2,
    "forward_errors": 0,
    "throttle_waits": 0,
    "throttle_wait_micros": 0,
    "should_process_micros": 8800,
    "send_waits": 1198,
    "send_wait_micros": 64000,
    "connect_elapsed_micros": 410000,
    "max_send_wait_micros": 9000,
    "max_should_process_micros": 1200,
    "max_throttle_wait_micros": 0,
    "last_connect_attempt_at": 1717239900,
    "last_connected_at": 1717239901,
    "last_frame_at": 1717239958,
    "last_decoded_at": 1717239958,
    "last_forwarded_at": 1717239958,
    "last_start_cursor": 123,
    "last_seq": 1322,
    "max_seq": 1322,
    "message_kinds": {
      "commit": 1197,
      "sync": 0,
      "identity": 0,
      "account": 3,
      "info": 0
    }
  },
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

`last_failure` is present while hydrant has recorded failure/backoff state for a source. `kind` is a compact category such as `dns`, `tcp_refused`, `tcp_timeout`, `tls`, `http_upgrade`, `websocket`, `decode`, `relay_error`, or `config`; `detail` contains the underlying error text.

`stats` is present only in builds compiled with the `firehose-diagnostics` feature. these counters are in-memory process diagnostics intended for polling and diffing. compare `last_seq`/`max_seq`, `frames_read`, `messages_forwarded`, `send_wait_micros`, `max_send_wait_micros`, and `stream_errors` across samples to distinguish source lag, reconnect churn, filtering, and worker-channel backpressure.

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

## GET /firehose/diagnostics

available only in builds compiled with the `firehose-diagnostics` feature. returns process-local diagnostics for firehose internals:

```json
{
  "relay_worker": {
    "shards": [
      {
        "id": 0,
        "received_messages": 1000,
        "info_messages": 0,
        "processed_messages": 999,
        "process_errors": 0,
        "commit_errors": 0,
        "process_message_micros": 100000,
        "stage_counts_micros": 20000,
        "stage_and_commit_micros": 900000,
        "apply_counts_micros": 30000,
        "broadcast_micros": 20000,
        "cursor_micros": 1000,
        "total_micros": 1100000,
        "max_process_message_micros": 1000,
        "max_stage_and_commit_micros": 10000,
        "max_total_micros": 12000,
        "last_received_at": 1717239958,
        "last_processed_at": 1717239958,
        "last_seq": 1322,
        "max_seq": 1322
      }
    ]
  }
}
```

poll this endpoint together with `/firehose/sources`. if source `send_wait_micros` is rising while relay-worker `stage_and_commit_micros` or `process_message_micros` dominates, the bottleneck is downstream of websocket reading rather than source connectivity.

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
