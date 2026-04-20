# firehose management

## GET /firehose/sources

list all currently active firehose sources. returns a JSON array of `{ "url": string, "persisted": bool, "is_pds": bool }`.

`persisted: true` means the source was added via the API and is stored in the database; it will survive a restart. `persisted: false` means the source came from `RELAY_HOSTS` and is not written to the database. `is_pds: true` means the source is a direct PDS connection with host authority enforcement enabled.

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
