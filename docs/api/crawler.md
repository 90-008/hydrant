---
title: crawler management
---

## GET /crawler/sources

list all currently active crawler sources. returns a JSON array of `{ "url": string, "mode": "relay" | "by_collection", "persisted": bool }`.

`persisted: true` means the source was added via the API and is stored in the database; it will survive a restart. `persisted: false` means the source came from `CRAWLER_URLS` and is not written to the database.

## POST /crawler/sources

add a crawler source at runtime.

| field | description |
| :--- | :--- |
| `url` | URL of the crawler source |
| `mode` | `"relay"` or `"by_collection"` |

the source is written to the database before the producer task is started, so it is safe to add sources and then immediately restart without losing them.

if a source with the same URL already exists (whether from `CRAWLER_URLS` or a previous `POST`), it is replaced: the running task is stopped and a new one is started with the new mode. any cursor state for that URL is preserved.

returns `201 Created` on success.

## DELETE /crawler/sources

remove a crawler source at runtime.

| field | description |
| :--- | :--- |
| `url` | URL of the source to remove |

the producer task is stopped immediately.

if the source was added via the API (`persisted: true`), it is removed from the database and will not reappear on restart. if it came from `CRAWLER_URLS` (`persisted: false`), only the running task is stopped; the source will reappear on the next restart since `CRAWLER_URLS` is re-applied at startup.

cursor state is not cleared. use `DELETE /crawler/cursors` separately if you want the source to restart from the beginning when re-added.

returns `200 OK` if the source was found and removed, `404 Not Found` otherwise.

## DELETE /crawler/cursors

reset stored cursors for a given crawler URL.

| field | description |
| :--- | :--- |
| `key` | URL of the crawler source to reset |

clears the list-repos crawler cursor as well as any by-collection cursors associated with that URL. causes the next crawler pass to restart from the beginning.
