---
title: blue.microcosm.repo.*
---

these queries are ergonomic variants of record lookup APIs.

## blue.microcosm.repo.getRecordByUri

alias of `com.bad-example.repo.getUriRecord` with intention to stabilize under this name.

| param | required | description |
| :--- | :--- | :--- |
| `at_uri` | yes | the at-uri of the record. the identifier can be a DID or atproto handle, and the collection and rkey segments must be present. |
| `cid` | no | optional: the CID of the version of the record. if a newer version exists, returns not found. |

returns `{ uri, cid, value }`.

## com.bad-example.repo.getUriRecord

ergonomic complement to `com.atproto.repo.getRecord` which accepts an `at-uri` instead of individual `repo` / `collection` / `rkey` params.

| param | required | description |
| :--- | :--- | :--- |
| `at_uri` | yes | the at-uri of the record. the identifier can be a DID or atproto handle, and the collection and rkey segments must be present. |
| `cid` | no | optional: the CID of the version of the record. if a newer version exists, returns not found. |

returns `{ uri, cid, value }`.
