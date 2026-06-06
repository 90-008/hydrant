---
title: blue.microcosm.identity.*
---

these queries expose the verified mini-doc view of an atproto identity.

## blue.microcosm.identity.resolveMiniDoc

alias of `com.bad-example.identity.resolveMiniDoc` with intention to stabilize under this name.

| param | required | description |
| :--- | :--- | :--- |
| `identifier` | yes | handle or DID to resolve. |

returns `{ did, handle, pds, signing_key }`.

## com.bad-example.identity.resolveMiniDoc

like `com.atproto.identity.resolveIdentity`, but instead of the full DID doc it returns an atproto-relevant subset.

| param | required | description |
| :--- | :--- | :--- |
| `identifier` | yes | handle or DID to resolve. |

returns `{ did, handle, pds, signing_key }`.
