---
title: systems.gaze.hydrant.*
---

these are some non-standard XRPCs that might be useful.

## systems.gaze.hydrant.countRecords

return the total number of stored records in a collection.

| param | required | description |
| :--- | :--- | :--- |
| `identifier` | yes | DID or handle of the repository. |
| `collection` | yes | NSID of the collection. |

returns `{ count }`.

## systems.gaze.hydrant.describeRepo

return account and identity information about this repo. this is equal to `com.atproto.repo.describeRepo`, except we don't return the full DID document. the handle is bi-directionally verified, if its invalid or the handle does not exist we return "handle.invalid".

| param | required | description |
| :--- | :--- | :--- |
| `identifier` | yes | DID or handle of the repository. |

returns `{ did, handle, pds, collections }`.
