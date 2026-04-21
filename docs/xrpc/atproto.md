---
title: com.atproto.*
---

these are standard atproto endpoints. you can look at [the atproto api reference](https://docs.bsky.app/docs/category/http-reference) for more info.

the following are implemented currently:
- `com.atproto.repo.getRecord`
- `com.atproto.repo.listRecords`
- `com.atproto.repo.describeRepo` (also see `systems.gaze.hydrant.describeRepo`)
- `com.atproto.sync.getRepo` (`since` parameter not implemented!)
- `com.atproto.sync.getHostStatus`
- `com.atproto.sync.listHosts`
- `com.atproto.sync.getRepoStatus`
- `com.atproto.sync.listRepos`
- `com.atproto.sync.getLatestCommit`
- `com.atproto.sync.requestCrawl` (adds the host to firehose sources in relay mode)
- `com.atproto.sync.subscribeRepos` (WebSocket firehose stream, requires `relay` feature)
