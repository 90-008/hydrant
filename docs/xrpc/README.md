---
title: xrpc
---

`hydrant` implements the following XRPC endpoints under `/xrpc/`. only expose `/xrpc/*` publicly, see [getting started](../getting-started.md#reverse-proxying) for guidance.

- [com.atproto.*](atproto.md): standard AT Protocol endpoints
- [systems.gaze.hydrant.*](hydrant.md): hydrant-specific extensions
- [blue.microcosm.identity.*](identity.md): verified mini-doc identity lookups
- [blue.microcosm.repo.*](repo.md): at-uri based record lookups
- [blue.microcosm.links.*](backlinks.md): backlinks (requires `--features backlinks`)
