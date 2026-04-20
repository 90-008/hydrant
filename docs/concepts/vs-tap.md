# hydrant vs tap

while [`tap`](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) is designed as a firehose consumer and simply just propagates events while handling sync, `hydrant` is flexible, it allows you to directly query the database for records, and it also provides an ordered view of events, allowing the use of a cursor to fetch events from a specific point. it can act as both an indexer or an ephemeral view of some window of events.

you can also read [this blogpost](https://90008.leaflet.pub/3mhp3t4kuw22e) for a longer comparison.

## stream behavior

the `WS /stream` (hydrant) and `WS /channel` (tap) endpoints have different designs:

| aspect | `tap` (`/channel`) | `hydrant` (`/stream`) |
| :--- | :--- | :--- |
| distribution | sharded work queue: events are load-balanced across connected clients. if 5 clients connect, each receives ~20% of events. | broadcast: every connected client receives a full copy of the event stream. if 5 clients connect, all 5 receive 100% of events. |
| cursors | server-managed: clients ACK messages. the server tracks progress and redelivers unacked messages. | client-managed: client provides `?cursor=123`. the server streams from that point. |
| persistence | events are stored in an outbox and sent to the consumer, and removed from the outbox when acked. nothing is replayable. | `record` events are replayable. `identity`/`account` are ephemeral. use `GET /repos/:did` to query identity / account info (handle, pds, signing key, etc.). |
| backfill | backfill events are mixed into the live queue and prioritized (per-repo, acting as synchronization barrier) by the server. | backfill simply inserts historical events (`live: false`) into the global event log. streaming is just reading this log sequentially. synchronization is the same as tap, `live: true` vs `live: false`. |
| event types | `record`, `identity` (includes status) | `record`, `identity` (handle, cache-buster), `account` (status) |
