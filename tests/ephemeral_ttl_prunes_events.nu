#!/usr/bin/env nu
use common.nu *
use helpers/ephemeral_gc.nu [run-ephemeral-instance trigger-ttl-tick]

def main [] {
    let port = resolve-test-port 3017
    let repo = "did:web:guestbook.gaze.systems"

    run-ephemeral-instance "TTL tick with past watermark deletes events" $port { |url, debug_url|
        print $"adding repo ($repo)..."
        http put -t application/json $"($url)/repos" [{ did: $repo }]

        if not (wait-for-backfill $url) {
            error make {msg: "backfill did not complete"}
        }

        let events = (http get -f -e $"($debug_url)/debug/iter?partition=events&limit=10000").body.items
        let event_count = ($events | length)
        if $event_count == 0 {
            error make {msg: "FAILED: expected events after backfill, found none"}
        }
        print $"found ($event_count) events after backfill"

        let max_event_id = ($events | each { |item| ($item | first | into int) } | math max)
        let past_ts = ((date now | into int) / 1_000_000_000 | into int) - 3601
        let cutoff_event_id = $max_event_id + 1
        print $"seeding watermark at ts=($past_ts) event_id=($cutoff_event_id)"

        let seed_response = (http post -f -e -H [Content-Length 0]
            $"($debug_url)/debug/seed_watermark?ts=($past_ts)&event_id=($cutoff_event_id)" "")
        if $seed_response.status != 200 {
            error make {msg: $"FAILED: seed_watermark returned ($seed_response.status)"}
        }

        trigger-ttl-tick $debug_url

        let remaining_events = ((http get -f -e $"($debug_url)/debug/iter?partition=events&limit=1000").body.items | length)
        if $remaining_events != 0 {
            error make {msg: $"FAILED: expected 0 events after TTL expiry, got ($remaining_events)"}
        }
        print "all index events pruned"
    }

    print "ephemeral TTL event pruning test passed!"
}
