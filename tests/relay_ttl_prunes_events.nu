#!/usr/bin/env nu
use common.nu *
use helpers/ephemeral_gc.nu [run-relay-instance trigger-ttl-tick]

def main [] {
    let port = resolve-test-port 3117

    run-relay-instance "Relay mode GC prunes relay_events" $port { |url, debug_url|
        print "seeding 50 dummy relay events..."
        let seed_res = (http post -f -e -H [Content-Length 0] $"($debug_url)/debug/seed_events?partition=relay_events&count=50" "")
        if $seed_res.status != 200 {
            error make {msg: $"FAILED: seed_events returned ($seed_res.status)"}
        }

        let events = (http get -f -e $"($debug_url)/debug/iter?partition=relay_events&limit=1000").body.items
        let event_count = ($events | length)
        if $event_count != 50 {
            error make {msg: $"FAILED: expected 50 events, found ($event_count)"}
        }
        print $"found ($event_count) relay events"

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

        let remaining_events = ((http get -f -e $"($debug_url)/debug/iter?partition=relay_events&limit=1000").body.items | length)
        if $remaining_events != 0 {
            error make {msg: $"FAILED: expected 0 relay events after TTL expiry, got ($remaining_events)"}
        }
        print "all relay events pruned"
    }

    print "relay TTL event pruning test passed!"
}
