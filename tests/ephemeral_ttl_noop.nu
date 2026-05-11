#!/usr/bin/env nu
use common.nu *
use helpers/ephemeral_gc.nu [run-ephemeral-instance trigger-ttl-tick]

def main [] {
    let port = resolve-test-port 3006
    let repo = "did:web:guestbook.gaze.systems"

    run-ephemeral-instance "TTL tick is safe with no eligible events" $port { |url, debug_url|
        print $"adding repo ($repo)..."
        http put -t application/json $"($url)/repos" [{ did: $repo }]

        if not (wait-for-backfill $url) {
            error make {msg: "backfill did not complete"}
        }

        let event_count = ((http get -f -e $"($debug_url)/debug/iter?partition=events&limit=1000").body.items | length)
        print $"found ($event_count) events after backfill"

        trigger-ttl-tick $debug_url

        let after_events = ((http get -f -e $"($debug_url)/debug/iter?partition=events&limit=1000").body.items | length)
        if $after_events != $event_count {
            error make {msg: $"FAILED: expected ($event_count) events after TTL tick, got ($after_events)"}
        }
        print "event count unchanged after TTL tick"
    }

    print "ephemeral TTL no-op test passed!"
}
