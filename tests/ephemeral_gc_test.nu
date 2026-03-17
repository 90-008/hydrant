#!/usr/bin/env nu
use common.nu *

# start hydrant in ephemeral mode
def run-ephemeral-instance [name: string, scenario_closure: closure] {
    let port = 3006
    let debug_port = $port + 1
    let url = $"http://localhost:($port)"
    let debug_url = $"http://localhost:($debug_port)"
    let db_path = (mktemp -d -t hydrant_ephemeral_gc_test.XXXXXX)

    print $"--- running scenario: ($name) ---"
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = (with-env { HYDRANT_EPHEMERAL: "true" } {
        start-hydrant $binary $db_path $port
    })

    try {
        if not (wait-for-api $url) {
            error make {msg: "api failed to start"}
        }

        do $scenario_closure $url $debug_url

        print $"PASSED: ($name)\n"
    } catch { |e|
        print $"test failed: ($e.msg)"
        try { kill --force $instance.pid }
        sleep 2sec
        exit 1
    }

    try { kill --force $instance.pid }
    sleep 2sec
}

def check-block-count [debug_url: string, expected: int] {
    let response = (http get -f -e $"($debug_url)/debug/iter?partition=blocks&limit=1000000")
    if $response.status != 200 {
        error make {msg: $"FAILED: debug/iter returned ($response.status)"}
    }
    let count = ($response.body.items | length)
    if $count != $expected {
        error make {msg: $"FAILED: expected ($expected) blocks, found ($count)"}
    }
    print $"block count verified: ($count)"
}

def trigger-ttl-tick [debug_url: string] {
    print "triggering ephemeral TTL tick..."
    let response = (http post -f -e -H [Content-Length 0] $"($debug_url)/debug/ephemeral_ttl_tick" "")
    if $response.status != 200 {
        error make {msg: $"FAILED: ephemeral_ttl_tick returned ($response.status)"}
    }
    print "TTL tick complete"
}

# in ephemeral mode, records are never written to the records keyspace, so
# common.nu's wait-for-backfill (which checks records > 0) hangs forever.
# poll pending == 0 and blocks > 0 instead.
def wait-for-ephemeral-backfill [url: string, debug_url: string] {
    print "waiting for ephemeral backfill to complete..."
    for i in 1..120 {
        let stats = (http get $"($url)/stats?accurate=true").counts
        let pending = ($stats.pending | into int)
        let blocks = ((http get -f -e $"($debug_url)/debug/iter?partition=blocks&limit=1").body.items | length)

        print $"[($i)/120] pending: ($pending), has_blocks: ($blocks > 0)"

        if ($pending == 0) and ($blocks > 0) {
            print "ephemeral backfill complete."
            return true
        }
        sleep 1sec
    }
    false
}

def main [] {
    let repo1 = "did:web:guestbook.gaze.systems"

    # verify TTL tick runs without error when no events are eligible for expiry
    run-ephemeral-instance "TTL tick is safe with no eligible events" { |url, debug_url|
        print $"adding repo ($repo1)..."
        http put -t application/json $"($url)/repos" [{ did: ($repo1) }]

        if not (wait-for-ephemeral-backfill $url $debug_url) {
            error make {msg: "backfill did not complete"}
        }

        let event_count = ((http get -f -e $"($debug_url)/debug/iter?partition=events&limit=1000").body.items | length)
        print $"found ($event_count) events after backfill"

        let before_blocks = ((http get -f -e $"($debug_url)/debug/iter?partition=blocks&limit=1000000").body.items | length)

        # immediately trigger TTL tick, watermarks are too recent, nothing should be pruned
        trigger-ttl-tick $debug_url

        let after_events = ((http get -f -e $"($debug_url)/debug/iter?partition=events&limit=1000").body.items | length)
        if $after_events != $event_count {
            error make {msg: $"FAILED: expected ($event_count) events after TTL tick, got ($after_events)"}
        }
        print "event count unchanged after TTL tick (no events eligible)"

        # blocks must also be unchanged
        check-block-count $debug_url $before_blocks
        print "block count unchanged after TTL tick (correct)"
    }

    # plant a past watermark, trigger the real TTL path, and verify all events and blocks are gone
    run-ephemeral-instance "TTL tick with past watermark deletes events and blocks" { |url, debug_url|
        print $"adding repo ($repo1)..."
        http put -t application/json $"($url)/repos" [{ did: ($repo1) }]

        if not (wait-for-ephemeral-backfill $url $debug_url) {
            error make {msg: "backfill did not complete"}
        }

        let events = (http get -f -e $"($debug_url)/debug/iter?partition=events&limit=10000").body.items
        let event_count = ($events | length)
        if $event_count == 0 {
            error make {msg: "FAILED: expected events after backfill, found none"}
        }
        print $"found ($event_count) events after backfill"

        let block_count = ((http get -f -e $"($debug_url)/debug/iter?partition=blocks&limit=1000000").body.items | length)
        if $block_count == 0 {
            error make {msg: "FAILED: expected blocks after backfill, found none"}
        }
        print $"found ($block_count) blocks after backfill"

        # get the highest event id so we can set the cutoff just above it
        let max_event_id = ($events | each { |item| ($item | first | into int) } | math max)
        print $"max event id: ($max_event_id)"

        # plant a watermark far enough in the past (now - 3601) pointing past all events
        # this causes the next TTL tick to see all events as eligible for pruning
        let past_ts = ((date now | into int) / 1_000_000_000 | into int) - 3601
        let cutoff_event_id = $max_event_id + 1
        print $"seeding watermark at ts=($past_ts) event_id=($cutoff_event_id)"
        let seed_response = (http post -f -e -H [Content-Length 0]
            $"($debug_url)/debug/seed_watermark?ts=($past_ts)&event_id=($cutoff_event_id)" "")
        if $seed_response.status != 200 {
            error make {msg: $"FAILED: seed_watermark returned ($seed_response.status)"}
        }

        trigger-ttl-tick $debug_url

        # all events should be pruned
        let remaining_events = ((http get -f -e $"($debug_url)/debug/iter?partition=events&limit=1000").body.items | length)
        if $remaining_events != 0 {
            error make {msg: $"FAILED: expected 0 events after TTL expiry, got ($remaining_events)"}
        }
        print "all events pruned"

        # all blocks should be deleted (refcounts all hit zero)
        check-block-count $debug_url 0
        print "all blocks deleted"
    }

    print "all ephemeral gc tests passed!"
}
