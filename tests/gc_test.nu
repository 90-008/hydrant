#!/usr/bin/env nu
use common.nu *

def run-test-instance [name: string, scenario_closure: closure] {
    let port = 3004
    let debug_port = $port + 1
    let url = $"http://localhost:($port)"
    let debug_url = $"http://localhost:($debug_port)"
    let db_path = (mktemp -d -t hydrant_gc_test.XXXXXX)

    print $"--- running scenario: ($name) ---"
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    try {
        if not (wait-for-api $url) {
            error make {msg: "api failed to start"}
        }

        do $scenario_closure $url $debug_url
        
        print $"PASSED: ($name)\n"
    } catch { |e|
        print $"test failed: ($e.msg)"
        try { kill $instance.pid }
        exit 1
    }

    try { kill $instance.pid }
}

def wait-for-blocks [debug_url: string] {
    print "waiting for blocks to appear..."
    mut blocks = {}
    mut count = 0
    for i in 1..30 {
        $blocks = (http get $"($debug_url)/debug/iter?partition=blocks&limit=1000")
        $count = ($blocks.items | length)
        if $count > 0 {
            break
        }
        sleep 2sec
    }
    if $count == 0 {
        error make {msg: "FAILED: no blocks found after backfill"}
    }
    $count
}

def compact-and-check-blocks [debug_url: string, expected_count: int] {
    print "triggering major compaction on blocks partition..."
    http post -H [Content-Length 0] $"($debug_url)/debug/compact?partition=blocks" ""

    let blocks_after = http get $"($debug_url)/debug/iter?partition=blocks&limit=1000"
    let after_count = ($blocks_after.items | length)

    if $after_count != $expected_count {
        error make {msg: $"FAILED: expected ($expected_count) blocks after compaction, found ($after_count)"}
    }
}

def ack-all-events [debug_url: string, url: string] {
    print "acking all events..."
    mut items = []
    for i in 1..30 {
        let events = http get $"($debug_url)/debug/iter?partition=events&limit=1000"
        $items = $events.items
        if ($items | length) > 0 {
            break
        }
        sleep 2sec
    }
    
    if ($items | length) == 0 {
        error make {msg: "FAILED: no events to ack"}
    }

    let event_ids = ($items | each { |x| ($x | first | into int) })

    http post -t application/json $"($url)/stream/ack" { ids: $event_ids }
    print $"acked ($event_ids | length) events"
}

def main [] {
    let repo1 = "did:web:guestbook.gaze.systems"
    let repo2 = "did:plc:dfl62fgb7wtjj3fcbb72naae"

    run-test-instance "delete repo only" { |url, debug_url|
        print $"adding repo ($repo1) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($repo1) } ]
        
        let before_count = (wait-for-blocks $debug_url)
        print $"found ($before_count) blocks before GC"

        print "deleting repo..."
        http delete -t application/json $"($url)/repos" --data [ { did: ($repo1), delete_data: true } ]
        sleep 1sec

        compact-and-check-blocks $debug_url $before_count
    }

    run-test-instance "ack events only" { |url, debug_url|
        print $"adding repo ($repo1) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($repo1) } ]
        
        let before_count = (wait-for-blocks $debug_url)
        print $"found ($before_count) blocks before GC"

        ack-all-events $debug_url $url
        sleep 1sec

        compact-and-check-blocks $debug_url $before_count
    }

    run-test-instance "delete repo, ack events" { |url, debug_url|
        print $"adding repo ($repo1) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($repo1) } ]
        
        let before_count = (wait-for-blocks $debug_url)
        print $"found ($before_count) blocks before GC"

        print "deleting repo..."
        http delete -t application/json $"($url)/repos" --data [ { did: ($repo1), delete_data: true } ]

        ack-all-events $debug_url $url
        sleep 1sec

        compact-and-check-blocks $debug_url 0
    }

    run-test-instance "multiple repos" { |url, debug_url|
        print $"adding repo ($repo2) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($repo2) } ]
        let repo2_blocks = (wait-for-blocks $debug_url)
        print $"found ($repo2_blocks) blocks for repo2"

        print $"adding repo ($repo1) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($repo1) } ]
        
        # wait a bit more for repo1 blocks to finish
        sleep 5sec
        let total_blocks = (http get $"($debug_url)/debug/iter?partition=blocks&limit=1000000" | get items | length)
        print $"found ($total_blocks) total blocks before GC"

        print $"deleting repo ($repo1)..."
        http delete -t application/json $"($url)/repos" --data [ { did: ($repo1), delete_data: true } ]

        # ack events specifically for repo1? Actually wait, the events endpoint contains all events.
        # we will ack all events to be safe. Since repo2 is NOT deleted, its refcount should be fine even if events are acked.
        ack-all-events $debug_url $url
        sleep 1sec

        compact-and-check-blocks $debug_url $repo2_blocks
    }

    print "all gc tests passed!"
}
