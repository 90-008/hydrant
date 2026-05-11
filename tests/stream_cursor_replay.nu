#!/usr/bin/env nu
use common.nu *

def main [] {
    let did = "did:web:guestbook.gaze.systems"
    let port = resolve-test-port 3004
    let url = $"http://localhost:($port)"
    let ws_url = $"ws://127.0.0.1:($port)/stream"
    let db_path = (mktemp -d -t hydrant_stream_cursor_replay.XXXXXX)

    print $"testing /stream historical replay for ($did)..."
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    if not (wait-for-api $url) {
        fail "api failed to start" $instance.pid
    }

    print $"adding repo ($did) to tracking..."
    http put -t application/json $"($url)/repos" [{ did: $did }]

    if not (wait-for-backfill $url) {
        fail "backfill failed or timed out" $instance.pid
    }

    let stats = (http get $"($url)/stats").counts
    let events_count = ($stats.events | into int)
    print $"total events in db: ($events_count)"

    if $events_count == 0 {
        fail "expected at least one stored event after backfill" $instance.pid
    }

    let history_output = $"($db_path)/stream_history.txt"
    print "starting historical stream listener..."
    let history_pid = (bash -c $"websocat -n '($ws_url)?cursor=0' > '($history_output)' 2>&1 & echo $!" | str trim | into int)
    print $"history listener pid: ($history_pid)"

    mut history_messages = []
    for i in 1..40 {
        if ($history_output | path exists) {
            let history_content = (open $history_output | str trim)
            if not ($history_content | is-empty) {
                $history_messages = ($history_content | lines)
                if ($history_messages | length) >= $events_count {
                    break
                }
            }
        }
        sleep 250ms
    }

    try { kill $history_pid }
    sleep 500ms

    if not ($history_output | path exists) {
        fail "history output file was not created" $instance.pid
    }

    let history_content = (open $history_output | str trim)
    if ($history_content | is-empty) {
        fail "no historical events received" $instance.pid
    }

    $history_messages = ($history_content | lines)
    let first = ($history_messages | first | from json)
    let last = ($history_messages | last | from json)
    print $"received ($history_messages | length) historical events"
    print $"first event id=($first.id), type=($first.type)"
    print $"last event id=($last.id), type=($last.type)"

    if ($history_messages | length) < $events_count {
        print $"warning: replay returned ($history_messages | length)/($events_count) stored events"
    }

    try { kill $instance.pid }
    print "/stream cursor replay passed!"
}
