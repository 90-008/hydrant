#!/usr/bin/env nu
use common.nu *

def main [] {
    let did = "did:web:guestbook.gaze.systems"
    let port = resolve-test-port 3002
    let url = $"http://localhost:($port)"
    let ws_url = $"ws://127.0.0.1:($port)/stream"
    let db_path = (mktemp -d -t hydrant_stream_live_backfill.XXXXXX)

    print $"testing live /stream events during backfill for ($did)..."
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    if not (wait-for-api $url) {
        fail "api failed to start" $instance.pid
    }

    let live_output = $"($db_path)/stream_live.txt"
    print $"starting stream listener -> ($live_output)"
    let stream_pid = (bash -c $"websocat -n '($ws_url)' > '($live_output)' 2>&1 & echo $!" | str trim | into int)
    print $"stream listener pid: ($stream_pid)"
    sleep 2sec

    print $"adding repo ($did) to tracking..."
    http put -t application/json $"($url)/repos" [{ did: $did }]

    if not (wait-for-backfill $url) {
        try { kill $stream_pid }
        fail "backfill failed or timed out" $instance.pid
    }

    for i in 1..50 {
        if ($live_output | path exists) {
            let live_content = (open $live_output | str trim)
            if not ($live_content | is-empty) {
                break
            }
        }
        sleep 100ms
    }

    try { kill $stream_pid }
    sleep 1sec

    if not ($live_output | path exists) {
        fail "stream output file was not created" $instance.pid
    }

    let live_content = (open $live_output | str trim)
    if ($live_content | is-empty) {
        fail "no live events received during backfill" $instance.pid
    }

    let live_messages = ($live_content | lines)
    let first = ($live_messages | first | from json)
    print $"received ($live_messages | length) live events; first event id=($first.id), type=($first.type)"

    try { kill $instance.pid }
    print "live /stream during backfill passed!"
}
