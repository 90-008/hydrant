#!/usr/bin/env nu
use common.nu *

def main [] {
    let port = resolve-test-port 3010
    let url = $"http://localhost:($port)"
    let ws_url = $"ws://localhost:($port)/stream"
    let db_path = (mktemp -d -t hydrant_stream_ping_test.XXXXXX)

    print "testing ping/pong handling on /stream..."
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    mut passed = false

    if (wait-for-api $url) {
        let log_file = $"($db_path)/ws.log"
        let pid_file = $"($db_path)/ws.pid"

        bash -c $"websocat -n --ping-interval 1 --ping-timeout 4 '($ws_url)' > '($log_file)' 2>&1 & echo $! > '($pid_file)'"
        sleep 200ms
        let ws_pid = (open $pid_file | str trim | into int)
        print $"websocat pid: ($ws_pid)"

        sleep 6sec

        let alive = (do { ^kill -0 $ws_pid } | complete | get exit_code) == 0
        if $alive {
            print "ping/pong test PASSED: connection alive after 6s of pings"
            $passed = true
            try { kill $ws_pid }
        } else {
            print "ping/pong test FAILED: websocat exited, pong likely not received in time"
            try { open $log_file | print }
        }
    } else {
        print "api failed to start."
    }

    let hydrant_pid = $instance.pid
    print $"stopping hydrant - pid: ($hydrant_pid)..."
    try { kill $hydrant_pid }

    if $passed {
        print "=== TEST PASSED ==="
    } else {
        print "=== TEST FAILED ==="
        exit 1
    }
}
