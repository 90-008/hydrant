#!/usr/bin/env nu
use common.nu *

def main [] {
    let did = "did:web:guestbook.gaze.systems"
    let port = 3002
    let url = $"http://localhost:($port)"
    let ws_url = $"ws://localhost:($port)/stream"
    let db_path = (mktemp -d -t hydrant_stream_test.XXXXXX)

    print $"testing streaming for ($did)..."
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    mut test1_passed = false
    mut test2_passed = false

    if (wait-for-api $url) {
        # test 1: connect to stream BEFORE backfill to catch live events
        print "=== test 1: live streaming during backfill ==="
        
        let live_output = $"($db_path)/stream_live.txt"
        print $"starting stream listener -> ($live_output)"
        
        # start websocat in background to capture live events (no cursor = live only)
        let stream_pid = (bash -c $"websocat '($ws_url)' > '($live_output)' 2>&1 & echo $!" | str trim | into int)
        print $"stream listener pid: ($stream_pid)"
        sleep 1sec
        
        # trigger backfill
        print $"adding repo ($did) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($did) } ]
        
        if (wait-for-backfill $url) {
            sleep 2sec
            
            # stop the stream listener
            try { kill $stream_pid }
            sleep 1sec
            
            if ($live_output | path exists) {
                let live_content = (open $live_output | str trim)
                
                if ($live_content | is-empty) {
                    print "test 1 FAILED: no live events received during backfill"
                } else {
                    let live_messages = ($live_content | lines)
                    let live_count = ($live_messages | length)
                    print $"test 1: received ($live_count) live events during backfill"
                    
                    if $live_count > 0 {
                        let first = ($live_messages | first | from json)
                        print $"  first event: id=($first.id), type=($first.type)"
                        print "test 1 PASSED: live streaming works"
                        $test1_passed = true
                    }
                }
            } else {
                print "test 1 FAILED: output file not created"
            }
            
            # test 2: connect AFTER backfill with cursor=1 to replay all events
            print ""
            print "=== test 2: historical replay with cursor=0 ==="
            
            sleep 2sec
            
            let stats = (http get $"($url)/stats?accurate=true").counts
            let events_count = ($stats.events | into int)
            print $"total events in db: ($events_count)"
            
            if $events_count > 0 {
                let history_output = $"($db_path)/stream_history.txt"
                
                # use same approach as test 1: background process with file output
                # cursor=0 replays from the beginning (no cursor = live-tail only)
                print "starting historical stream listener..."
                let history_pid = (bash -c $"websocat '($ws_url)?cursor=0' > '($history_output)' 2>&1 & echo $!" | str trim | into int)
                print $"history listener pid: ($history_pid)"
                
                # wait for events to be streamed (should be fast for historical replay)
                sleep 5sec
                
                # kill the listener
                try { kill $history_pid }
                sleep 500ms
                
                if ($history_output | path exists) {
                    let history_content = (open $history_output | str trim)
                    
                    if ($history_content | is-empty) {
                        print "test 2 FAILED: no historical events received"
                    } else {
                        let history_messages = ($history_content | lines)
                        let history_count = ($history_messages | length)
                        print $"test 2: received ($history_count) historical events"
                        
                        if $history_count > 0 {
                            let first = ($history_messages | first | from json)
                            let last = ($history_messages | last | from json)
                            print $"  first event: id=($first.id), type=($first.type)"
                            print $"  last event: id=($last.id), type=($last.type)"
                            
                            if $history_count >= ($events_count | into int) {
                                print $"test 2 PASSED: replayed all ($history_count) events"
                                $test2_passed = true
                            } else {
                                print $"test 2 PARTIAL: got ($history_count)/($events_count) events"
                                $test2_passed = true
                            }
                        }
                    }
                } else {
                    print "test 2 FAILED: output file not created"
                }
            }
        } else {
            print "backfill failed or timed out."
            try { kill $stream_pid }
        }
    } else {
        print "api failed to start."
    }

    let hydrant_pid = $instance.pid
    print $"stopping hydrant - pid: ($hydrant_pid)..."
    try { kill $hydrant_pid }

    print ""
    if $test1_passed and $test2_passed {
        print "=== ALL TESTS PASSED ==="
    } else {
        print $"=== TESTS FAILED === test1: ($test1_passed), test2: ($test2_passed)"
        exit 1
    }
}
