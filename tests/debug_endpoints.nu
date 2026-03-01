#!/usr/bin/env nu
use common.nu *

def main [] {
    let did = "did:web:guestbook.gaze.systems"
    let port = 3003
    let debug_port = $port + 1
    let url = $"http://localhost:($port)"
    let debug_url = $"http://localhost:($debug_port)"
    let db_path = (mktemp -d -t hydrant_debug_test.XXXXXX)

    print $"testing debug endpoints..."
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    if (wait-for-api $url) {
        # Trigger backfill to populate some data
        print $"adding repo ($did) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($did) } ]
        
        if (wait-for-backfill $url) {
            print "backfill complete, testing debug endpoints"

            # 1. Test /debug/iter to find a key
            print "testing /debug/iter on records partition"
            let records = http get $"($debug_url)/debug/iter?partition=records&limit=1"
            
            if ($records.items | is-empty) {
                print "FAILED: /debug/iter returned empty items"
                exit 1
            }

            let first_item = ($records.items | first)
            let key_str = $first_item.0
            let value_cid = $first_item.1
            
            print $"found key: ($key_str)"
            print $"found value [cid]: ($value_cid)"

            if not ($key_str | str contains "|") {
                print "FAILED: key does not contain pipe separator"
                exit 1
            }

            # 2. Test /debug/get with that key (sent as string)
            print "testing /debug/get"
            let encoded_key = ($key_str | url encode)
            let get_res = http get $"($debug_url)/debug/get?partition=records&key=($encoded_key)"
            
            if $get_res.value != $value_cid {
                print $"FAILED: /debug/get returned different value. expected: ($value_cid), got: ($get_res.value)"
                exit 1
            }
            
            print "PASSED: /debug/iter and /debug/get works with string keys and JSON values"

            # 3. Test /debug/iter on events partition (should be JSON objects)
            print "testing /debug/iter on events partition"
            let events = http get $"($debug_url)/debug/iter?partition=events&limit=1"
            
            if ($events.items | is-empty) {
                # might be empty if no events yet (backfill only fills records?)
                # Backfill should generate events? ops.rs makes events.
                 print "WARNING: /debug/iter returned empty items for events (expected if async?)"
            } else {
                 let first_evt = ($events.items | first)
                 let val = $first_evt.1
                 let type = ($val | describe)
                 print $"found event value type: ($type)"
                 if not ($type | str starts-with "record") {
                     print $"FAILED: events value is not a record/object. got: ($type)"
                     exit 1
                 }
                 print "PASSED: /debug/iter on events returns JSON objects"
            }

        } else {
            print "backfill failed"
            exit 1
        }
    } else {
        print "api failed to start"
        exit 1
    }

    try { kill $instance.pid }
}
