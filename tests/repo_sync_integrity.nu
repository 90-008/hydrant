#!/usr/bin/env nu
use common.nu *

# compare records between hydrant and upstream pds
export def check-consistency [hydrant_url: string, pds_url: string, did: string] {
    print "comparing records with pds..."
    let collections = [
        "app.bsky.feed.post"
        "app.bsky.actor.profile"
    ]

    mut success = true

    for coll in $collections {
        for is_rev in [false, true] {
            print $"checking collection: ($coll) reverse:($is_rev)"

            let hydrant_records = (http get $"($hydrant_url)/xrpc/com.atproto.repo.listRecords?repo=($did)&collection=($coll)&reverse=($is_rev)").records
            let pds_records = (http get $"($pds_url)/xrpc/com.atproto.repo.listRecords?repo=($did)&collection=($coll)&reverse=($is_rev)").records

            let hydrant_count = ($hydrant_records | length)
            let pds_count = ($pds_records | length)

            print $"    hydrant count: ($hydrant_count), pds count: ($pds_count)"

            if $hydrant_count != $pds_count {
                print $"    mismatch in count for ($coll) rev:($is_rev)!"
                $success = false
                continue
            }

            if $hydrant_count > 0 {
                let h_first = ($hydrant_records | first).uri | split row "/" | last
                let p_first = ($pds_records | first).uri | split row "/" | last
                print $"    first rkey - hydrant: ($h_first), pds: ($p_first)"
            }

            # compare cids and rkeys
            for i in 0..($hydrant_count - 1) {
                let h_record = ($hydrant_records | get $i)
                let p_record = ($pds_records | get $i)
                
                if $h_record.cid != $p_record.cid {
                    let h_rkey = ($h_record.uri | split row "/" | last)
                    let p_rkey = ($p_record.uri | split row "/" | last)
                    print $"    mismatch at index ($i) for ($coll) rev:($is_rev):"
                    print $"      hydrant: ($h_rkey) -> ($h_record.cid)"
                    print $"      pds:     ($p_rkey) -> ($p_record.cid)"
                    $success = false
                }
            }
        }
    }
    $success
}

# verify countRecords API against debug endpoint
def check-count [hydrant_url: string, debug_url: string, did: string] {
    print "verifying countRecords API..."
    let collections = [
         "app.bsky.feed.post"
         "app.bsky.actor.profile"
    ]
    
    mut success = true

    for coll in $collections {
        print $"  checking count for ($coll)..."
        
        # 1. get cached count from API
        let api_count = try {
            (http get $"($hydrant_url)/xrpc/systems.gaze.hydrant.countRecords?identifier=($did)&collection=($coll)").count
        } catch {
            print $"    error calling countRecords API for ($coll)"
            return false
        }

        # 2. get actual scan count from debug endpoint
        let debug_count = try {
            (http get $"($debug_url)/debug/count?did=($did)&collection=($coll)").count
        } catch {
             print $"    error calling debug count for ($coll)"
             return false
        }

        print $"    api: ($api_count), debug scan: ($debug_count)"

        if $api_count != $debug_count {
            print $"    COUNT MISMATCH for ($coll)! api: ($api_count) vs scan: ($debug_count)"
            $success = false
        }
    }
    $success
}

def main [] {
    let did = "did:plc:dfl62fgb7wtjj3fcbb72naae"
    let pds = "https://zwsp.xyz"
    let port = 3001
    let url = $"http://localhost:($port)"
    let debug_url = $"http://127.0.0.1:($port + 1)"
    let db_path = (mktemp -d -t hydrant_test.XXXXXX)

    print $"testing backfill integrity for ($did)..."
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    mut success = false
    
    if (wait-for-api $url) {
        # track the repo via API
        print $"adding repo ($did) to tracking..."
        http post -t application/json $"($url)/repo/add" { dids: [($did)] }

        if (wait-for-backfill $url) {
            # Run both consistency checks
            let integrity_passed = (check-consistency $url $pds $did)
            let count_passed = (check-count $url $debug_url $did)

            if $integrity_passed and $count_passed {
                print "all integrity checks passed!"
                $success = true
            } else {
                print $"integrity checks failed. consistency: ($integrity_passed), count: ($count_passed)"
            }
        } else {
            print "backfill failed or timed out."
        }
    } else {
        print "api failed to start."
    }

    let hydrant_pid = $instance.pid
    print $"stopping hydrant - pid: ($hydrant_pid)..."
    try { kill $hydrant_pid }
    
    if not $success { exit 1 }
}