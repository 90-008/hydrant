#!/usr/bin/env nu
use common.nu *

def test-repos [url: string] {
    print "verifying /repos pagination and filtering..."

    # 1. test limit
    print "  testing limit=1..."
    let items = (http get $"($url)/repos?limit=1" | from json -o)
    if ($items | length) != 1 {
        fail "expected 1 item with limit=1"
    }
    print $"    count: ($items | length)"

    # 2. test partition=all
    print "  testing partition=all..."
    let all_items = (http get $"($url)/repos?partition=all" | from json -o)
    print $"    count: ($all_items | length)"

    # 3. test cursor (if we have enough items)
    if ($all_items | length) > 1 {
        let first_did = ($all_items | get 0).did
        print $"  testing cursor with did ($first_did)..."
        let cursor_items = (http get $"($url)/repos?cursor=($first_did)&limit=1" | from json -o)
        if ($cursor_items | length) > 0 {
            let next_did = ($cursor_items | get 0).did
            if $first_did == $next_did {
                fail "cursor did should be excluded from results"
            }
            print $"    next did: ($next_did)"
        }
    }

    # 4. test partition=pending
    print "  testing partition=pending..."
    let pending_items = (http get $"($url)/repos?partition=pending" | from json -o)
    print $"    pending count: ($pending_items | length)"

    # 5. test partition=resync
    print "  testing partition=resync..."
    let resync_items = (http get $"($url)/repos?partition=resync" | from json -o)
    print $"    resync count: ($resync_items | length)"

    print "all /repos pagination and filtering tests passed!"
}

def test-errors [url: string] {
    print "verifying /repos error handling..."

    # invalid DID in PUT
    print "  testing PUT /repos with invalid DID..."
    http put -f -e -t application/json $"($url)/repos" { did: "invalid" }
    | assert-status 400 "PUT /repos invalid DID"

    # invalid DID in DELETE
    print "  testing DELETE /repos with invalid DID..."
    http delete -f -e -t application/json $"($url)/repos" --data { did: "invalid" }
    | assert-status 400 "DELETE /repos invalid DID"

    # invalid cursor in GET
    print "  testing GET /repos with invalid cursor..."
    http get -f -e $"($url)/repos?cursor=invalid"
    | assert-status 400 "GET /repos invalid cursor"

    # invalid partition in GET
    print "  testing GET /repos with invalid partition..."
    http get -f -e $"($url)/repos?partition=invalid"
    | assert-status 400 "GET /repos invalid partition"

    print "all /repos error handling tests passed!"
}

def main [] {
    let port = resolve-test-port 3001
    let url = $"http://localhost:($port)"
    let db_path = (mktemp -d -t hydrant_repos_api.XXXXXX)

    print $"starting hydrant for repos API verification..."
    let binary = build-hydrant
    let instance = (with-env { HYDRANT_MODE: "filter" } {
        start-hydrant $binary $db_path $port
    })

    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    # seed a couple of repos so pagination tests have data
    let dids = [
        "did:plc:dfl62fgb7wtjj3fcbb72naae"
        "did:plc:q6gjnv26m4ay3m42ojvzx2m4"
    ]
    http put -t application/json $"($url)/repos" ($dids | each { |d| { did: $d } })

    test-repos $url
    test-errors $url

    kill $instance.pid
}
