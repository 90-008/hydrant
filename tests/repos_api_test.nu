#!/usr/bin/env nu

def test-repos [url: string] {
    print "verifying /repos pagination and filtering..."
    
    # 1. Test limit
    print "  testing limit=1..."
    let items = (http get $"($url)/repos?limit=1" | from json -o)
    let count = ($items | length)
    print $"    count: ($count)"
    if $count != 1 {
        print "    FAILED: expected 1 item"
        exit 1
    }

    # 2. Test partition=all
    print "  testing partition=all..."
    let all_items = (http get $"($url)/repos?partition=all" | from json -o)
    print $"    count: ($all_items | length)"

    # 3. Test cursor (if we have items)
    if ($all_items | length) > 1 {
        let first_did = ($all_items | get 0).did
        print $"  testing cursor with did ($first_did)..."
        let cursor_items = (http get $"($url)/repos?cursor=($first_did)&limit=1" | from json -o)
        if ($cursor_items | length) > 0 {
            let next_did = ($cursor_items | get 0).did
            if $first_did == $next_did {
                print "    FAILED: cursor did should be excluded"
                exit 1
            }
            print $"    next did: ($next_did)"
        }
    }

    # 4. Test partition=pending
    print "  testing partition=pending..."
    let pending_items = (http get $"($url)/repos?partition=pending" | from json -o)
    print $"    pending count: ($pending_items | length)"

    # 5. Test partition=resync
    print "  testing partition=resync..."
    let resync_items = (http get $"($url)/repos?partition=resync" | from json -o)
    print $"    resync count: ($resync_items | length)"

    print "all /repos pagination and filtering tests passed!"
}

def test-errors [url: string] {
    print "verifying /repos error handling..."

    # 1. Invalid DID in PUT
    print "  testing PUT /repos with invalid DID..."
    let resp_put = (http put -f -e -t application/json $"($url)/repos" { did: "invalid" })
    if $resp_put.status != 400 {
        print $"    FAILED: expected 400, got ($resp_put.status)"
        exit 1
    }

    # 2. Invalid DID in DELETE
    print "  testing DELETE /repos with invalid DID..."
    let resp_del = (http delete -f -e -t application/json $"($url)/repos" --data { did: "invalid" })
    if $resp_del.status != 400 {
        print $"    FAILED: expected 400, got ($resp_del.status)"
        exit 1
    }

    # 3. Invalid cursor in GET
    print "  testing GET /repos with invalid cursor..."
    let resp_get_cursor = (http get -f -e $"($url)/repos?cursor=invalid")
    if $resp_get_cursor.status != 400 {
        print $"    FAILED: expected 400, got ($resp_get_cursor.status)"
        exit 1
    }

    # 4. Invalid partition in GET
    print "  testing GET /repos with invalid partition..."
    let resp_get_part = (http get -f -e $"($url)/repos?partition=invalid")
    if $resp_get_part.status != 400 {
        print $"    FAILED: expected 400, got ($resp_get_part.status)"
        exit 1
    }

    print "all /repos error handling tests passed!"
}

def main [] {
    let port = 3001
    let url = $"http://localhost:($port)"
    let db_path = (mktemp -d -t hydrant_api_test.XXXXXX)

    print $"starting hydrant for API verification..."
    let binary = (build-hydrant)
    let instance = (start-hydrant $binary $db_path $port)

    if (wait-for-api $url) {
        # add a few repos
        let dids = [
            "did:plc:dfl62fgb7wtjj3fcbb72naae"
            "did:plc:q6gjnv26m4ay3m42ojvzx2m4"
        ]
        http put -t application/json $"($url)/repos" ($dids | each { |d| { did: $d } })
        
        test-repos $url
        test-errors $url
    }

    kill $instance.pid
}

# Helper to build hydrant
def build-hydrant [] {
    cargo build --release --quiet
    "./target/release/hydrant"
}

# Helper to start hydrant
def start-hydrant [binary: string, db_path: string, port: int] {
    let log_file = $"($db_path)/hydrant.log"
    let pid = (with-env { 
        HYDRANT_DATABASE_PATH: $db_path,
        HYDRANT_API_PORT: ($port | into string),
        HYDRANT_DEBUG_PORT: (($port + 1) | into string),
        HYDRANT_MODE: "filter",
        HYDRANT_LOG_LEVEL: "info"
    } {
        sh -c $"($binary) >($log_file) 2>&1 & echo $!" | str trim | into int
    })
    { pid: $pid, log: $log_file }
}

# Helper to wait for api
def wait-for-api [url: string] {
    for i in 1..20 {
        if (try { (http get $"($url)/health") == "OK" } catch { false }) {
            return true
        }
        sleep 500ms
    }
    false
}
