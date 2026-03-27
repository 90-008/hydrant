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

# compare getLatestCommit between hydrant and pds; returns the commit record on success, null on failure
def check-latest-commit [hydrant_url: string, pds_url: string, did: string] {
    print "checking getLatestCommit..."

    let h = try {
        http get $"($hydrant_url)/xrpc/com.atproto.sync.getLatestCommit?did=($did)"
    } catch {
        print "  error fetching getLatestCommit from hydrant"
        return null
    }

    let p = try {
        http get $"($pds_url)/xrpc/com.atproto.sync.getLatestCommit?did=($did)"
    } catch {
        print "  error fetching getLatestCommit from pds"
        return null
    }

    print $"  cid=($h.cid) rev=($h.rev)"

    if $h.cid != $p.cid or $h.rev != $p.rev {
        print $"  MISMATCH: hydrant cid=($h.cid) rev=($h.rev) vs pds cid=($p.cid) rev=($p.rev)"
        return null
    }

    print "  ok"
    $h
}

# parse `goat repo inspect` output into a record
def parse-goat-inspect []: string -> record {
    $in | lines | parse "{key}: {value}" | transpose -r -d
}

# fetch a getRepo CAR and return its `goat repo inspect` info
def fetch-car-info [url: string, did: string] {
    let car = http get $"($url)/xrpc/com.atproto.sync.getRepo?did=($did)"
    let tmp = (mktemp --suffix ".car")
    $car | save --force $tmp
    let info = (nix-shell -p atproto-goat --run $"goat repo inspect ($tmp)" | parse-goat-inspect)
    rm $tmp
    $info
}

# fetch getRepo CARs from hydrant and pds and compare via `goat repo inspect`
def check-car [hydrant_url: string, pds_url: string, did: string] {
    print "checking getRepo CAR..."

    let h = try { fetch-car-info $hydrant_url $did } catch {
        print "  error fetching CAR from hydrant"
        return false
    }

    let p = try { fetch-car-info $pds_url $did } catch {
        print "  error fetching CAR from pds"
        return false
    }

    print $"  hydrant: data=($h.'Data CID') prev=($h.'Prev CID') rev=($h.Revision)"
    print $"  pds:     data=($p.'Data CID') prev=($p.'Prev CID') rev=($p.Revision)"

    if $h.'Data CID' != $p.'Data CID' or $h.'Prev CID' != $p.'Prev CID' or $h.Revision != $p.Revision {
        print "  MISMATCH: CARs differ!"
        return false
    }

    print "  ok"
    true
}

def main [] {
    let did = "did:plc:dfl62fgb7wtjj3fcbb72naae"
    let pds = "https://zwsp.xyz"
    let port = resolve-test-port 3001
    let url = $"http://localhost:($port)"
    let debug_port = resolve-test-debug-port ($port + 1)
    let debug_url = $"http://127.0.0.1:($debug_port)"
    let db_path = (mktemp -d -t hydrant_test.XXXXXX)

    print $"testing backfill integrity for ($did)..."
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    mut success = false

    if (wait-for-api $url) {
        # track the repo via API
        print $"adding repo ($did) to tracking..."
        http put -t application/json $"($url)/repos" [ { did: ($did) } ]

        if (wait-for-backfill $url) {
            let integrity_passed = (check-consistency $url $pds $did)
            let count_passed = (check-count $url $debug_url $did)
            let commit = (check-latest-commit $url $pds $did)
            let commit_passed = $commit != null
            let car_passed = if $commit_passed { check-car $url $pds $did } else { false }

            if $integrity_passed and $count_passed and $commit_passed and $car_passed {
                print "all integrity checks passed!"
                $success = true
            } else {
                print $"integrity checks failed. consistency: ($integrity_passed), count: ($count_passed), commit: ($commit_passed), car: ($car_passed)"
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
