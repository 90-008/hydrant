#!/usr/bin/env nu
use common.nu *

def main [] {
    let env_vars = load-env-file
    let did = ($env_vars | get --optional TEST_REPO)
    let password = ($env_vars | get --optional TEST_PASSWORD)

    if ($did | is-empty) or ($password | is-empty) {
        print "error: TEST_REPO and TEST_PASSWORD must be set in .env"
        exit 1
    }

    let port = 3007
    let url = $"http://localhost:($port)"
    let db_path = (mktemp -d -t hydrant_signal_test.XXXXXX)
    let collection = "app.bsky.feed.post"

    print $"database path: ($db_path)"

    let pds_url = resolve-pds $did
    print $"resolved pds: ($pds_url)"

    let session = authenticate $pds_url $did $password
    let jwt = $session.accessJwt
    print "authenticated"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    mut test_passed = false

    if (wait-for-api $url) {
        # configure signal mode: index app.bsky.feed.post from anyone on the network
        print "configuring signal mode..."
        http patch -t application/json $"($url)/filter" {
            mode: "signal",
            signals: [$collection]
        }

        # verify filter state
        let filter = (http get $"($url)/filter")
        print $"filter state: ($filter | to json)"

        if $filter.mode != "signal" {
            print "FAILED: mode was not set to signal"
        } else if not ($filter.signals | any { |s| $s == $collection }) {
            print $"FAILED: ($collection) not in signals"
        } else {
            print "filter configured correctly"

            # wait a moment for the firehose to connect and the filter to take effect
            sleep 3sec

            let timestamp = (date now | format date "%Y-%m-%dT%H:%M:%SZ")
            let record_data = {
                "$type": $collection,
                text: $"hydrant signal filter test ($timestamp)",
                createdAt: $timestamp
            }

            print "creating post..."
            let create_res = (http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.createRecord" {
                repo: $did,
                collection: $collection,
                record: $record_data
            })
            let rkey = ($create_res.uri | split row "/" | last)
            print $"created: ($create_res.uri)"

            # give hydrant time to receive and process the firehose event
            sleep 5sec

            # verify the record was indexed
            print "checking indexed record..."
            let result = (try {
                http get $"($url)/xrpc/com.atproto.repo.getRecord?repo=($did)&collection=($collection)&rkey=($rkey)"
            } catch {
                null
            })

            if ($result | is-empty) {
                print "FAILED: record not found in hydrant index"
            } else {
                print $"indexed record cid: ($result.cid)"
                print "test PASSED: signal filter correctly indexed the post"
                $test_passed = true
            }

            # cleanup: delete the test post
            print "cleaning up test post..."
            try {
                http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.deleteRecord" {
                    repo: $did,
                    collection: $collection,
                    rkey: $rkey
                }
            }
        }
    } else {
        print "hydrant failed to start"
    }

    print "stopping hydrant..."
    try { kill -9 $instance.pid }

    if $test_passed {
        exit 0
    } else {
        exit 1
    }
}
