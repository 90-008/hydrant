#!/usr/bin/env nu
use common.nu *

def test-firehose-source-management [url: string, pid: int] {
    print "=== test: firehose source management ==="

    print "  GET /firehose/sources starts empty..."
    let initial = (http get $"($url)/firehose/sources")
    if ($initial | length) != 0 {
        fail $"expected empty list, got ($initial | length) entries" $pid
    }

    print "  POST /firehose/sources creates a source..."
    http post -f -e -t application/json $"($url)/firehose/sources" {
        url: "wss://test.bsky.network"
    } | assert-status 201 "POST /firehose/sources" $pid

    let sources = (http get $"($url)/firehose/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $pid
    }

    print "  POST /firehose/sources accepts an existing URL..."
    http post -f -e -t application/json $"($url)/firehose/sources" {
        url: "wss://test.bsky.network"
    } | assert-status 201 "POST /firehose/sources override" $pid

    let after_replace = (http get $"($url)/firehose/sources")
    if ($after_replace | length) != 1 {
        fail $"expected 1 source after override, got ($after_replace | length)" $pid
    }

    print "  DELETE /firehose/sources removes the source..."
    http delete -f -e -t application/json $"($url)/firehose/sources" --data {
        url: "wss://test.bsky.network"
    } | assert-status 200 "DELETE /firehose/sources" $pid

    let after_del = (http get $"($url)/firehose/sources")
    if ($after_del | length) != 0 {
        fail "expected empty list after delete" $pid
    }

    print "  DELETE /firehose/sources returns 404 for a missing source..."
    http delete -f -e -t application/json $"($url)/firehose/sources" --data {
        url: "wss://test.bsky.network"
    } | assert-status 404 "DELETE /firehose/sources missing" $pid

    print "firehose source management passed!"
}

def main [] {
    let port = resolve-test-port 3009
    let url = $"http://localhost:($port)"
    let binary = build-hydrant
    let db = (mktemp -d -t hydrant_api_firehose_sources.XXXXXX)
    print $"db: ($db)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    test-firehose-source-management $url $instance.pid

    kill $instance.pid
    print ""
    print "all firehose source API tests passed!"
}
