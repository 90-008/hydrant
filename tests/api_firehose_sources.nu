#!/usr/bin/env nu
use common.nu *
use mock_pds.nu *

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

def test-firehose-source-filters [url: string, pid: int, mock_port: int] {
    print "=== test: firehose source filters ==="

    let live_host = "127.0.0.1"
    let failing_host = "localhost"
    let failing_port = ($mock_port + 1)
    let ambiguous_port = ($mock_port + 2)
    let live_url = $"ws://($live_host):($mock_port)/"
    let failing_url = $"ws://($failing_host):($failing_port)/"
    let ambiguous_url = $"ws://($live_host):($ambiguous_port)/"

    print "  starting mock pds websocket server..."
    let mock_pds = (start-mock-pds $mock_port)
    sleep 500ms

    print "  POST /firehose/sources adds one live and one failing PDS source..."
    http post -f -e -t application/json $"($url)/firehose/sources" {
        url: $live_url,
        is_pds: true
    } | assert-status 201 "POST /firehose/sources live pds" $pid

    http post -f -e -t application/json $"($url)/firehose/sources" {
        url: $failing_url,
        is_pds: true
    } | assert-status 201 "POST /firehose/sources failing pds" $pid

    print "  GET /firehose/sources?host=...&failing=true waits for the failing source..."
    mut failing = null
    for _ in 1..40 {
        let res = (http get $"($url)/firehose/sources?host=($failing_host)&is_pds=true&failing=true")
        if ($res | length) == 1 {
            let source = ($res | first)
            if ($source.running == false) and (($source.consecutive_failures | into int) >= 1) and ($source.throttled == true) and ($source.host_status? == "offline") {
                $failing = $source
                break
            }
        }
        sleep 100ms
    }
    if $failing == null {
        stop-mock-pds $mock_pds
        fail "expected one failing localhost source with throttle metadata" $pid
    }

    if ($failing.url != $failing_url) {
        stop-mock-pds $mock_pds
        fail $"expected failing url ($failing_url), got ($failing.url)" $pid
    }
    if ($failing.retry_in_secs? | default 0 | into int) <= 0 {
        stop-mock-pds $mock_pds
        fail "expected failing source to expose retry_in_secs" $pid
    }
    let failing_pds = ($failing.pds? | default null)
    if $failing_pds == null {
        stop-mock-pds $mock_pds
        fail "expected failing source to include pds info" $pid
    }
    if ($failing_pds.host != $failing_host) or ($failing_pds.status != "offline") {
        stop-mock-pds $mock_pds
        fail "expected failing source pds info to reflect localhost offline state" $pid
    }

    print "  GET /firehose/source?host=... resolves the single failing source..."
    let single_by_host = (http get $"($url)/firehose/source?host=($failing_host)")
    if ($single_by_host.url != $failing_url) or ($single_by_host.running != false) or ($single_by_host.failing != true) {
        stop-mock-pds $mock_pds
        fail "expected /firehose/source?host=localhost to return the failing source" $pid
    }
    if (($single_by_host.pds?.host? | default "") != $failing_host) or (($single_by_host.pds?.status? | default "") != "offline") {
        stop-mock-pds $mock_pds
        fail "expected /firehose/source?host=localhost to include pds metadata" $pid
    }

    print "  GET /firehose/source?url=... resolves the live source..."
    let single_live = (http get $"($url)/firehose/source?url=($live_url)")
    if ($single_live.url != $live_url) or ($single_live.running != true) or ($single_live.failing != false) {
        stop-mock-pds $mock_pds
        fail "expected /firehose/source?url=... to return the live source" $pid
    }
    if (($single_live.pds?.host? | default "") != $live_host) or (($single_live.pds?.status? | default "") != "active") {
        stop-mock-pds $mock_pds
        fail "expected /firehose/source?url=... to include active pds metadata" $pid
    }

    print "  GET /firehose/source validates query shape..."
    http get -f -e $"($url)/firehose/source" | assert-status 400 "GET /firehose/source missing query" $pid
    http get -f -e $"($url)/firehose/source?host=($failing_host)&url=($failing_url)" | assert-status 400 "GET /firehose/source conflicting query" $pid
    http get -f -e $"($url)/firehose/source?url=ws://missing.example.com/" | assert-status 404 "GET /firehose/source missing source" $pid

    print "  GET /firehose/sources?running=true filters to the live source..."
    mut live = null
    for _ in 1..30 {
        let res = (http get $"($url)/firehose/sources?host=($live_host)&is_pds=true&running=true")
        if ($res | length) == 1 {
            let source = ($res | first)
            if ($source.url == $live_url) and ($source.running == true) and ($source.failing == false) {
                $live = $source
                break
            }
        }
        sleep 100ms
    }
    if $live == null {
        stop-mock-pds $mock_pds
        fail "expected one running live source for 127.0.0.1" $pid
    }

    print "  GET /firehose/sources?throttled=true returns only the failing source..."
    let throttled = (http get $"($url)/firehose/sources?throttled=true")
    if ($throttled | length) != 1 {
        stop-mock-pds $mock_pds
        fail $"expected 1 throttled source, got ($throttled | length)" $pid
    }
    if (($throttled | first).url != $failing_url) {
        stop-mock-pds $mock_pds
        fail $"expected throttled filter to return ($failing_url)" $pid
    }

    print "  GET /firehose/source?host=... returns 409 when the host is ambiguous..."
    http post -f -e -t application/json $"($url)/firehose/sources" {
        url: $ambiguous_url,
        is_pds: true
    } | assert-status 201 "POST /firehose/sources ambiguous host" $pid
    http get -f -e $"($url)/firehose/source?host=($live_host)" | assert-status 409 "GET /firehose/source ambiguous host" $pid

    stop-mock-pds $mock_pds
    print "firehose source filters passed!"
}

def main [] {
    let port = resolve-test-port 3009
    let mock_port = resolve-test-mock-port 3109
    let url = $"http://localhost:($port)"
    let binary = build-hydrant
    let db_basic = (mktemp -d -t hydrant_api_firehose_sources.XXXXXX)
    print $"db: ($db_basic)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db_basic $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    test-firehose-source-management $url $instance.pid
    kill $instance.pid
    sleep 2sec

    let db_filters = (mktemp -d -t hydrant_api_firehose_sources.XXXXXX)
    print $"db: ($db_filters)"
    let filters_instance = (with-env {
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_RELAY_HOSTS: "",
        HYDRANT_FIREHOSE_MAX_FAILURES: "1"
    } {
        start-hydrant $binary $db_filters $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not restart for filters" $filters_instance.pid
    }

    test-firehose-source-filters $url $filters_instance.pid $mock_port

    kill $filters_instance.pid
    print ""
    print "all firehose source API tests passed!"
}
