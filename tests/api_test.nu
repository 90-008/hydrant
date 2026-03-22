#!/usr/bin/env nu
use common.nu *

# print a failure message, kill any running hydrant instances, and exit.
def fail [msg: string, ...pids: int] {
    print $"  FAILED: ($msg)"
    for pid in $pids {
        try { kill $pid }
    }
    exit 1
}

def test-crawler-sources [url: string, pid: int] {
    print "=== test: crawler sources ==="

    # initial state: no sources
    print "  GET /crawler/sources (expect empty)..."
    let initial = (http get $"($url)/crawler/sources")
    if ($initial | length) != 0 {
        fail $"expected empty list, got ($initial | length) entries" $pid
    }
    print "  ok: starts empty"

    # add a relay source
    print "  POST /crawler/sources (relay)..."
    let resp_add = (http post -f -e -t application/json $"($url)/crawler/sources" {
        url: "https://bsky.network",
        mode: "relay"
    })
    if $resp_add.status != 201 {
        fail $"expected 201, got ($resp_add.status)" $pid
    }
    print "  ok: 201 Created"

    # verify the source appears with correct fields
    print "  GET /crawler/sources (expect 1 entry)..."
    let sources = (http get $"($url)/crawler/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $pid
    }
    let s = ($sources | first)
    if $s.mode != "relay" {
        fail $"expected mode=relay, got ($s.mode)" $pid
    }
    if not $s.persisted {
        fail "expected persisted=true for dynamically added source" $pid
    }
    print $"  ok: 1 source, url=($s.url), mode=($s.mode), persisted=($s.persisted)"

    # posting the same URL with a different mode replaces the existing entry
    print "  POST /crawler/sources (should override)..."
    let resp_replace = (http post -f -e -t application/json $"($url)/crawler/sources" {
        url: "https://bsky.network",
        mode: "by_collection"
    })
    if $resp_replace.status != 201 {
        fail $"expected 201, got ($resp_replace.status)" $pid
    }
    let after_replace = (http get $"($url)/crawler/sources")
    if ($after_replace | length) != 1 {
        fail $"expected 1 source after override, got ($after_replace | length)" $pid
    }
    if ($after_replace | first).mode != "by_collection" {
        fail "expected mode to be updated to by_collection after override" $pid
    }
    print "  ok: duplicate add replaced existing entry (mode updated)"

    # remove the source
    print "  DELETE /crawler/sources..."
    let resp_del = (http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: "https://bsky.network"
    })
    if $resp_del.status != 200 {
        fail $"expected 200, got ($resp_del.status)" $pid
    }
    let after_del = (http get $"($url)/crawler/sources")
    if ($after_del | length) != 0 {
        fail "expected empty list after delete" $pid
    }
    print "  ok: source removed"

    # deleting a non-existent source returns 404
    print "  DELETE /crawler/sources (should be 404)..."
    let resp_del_missing = (http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: "https://bsky.network"
    })
    if $resp_del_missing.status != 404 {
        fail $"expected 404, got ($resp_del_missing.status)" $pid
    }
    print "  ok: 404 for non-existent source"

    print "crawler source tests passed!"
}

# verify that dynamically added sources are written to the database and survive a restart.
def test-source-persistence [binary: string, db_path: string, port: int] {
    print "=== test: dynamically added sources persist across restart ==="

    let url = $"http://localhost:($port)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start"
    }

    print "  adding source..."
    http post -t application/json $"($url)/crawler/sources" {
        url: "https://lightrail.microcosm.blue",
        mode: "by_collection"
    }

    let before = (http get $"($url)/crawler/sources")
    if ($before | length) != 1 {
        fail "source was not added" $instance.pid
    }

    # restart hydrant against the same database
    print "  restarting hydrant..."
    kill $instance.pid
    sleep 2sec

    let instance2 = (with-env { HYDRANT_CRAWLER_URLS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not restart" $instance2.pid
    }

    print "  checking source survived restart..."
    let after = (http get $"($url)/crawler/sources")
    if ($after | length) != 1 {
        fail $"expected 1 source after restart, got ($after | length)" $instance2.pid
    }
    let s = ($after | first)
    if not $s.persisted {
        fail "expected persisted=true after restart" $instance2.pid
    }
    if $s.mode != "by_collection" {
        fail $"expected mode=by_collection after restart, got ($s.mode)" $instance2.pid
    }
    print "  ok: persisted source survived restart"

    kill $instance2.pid
    print "source persistence test passed!"
}

# verify that CRAWLER_URLS sources are not written to the database (persisted=false),
# can be stopped at runtime, but reappear on the next restart because the env var
# is re-applied at startup.
def test-config-source-not-persisted [binary: string, db_path: string, port: int] {
    print "=== test: CRAWLER_URLS sources are not persisted ==="

    let url = $"http://localhost:($port)"
    let crawler_url = "https://lightrail.microcosm.blue"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: $"by_collection::($crawler_url)" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start"
    }

    # config source should appear, but with persisted=false
    print "  checking config source appears with persisted=false..."
    let sources = (http get $"($url)/crawler/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $instance.pid
    }
    if ($sources | first).persisted {
        fail "expected persisted=false for a CRAWLER_URLS source" $instance.pid
    }
    print "  ok: config source has persisted=false"

    # the task can be stopped at runtime
    print "  deleting config source at runtime..."
    let resp = (http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: $crawler_url
    })
    if $resp.status != 200 {
        fail $"expected 200, got ($resp.status)" $instance.pid
    }
    let after_del = (http get $"($url)/crawler/sources")
    if ($after_del | length) != 0 {
        fail "expected source to be gone after runtime delete" $instance.pid
    }
    print "  ok: config source removed at runtime"

    # after a restart with the same CRAWLER_URLS, the config source reappears
    print "  restarting with same CRAWLER_URLS..."
    kill $instance.pid
    sleep 2sec

    let instance2 = (with-env { HYDRANT_CRAWLER_URLS: $"by_collection::($crawler_url)" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not restart" $instance2.pid
    }

    let after_restart = (http get $"($url)/crawler/sources")
    if ($after_restart | length) != 1 {
        fail $"expected config source to reappear after restart, got ($after_restart | length)" $instance2.pid
    }
    if ($after_restart | first).persisted {
        fail "expected persisted=false after restart" $instance2.pid
    }
    print "  ok: config source reappears on restart (not persisted to DB)"

    kill $instance2.pid
    print "config source persistence test passed!"
}

def test-firehose-sources [url: string, pid: int] {
    print "=== test: firehose sources ==="

    # initial state: no sources (we start with HYDRANT_RELAY_HOSTS="")
    print "  GET /firehose/sources (expect empty)..."
    let initial = (http get $"($url)/firehose/sources")
    if ($initial | length) != 0 {
        fail $"expected empty list, got ($initial | length) entries" $pid
    }
    print "  ok: starts empty"

    # add a relay source
    print "  POST /firehose/sources..."
    let resp_add = (http post -f -e -t application/json $"($url)/firehose/sources" {
        url: "wss://test.bsky.network"
    })
    if $resp_add.status != 201 {
        fail $"expected 201, got ($resp_add.status)" $pid
    }
    print "  ok: 201 Created"

    # verify it appears
    print "  GET /firehose/sources (expect 1 entry)..."
    let sources = (http get $"($url)/firehose/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $pid
    }
    let s = ($sources | first)
    if not $s.persisted {
        fail "expected persisted=true for dynamically added source" $pid
    }
    print $"  ok: 1 source, url=($s.url), persisted=($s.persisted)"

    # posting the same URL replaces the existing entry
    print "  POST /firehose/sources (should override)..."
    let resp_replace = (http post -f -e -t application/json $"($url)/firehose/sources" {
        url: "wss://test.bsky.network"
    })
    if $resp_replace.status != 201 {
        fail $"expected 201, got ($resp_replace.status)" $pid
    }
    let after_replace = (http get $"($url)/firehose/sources")
    if ($after_replace | length) != 1 {
        fail $"expected 1 source after override, got ($after_replace | length)" $pid
    }
    print "  ok: duplicate add replaced existing entry"

    # remove the source
    print "  DELETE /firehose/sources..."
    let resp_del = (http delete -f -e -t application/json $"($url)/firehose/sources" --data {
        url: "wss://test.bsky.network"
    })
    if $resp_del.status != 200 {
        fail $"expected 200, got ($resp_del.status)" $pid
    }
    let after_del = (http get $"($url)/firehose/sources")
    if ($after_del | length) != 0 {
        fail "expected empty list after delete" $pid
    }
    print "  ok: source removed"

    # deleting a non-existent source returns 404
    print "  DELETE /firehose/sources (should be 404)..."
    let resp_del_missing = (http delete -f -e -t application/json $"($url)/firehose/sources" --data {
        url: "wss://test.bsky.network"
    })
    if $resp_del_missing.status != 404 {
        fail $"expected 404, got ($resp_del_missing.status)" $pid
    }
    print "  ok: 404 for non-existent source"

    print "firehose source tests passed!"
}

def main [] {
    let port = 3007
    let url = $"http://localhost:($port)"

    let binary = build-hydrant

    let db = (mktemp -d -t hydrant_api_test.XXXXXX)
    print $"db: ($db)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    test-crawler-sources $url $instance.pid
    test-firehose-sources $url $instance.pid

    kill $instance.pid
    sleep 2sec

    let db_persist = (mktemp -d -t hydrant_api_test.XXXXXX)
    print $"db: ($db_persist)"
    test-source-persistence $binary $db_persist $port

    sleep 1sec

    let db_config = (mktemp -d -t hydrant_api_test.XXXXXX)
    print $"db: ($db_config)"
    test-config-source-not-persisted $binary $db_config $port

    print ""
    print "all api tests passed!"
}
