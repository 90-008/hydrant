#!/usr/bin/env nu
use common.nu *

def test-crawler-source-management [url: string, pid: int] {
    print "=== test: crawler source management ==="

    print "  GET /crawler/sources starts empty..."
    let initial = (http get $"($url)/crawler/sources")
    if ($initial | length) != 0 {
        fail $"expected empty list, got ($initial | length) entries" $pid
    }

    print "  POST /crawler/sources creates a list_repos source..."
    http post -f -e -t application/json $"($url)/crawler/sources" {
        url: "https://bsky.network",
        mode: "list_repos"
    } | assert-status 201 "POST /crawler/sources" $pid

    let sources = (http get $"($url)/crawler/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $pid
    }
    let s = ($sources | first)
    if $s.mode != "list_repos" {
        fail $"expected mode=list_repos, got ($s.mode)" $pid
    }

    print "  POST /crawler/sources replaces an existing URL..."
    http post -f -e -t application/json $"($url)/crawler/sources" {
        url: "https://bsky.network",
        mode: "by_collection"
    } | assert-status 201 "POST /crawler/sources override" $pid

    let after_replace = (http get $"($url)/crawler/sources")
    if ($after_replace | length) != 1 {
        fail $"expected 1 source after override, got ($after_replace | length)" $pid
    }
    if ($after_replace | first).mode != "by_collection" {
        fail "expected mode to be updated to by_collection after override" $pid
    }

    print "  DELETE /crawler/sources removes the source..."
    http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: "https://bsky.network"
    } | assert-status 200 "DELETE /crawler/sources" $pid

    let after_del = (http get $"($url)/crawler/sources")
    if ($after_del | length) != 0 {
        fail "expected empty list after delete" $pid
    }

    print "  DELETE /crawler/sources returns 404 for a missing source..."
    http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: "https://bsky.network"
    } | assert-status 404 "DELETE /crawler/sources missing" $pid

    print "crawler source management passed!"
}

def test-dynamic-crawler-source-persistence [binary: string, db_path: string, port: int] {
    print "=== test: dynamic crawler sources persist across restart ==="

    let url = $"http://localhost:($port)"
    let instance = (with-env { HYDRANT_CRAWLER_URLS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
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

    print "  restarting hydrant..."
    kill $instance.pid
    sleep 2sec

    let instance2 = (with-env { HYDRANT_CRAWLER_URLS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not restart" $instance2.pid
    }

    let after = (http get $"($url)/crawler/sources")
    if ($after | length) != 1 {
        fail $"expected 1 source after restart, got ($after | length)" $instance2.pid
    }
    if ($after | first).mode != "by_collection" {
        fail $"expected mode=by_collection after restart, got (($after | first).mode)" $instance2.pid
    }

    kill $instance2.pid
    print "dynamic crawler source persistence passed!"
}

def test-configured-crawler-source-runtime-delete [binary: string, db_path: string, port: int] {
    print "=== test: configured crawler sources are runtime-only ==="

    let url = $"http://localhost:($port)"
    let crawler_url = "https://lightrail.microcosm.blue"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: $"by_collection::($crawler_url)" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    let sources = (http get $"($url)/crawler/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $instance.pid
    }

    print "  deleting configured source at runtime..."
    http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: $crawler_url
    } | assert-status 200 "DELETE /crawler/sources runtime" $instance.pid

    let after_del = (http get $"($url)/crawler/sources")
    if ($after_del | length) != 0 {
        fail "expected source to be gone after runtime delete" $instance.pid
    }

    print "  restarting with the same HYDRANT_CRAWLER_URLS..."
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

    kill $instance2.pid
    print "configured crawler source runtime delete passed!"
}

def main [] {
    let port = resolve-test-port 3007
    let url = $"http://localhost:($port)"
    let binary = build-hydrant

    let db = (mktemp -d -t hydrant_api_crawler_sources.XXXXXX)
    print $"db: ($db)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    test-crawler-source-management $url $instance.pid

    kill $instance.pid
    sleep 2sec

    let db_persist = (mktemp -d -t hydrant_api_crawler_sources.XXXXXX)
    print $"db: ($db_persist)"
    test-dynamic-crawler-source-persistence $binary $db_persist $port

    sleep 1sec

    let db_config = (mktemp -d -t hydrant_api_crawler_sources.XXXXXX)
    print $"db: ($db_config)"
    test-configured-crawler-source-runtime-delete $binary $db_config $port

    print ""
    print "all crawler source API tests passed!"
}
