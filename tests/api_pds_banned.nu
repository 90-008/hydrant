#!/usr/bin/env nu
use common.nu *

def test-pds-ban-management [url: string, pid: int] {
    print "=== test: pds ban management ==="

    let initial = (http get $"($url)/pds/banned")
    if ($initial | length) != 0 {
        fail $"expected empty banned list, got ($initial | length)" $pid
    }

    print "  PUT /pds/banned bans a host..."
    http put -f -e -t application/json $"($url)/pds/banned" {
        host: "bad.example.com"
    } | assert-status 200 "PUT /pds/banned" $pid

    let after_ban = (http get $"($url)/pds/banned")
    if ($after_ban | length) != 1 {
        fail $"expected 1 banned host, got ($after_ban | length)" $pid
    }
    if ($after_ban | first) != "bad.example.com" {
        fail $"expected bad.example.com, got ($after_ban | first)" $pid
    }

    print "  DELETE /pds/banned unbans a host..."
    http delete -f -e -t application/json $"($url)/pds/banned" --data {
        host: "bad.example.com"
    } | assert-status 200 "DELETE /pds/banned" $pid

    let after_unban = (http get $"($url)/pds/banned")
    if ($after_unban | length) != 0 {
        fail "expected empty banned list after unban" $pid
    }

    print "pds ban management passed!"
}

def test-pds-ban-persistence [binary: string, db_path: string, port: int] {
    print "=== test: pds banned hosts persist across restart ==="

    let url = $"http://localhost:($port)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    http put -t application/json $"($url)/pds/banned" {
        host: "persist-ban.example.com"
    }

    let before = (http get $"($url)/pds/banned")
    if ($before | length) != 1 {
        fail "host was not banned" $instance.pid
    }

    print "  restarting hydrant..."
    kill $instance.pid
    sleep 2sec

    let instance2 = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not restart" $instance2.pid
    }

    let after = (http get $"($url)/pds/banned")
    if ($after | length) != 1 {
        fail $"expected 1 banned host after restart, got ($after | length)" $instance2.pid
    }
    if ($after | first) != "persist-ban.example.com" {
        fail $"expected persist-ban.example.com after restart, got ($after | first)" $instance2.pid
    }

    kill $instance2.pid
    print "pds ban persistence passed!"
}

def main [] {
    let port = resolve-test-port 3014
    let url = $"http://localhost:($port)"
    let binary = build-hydrant

    let db = (mktemp -d -t hydrant_api_pds_banned.XXXXXX)
    print $"db: ($db)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    test-pds-ban-management $url $instance.pid

    kill $instance.pid
    sleep 2sec

    let db_persist = (mktemp -d -t hydrant_api_pds_banned.XXXXXX)
    print $"db: ($db_persist)"
    test-pds-ban-persistence $binary $db_persist $port

    print ""
    print "all pds ban API tests passed!"
}
