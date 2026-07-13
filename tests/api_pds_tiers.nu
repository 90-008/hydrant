#!/usr/bin/env nu
use common.nu *

def test-pds-tier-management [url: string, pid: int] {
    print "=== test: pds tier management ==="

    let initial = (http get $"($url)/pds/tiers")
    if ($initial.assignments | columns | length) != 0 {
        fail $"expected empty assignments, got ($initial.assignments | columns | length)" $pid
    }
    if not ("default" in $initial.rate_tiers) {
        fail "expected 'default' tier in rate_tiers" $pid
    }
    if not ("trusted" in $initial.rate_tiers) {
        fail "expected 'trusted' tier in rate_tiers" $pid
    }

    print "  GET /pds/rate-tiers returns built-in definitions..."
    let rate_tiers = (http get $"($url)/pds/rate-tiers")
    for tier_name in ["default", "trusted"] {
        let tier = ($rate_tiers | get $tier_name)
        for field in ["per_second_base", "per_second_account_mul", "per_hour", "per_day", "account_limit"] {
            if not ($field in $tier) {
                fail $"($tier_name) tier missing field ($field)" $pid
            }
        }
    }
    if $rate_tiers.default != $rate_tiers.trusted {
        fail "expected indexer default rate tier to use trusted limits" $pid
    }

    print "  PUT /pds/tiers creates an assignment..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "trusted"
    } | assert-status 200 "PUT /pds/tiers" $pid

    let after_assign = (http get $"($url)/pds/tiers")
    if ($after_assign.assignments | columns | length) != 1 {
        fail $"expected 1 assignment, got ($after_assign.assignments | columns | length)" $pid
    }
    if ($after_assign.assignments | get "pds.example.com") != "trusted" {
        fail "expected pds.example.com to be assigned to trusted" $pid
    }

    print "  PUT /pds/tiers updates an existing assignment..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "default"
    } | assert-status 200 "PUT /pds/tiers re-assign" $pid

    let after_reassign = (http get $"($url)/pds/tiers")
    if ($after_reassign.assignments | columns | length) != 1 {
        fail $"expected 1 assignment after re-assign, got ($after_reassign.assignments | columns | length)" $pid
    }
    if ($after_reassign.assignments | get "pds.example.com") != "default" {
        fail "expected tier=default after re-assign" $pid
    }

    print "  PUT /pds/tiers rejects an unknown tier..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "nonexistent"
    } | assert-status 400 "PUT /pds/tiers unknown tier" $pid

    let after_bad = (http get $"($url)/pds/tiers")
    if ($after_bad.assignments | get "pds.example.com") != "default" {
        fail "expected tier unchanged after rejected request" $pid
    }

    print "  PUT /pds/tiers supports multiple hosts..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "other.example.com",
        tier: "trusted"
    } | assert-status 200 "PUT /pds/tiers second host" $pid

    let after_second = (http get $"($url)/pds/tiers")
    if ($after_second.assignments | columns | length) != 2 {
        fail $"expected 2 assignments, got ($after_second.assignments | columns | length)" $pid
    }

    print "  DELETE /pds/tiers removes one host..."
    http delete -f -e $"($url)/pds/tiers?host=pds.example.com" | assert-status 200 "DELETE /pds/tiers" $pid

    let after_del = (http get $"($url)/pds/tiers")
    if ($after_del.assignments | columns | length) != 1 {
        fail $"expected 1 assignment after delete, got ($after_del.assignments | columns | length)" $pid
    }
    if not ("other.example.com" in $after_del.assignments) {
        fail "expected only other.example.com to remain after delete" $pid
    }

    http delete -f -e $"($url)/pds/tiers?host=other.example.com" | assert-status 200 "DELETE /pds/tiers second" $pid

    print "  DELETE /pds/tiers is idempotent for a missing host..."
    http delete -f -e $"($url)/pds/tiers?host=pds.example.com" | assert-status 200 "DELETE /pds/tiers non-existent" $pid

    let after_idempotent = (http get $"($url)/pds/tiers")
    if ($after_idempotent.assignments | columns | length) != 0 {
        fail "expected empty assignments after cleanup" $pid
    }

    print "pds tier management passed!"
}

def test-pds-tier-persistence [binary: string, db_path: string, port: int] {
    print "=== test: pds tier assignments persist across restart ==="

    let url = $"http://localhost:($port)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    http put -t application/json $"($url)/pds/tiers" {
        host: "persist.example.com",
        tier: "trusted"
    }

    let before = (http get $"($url)/pds/tiers")
    if ($before.assignments | columns | length) != 1 {
        fail "assignment was not created" $instance.pid
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

    let after = (http get $"($url)/pds/tiers")
    if ($after.assignments | columns | length) != 1 {
        fail $"expected 1 assignment after restart, got ($after.assignments | columns | length)" $instance2.pid
    }
    if ($after.assignments | get "persist.example.com") != "trusted" {
        fail "expected persist.example.com to stay on trusted tier" $instance2.pid
    }

    kill $instance2.pid
    print "pds tier persistence passed!"
}

def test-pds-custom-rate-tier [binary: string, db_path: string, port: int] {
    print "=== test: custom rate tier via HYDRANT_RATE_TIERS ==="

    let url = $"http://localhost:($port)"

    let instance = (with-env {
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_RELAY_HOSTS: "",
        HYDRANT_RATE_TIERS: "custom:100/1.0/360000/8640000"
    } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    let rate_tiers = (http get $"($url)/pds/rate-tiers")
    if not ("custom" in $rate_tiers) {
        fail "expected 'custom' tier in rate_tiers" $instance.pid
    }
    if not ("default" in $rate_tiers) {
        fail "built-in 'default' tier should still be present alongside custom tier" $instance.pid
    }

    let custom = ($rate_tiers | get custom)
    if $custom.per_second_base != 100 {
        fail $"expected custom.per_second_base=100, got ($custom.per_second_base)" $instance.pid
    }
    if $custom.per_hour != 360000 {
        fail $"expected custom.per_hour=360000, got ($custom.per_hour)" $instance.pid
    }

    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "custom.example.com",
        tier: "custom"
    } | assert-status 200 "PUT /pds/tiers custom tier" $instance.pid

    let after = (http get $"($url)/pds/tiers")
    if ($after.assignments | get "custom.example.com") != "custom" {
        fail "expected custom.example.com to be assigned to custom tier" $instance.pid
    }

    kill $instance.pid
    print "custom rate tier passed!"
}

def main [] {
    let port = resolve-test-port 3013
    let url = $"http://localhost:($port)"
    let binary = build-hydrant

    let db = (mktemp -d -t hydrant_api_pds_tiers.XXXXXX)
    print $"db: ($db)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    test-pds-tier-management $url $instance.pid

    kill $instance.pid
    sleep 2sec

    let db_persist = (mktemp -d -t hydrant_api_pds_tiers.XXXXXX)
    print $"db: ($db_persist)"
    test-pds-tier-persistence $binary $db_persist $port

    sleep 1sec

    let db_custom = (mktemp -d -t hydrant_api_pds_tiers.XXXXXX)
    print $"db: ($db_custom)"
    test-pds-custom-rate-tier $binary $db_custom $port

    print ""
    print "all pds tier API tests passed!"
}
