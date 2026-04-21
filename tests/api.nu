#!/usr/bin/env nu
use common.nu *

def test-crawler-sources [url: string, pid: int] {
    print "=== test: crawler sources ==="

    # initial state: no sources
    print "  GET /crawler/sources (expect empty)..."
    let initial = (http get $"($url)/crawler/sources")
    if ($initial | length) != 0 {
        fail $"expected empty list, got ($initial | length) entries" $pid
    }
    print "  ok: starts empty"

    # add a list_repos source
    print "  POST /crawler/sources (list_repos)..."
    http post -f -e -t application/json $"($url)/crawler/sources" {
        url: "https://bsky.network",
        mode: "list_repos"
    } | assert-status 201 "POST /crawler/sources" $pid
    print "  ok: 201 Created"

    # verify the source appears with correct fields
    print "  GET /crawler/sources (expect 1 entry)..."
    let sources = (http get $"($url)/crawler/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $pid
    }
    let s = ($sources | first)
    if $s.mode != "list_repos" {
        fail $"expected mode=list_repos, got ($s.mode)" $pid
    }
    print $"  ok: 1 source, url=($s.url), mode=($s.mode)"

    # posting the same URL with a different mode replaces the existing entry
    print "  POST /crawler/sources (should override)..."
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
    print "  ok: duplicate add replaced existing entry (mode updated)"

    # remove the source
    print "  DELETE /crawler/sources..."
    http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: "https://bsky.network"
    } | assert-status 200 "DELETE /crawler/sources" $pid
    let after_del = (http get $"($url)/crawler/sources")
    if ($after_del | length) != 0 {
        fail "expected empty list after delete" $pid
    }
    print "  ok: source removed"

    # deleting a non-existent source returns 404
    print "  DELETE /crawler/sources (should be 404)..."
    http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: "https://bsky.network"
    } | assert-status 404 "DELETE /crawler/sources missing" $pid
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

    # config source should appear
    print "  checking config source appears..."
    let sources = (http get $"($url)/crawler/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $instance.pid
    }
    print "  ok: config source present"

    # the task can be stopped at runtime
    print "  deleting config source at runtime..."
    http delete -f -e -t application/json $"($url)/crawler/sources" --data {
        url: $crawler_url
    } | assert-status 200 "DELETE /crawler/sources runtime" $instance.pid
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
    http post -f -e -t application/json $"($url)/firehose/sources" {
        url: "wss://test.bsky.network"
    } | assert-status 201 "POST /firehose/sources" $pid
    print "  ok: 201 Created"

    # verify it appears
    print "  GET /firehose/sources (expect 1 entry)..."
    let sources = (http get $"($url)/firehose/sources")
    if ($sources | length) != 1 {
        fail $"expected 1 source, got ($sources | length)" $pid
    }
    let s = ($sources | first)
    print $"  ok: 1 source, url=($s.url)"

    # posting the same URL replaces the existing entry
    print "  POST /firehose/sources (should override)..."
    http post -f -e -t application/json $"($url)/firehose/sources" {
        url: "wss://test.bsky.network"
    } | assert-status 201 "POST /firehose/sources override" $pid
    let after_replace = (http get $"($url)/firehose/sources")
    if ($after_replace | length) != 1 {
        fail $"expected 1 source after override, got ($after_replace | length)" $pid
    }
    print "  ok: duplicate add replaced existing entry"

    # remove the source
    print "  DELETE /firehose/sources..."
    http delete -f -e -t application/json $"($url)/firehose/sources" --data {
        url: "wss://test.bsky.network"
    } | assert-status 200 "DELETE /firehose/sources" $pid
    let after_del = (http get $"($url)/firehose/sources")
    if ($after_del | length) != 0 {
        fail "expected empty list after delete" $pid
    }
    print "  ok: source removed"

    # deleting a non-existent source returns 404
    print "  DELETE /firehose/sources (should be 404)..."
    http delete -f -e -t application/json $"($url)/firehose/sources" --data {
        url: "wss://test.bsky.network"
    } | assert-status 404 "DELETE /firehose/sources missing" $pid
    print "  ok: 404 for non-existent source"

    print "firehose source tests passed!"
}

def test-pds-tiers [url: string, pid: int] {
    print "=== test: pds tier management ==="

    # initial state: no assignments, built-in rate tiers present
    print "  GET /pds/tiers (expect empty assignments, built-in rate_tiers)..."
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
    print "  ok: empty assignments and built-in rate tiers present"

    # GET /pds/rate-tiers returns the same definitions with the right fields
    print "  GET /pds/rate-tiers (check structure)..."
    let rate_tiers = (http get $"($url)/pds/rate-tiers")
    for tier_name in ["default", "trusted"] {
        let tier = ($rate_tiers | get $tier_name)
        for field in ["per_second_base", "per_second_account_mul", "per_hour", "per_day", "account_limit"] {
            if not ($field in $tier) {
                fail $"($tier_name) tier missing field ($field)" $pid
            }
        }
    }
    # trusted tier must have higher per-second limit than default
    if ($rate_tiers.trusted.per_second_base) <= ($rate_tiers.default.per_second_base) {
        fail $"expected trusted.per_second_base > default, got ($rate_tiers.trusted.per_second_base) vs ($rate_tiers.default.per_second_base)" $pid
    }
    print "  ok: rate tier definitions have correct fields and expected ordering"

    # assign a host to the trusted tier
    print "  PUT /pds/tiers (assign to trusted)..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "trusted"
    } | assert-status 200 "PUT /pds/tiers" $pid
    let after_assign = (http get $"($url)/pds/tiers")
    if ($after_assign.assignments | columns | length) != 1 {
        fail $"expected 1 assignment, got ($after_assign.assignments | columns | length)" $pid
    }
    if not ("pds.example.com" in $after_assign.assignments) {
        fail $"expected host=pds.example.com to be assigned" $pid
    }
    if ($after_assign.assignments | get "pds.example.com") != "trusted" {
        fail $"expected tier=trusted" $pid
    }
    print $"  ok: assignment created host=pds.example.com, tier=trusted"

    # re-assigning the same host to a different tier updates without creating a duplicate
    print "  PUT /pds/tiers (re-assign to default)..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "default"
    } | assert-status 200 "PUT /pds/tiers re-assign" $pid
    let after_reassign = (http get $"($url)/pds/tiers")
    if ($after_reassign.assignments | columns | length) != 1 {
        fail $"expected 1 assignment after re-assign, got ($after_reassign.assignments | columns | length)" $pid
    }
    if ($after_reassign.assignments | get "pds.example.com") != "default" {
        fail $"expected tier=default after re-assign" $pid
    }
    print "  ok: re-assign updates tier without creating a duplicate"

    # assigning an unknown tier name is rejected with 400
    print "  PUT /pds/tiers (unknown tier, expect 400)..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "nonexistent"
    } | assert-status 400 "PUT /pds/tiers unknown tier" $pid
    let after_bad = (http get $"($url)/pds/tiers")
    if ($after_bad.assignments | columns | length) != 1 {
        fail "expected assignment count unchanged after rejected request" $pid
    }
    if ($after_bad.assignments | get "pds.example.com") != "default" {
        fail "expected tier unchanged after rejected request" $pid
    }
    print "  ok: unknown tier name rejected with 400, existing assignment unchanged"

    # add a second host to verify multi-assignment listing works
    print "  PUT /pds/tiers (second host)..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "other.example.com",
        tier: "trusted"
    } | assert-status 200 "PUT /pds/tiers second host" $pid
    let after_second = (http get $"($url)/pds/tiers")
    if ($after_second.assignments | columns | length) != 2 {
        fail $"expected 2 assignments, got ($after_second.assignments | columns | length)" $pid
    }
    print "  ok: two distinct hosts listed independently"

    # remove the first host
    print "  DELETE /pds/tiers (first host)..."
    http delete -f -e $"($url)/pds/tiers?host=pds.example.com" | assert-status 200 "DELETE /pds/tiers" $pid
    let after_del = (http get $"($url)/pds/tiers")
    if ($after_del.assignments | columns | length) != 1 {
        fail $"expected 1 assignment after delete, got ($after_del.assignments | columns | length)" $pid
    }
    if not ("other.example.com" in $after_del.assignments) {
        fail "expected only other.example.com to remain after delete" $pid
    }
    print "  ok: correct host removed, other assignment intact"

    # remove the second host
    http delete -f -e $"($url)/pds/tiers?host=other.example.com" | assert-status 200 "DELETE /pds/tiers second" $pid

    # deleting a non-existent host is idempotent (returns 200, not an error)
    print "  DELETE /pds/tiers (non-existent, expect 200)..."
    http delete -f -e $"($url)/pds/tiers?host=pds.example.com" | assert-status 200 "DELETE /pds/tiers non-existent" $pid
    let after_idempotent = (http get $"($url)/pds/tiers")
    if ($after_idempotent.assignments | columns | length) != 0 {
        fail "expected empty assignments after cleanup" $pid
    }
    print "  ok: delete of non-existent host is idempotent"

    print "pds tier management tests passed!"
}

# verify that tier assignments are written to the database and survive a restart.
def test-pds-tier-persistence [binary: string, db_path: string, port: int] {
    print "=== test: pds tier assignments persist across restart ==="

    let url = $"http://localhost:($port)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start"
    }

    print "  assigning host to trusted tier..."
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

    print "  checking assignment survived restart..."
    let after = (http get $"($url)/pds/tiers")
    if ($after.assignments | columns | length) != 1 {
        fail $"expected 1 assignment after restart, got ($after.assignments | columns | length)" $instance2.pid
    }
    if not ("persist.example.com" in $after.assignments) {
        fail $"expected host=persist.example.com after restart" $instance2.pid
    }
    if ($after.assignments | get "persist.example.com") != "trusted" {
        fail $"expected tier=trusted after restart" $instance2.pid
    }
    print "  ok: tier assignment persisted across restart"

    kill $instance2.pid
    print "pds tier persistence test passed!"
}

# verify that a custom tier defined via HYDRANT_RATE_TIERS is visible and assignable.
def test-pds-custom-rate-tier [binary: string, db_path: string, port: int] {
    print "=== test: custom rate tier via HYDRANT_RATE_TIERS ==="

    let url = $"http://localhost:($port)"

    # custom:100/1.0/360000/8640000 — base=100, mul=1.0, hourly=360000, daily=8640000
    let instance = (with-env {
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_RELAY_HOSTS: "",
        HYDRANT_RATE_TIERS: "custom:100/1.0/360000/8640000"
    } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start"
    }

    # custom tier should appear alongside the built-in tiers
    print "  checking custom tier is listed in /pds/rate-tiers..."
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
    print $"  ok: custom tier listed with correct parameters"

    # a host can be assigned to the custom tier
    print "  assigning host to custom tier..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "custom.example.com",
        tier: "custom"
    } | assert-status 200 "PUT /pds/tiers custom tier" $instance.pid
    let after = (http get $"($url)/pds/tiers")
    if not ("custom.example.com" in $after.assignments) {
        fail "expected assignment for custom.example.com" $instance.pid
    }
    if ($after.assignments | get "custom.example.com") != "custom" {
        fail $"expected tier=custom" $instance.pid
    }
    print "  ok: host assigned to custom tier successfully"

    kill $instance.pid
    print "custom rate tier test passed!"
}

def test-pds-banned [url: string, pid: int] {
    print "=== test: pds ban management ==="

    print "  GET /pds/banned (expect empty)..."
    let initial = (http get $"($url)/pds/banned")
    if ($initial | length) != 0 {
        fail $"expected empty banned list, got ($initial | length)" $pid
    }
    print "  ok: starts empty"

    print "  PUT /pds/banned (ban host)..."
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
    print "  ok: host banned"

    print "  DELETE /pds/banned (unban host)..."
    http delete -f -e -t application/json $"($url)/pds/banned" --data {
        host: "bad.example.com"
    } | assert-status 200 "DELETE /pds/banned" $pid

    let after_unban = (http get $"($url)/pds/banned")
    if ($after_unban | length) != 0 {
        fail "expected empty banned list after unban" $pid
    }
    print "  ok: host unbanned"

    print "pds ban management tests passed!"
}

# verify that banned hosts are written to the database and survive a restart.
def test-pds-banned-persistence [binary: string, db_path: string, port: int] {
    print "=== test: pds banned assignments persist across restart ==="

    let url = $"http://localhost:($port)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start"
    }

    print "  banning host..."
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

    print "  checking ban survived restart..."
    let after = (http get $"($url)/pds/banned")
    if ($after | length) != 1 {
        fail $"expected 1 banned host after restart, got ($after | length)" $instance2.pid
    }
    if ($after | first) != "persist-ban.example.com" {
        fail $"expected persist-ban.example.com after restart, got ($after | first)" $instance2.pid
    }
    print "  ok: banned host persisted across restart"

    kill $instance2.pid
    print "pds ban persistence test passed!"
}

def main [] {
    let port = resolve-test-port 3007
    let url = $"http://localhost:($port)"

    let binary = build-hydrant

    let db = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db)"

    let instance = (with-env { HYDRANT_CRAWLER_URLS: "", HYDRANT_RELAY_HOSTS: "" } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    test-crawler-sources $url $instance.pid
    test-firehose-sources $url $instance.pid
    test-pds-tiers $url $instance.pid
    test-pds-banned $url $instance.pid

    kill $instance.pid
    sleep 2sec

    let db_persist = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db_persist)"
    test-source-persistence $binary $db_persist $port

    sleep 1sec

    let db_config = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db_config)"
    test-config-source-not-persisted $binary $db_config $port

    sleep 1sec

    let db_pds_persist = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db_pds_persist)"
    test-pds-tier-persistence $binary $db_pds_persist $port

    sleep 1sec

    let db_pds_custom = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db_pds_custom)"
    test-pds-custom-rate-tier $binary $db_pds_custom $port

    sleep 1sec

    let db_pds_banned = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db_pds_banned)"
    test-pds-banned-persistence $binary $db_pds_banned $port

    print ""
    print "all api tests passed!"
}
