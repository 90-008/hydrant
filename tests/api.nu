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
    if not $s.persisted {
        fail "expected persisted=true for dynamically added source" $pid
    }
    print $"  ok: 1 source, url=($s.url), mode=($s.mode), persisted=($s.persisted)"

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
    if not $s.persisted {
        fail "expected persisted=true for dynamically added source" $pid
    }
    print $"  ok: 1 source, url=($s.url), persisted=($s.persisted)"

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
    if ($initial.assignments | length) != 0 {
        fail $"expected empty assignments, got ($initial.assignments | length)" $pid
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
        for field in ["per_second_base", "per_second_account_mul", "per_hour", "per_day"] {
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
    if ($after_assign.assignments | length) != 1 {
        fail $"expected 1 assignment, got ($after_assign.assignments | length)" $pid
    }
    let a = ($after_assign.assignments | first)
    if $a.host != "pds.example.com" {
        fail $"expected host=pds.example.com, got ($a.host)" $pid
    }
    if $a.tier != "trusted" {
        fail $"expected tier=trusted, got ($a.tier)" $pid
    }
    print $"  ok: assignment created host=($a.host), tier=($a.tier)"

    # re-assigning the same host to a different tier updates without creating a duplicate
    print "  PUT /pds/tiers (re-assign to default)..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "default"
    } | assert-status 200 "PUT /pds/tiers re-assign" $pid
    let after_reassign = (http get $"($url)/pds/tiers")
    if ($after_reassign.assignments | length) != 1 {
        fail $"expected 1 assignment after re-assign, got ($after_reassign.assignments | length)" $pid
    }
    if ($after_reassign.assignments | first).tier != "default" {
        fail $"expected tier=default after re-assign, got (($after_reassign.assignments | first).tier)" $pid
    }
    print "  ok: re-assign updates tier without creating a duplicate"

    # assigning an unknown tier name is rejected with 400
    print "  PUT /pds/tiers (unknown tier, expect 400)..."
    http put -f -e -t application/json $"($url)/pds/tiers" {
        host: "pds.example.com",
        tier: "nonexistent"
    } | assert-status 400 "PUT /pds/tiers unknown tier" $pid
    let after_bad = (http get $"($url)/pds/tiers")
    if ($after_bad.assignments | length) != 1 {
        fail "expected assignment count unchanged after rejected request" $pid
    }
    if ($after_bad.assignments | first).tier != "default" {
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
    if ($after_second.assignments | length) != 2 {
        fail $"expected 2 assignments, got ($after_second.assignments | length)" $pid
    }
    print "  ok: two distinct hosts listed independently"

    # remove the first host
    print "  DELETE /pds/tiers (first host)..."
    http delete -f -e -t application/json $"($url)/pds/tiers" --data {
        host: "pds.example.com"
    } | assert-status 200 "DELETE /pds/tiers" $pid
    let after_del = (http get $"($url)/pds/tiers")
    if ($after_del.assignments | length) != 1 {
        fail $"expected 1 assignment after delete, got ($after_del.assignments | length)" $pid
    }
    if ($after_del.assignments | first).host != "other.example.com" {
        fail "expected only other.example.com to remain after delete" $pid
    }
    print "  ok: correct host removed, other assignment intact"

    # remove the second host
    http delete -f -e -t application/json $"($url)/pds/tiers" --data {
        host: "other.example.com"
    } | assert-status 200 "DELETE /pds/tiers second" $pid

    # deleting a non-existent host is idempotent (returns 200, not an error)
    print "  DELETE /pds/tiers (non-existent, expect 200)..."
    http delete -f -e -t application/json $"($url)/pds/tiers" --data {
        host: "pds.example.com"
    } | assert-status 200 "DELETE /pds/tiers non-existent" $pid
    let after_idempotent = (http get $"($url)/pds/tiers")
    if ($after_idempotent.assignments | length) != 0 {
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
    if ($before.assignments | length) != 1 {
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
    if ($after.assignments | length) != 1 {
        fail $"expected 1 assignment after restart, got ($after.assignments | length)" $instance2.pid
    }
    let a = ($after.assignments | first)
    if $a.host != "persist.example.com" {
        fail $"expected host=persist.example.com after restart, got ($a.host)" $instance2.pid
    }
    if $a.tier != "trusted" {
        fail $"expected tier=trusted after restart, got ($a.tier)" $instance2.pid
    }
    print "  ok: tier assignment persisted across restart"

    kill $instance2.pid
    print "pds tier persistence test passed!"
}

# verify that HYDRANT_TRUSTED_HOSTS pre-assigns hosts to the trusted tier at startup.
def test-pds-trusted-hosts [binary: string, db_path: string, port: int] {
    print "=== test: HYDRANT_TRUSTED_HOSTS pre-assigns tier at startup ==="

    let url = $"http://localhost:($port)"
    let host_a = "alpha.example.com"
    let host_b = "beta.example.com"

    let instance = (with-env {
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_RELAY_HOSTS: "",
        HYDRANT_TRUSTED_HOSTS: $"($host_a),($host_b)"
    } {
        start-hydrant $binary $db_path $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start"
    }

    print "  checking pre-assigned trusted hosts..."
    let tiers = (http get $"($url)/pds/tiers")
    let assignments = $tiers.assignments

    for host in [$host_a, $host_b] {
        let match = ($assignments | where host == $host)
        if ($match | length) != 1 {
            fail $"expected assignment for ($host) from HYDRANT_TRUSTED_HOSTS, got ($assignments)" $instance.pid
        }
        if ($match | first).tier != "trusted" {
            fail $"expected tier=trusted for ($host), got (($match | first).tier)" $instance.pid
        }
    }
    print $"  ok: ($host_a) and ($host_b) pre-assigned to trusted tier"

    kill $instance.pid
    print "trusted hosts startup test passed!"
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
    let match = ($after.assignments | where host == "custom.example.com")
    if ($match | length) != 1 {
        fail "expected assignment for custom.example.com" $instance.pid
    }
    if ($match | first).tier != "custom" {
        fail $"expected tier=custom, got (($match | first).tier)" $instance.pid
    }
    print "  ok: host assigned to custom tier successfully"

    kill $instance.pid
    print "custom rate tier test passed!"
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

    let db_pds_trusted = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db_pds_trusted)"
    test-pds-trusted-hosts $binary $db_pds_trusted $port

    sleep 1sec

    let db_pds_custom = (mktemp -d -t hydrant_api.XXXXXX)
    print $"db: ($db_pds_custom)"
    test-pds-custom-rate-tier $binary $db_pds_custom $port

    print ""
    print "all api tests passed!"
}
