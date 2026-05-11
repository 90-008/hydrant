use ../common.nu *
use ../mock_pds.nu *

export def run-pds-status-lifecycle [binary: string, port: int, mock_port: int] {
    let url = $"http://localhost:($port)"
    let db = (mktemp -d -t hydrant_pds_status.XXXXXX)
    let mock_host = "127.0.0.1"

    let instance = (with-env {
        HYDRANT_RELAY_HOSTS: "",
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_FIREHOSE_MAX_FAILURES: "1",
        HYDRANT_RATE_TIERS: "custom:1/1/1/1/0"
    } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    try { bash -c $"fuser -k ($mock_port)/tcp" } catch {}
    sleep 100ms

    print "adding offline mock pds via firehose sources..."
    http post -t application/json $"($url)/firehose/sources" {
        url: $"ws://($mock_host):($mock_port)/",
        is_pds: true
    }

    print "checking status transitions to Offline..."
    mut offline = false

    for i in 1..20 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 {
            if $res.body.status == "offline" {
                $offline = true
                break
            }
            if $res.body.status == "active" {
                print $"  ... currently ($res.body.status), waiting for offline"
            }
        } else {
            print $"  ... could not get status, waiting: ($res.status)"
        }
        sleep 100ms
    }

    if not $offline {
        fail "host did not transition to offline within time limit" $instance.pid
    }
    print "ok: host transitioned to offline successfully."

    print "starting mock pds websocket server..."
    let mock_pds_handle = (start-mock-pds $mock_port)

    sleep 500ms
    http post -t application/json $"($url)/firehose/sources" {
        url: $"ws://($mock_host):($mock_port)/",
        is_pds: true
    }

    print "checking status transitions back to Active..."
    mut active = false

    for i in 1..40 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 {
            if $res.body.status == "active" {
                $active = true
                break
            }
            if $res.body.status == "offline" {
                print $"  ... currently ($res.body.status), waiting for active"
            }
        } else {
            print $"  ... could not get status, waiting: ($res.status)"
        }
        sleep 100ms
    }

    if not $active {
        stop-mock-pds $mock_pds_handle
        fail "host did not transition to active within time limit" $instance.pid
    }
    print "ok: host transitioned to active successfully."

    print "checking status transitions to Throttled..."
    let put_res = (http put -fe -t application/json $"($url)/pds/tiers" {
        host: $mock_host,
        tier: "custom"
    })
    if $put_res.status != 200 {
        print $"PUT /pds/tiers failed with status ($put_res.status)"
        print $put_res.body
        stop-mock-pds $mock_pds_handle
        fail "failed to change tier" $instance.pid
    }

    mut throttled = false
    for i in 1..10 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 and $res.body.status == "throttled" {
            $throttled = true
            break
        }
        sleep 100ms
    }
    if not $throttled {
        stop-mock-pds $mock_pds_handle
        fail "host did not transition to throttled after tier update" $instance.pid
    }
    print "ok: host transitioned to throttled successfully."

    print "checking status transitions back to Active when limits loosen..."
    http delete -fe $"($url)/pds/tiers?host=($mock_host)"

    mut re_active = false
    for i in 1..10 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 and $res.body.status == "active" {
            $re_active = true
            break
        }
        sleep 100ms
    }

    stop-mock-pds $mock_pds_handle
    try { kill $instance.pid }

    if $re_active {
        print "ok: host transitioned back to active successfully."
    } else {
        fail "host did not transition back to active after tier removed"
    }
}

export def run-pds-tier-rule-status [binary: string, port: int, mock_port: int] {
    let url = $"http://localhost:($port)"
    let db = (mktemp -d -t hydrant_pds_tier_rules.XXXXXX)
    let mock_host = "127.0.0.1"

    print "starting hydrant instance with a glob tier rule..."
    let instance = (with-env {
        HYDRANT_RELAY_HOSTS: "",
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_FIREHOSE_MAX_FAILURES: "1",
        HYDRANT_RATE_TIERS: "custom:1/1/1/1/0",
        HYDRANT_TIER_RULES: $"127.0.0.*:custom"
    } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant instance did not start" $instance.pid
    }

    try { bash -c $"fuser -k ($mock_port)/tcp" } catch {}
    sleep 100ms

    http post -t application/json $"($url)/firehose/sources" {
        url: $"ws://($mock_host):($mock_port)/",
        is_pds: true
    }

    print "waiting for offline..."
    mut offline = false
    for i in 1..20 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 and $res.body.status == "offline" {
            $offline = true
            break
        }
        sleep 100ms
    }
    if not $offline {
        fail "glob test: host did not go offline" $instance.pid
    }

    print "starting mock pds for glob test..."
    let mock_pds = (start-mock-pds $mock_port)
    sleep 500ms
    http post -t application/json $"($url)/firehose/sources" {
        url: $"ws://($mock_host):($mock_port)/",
        is_pds: true
    }

    print "waiting for connected status..."
    mut connected = false
    for i in 1..40 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 and $res.body.status != "offline" {
            $connected = true
            print $"  connected with status: ($res.body.status)"
            break
        }
        sleep 100ms
    }
    if not $connected {
        stop-mock-pds $mock_pds
        fail "glob test: host did not reconnect" $instance.pid
    }

    print "checking glob rule throttles host without explicit tier assignment..."
    mut glob_throttled = false
    for i in 1..10 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        print $"  status \(no set_tier\): ($res.body.status?)"
        if $res.status == 200 and $res.body.status == "throttled" {
            $glob_throttled = true
            break
        }
        sleep 100ms
    }
    if not $glob_throttled {
        stop-mock-pds $mock_pds
        fail $"glob test: expected throttled without set_tier" $instance.pid
    }

    print "checking explicit tier assignment also throttles..."
    http put -fe -t application/json $"($url)/pds/tiers" { host: $mock_host, tier: "custom" }
    mut set_tier_throttled = false
    for i in 1..10 {
        let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 and $res.body.status == "throttled" {
            $set_tier_throttled = true
            break
        }
        sleep 100ms
    }
    if not $set_tier_throttled {
        stop-mock-pds $mock_pds
        fail $"glob test: expected throttled after set_tier" $instance.pid
    }

    print "checking removing explicit tier keeps host throttled via glob rule..."
    http delete -fe $"($url)/pds/tiers?host=($mock_host)"

    sleep 100ms
    let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
    let still_throttled = ($res.status == 200 and $res.body.status == "throttled")

    stop-mock-pds $mock_pds
    try { kill $instance.pid }

    if $still_throttled {
        print "ok: host remains throttled after tier override removed."
    } else {
        fail $"glob test: expected throttled after remove_tier, got ($res.body.status?)"
    }
}
