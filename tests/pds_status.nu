source common.nu

source mock_pds.nu

def main [] {
    let port = resolve-test-port 3033
    let url = $"http://localhost:($port)"
    let binary = build-hydrant
    let db = (mktemp -d -t hydrant_test.XXXXXX)

    let instance = (with-env { 
        HYDRANT_RELAY_HOSTS: "", 
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_RATE_TIERS: "custom:1/1/1/1/0"
    } {
        start-hydrant $binary $db $port
    })
    if not (wait-for-api $url) {
        fail "hydrant did not start" $instance.pid
    }

    let mock_port = resolve-test-mock-port 9999
    let mock_host = "127.0.0.1"

    # kill any stale listener on the mock port from a previous failed run
    try { bash -c $"fuser -k ($mock_port)/tcp" } catch {}
    sleep 100ms

    print "adding offline mock pds via firehose sources..."
    http post -t application/json $"($url)/firehose/sources" {
        url: $"ws://($mock_host):($mock_port)/",
        is_pds: true
    }

    print "checking status transitions to Offline..."
    mut offline = false

    # the throttle backoff will cap at 1 second in debug builds.
    # it takes 4 consecutive failures to mark as offline.
    # therefore, 4 * 1 = ~4 seconds maximum for transition.
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
        sleep 2sec
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

    # now wait for it to successfully reconnect and the active_sleep of 1s to pass.
    for i in 1..20 {
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
        sleep 2sec
    }

    if $active {
        print "ok: host transitioned to active successfully."
    } else {
        stop-mock-pds $mock_pds_handle
        try { kill $instance.pid }
        fail "host did not transition to active within time limit"
    }

    print "checking status transitions to Throttled..."
    let put_res = (http put -fe -t application/json $"($url)/pds/tiers" {
        host: $mock_host,
        tier: "custom"
    })
    if $put_res.status != 200 {
        print $"PUT /pds/tiers failed with status ($put_res.status)"
        print $put_res.body
        stop-mock-pds $mock_pds_handle
        try { kill $instance.pid }
        fail "failed to change tier"
    }

    # since we updated the tier via API, the status should change immediately
    mut throttled = false
    let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
    if $res.status == 200 and $res.body.status == "throttled" {
        $throttled = true
    }

    if not $throttled {
        stop-mock-pds $mock_pds_handle
        try { kill $instance.pid }
        fail "host did not transition to throttled after tier update"
    }
    print "ok: host transitioned to throttled successfully."

    print "checking status transitions back to Active when limits loosen..."
    http delete -fe $"($url)/pds/tiers?host=($mock_host)"

    # should change back immediately
    mut re_active = false
    let res = (http get -fe $"($url)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
    if $res.status == 200 and $res.body.status == "active" {
        $re_active = true
    }

    stop-mock-pds $mock_pds_handle
    try { kill $instance.pid }

    if $re_active {
        print "ok: host transitioned back to active successfully."
    } else {
        fail "host did not transition back to active after tier removed"
    }

    # verify that,
    # 1. the glob rule resolves the tier for unassigned hosts (no explicit PUT /pds/tiers needed)
    # 2. after removing an explicit tier override, the glob rule still applies -> host stays throttled

    print "starting hydrant instance with a glob tier rule..."
    let port2 = ($port + 100)
    let url2 = $"http://localhost:($port2)"
    let db2 = (mktemp -d -t hydrant_test.XXXXXX)

    let instance2 = (with-env {
        HYDRANT_RELAY_HOSTS: "",
        HYDRANT_CRAWLER_URLS: "",
        HYDRANT_RATE_TIERS: "custom:1/1/1/1/0",
        HYDRANT_TIER_RULES: $"127.0.0.*:custom"
    } {
        start-hydrant $binary $db2 $port2
    })
    if not (wait-for-api $url2) {
        try { kill $instance2.pid }
        fail "hydrant instance did not start"
    }

    # kill any stale listener from first round
    try { bash -c $"fuser -k ($mock_port)/tcp" } catch {}
    sleep 100ms

    # connect mock pds and wait for offline -> active cycle (same as above)
    http post -t application/json $"($url2)/firehose/sources" {
        url: $"ws://($mock_host):($mock_port)/",
        is_pds: true
    }

    # wait for offline
    print "waiting for offline..."
    mut offline2 = false
    for i in 1..20 {
        let res = (http get -fe $"($url2)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 and $res.body.status == "offline" {
            $offline2 = true
            break
        }
        sleep 2sec
    }
    if not $offline2 {
        try { kill $instance2.pid }
        fail "glob test: host did not go offline"
    }

    print "starting mock pds for glob test..."
    let mock_pds2 = (start-mock-pds $mock_port)
    sleep 500ms
    http post -t application/json $"($url2)/firehose/sources" {
        url: $"ws://($mock_host):($mock_port)/",
        is_pds: true
    }

    # with account_limit=0 and the glob rule active, the host goes straight to throttled
    # on the first successful connection — no explicit set_tier call needed.
    print "waiting for connected (expect throttled, not active)..."
    mut connected2 = false
    for i in 1..20 {
        let res = (http get -fe $"($url2)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
        if $res.status == 200 and $res.body.status != "offline" {
            $connected2 = true
            print $"  connected with status: ($res.body.status)"
            break
        }
        sleep 2sec
    }
    if not $connected2 {
        stop-mock-pds $mock_pds2
        try { kill $instance2.pid }
        fail "glob test: host did not reconnect"
    }

    # verify the glob rule throttled the host automatically (no set_tier was called)
    print "checking glob test: glob rule throttles host without explicit tier assignment..."
    let res = (http get -fe $"($url2)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
    print $"  status \(no set_tier\): ($res.body.status?)"
    if $res.status != 200 or $res.body.status != "throttled" {
        stop-mock-pds $mock_pds2
        try { kill $instance2.pid }
        fail $"glob test: expected throttled without set_tier \(glob rule should apply\), got ($res.body.status?)"
    }
    print "ok: host throttled by glob rule without explicit tier assignment."

    # set explicit tier -> still throttled (sanity check)
    print "checking glob test: explicit tier assignment also throttles..."
    http put -fe -t application/json $"($url2)/pds/tiers" { host: $mock_host, tier: "custom" }
    let res = (http get -fe $"($url2)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
    print $"  status after set_tier: ($res.body.status?)"
    if $res.status != 200 or $res.body.status != "throttled" {
        stop-mock-pds $mock_pds2
        try { kill $instance2.pid }
        fail $"glob test: expected throttled after set_tier, got ($res.body.status?)"
    }
    print "ok: host throttled via explicit tier."

    # remove explicit override -> glob rule still applies -> still throttled
    print "checking glob test: remove explicit tier keeps host throttled via glob rule..."
    http delete -fe $"($url2)/pds/tiers?host=($mock_host)"

    sleep 500ms
    let res = (http get -fe $"($url2)/xrpc/com.atproto.sync.getHostStatus?hostname=($mock_host)")
    print $"  status after remove_tier: ($res.body.status?)"
    let still_throttled = ($res.status == 200 and $res.body.status == "throttled")

    stop-mock-pds $mock_pds2
    try { kill $instance2.pid }

    if $still_throttled {
        print "ok: host remains throttled after tier override removed (glob rule applies)."
        exit 0
    } else {
        fail $"glob test: expected throttled after remove_tier \(glob rule should apply\), got ($res.body.status?)"
    }
}
