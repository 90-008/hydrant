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
        exit 0
    } else {
        fail "host did not transition back to active after tier removed"
    }
}
