use ../common.nu *

export def run-ephemeral-instance [name: string, port: int, scenario_closure: closure] {
    let debug_port = resolve-test-debug-port ($port + 1)
    let url = $"http://localhost:($port)"
    let debug_url = $"http://localhost:($debug_port)"
    let db_path = (mktemp -d -t hydrant_ephemeral_ttl.XXXXXX)

    print $"--- running scenario: ($name) ---"
    print $"database path: ($db_path)"

    let binary = build-hydrant
    let instance = (with-env { HYDRANT_EPHEMERAL: "true", HYDRANT_EPHEMERAL_TTL: "60min" } {
        start-hydrant $binary $db_path $port
    })

    try {
        if not (wait-for-api $url) {
            error make {msg: "api failed to start"}
        }

        do $scenario_closure $url $debug_url

        print $"PASSED: ($name)\n"
    } catch { |e|
        print $"test failed: ($e.msg)"
        try { kill --force $instance.pid }
        sleep 2sec
        exit 1
    }

    try { kill --force $instance.pid }
    sleep 2sec
}

export def run-relay-instance [name: string, port: int, scenario_closure: closure] {
    let debug_port = resolve-test-debug-port ($port + 1)
    let url = $"http://localhost:($port)"
    let debug_url = $"http://localhost:($debug_port)"
    let db_path = (mktemp -d -t hydrant_relay_ttl.XXXXXX)

    print $"--- running scenario: ($name) ---"
    print $"database path: ($db_path)"

    let binary = build-hydrant-relay
    let instance = (with-env { HYDRANT_RELAY: "true", HYDRANT_EPHEMERAL_TTL: "60min" } {
        start-hydrant $binary $db_path $port
    })

    try {
        if not (wait-for-api $url) {
            error make {msg: "api failed to start"}
        }

        do $scenario_closure $url $debug_url

        print $"PASSED: ($name)\n"
    } catch { |e|
        print $"test failed: ($e.msg)"
        try { kill --force $instance.pid }
        sleep 2sec
        exit 1
    }

    try { kill --force $instance.pid }
    sleep 2sec
}

export def trigger-ttl-tick [debug_url: string] {
    print "triggering ephemeral TTL tick..."
    let response = (http post -f -e -H [Content-Length 0] $"($debug_url)/debug/ephemeral_ttl_tick" "")
    if $response.status != 200 {
        error make {msg: $"FAILED: ephemeral_ttl_tick returned ($response.status)"}
    }
    print "TTL tick complete"
}
