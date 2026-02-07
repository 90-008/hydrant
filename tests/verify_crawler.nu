#!/usr/bin/env nu
use common.nu *

def main [] {
    # 1. ensure http-nu is installed
    if (which http-nu | is-empty) {
        print "http-nu not found, installing..."
        cargo install http-nu
    }

    # 2. setup ports and paths
    let port = 3006
    let mock_port = 3008
    let url = $"http://localhost:($port)"
    let debug_url = $"http://localhost:($port + 1)"
    let mock_url = $"http://localhost:($mock_port)"
    let db_path = (mktemp -d -t hydrant_full_net.XXXXXX)

    print $"testing full network crawler..."
    print $"database path: ($db_path)"

    # 3. start mock relay
    print $"starting mock relay on ($mock_port)..."
    let mock_pid = (
        bash -c $"http-nu :($mock_port) tests/mock_relay.nu > ($db_path)/mock.log 2>&1 & echo $!" 
        | str trim 
        | into int
    )
    print $"mock relay pid: ($mock_pid)"

    # give mock relay a moment
    sleep 1sec

    # 4. start hydrant in full network mode, firehose disabled
    let binary = build-hydrant
    
    let log_file = $"($db_path)/hydrant.log"
    print $"starting hydrant - logs at ($log_file)..."
    
    let hydrant_pid = (
        with-env {
            HYDRANT_DATABASE_PATH: ($db_path),
            HYDRANT_FULL_NETWORK: "true",
            HYDRANT_RELAY_HOST: ($mock_url),
            HYDRANT_DISABLE_FIREHOSE: "true",
            HYDRANT_DISABLE_BACKFILL: "true",
            HYDRANT_API_PORT: ($port | into string),
            HYDRANT_ENABLE_DEBUG: "true", # for stats checking
            HYDRANT_DEBUG_PORT: ($port + 1 | into string),
            HYDRANT_LOG_LEVEL: "debug",
            HYDRANT_CURSOR_SAVE_INTERVAL: "1" # faster save
        } {
            sh -c $"($binary) >($log_file) 2>&1 & echo $!" | str trim | into int
        }
    )
    print $"hydrant started with pid: ($hydrant_pid)"

    mut success = false
    
    try {
        if (wait-for-api $url) {
            print "hydrant api is up."

            # wait for crawler to run (it runs on startup)
            print "waiting for crawler to fetch repos..."
            
            # retry check for 30s
            for i in 1..30 {
                let stats = (http get $"($url)/stats?accurate=true").counts
                let pending = ($stats.pending | into int)
                let repos = ($stats.repos | default 0 | into int) 
                
                # we expect 5 repos from the mock
                print $"[($i)/30] pending: ($pending), known_repos: ($repos)"
                
                if $repos >= 5 {
                    print "crawler successfully discovered repos!"
                    $success = true
                    break
                }
                
                sleep 1sec
            }
            
            if not $success {
                print "timeout waiting for crawler."
            }
            
            # check cursor persistence
            print "verifying crawler cursor persistence..."
            let cursor_check = try {
                let cursor_res = (http get $"($debug_url)/debug/get?partition=cursors&key=crawler_cursor").value
                print $"cursor value from debug: ($cursor_res)"
                
                if $cursor_res == "50" {
                    print "cursor verified."
                    true
                } else {
                    print "cursor mismatch or missing."
                    false
                }
            } catch {
                print "failed to get cursor from debug endpoint"
                false
            }
            if not $cursor_check { $success = false }

        } else {
             print "hydrant failed to start."
        }
    } catch { |e|
        print $"test failed with error: ($e)"
    }

    # cleanup
    print "stopping processes..."
    try { kill $hydrant_pid }
    try { kill $mock_pid }
    
    if $success {
        print "test passed!"
        exit 0
    } else {
        print "test failed!"
        print "hydrant logs:"
        open $log_file | tail -n 20
        print "mock logs:"
        open $"($db_path)/mock.log"
        exit 1
    }
}
