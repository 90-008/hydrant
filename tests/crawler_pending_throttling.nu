#!/usr/bin/env nu
use common.nu *

def log-contains [path: string, needle: string] {
    ($path | path exists) and ((open $path | str replace --all "\n" " ") | str contains $needle)
}

def main [] {
    # 1. ensure http-nu is installed
    if (which http-nu | is-empty) {
        print "http-nu not found, installing..."
        cargo install http-nu
    }

    # 2. setup ports and paths
    let port = resolve-test-port 3010
    let mock_port = resolve-test-mock-port 3012
    let url = $"http://localhost:($port)"
    let mock_url = $"http://localhost:($mock_port)"
    let db_path = (mktemp -d -t hydrant_throttling.XXXXXX)

    print $"testing crawler throttling..."
    print $"database path: ($db_path)"

    # 3. start mock relay
    print $"starting mock relay on ($mock_port)..."
    let mock_pid = (
        bash -c $"http-nu :($mock_port) tests/mock_relay.nu > ($db_path)/mock.log 2>&1 & echo $!" 
        | str trim 
        | into int
    )
    print $"mock relay pid: ($mock_pid)"

    # 4. start hydrant with low throttling limits
    let binary = build-hydrant
    
    let log_file = $"($db_path)/hydrant.log"
    print $"starting hydrant - logs at ($log_file)..."
    
    let hydrant_pid = (
        with-env {
            HYDRANT_DATABASE_PATH: ($db_path),
            HYDRANT_FULL_NETWORK: "true",
            HYDRANT_RELAY_HOST: ($mock_url),
            HYDRANT_ENABLE_FIREHOSE: "false",
            HYDRANT_ENABLE_CRAWLER: "false",
            HYDRANT_API_BIND: $"127.0.0.1:($port)",
            HYDRANT_LOG_LEVEL: "debug",
            RUST_LOG: "debug",
            HYDRANT_CRAWLER_MAX_PENDING_REPOS: "2",
            HYDRANT_CRAWLER_RESUME_PENDING_REPOS: "1"
        } {
            sh -c $"($binary) >($log_file) 2>&1 & echo $!" | str trim | into int
        }
    )
    print $"hydrant started with pid: ($hydrant_pid)"

    mut success = false
    
    try {
        if (wait-for-api $url) {
            print "hydrant api is up."

            print "pausing backfill..."
            http patch -t application/json $"($url)/ingestion" {
                backfill: false,
                firehose: false,
            } | ignore

            print "enabling crawler..."
            http patch -t application/json $"($url)/ingestion" {
                crawler: true
            } | ignore

            print "waiting for crawler to hit throttling limit..."

            mut discovered = false
            for i in 1..40 {
                let stats = (http get $"($url)/stats").counts
                let pending = ($stats.pending | into int)
                print $"[($i)/40] pending: ($pending)"

                if $pending >= 5 {
                    print "crawler discovered repos."
                    $discovered = true
                    break
                }

                sleep 250ms
            }

            if not $discovered {
                print "FAILED: crawler did not enqueue mock repos"
            } else {
                print "checking logs for throttling message..."

                mut throttled = false
                for i in 1..12 {
                    if (log-contains $log_file "throttling: above max pending") {
                        $throttled = true
                        break
                    }
                    sleep 500ms
                }

                if $throttled {
                    print "CONFIRMED: crawler is throttling!"

                    print "testing resumption by letting backfill drain the queue..."
                    http patch -t application/json $"($url)/ingestion" {
                        backfill: true
                    } | ignore

                    print "waiting for crawler to release throttling..."
                    mut released = false
                    for i in 1..12 {
                        if (log-contains $log_file "throttling released") {
                            $released = true
                            break
                        }
                        sleep 500ms
                    }

                    if $released {
                        print "CONFIRMED: crawler resumed!"
                        $success = true
                    } else {
                        print "FAILED: resumption message not found in logs"
                        $success = false
                    }
                } else {
                    print "FAILED: throttling message not found in logs"
                }
            }

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
        exit 1
    }
}
