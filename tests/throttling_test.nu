#!/usr/bin/env nu
use common.nu *

def main [] {
    # 1. ensure http-nu is installed
    if (which http-nu | is-empty) {
        print "http-nu not found, installing..."
        cargo install http-nu
    }

    # 2. setup ports and paths
    let port = 3010
    let mock_port = 3012
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

    # give mock relay a moment
    sleep 1sec

    # 4. start hydrant with low throttling limits
    let binary = build-hydrant
    
    let log_file = $"($db_path)/hydrant.log"
    print $"starting hydrant - logs at ($log_file)..."
    
    let hydrant_pid = (
        with-env {
            HYDRANT_DATABASE_PATH: ($db_path),
            HYDRANT_FULL_NETWORK: "true",
            HYDRANT_RELAY_HOST: ($mock_url),
            HYDRANT_DISABLE_FIREHOSE: "true",
            HYDRANT_DISABLE_BACKFILL: "true", # disable backfill so pending count stays up
            HYDRANT_API_PORT: ($port | into string),
            HYDRANT_LOG_LEVEL: "debug",
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

            # wait for crawler to run and hit limit
            print "waiting for crawler to hit throttling limit..."
            
            # retry check for 30s
            for i in 1..30 {
                let stats = (http get $"($url)/stats?accurate=true").counts
                let pending = ($stats.pending | into int)
                
                # we expect 5 repos from the mock, but max pending is 2. 
                # wait, the crawler fetches a page (5 repos) THEN adds to DB.
                # so pending will jump to 5.
                # then next loop, it checks pending > 2.
                # so pending should be 5.
                
                print $"[($i)/30] pending: ($pending)"
                
                if $pending >= 5 {
                    print "crawler discovered repos."
                    break
                }
                
                sleep 1sec
            }
            
            # now check logs for throttling message
            print "checking logs for throttling message..."
            sleep 2sec # give logging a moment
            
            let logs = (open $log_file | str replace --all "\n" " ")
            if ($logs | str contains "crawler throttling: pending repos") {
                print "CONFIRMED: crawler is throttling!"
                
                # now testing resumption
                print "testing resumption by removing repos..."
                
                # remove 4 repos to drop pending (5) to 1 (<= resume limit 1)
                # mock repos are did:web:mock1.com ... mock5.com
                curl -s -X DELETE -H "Content-Type: application/json" -d '[
                    {"did": "did:web:mock1.com"},
                    {"did": "did:web:mock2.com"},
                    {"did": "did:web:mock3.com"},
                    {"did": "did:web:mock4.com"}
                ]' $"($url)/repos"
                
                print "waiting for crawler to wake up (max 10s)..."
                sleep 15sec
                
                # check logs for resumption message
                let logs_after = (open $log_file | str replace --all "\n" " ")
                if ($logs_after | str contains "crawler resuming") {
                     print "CONFIRMED: crawler resumed!"
                     $success = true
                } else {
                     print "FAILED: resumption message not found in logs"
                     $success = false
                }
                
            } else {
                print "FAILED: throttling message not found in logs"
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
