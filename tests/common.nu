# build the hydrant binary
export def build-hydrant [] {
    print "building hydrant..."
    cargo build --release --quiet
    "target/release/hydrant"
}

# start hydrant in the background
export def start-hydrant [binary: string, db_path: string, port: int] {
    let log_file = $"($db_path)/hydrant.log"
    print $"starting hydrant - logs at ($log_file)..."
    
    let pid = (
        with-env {
            HYDRANT_DATABASE_PATH: ($db_path),
            HYDRANT_FULL_NETWORK: "false",
            HYDRANT_API_PORT: ($port | into string),
            HYDRANT_LOG_LEVEL: "debug"
        } {
            sh -c $"($binary) >($log_file) 2>&1 & echo $!" | str trim | into int
        }
    )
    
    print $"hydrant started with pid: ($pid)"
    { pid: $pid, log: $log_file }
}

# wait for the api to become responsive
export def wait-for-api [url: string] {
    print "waiting for api to be ready..."
    for i in 1..30 {
        try {
            http get $"($url)/stats"
            return true
        } catch {
            sleep 1sec
        }
    }
    false
}

# poll stats until backfill is complete or fails
export def wait-for-backfill [url: string] {
    print "waiting for backfill to complete..."
    for i in 1..120 {
        let stats = (http get $"($url)/stats?accurate=true").keyspace_stats
        let pending = ($stats | where name == "pending" | first).count
        let records = ($stats | where name == "records" | first).count
        let repos = ($stats | where name == "repos" | first).count
        let resync = ($stats | where name == "resync" | first).count

        print $"[($i)/120] pending: ($pending), records: ($records), repos: ($repos), resync: ($resync)"

        if $resync > 0 {
            print "resync state detected (failure or gone)!"
            print ($stats | table)
            return false
        }

        if ($pending == 0) and ($repos > 0) and ($records > 0) {
            print "backfill complete."
            return true
        }
        
        sleep 2sec
    }
    false
}