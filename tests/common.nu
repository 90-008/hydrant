export def load-env-file [] {
    if (".env" | path exists) {
        let content = (open .env)
        $content | lines
        | where { |x| ($x | str trim | is-empty) == false and ($x | str trim | str starts-with "#") == false }
        | each { |x|
            let parts = ($x | split row "=" -n 2)
            { key: ($parts.0 | str trim), value: ($parts.1 | str trim | str trim -c '"' | str trim -c "'") }
        }
        | reduce -f {} { |it, acc| $acc | insert $it.key $it.value }
    } else {
        {}
    }
}

export def resolve-pds [did: string] {
    let doc = (http get $"https://plc.wtf/($did)" | from json)
    ($doc.service | where type == "AtprotoPersonalDataServer" | first).serviceEndpoint
}

export def authenticate [pds_url: string, identifier: string, password: string] {
    http post -t application/json $"($pds_url)/xrpc/com.atproto.server.createSession" {
        identifier: $identifier,
        password: $password
    }
}

export def create-record [pds_url: string, jwt: string, repo: string, collection: string, record: any] {
    http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.createRecord" {
        repo: $repo,
        collection: $collection,
        record: $record
    }
}

export def delete-record [pds_url: string, jwt: string, repo: string, collection: string, rkey: string] {
    http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.deleteRecord" {
        repo: $repo,
        collection: $collection,
        rkey: $rkey
    }
}

export def deactivate-account [pds_url: string, jwt: string] {
    http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.server.deactivateAccount" {}
}

export def activate-account [pds_url: string, jwt: string] {
    curl -X POST -H "Content-Type: application/json" -H $"Authorization: Bearer ($jwt)" $"($pds_url)/xrpc/com.atproto.server.activateAccount"
}

# build the hydrant binary
export def build-hydrant [] {
    print "building hydrant..."
    cargo build --release
    "target/release/hydrant"
}

# start hydrant in the background
export def start-hydrant [binary: string, db_path: string, port: int] {
    let log_file = $"($db_path)/hydrant.log"
    print $"starting hydrant - logs at ($log_file)..."
    
    let hydrant_vars = ($env | transpose k v | where k =~ "HYDRANT_" | reduce -f {} { |it, acc| $acc | upsert $it.k $it.v })
    let env_vars = {
        HYDRANT_DATABASE_PATH: ($db_path),
        HYDRANT_FULL_NETWORK: "false",
        HYDRANT_API_PORT: ($port | into string),
        HYDRANT_ENABLE_DEBUG: "true",
        HYDRANT_DEBUG_PORT: ($port + 1 | into string),
        HYDRANT_LOG_LEVEL: "debug"
    } | merge $hydrant_vars

    let pid = (with-env $env_vars {
        sh -c $"($binary) >($log_file) 2>&1 & echo $!" | str trim | into int
    })
    
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
        let stats = (http get $"($url)/stats?accurate=true").counts
        let pending = ($stats.pending | into int)
        let records = ($stats.records | into int)
        let repos = ($stats.repos | into int)
        let resync = ($stats.resync | into int)

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
