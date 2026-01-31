#!/usr/bin/env nu
use common.nu *

# simplistic dotenv parser
def load-env-file [] {
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

def authenticate [pds_url: string, identifier: string, password: string] {
    print $"authenticating with ($pds_url) for ($identifier)..."
    let resp = (http post -t application/json $"($pds_url)/xrpc/com.atproto.server.createSession" {
        identifier: $identifier,
        password: $password
    })
    return $resp
}

def create-record [pds_url: string, jwt: string, repo: string, collection: string, record: any] {
    http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.createRecord" {
        repo: $repo,
        collection: $collection,
        record: $record
    }
}

def delete-record [pds_url: string, jwt: string, repo: string, collection: string, rkey: string] {
    http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.deleteRecord" {
        repo: $repo,
        collection: $collection,
        rkey: $rkey
    }
}

def resolve-pds [did: string] {
    print $"resolving pds for ($did)..."
    let doc = (http get $"https://plc.wtf/($did)" | from json)
    let pds = ($doc.service | where type == "AtprotoPersonalDataServer" | first).serviceEndpoint
    print $"resolved pds: ($pds)"
    return $pds
}

def main [] {
    let env_vars = load-env-file
    let did = ($env_vars | get --optional TEST_REPO)
    let password = ($env_vars | get --optional TEST_PASSWORD)
    
    if ($did | is-empty) or ($password | is-empty) {
        print "error: TEST_REPO and TEST_PASSWORD must be set in .env"
        exit 1
    }

    let pds_url = resolve-pds $did
    
    let port = 3003
    let url = $"http://localhost:($port)"
    let ws_url = $"ws://localhost:($port)/stream"
    let db_path = (mktemp -d -t hydrant_auth_test.XXXXXX)
    
    # 1. authenticate
    print $"authenticating with ($pds_url)..."
    let session = authenticate $pds_url $did $password
    let jwt = $session.accessJwt
    print "authentication successful"

    # 2. start hydrant
    print $"starting hydrant on port ($port)..."
    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port
    
    mut test_passed = false

    if (wait-for-api $url) {
        # 3. start listener (live stream)
        let output_file = $"($db_path)/stream_output.txt"
        print $"starting stream listener -> ($output_file)"
        # use websocat to capture output. 
        let stream_pid = (bash -c $"websocat '($ws_url)' > '($output_file)' 2>&1 & echo $!" | str trim | into int)
        print $"listener pid: ($stream_pid)"
        
        # 4. add repo to hydrant (backfill trigger)
        print $"adding repo ($did) to tracking..."
        try {
            http post -t application/json $"($url)/repo/add" { dids: [($did)] }
        } catch {
            print "warning: failed to add repo (might already be tracked), continuing..."
        }

        # wait for connection stability and potential backfill start
        sleep 2sec

        # 5. perform actions
        let collection = "app.bsky.feed.post"
        let timestamp = (date now | format date "%Y-%m-%dT%H:%M:%SZ")
        let record_data = {
            "$type": "app.bsky.feed.post",
            text: $"hydrant integration test ($timestamp)",
            createdAt: $timestamp
        }

        print "--- action: create ---"
        let create_res = create-record $pds_url $jwt $did $collection $record_data
        print $"created uri: ($create_res.uri)"
        print $"created cid: ($create_res.cid)"
        let rkey = ($create_res.uri | split row "/" | last)

        print "--- action: update ---"
        let update_data = ($record_data | update text $"updated text ($timestamp)")
        
        try {
            http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.putRecord" {
                repo: $did,
                collection: $collection,
                rkey: $rkey,
                record: $update_data,
            }
            print "updated record"
        } catch { |err|
            print $"update failed: ($err)"
            # try to continue to delete
        }

        print "--- action: delete ---"
        delete-record $pds_url $jwt $did $collection $rkey
        print "deleted record"

        # 6. verify
        sleep 2sec
        print "stopping listener..."
        try { kill $stream_pid }
        
        if ($output_file | path exists) {
            let content = (open $output_file | str trim)
            if ($content | is-empty) {
                print "failed: no events captured"
            } else {
                # parse json lines
                let events = ($content | lines | each { |it| $it | from json })
                print $"captured ($events | length) events"
                
                # hydrant stream events seem to be type: "record" with payload in "record" field
                # structure: { id: ..., type: "record", record: { action: ..., collection: ..., rkey: ... } }
                
                let relevant_events = ($events | where type == "record" and record.collection == $collection and record.rkey == $rkey)
                
                let creates = ($relevant_events | where record.action == "create")
                let updates = ($relevant_events | where record.action == "update")
                let deletes = ($relevant_events | where record.action == "delete")
                
                print $"found creates: ($creates | length)"
                print $"found updates: ($updates | length)"
                print $"found deletes: ($deletes | length)"

                if ($relevant_events | length) != 3 {
                     print "test failed: expected exactly 3 events"
                     print "captured events:"
                     print ($events | table -e)
                } else {
                     let first = ($relevant_events | get 0)
                     let second = ($relevant_events | get 1)
                     let third = ($relevant_events | get 2)

                     if ($first.record.action == "create") and ($second.record.action == "update") and ($third.record.action == "delete") {
                         print "test passed: all operations (create, update, delete) captured in correct order"
                         $test_passed = true
                     } else {
                         print "test failed: events out of order or incorrect"
                         print "captured events:"
                         print ($events | table -e)
                     }
                }
            }
        } else {
            print "failed: output file missing"
        }

    } else {
        print "hydrant failed to start"
    }

    # cleanup
    print "cleaning up..."
    try { kill $instance.pid }
    
    if $test_passed {
        exit 0
    } else {
        exit 1
    }
}
