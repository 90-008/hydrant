use ../common.nu *

export def load-authenticated-stream-config [] {
    let env_vars = load-env-file
    let did = ($env_vars | get --optional TEST_REPO)
    let password = ($env_vars | get --optional TEST_PASSWORD)

    if ($did | is-empty) or ($password | is-empty) {
        print "SKIP: TEST_REPO and TEST_PASSWORD not set in .env"
        exit 0
    }

    {
        did: $did,
        password: $password,
        pds_url: (resolve-pds $did),
        binary: (build-hydrant)
    }
}

def relevant-auth-events [output_file: string] {
    if not ($output_file | path exists) {
        return []
    }

    let content = (open $output_file | str trim)
    if ($content | is-empty) {
        return []
    }

    $content
        | lines
        | each { |it| try { $it | from json } catch { null } }
        | compact
        | where { |it|
            if $it.type == "record" {
                if ($it.record | get -o live) == false {
                    return false
                }
            }
            true
        }
}

def auth-events-match [events: list] {
    if ($events | length) != 6 {
        return false
    }

    let e0 = ($events | get 0)
    let e1 = ($events | get 1)
    let e2 = ($events | get 2)
    let e3 = ($events | get 3)
    let e4 = ($events | get 4)
    let e5 = ($events | get 5)

    let ok0 = ($e0.type == "account" and $e0.account.active == true)
    let ok1 = ($e1.type == "record" and $e1.record.action == "create")
    let ok2 = ($e2.type == "record" and $e2.record.action == "update")
    let ok3 = ($e3.type == "record" and $e3.record.action == "delete")
    let ok4 = ($e4.type == "account" and $e4.account.active == false)
    let ok5 = ($e5.type == "account" and $e5.account.active == true)

    $ok0 and $ok1 and $ok2 and $ok3 and $ok4 and $ok5
}

def ensure-account-active [pds_url: string, did: string, password: string] {
    let session = authenticate $pds_url $did $password
    let active = ($session | get -o active | default true)

    if $active == false {
        print "account is deactivated; activating before test..."
        activate-account $pds_url $session.accessJwt
        sleep 2sec

        let activated = authenticate $pds_url $did $password
        let activated_active = ($activated | get -o active | default true)
        if $activated_active == false {
            fail "account remained deactivated after activateAccount"
        }
        return $activated
    }

    $session
}

def wait-for-auth-events [output_file: string, timeout_secs: int] {
    let attempts = $timeout_secs * 2
    mut latest = []

    for i in 1..($attempts) {
        $latest = relevant-auth-events $output_file
        if (auth-events-match $latest) {
            return $latest
        }
        sleep 500ms
    }

    $latest
}

export def run-authenticated-stream-test [
    binary: string,
    did: string,
    password: string,
    pds_url: string,
    relays: string,
    port: int,
    label: string
] {
    let url = $"http://localhost:($port)"
    let ws_url = $"ws://127.0.0.1:($port)/stream"
    let db_path = (mktemp -d -t hydrant_auth_stream.XXXXXX)

    print $"=== running authenticated stream test: ($label) ==="
    print $"authenticating with ($pds_url)..."
    let session = ensure-account-active $pds_url $did $password
    mut jwt = $session.accessJwt
    print "authentication successful"

    print $"starting hydrant on port ($port) with relays: ($relays)..."
    let instance = (with-env { HYDRANT_RELAY_HOSTS: $relays } {
        start-hydrant $binary $db_path $port
    })

    mut test_passed = false

    if (wait-for-api $url) {
        let output_file = $"($db_path)/stream_output.txt"
        print $"starting stream listener -> ($output_file)"
        let stream_pid = (bash -c $"websocat -n '($ws_url)' > '($output_file)' & echo $!" | str trim | into int)
        print $"listener pid: ($stream_pid)"

        print $"adding repo ($did) to tracking..."
        try {
            http put -t application/json $"($url)/repos" [{ did: $did }]
        } catch {
            print "warning: failed to add repo, continuing..."
        }
        if not (wait-for-backfill $url) {
            print "failed: backfill did not complete"
            print "stopping listener..."
            try { kill -9 $stream_pid }
            print "cleaning up..."
            try { kill -9 $instance.pid }
            return false
        }

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
        }

        print "--- action: delete ---"
        delete-record $pds_url $jwt $did $collection $rkey
        print "deleted record"

        print "--- action: deactivate ---"
        deactivate-account $pds_url $jwt
        mut account_deactivated = true

        sleep 1sec

        print "re-authenticating..."
        let session = authenticate $pds_url $did $password
        $jwt = $session.accessJwt

        sleep 1sec

        print "--- action: activate ---"
        try {
            activate-account $pds_url $jwt
            $account_deactivated = false
        } catch { |err|
            print $"activation failed: ($err.msg)"
        }

        print "waiting for expected stream events..."
        let relevant_events = wait-for-auth-events $output_file 20

        print "stopping listener..."
        try { kill -9 $stream_pid }

        if ($output_file | path exists) {
            let content = (open $output_file | str trim)
            if ($content | is-empty) {
                print "failed: no events captured"
            } else {
                let events = ($content | lines | each { |it| $it | from json })
                let display_events = ($events | each { |e|
                    let value = if $e.type == "record" { $e | get -o record } else if $e.type == "account" { $e | get -o account } else { $e | get -o identity }
                    $e | select id type | insert value $value
                })
                print $"captured ($events | length) events"
                $display_events | to text | print

                if ($relevant_events | length) != 6 {
                    print $"verification failed: expected 6 events, got ($relevant_events | length)"
                } else {
                    if (auth-events-match $relevant_events) {
                        print "test success!"
                        $test_passed = true
                    } else {
                        print "verification failed: event sequence did not match create/update/delete/deactivate/activate"
                        print $"events: ($relevant_events)"
                    }
                }
            }
        } else {
            print "failed: output file missing"
        }

        if $account_deactivated {
            print "ensuring account is active after test..."
            try { activate-account $pds_url $jwt }
        }
    } else {
        print "hydrant failed to start"
    }

    print "cleaning up..."
    try { kill -9 $instance.pid }

    $test_passed
}
