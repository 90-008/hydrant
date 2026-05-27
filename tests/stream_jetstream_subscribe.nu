#!/usr/bin/env nu
# Tests the Jetstream-compatible /subscribe WebSocket endpoint.
#
# Requires TEST_REPO and TEST_PASSWORD in .env (same credentials used by
# authenticated_stream tests). Skips gracefully when credentials are absent.
#
# Jetstream event format:
#   {did, time_us, kind: "commit"|"identity"|"account", commit?|identity?|account?}
#   commit:   {rev, operation, collection, rkey, record?, cid?}
#   identity: {did, seq, time, handle?}
#   account:  {did, active, seq, time, status?}

use common.nu *

# Collect newline-delimited JSON events from a WebSocket into a list.
def collect-ws-json [ws_url: string, output_file: string, duration: duration] {
    let pid_file = $"($output_file).pid"
    bash -c $"websocat -n '($ws_url)' > '($output_file)' 2>&1 & echo $! > '($pid_file)'"
    sleep $duration
    let pid = (open $pid_file | str trim | into int)
    try { kill $pid }
    sleep 200ms
    if ($output_file | path exists) {
        open $output_file
        | str trim
        | lines
        | each { |line| try { $line | from json } catch { null } }
        | compact
    } else {
        []
    }
}

def assert-no-error-events [events: list, label: string, ...pids: int] {
    let errors = ($events | where { |e|
        let t = ($e | get -o type | default "")
        let k = ($e | get -o kind | default "")
        $t == "error" or $k == "error"
    })
    if ($errors | length) > 0 {
        fail $"($label): unexpected error event(s): ($errors | first)" ...$pids
    }
}

def assert-commit-structure [c: record, label: string, ...pids: int] {
    for field in ["rev", "operation", "collection", "rkey"] {
        if not ($field in $c) {
            fail $"($label): commit missing field ($field)" ...$pids
        }
    }
    if $c.operation == "create" or $c.operation == "update" {
        if not ("cid" in $c) {
            fail $"($label): create/update commit missing cid" ...$pids
        }
        if not ("record" in $c) {
            fail $"($label): create/update commit missing record" ...$pids
        }
    }
}

def main [] {
    let env_vars = load-env-file
    let did = ($env_vars | get --optional TEST_REPO)
    let password = ($env_vars | get --optional TEST_PASSWORD)

    if ($did | is-empty) or ($password | is-empty) {
        print "SKIP: TEST_REPO and TEST_PASSWORD not set in .env"
        exit 0
    }

    let port = resolve-test-port 3016
    let url = $"http://localhost:($port)"
    let ws_base = $"ws://127.0.0.1:($port)"
    let db_path = (mktemp -d -t hydrant_stream_jetstream_subscribe.XXXXXX)

    print $"testing /subscribe \(Jetstream\) for ($did)..."
    print $"database path: ($db_path)"

    let pds_url = resolve-pds $did

    # resolve session, activating if needed
    let session = (do {
        let s = authenticate $pds_url $did $password
        let active = ($s | get -o active | default true)
        if $active == false {
            print "account deactivated; activating before test..."
            activate-account $pds_url $s.accessJwt
            sleep 2sec
            authenticate $pds_url $did $password
        } else {
            $s
        }
    })
    mut jwt = $session.accessJwt
    print "authentication successful"

    let binary = build-hydrant-features "jetstream"
    let instance = (with-env { HYDRANT_RELAY_HOSTS: "wss://bsky.network" } {
        start-hydrant $binary $db_path $port
    })

    if not (wait-for-api $url) {
        fail "api failed to start" $instance.pid
    }

    print $"adding repo ($did)..."
    http put -t application/json $"($url)/repos" [{ did: $did }]

    if not (wait-for-backfill $url) {
        fail "backfill timed out" $instance.pid
    }

    # start live subscriber before creating records so we catch the events
    let live_file = $"($db_path)/live.txt"
    print "starting live /subscribe listener..."
    let live_pid_file = $"($live_file).pid"
    bash -c $"websocat -n '($ws_base)/subscribe' > '($live_file)' 2>&1 & echo $! > '($live_pid_file)'"
    let live_pid = (open $live_pid_file | str trim | into int)
    print $"live listener pid: ($live_pid)"
    sleep 1sec

    # --- create / update / delete a record to generate live jetstream events ---
    let collection = "app.bsky.feed.post"
    let timestamp = (date now | format date "%Y-%m-%dT%H:%M:%SZ")
    let record_data = {
        "$type": "app.bsky.feed.post",
        text: $"hydrant jetstream test ($timestamp)",
        createdAt: $timestamp
    }

    print "--- action: create ---"
    let create_res = create-record $pds_url $jwt $did $collection $record_data
    let rkey = ($create_res.uri | split row "/" | last)
    print $"created rkey: ($rkey)"

    print "--- action: update ---"
    try {
        http post -t application/json -H ["Authorization" $"Bearer ($jwt)"] $"($pds_url)/xrpc/com.atproto.repo.putRecord" {
            repo: $did, collection: $collection, rkey: $rkey,
            record: ($record_data | update text $"updated text ($timestamp)")
        }
        print "updated record"
    } catch { |e| print $"update skipped: ($e.msg)" }

    print "--- action: delete ---"
    delete-record $pds_url $jwt $did $collection $rkey
    print "deleted record"

    # wait for live events to arrive (up to 30s)
    print "waiting for live commit events..."
    mut live_events = []
    for i in 1..60 {
        sleep 500ms
        if ($live_file | path exists) {
            $live_events = (
                open $live_file | str trim | lines
                | each { |line| try { $line | from json } catch { null } }
                | compact
            )
            let commits = ($live_events | where { |e| ($e | get -o kind | default "") == "commit" and ($e | get -o did | default "") == $did })
            if ($commits | length) >= 2 {
                break
            }
        }
    }
    try { kill $live_pid }
    sleep 200ms

    print $"live events received: ($live_events | length)"
    assert-no-error-events $live_events "live subscription" $instance.pid

    let our_commits = ($live_events | where { |e| ($e | get -o kind | default "") == "commit" and ($e | get -o did | default "") == $did })
    print $"our commit events: ($our_commits | length)"
    if ($our_commits | length) < 2 {
        fail $"expected >= 2 live commit events, got ($our_commits | length)" $instance.pid
    }

    # validate commit event structure
    for evt in $our_commits {
        if not ("time_us" in $evt) {
            fail "live commit event missing time_us" $instance.pid
        }
        assert-commit-structure $evt.commit "live commit" $instance.pid
    }
    print "live commit event structure is correct"

    # verify create event has record body and cid
    let create_evts = ($our_commits | where { |e| ($e | get -o commit.operation | default "") == "create" })
    if ($create_evts | is-empty) {
        fail "no create event found in live events" $instance.pid
    }
    let create_evt = ($create_evts | first)
    let create_rkey = ($create_evt | get -o commit.rkey | default "")
    if $create_rkey != $rkey {
        fail $"create event rkey mismatch: expected ($rkey), got ($create_rkey)" $instance.pid
    }
    print "create event matched expected rkey and has record body"

    # --- scenario: cursor replay ---
    print "--- scenario: cursor replay ---"
    let replay_file = $"($db_path)/replay.txt"

    # cursor is time_us (microseconds since epoch); 60 seconds before now
    # use %s (epoch seconds) then integer multiply to avoid nushell float division
    let now_s = (date now | format date "%s" | into int)
    let cursor_us = (($now_s - 60) * 1_000_000)
    let replay_events = (collect-ws-json $"($ws_base)/subscribe?cursor=($cursor_us)" $replay_file 10sec)
    print $"cursor replay: ($replay_events | length) events"
    assert-no-error-events $replay_events "cursor replay" $instance.pid

    let replay_commits = ($replay_events | where { |e| ($e | get -o kind | default "") == "commit" and ($e | get -o did | default "") == $did })
    if ($replay_commits | length) < 2 {
        fail $"cursor replay: expected >= 2 commit events for our DID, got ($replay_commits | length)" $instance.pid
    }
    print $"cursor replay: ($replay_commits | length) commit events for our DID"

    # verify monotonic time_us ordering
    let time_us_vals = ($replay_events | get time_us)
    let monotonic = (
        $time_us_vals
        | zip ($time_us_vals | skip 1)
        | all { |pair| $pair.0 <= $pair.1 }
    )
    if not $monotonic {
        fail "cursor replay events are not monotonically ordered by time_us" $instance.pid
    }
    print "time_us ordering is monotonically non-decreasing"

    # --- scenario: wantedCollections filter ---
    print "--- scenario: wantedCollections filter ---"
    let col_file = $"($db_path)/col_filter.txt"
    let col_url = $"($ws_base)/subscribe?cursor=($cursor_us)&wantedCollections=($collection)"
    let col_events = (collect-ws-json $col_url $col_file 8sec)
    let col_commits = ($col_events | where { |e| ($e | get -o kind | default "") == "commit" })
    print $"wantedCollections=($collection): ($col_commits | length) commit events"

    let col_commits_with_data = ($col_commits | where { |e| ($e | get -o commit.collection | is-not-empty) })
    if ($col_commits_with_data | any { |e| ($e | get -o commit.collection | default "") != $collection }) {
        fail "wantedCollections filter returned commits for unexpected collection" $instance.pid
    }
    # account/identity events always pass through even with collection filter
    let non_commit = ($col_events | where { |e| ($e | get -o kind | default "") != "commit" })
    print $"non-commit events through collection filter: ($non_commit | length) \(expected: account/identity pass-through\)"
    print "wantedCollections filter is correct"

    # --- scenario: wantedDids filter ---
    print "--- scenario: wantedDids filter ---"
    let did_file = $"($db_path)/did_filter.txt"
    let did_url = $"($ws_base)/subscribe?cursor=($cursor_us)&wantedDids=($did)"
    let did_events = (collect-ws-json $did_url $did_file 8sec)
    print $"wantedDids=($did): ($did_events | length) events"

    let did_jetstream_events = ($did_events | where { |e| ($e | get -o did | is-not-empty) })
    if ($did_jetstream_events | any { |e| ($e | get -o did | default "") != $did }) {
        fail "wantedDids filter returned events for wrong DID" $instance.pid
    }
    print "wantedDids filter is correct"

    try { kill $instance.pid }
    print "=== jetstream /subscribe test PASSED ==="
}
