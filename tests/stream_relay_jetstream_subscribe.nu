#!/usr/bin/env nu
# Tests the Jetstream-compatible /subscribe WebSocket endpoint.
#
# Starts hydrant with the jetstream feature, connects to wss://bsky.network
# as the upstream firehose with full-network mode, and verifies that events
# arrive and are correctly formatted. Crawler and backfill are disabled so
# only the single firehose connection is used (no PDS connections).
#
# Does not require credentials — relies on the public bsky.network firehose.

use common.nu *

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
    let port = resolve-test-port 3019
    let url = $"http://localhost:($port)"
    let ws_base = $"ws://127.0.0.1:($port)"
    let db_path = (mktemp -d -t hydrant_stream_relay_jetstream_subscribe.XXXXXX)

    print "testing Jetstream /subscribe..."
    print $"database path: ($db_path)"

    let binary = build-hydrant-relay-jetstream
    let instance = (with-env {
        HYDRANT_RELAY_HOSTS: "wss://bsky.network",
        HYDRANT_FULL_NETWORK: "true",
        HYDRANT_SEED_HOSTS: ""
    } {
        start-hydrant $binary $db_path $port
    })

    if not (wait-for-api $url) {
        fail "api failed to start" $instance.pid
    }

    # --- scenario: basic live events ---
    print "--- scenario: basic live events ---"
    # capture cursor_us before the live window so replay is guaranteed to cover it
    let cursor_us = ((date now | format date "%s" | into int) * 1_000_000)
    let live_file = $"($db_path)/live.txt"
    # bsky.network has high throughput; 15 seconds gives a large event sample
    # and a reasonable chance of capturing identity/account events alongside commits
    let events = (collect-ws-json $"($ws_base)/subscribe" $live_file 15sec)
    print $"received ($events | length) events"
    assert-no-error-events $events "live subscription" $instance.pid

    let commits = ($events | where { |e| ($e | get -o kind | default "") == "commit" })
    print $"commit events: ($commits | length)"
    if ($commits | length) < 10 {
        fail $"expected >= 10 live commit events, got ($commits | length)" $instance.pid
    }

    # validate commit event structure for a sample
    for evt in ($commits | first 10) {
        if not ("time_us" in $evt) {
            fail "commit event missing time_us" $instance.pid
        }
        if not ("did" in $evt) {
            fail "commit event missing did" $instance.pid
        }
        assert-commit-structure $evt.commit "relay commit" $instance.pid
    }
    print "commit event structure is correct"

    # validate identity events if any arrived (lazy RelayIdentity inflation path)
    let identity_events = ($events | where { |e| ($e | get -o kind | default "") == "identity" })
    print $"identity events: ($identity_events | length)"
    for evt in $identity_events {
        if not ("did" in $evt) { fail "identity event missing did" $instance.pid }
        if not ("time_us" in $evt) { fail "identity event missing time_us" $instance.pid }
        let identity = ($evt | get -o identity | default {})
        for field in ["did", "seq", "time"] {
            if not ($field in $identity) {
                fail $"identity event missing identity.($field)" $instance.pid
            }
        }
    }
    if ($identity_events | length) > 0 {
        print "identity event structure is correct \(lazy RelayIdentity path\)"
    }

    # validate account events if any arrived (lazy RelayAccount inflation path)
    let account_events = ($events | where { |e| ($e | get -o kind | default "") == "account" })
    print $"account events: ($account_events | length)"
    for evt in $account_events {
        if not ("did" in $evt) { fail "account event missing did" $instance.pid }
        if not ("time_us" in $evt) { fail "account event missing time_us" $instance.pid }
        let account = ($evt | get -o account | default {})
        for field in ["active", "did", "seq", "time"] {
            if not ($field in $account) {
                fail $"account event missing account.($field)" $instance.pid
            }
        }
    }
    if ($account_events | length) > 0 {
        print "account event structure is correct \(lazy RelayAccount path\)"
    }

    # --- scenario: cursor replay ---
    print "--- scenario: cursor replay ---"
    # cursor_us was captured before the live window so all live events fall after it
    print $"replaying from cursor_us: ($cursor_us)"

    let replay_file = $"($db_path)/replay.txt"
    let replay_events = (collect-ws-json $"($ws_base)/subscribe?cursor=($cursor_us)" $replay_file 8sec)
    print $"cursor replay: ($replay_events | length) events"
    assert-no-error-events $replay_events "cursor replay" $instance.pid

    let replay_commits = ($replay_events | where { |e| ($e | get -o kind | default "") == "commit" })
    if ($replay_commits | length) < 1 {
        fail $"cursor replay: expected >= 1 commit event, got ($replay_commits | length)" $instance.pid
    }
    print $"cursor replay: ($replay_commits | length) commit events"

    # verify monotonic time_us ordering
    let replay_timed = ($replay_events | where { |e| ($e | get -o time_us | default 0) > 0 })
    if ($replay_timed | length) > 1 {
        let time_us_vals = ($replay_timed | get time_us)
        let monotonic = (
            $time_us_vals
            | zip ($time_us_vals | skip 1)
            | all { |pair| $pair.0 <= $pair.1 }
        )
        if not $monotonic {
            fail "cursor replay events are not monotonically ordered by time_us" $instance.pid
        }
        print "time_us ordering is monotonically non-decreasing"
    }

    # --- scenario: wantedCollections filter ---
    print "--- scenario: wantedCollections filter ---"
    let collection = "app.bsky.feed.post"
    let col_file = $"($db_path)/col_filter.txt"
    let col_url = $"($ws_base)/subscribe?cursor=($cursor_us)&wantedCollections=($collection)"
    let col_events = (collect-ws-json $col_url $col_file 8sec)
    let col_commits = ($col_events | where { |e| ($e | get -o kind | default "") == "commit" })
    print $"wantedCollections=($collection): ($col_commits | length) commit events"
    if ($col_commits | length) < 1 {
        fail "wantedCollections filter returned no commits" $instance.pid
    }
    let col_commits_with_data = ($col_commits | where { |e| ($e | get -o commit.collection | is-not-empty) })
    if ($col_commits_with_data | any { |e| ($e | get -o commit.collection | default "") != $collection }) {
        fail "wantedCollections filter returned commits for unexpected collection" $instance.pid
    }
    # account/identity events pass through even with collection filter
    let non_commit = ($col_events | where { |e| ($e | get -o kind | default "") != "commit" })
    print $"non-commit events through collection filter: ($non_commit | length) \(expected: account/identity pass-through\)"
    print "wantedCollections filter is correct"

    # --- scenario: wantedDids filter ---
    print "--- scenario: wantedDids filter ---"
    # pick a DID from the live commits and replay events for it; verify the filter
    # excludes all other DIDs without requiring a specific minimum event count
    let sample_did = ($commits | first | get did)
    print $"filtering to DID: ($sample_did)"
    let did_file = $"($db_path)/did_filter.txt"
    let did_url = $"($ws_base)/subscribe?cursor=($cursor_us)&wantedDids=($sample_did)"
    let did_events = (collect-ws-json $did_url $did_file 8sec)
    print $"wantedDids=($sample_did): ($did_events | length) events"
    let did_jetstream_events = ($did_events | where { |e| ($e | get -o did | is-not-empty) })
    if ($did_jetstream_events | any { |e| ($e | get -o did | default "") != $sample_did }) {
        fail "wantedDids filter returned events for wrong DID" $instance.pid
    }
    print "wantedDids filter is correct"

    try { kill $instance.pid }
    print "=== jetstream /subscribe test PASSED ==="
}
