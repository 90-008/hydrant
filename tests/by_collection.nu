#!/usr/bin/env nu
# tests that the collection-index crawler (listReposByCollection) correctly discovers
# and backfills repos from a lightrail-style index server.
#
# usage: nu tests/collection_index_test.nu
#
# requires network access to lightrail.microcosm.blue.
use common.nu *

def main [] {
    let port = resolve-test-port 3015
    let url = $"http://localhost:($port)"
    let db_path = (mktemp -d -t hydrant_collection_index_test.XXXXXX)
    let collection = "app.bsky.graph.starterpack"
    let index_url = "https://lightrail.microcosm.blue"

    print $"database path: ($db_path)"

    # fetch a small known set of repos from the collection index so we can verify
    # they appear in hydrant after the discovery pass
    print $"fetching known repos for ($collection) from ($index_url)..."
    let index_resp = (http get $"($index_url)/xrpc/com.atproto.sync.listReposByCollection?collection=($collection)&limit=3")
    let known_dids = ($index_resp.repos | each { |r| $r.did })

    if ($known_dids | is-empty) {
        print "SKIP: collection index returned no repos, cannot verify discovery"
        rm -rf $db_path
        exit 0
    }

    print $"will verify these DIDs are discovered: ($known_dids | str join ', ')"

    # start hydrant in filter mode with the test collection as a signal.
    # HYDRANT_ENABLE_COLLECTION_INDEX is true by default when signals are set,
    # so no explicit override needed.
    $env.HYDRANT_FILTER_SIGNALS = $collection
    $env.HYDRANT_COLLECTION_INDEX_URL = $index_url
    # keep the pending queue very small so throttling doesn't interfere with the test
    $env.HYDRANT_CRAWLER_MAX_PENDING_REPOS = "500"
    $env.HYDRANT_CRAWLER_RESUME_PENDING_REPOS = "200"
    # no relay needed for collection-index testing
    $env.HYDRANT_ENABLE_FIREHOSE = "false"

    let binary = build-hydrant
    let instance = start-hydrant $binary $db_path $port

    mut test_passed = false

    if not (wait-for-api $url) {
        print "ERROR: hydrant failed to start"
        try { kill -9 $instance.pid }
        rm -rf $db_path
        exit 1
    }

    # verify the filter was applied correctly
    let filter = (http get $"($url)/filter")
    print $"filter state: ($filter | to json)"
    if not ($filter.signals | any { |s| $s == $collection }) {
        print $"FAILED: ($collection) not in signals, filter not configured"
        try { kill -9 $instance.pid }
        rm -rf $db_path
        exit 1
    }

    # wait for the collection-index pass to discover and enqueue repos.
    # the first pass runs immediately on startup; repos should appear within ~30s
    # depending on pagination speed and backfill concurrency.
    print "waiting for collection-index discovery pass..."
    mut discovered = false
    for i in 1..60 {
        let stats = (try { (http get $"($url)/stats?accurate=true").counts } catch { {} })
        let repos = ($stats | get --optional repos | default 0 | into int)
        print $"[($i)/60] repos: ($repos)"
        if $repos >= ($known_dids | length) {
            $discovered = true
            break
        }
        sleep 2sec
    }

    if not $discovered {
        print "FAILED: collection-index did not discover any repos within timeout"
        try { kill -9 $instance.pid }
        rm -rf $db_path
        exit 1
    }

    # verify each known DID appears in the repos API
    print "verifying known DIDs were discovered..."
    mut all_found = true
    for did in $known_dids {
        let repo = (try {
            http get $"($url)/repos/($did)"
        } catch {
            null
        })
        if ($repo | is-empty) {
            print $"FAILED: ($did) not found in repos API"
            $all_found = false
        } else {
            print $"ok: ($did), status: ($repo.status)"
        }
    }

    if $all_found {
        print "test PASSED: collection-index correctly discovered repos from listReposByCollection"
        $test_passed = true
    }

    print "stopping hydrant..."
    try { kill -9 $instance.pid }
    rm -rf $db_path

    if $test_passed {
        exit 0
    } else {
        exit 1
    }
}
