#!/usr/bin/env nu
use common.nu *

def record-data [text: string] {
    let timestamp = (date now | format date "%Y-%m-%dT%H:%M:%SZ")
    {
        "$type": "app.bsky.feed.post",
        text: $text,
        createdAt: $timestamp
    }
}

def api-count [url: string, did: string, collection: string] {
    (http get $"($url)/xrpc/systems.gaze.hydrant.countRecords?identifier=($did)&collection=($collection)").count | into int
}

def debug-count [debug_url: string, did: string, collection: string] {
    (http get $"($debug_url)/debug/count?did=($did)&collection=($collection)").count | into int
}

def stats-records [url: string] {
    (http get $"($url)/stats").counts.records | into int
}

def count-state [url: string, debug_url: string, did: string, collection: string] {
    {
        api: (api-count $url $did $collection),
        debug: (debug-count $debug_url $did $collection),
        records: (stats-records $url)
    }
}

def assert-count-state [label: string, state: record, expected_collection: int, expected_records: int] {
    print $"($label): api=($state.api), debug=($state.debug), stats.records=($state.records)"

    if $state.api != $state.debug {
        error make {msg: $"($label): countRecords mismatch. api=($state.api) debug=($state.debug)"}
    }

    if $state.api != $expected_collection {
        error make {msg: $"($label): expected collection count ($expected_collection), got ($state.api)"}
    }

    if $state.records != $expected_records {
        error make {msg: $"($label): expected stats.records ($expected_records), got ($state.records)"}
    }
}

def set-firehose [url: string, enabled: bool] {
    http patch -t application/json $"($url)/ingestion" { firehose: $enabled } | ignore
    let status = (http get $"($url)/ingestion")
    if $status.firehose != $enabled {
        error make {msg: $"expected firehose=($enabled), got ($status.firehose)"}
    }
}

def force-resync [url: string, did: string] {
    let resp = (http post -f -e -t application/json $"($url)/repos/resync" [{ did: $did }])
    if $resp.status != 200 {
        error make {msg: $"repos/resync failed: ($resp.status) body=($resp.body)"}
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

    let pds_url = resolve-pds $did
    let collection = "app.bsky.feed.post"
    let port = resolve-test-port 3008
    let debug_port = resolve-test-debug-port ($port + 1)
    let url = $"http://localhost:($port)"
    let debug_url = $"http://localhost:($debug_port)"
    let db_path = (mktemp -d -t hydrant_count_tracking.XXXXXX)

    print $"testing count tracking for ($did)..."
    print $"database path: ($db_path)"

    let session = authenticate $pds_url $did $password
    let jwt = $session.accessJwt

    let seed = create-record $pds_url $jwt $did $collection (record-data "hydrant count seed")
    let seed_rkey = ($seed.uri | split row "/" | last)
    mut stale_rkey = ""
    mut instance = null
    mut success = false

    try {
        let binary = build-hydrant
        let relay = "wss://relay.fire.hose.cam"
        $instance = (with-env { HYDRANT_RELAY_HOSTS: $relay } {
            start-hydrant $binary $db_path $port
        })

        if not (wait-for-api $url) {
            error make {msg: "api failed to start"}
        }

        print $"adding repo ($did) to tracking..."
        http put -t application/json $"($url)/repos" [{ did: $did }]

        if not (wait-for-backfill $url) {
            error make {msg: "initial backfill failed"}
        }

        let baseline = (count-state $url $debug_url $did $collection)
        if $baseline.api < 1 {
            error make {msg: $"expected at least one seeded record, got ($baseline.api)"}
        }
        assert-count-state "baseline" $baseline $baseline.api $baseline.records

        print "pausing firehose..."
        set-firehose $url false

        let stale = create-record $pds_url $jwt $did $collection (record-data "hydrant count stale create")
        $stale_rkey = ($stale.uri | split row "/" | last)
        sleep 2sec

        let stale_before_resync = (count-state $url $debug_url $did $collection)
        assert-count-state "stale before resync" $stale_before_resync $baseline.api $baseline.records

        print "forcing resync after stale create..."
        force-resync $url $did
        if not (wait-for-backfill $url) {
            error make {msg: "resync after create failed"}
        }

        let after_create = (count-state $url $debug_url $did $collection)
        assert-count-state "after create resync" $after_create ($baseline.api + 1) ($baseline.records + 1)

        delete-record $pds_url $jwt $did $collection $stale_rkey
        sleep 2sec

        let stale_before_delete_resync = (count-state $url $debug_url $did $collection)
        assert-count-state "stale before delete resync" $stale_before_delete_resync ($baseline.api + 1) ($baseline.records + 1)

        print "forcing resync after stale delete..."
        force-resync $url $did
        if not (wait-for-backfill $url) {
            error make {msg: "resync after delete failed"}
        }

        let after_delete = (count-state $url $debug_url $did $collection)
        assert-count-state "after delete resync" $after_delete $baseline.api $baseline.records

        $success = true
    } catch { |e|
        print $"test failed: ($e.msg)"
    }

    if ($instance | describe) != "nothing" {
        try { set-firehose $url true }
        try { kill --force $instance.pid }
    }

    if ($stale_rkey | is-not-empty) {
        try { delete-record $pds_url $jwt $did $collection $stale_rkey }
    }
    try { delete-record $pds_url $jwt $did $collection $seed_rkey }

    if not $success {
        exit 1
    }
}
