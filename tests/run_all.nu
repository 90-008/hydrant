#!/usr/bin/env nu
# run all hydrant integration tests in parallel with automatically assigned free ports.
#
# usage:
#   nu tests/run_all.nu
#   nu tests/run_all.nu --only [stream_live_backfill api_repos]
use common.nu [build-hydrant-features build-hydrant-relay]

def get_free_ports [count: int] {
    if $count == 0 {
        return []
    }

    mut chosen = []
    loop {
        let p = port
        if ($chosen | any {$in == $p}) {
            continue;
        }
        $chosen = $chosen | append $p
        if ($chosen | length) == $count {
            break
        }
    }
    $chosen
}

def run-test [] {
    let name = $in.name
    let base_env = {
        HYDRANT_API_PORT: $in.api
        HYDRANT_DEBUG_PORT: $in.debug
        HYDRANT_TEST_MOCK_PORT: $in.mock
    }
    let binary = $in.binary?
    let env_vars = if $binary == null { $base_env } else { $base_env | insert HYDRANT_BINARY $binary }

    let result = (with-env $env_vars {
        ^nu $"tests/($name).nu" | complete
    })
    {
        name: $name
        success: ($result.exit_code == 0)
        output: $result.stdout
        stderr: $result.stderr
    }
}

def snapshot-binary [binary: string, label: string] {
    let dir = (mktemp -d -t $"hydrant_($label)_binary.XXXXXX")
    let out = $"($dir)/hydrant"
    cp $binary $out
    $out
}

def test-needs-relay-binary [name: string] {
    let relay_binary_tests = ["relay_subscribe_repos_ping", "relay_ttl_prunes_events"]
    if ($relay_binary_tests | any {$in == $name}) {
        return true
    }

    # tests that build a relay-only binary must run last (and serially) to avoid racing on
    # `target/` artifacts while other tests are executing.
    try {
        open --raw $"tests/($name).nu" | str contains "build-hydrant-relay"
    } catch {
        false
    }
}

def main [--only: list<string> = [], --skip-creds] {
    # discover all test scripts, excluding infrastructure files
    mut excluded = ["common", "mock_relay", "mock_pds", "run_all"]
    if $skip_creds {
        $excluded = ($excluded | append [
            "authenticated_stream_multi_relay",
            "authenticated_stream_single_relay",
            "repo_count_resync",
            "repo_sync_integrity",
            "signal_filter"
        ])
    }
    let discovered = (
        ls tests/*.nu
        | get name
        | each {path basename | str replace ".nu" ""}
        | where {|name| not ($excluded | any {$in == $name})}
    )

    let tests = if ($only | is-empty) {
        $discovered
    } else {
        $discovered | where {|t| $only | any {$in == $t}}
    }

    if ($tests | is-empty) {
        print "running 0 tests"
        return
    }

    let relay_tests = $tests | where {|t| test-needs-relay-binary $t }
    let indexer_tests = $tests | where {|t| not ($relay_tests | any {$in == $t}) }

    mut indexer_binary = null
    mut relay_binary = null

    if not ($indexer_tests | is-empty) {
        let built = build-hydrant-features "backlinks"
        $indexer_binary = if not ($relay_tests | is-empty) {
            snapshot-binary $built "indexer"
        } else {
            $built
        }
    }

    if not ($relay_tests | is-empty) {
        let built = build-hydrant-relay
        $relay_binary = snapshot-binary $built "relay"
    }

    print ""

    let ports = get_free_ports (($tests | length) * 3)

    mut assigned = []
    for test in ($tests | enumerate) {
        let p = {($test | get index) * 3 + $in}
        let name = ($test | get item)
        let binary = if ($relay_tests | any {$in == $name}) { $relay_binary } else { $indexer_binary }
        let entry = {
            name: $name,
            api: ($ports | get (0 | do $p)),
            debug: ($ports | get (1 | do $p)),
            mock: ($ports | get (2 | do $p)),
            binary: $binary,
        }
        $assigned = ($assigned | append $entry)
    }

    let groups = {
        "authenticated_stream_multi_relay": "event_dependent",
        "authenticated_stream_single_relay": "event_dependent",
        "repo_count_resync": "event_dependent",
        "signal_filter": "event_dependent",
    }
    let grouped = $assigned | group-by {
        let name = $in.name
        $groups | get -o $name | default $name
    }

    let relay_assigned = $assigned | where {|t| $relay_tests | any {$in == $t.name} }

    print $"running ($assigned | length) tests...\n"

    let run_group = {each {timeit -o {run-test} | {time: $in.time, ...$in.output}}};
    let results = $grouped | values | par-each {do $run_group} | flatten

    print "\n=== results ===\n"
    for r in $results {
        if $r.success {
            print $"  PASSED  ($r.name) in ($r.time)"
        } else {
            print $"  FAILED  ($r.name) in ($r.time)"
            let combined = $"($r.output)\n($r.stderr)" | str trim
            $combined | lines | each {print $"    ($in)"}
            print ""
        }
    }

    let res = $results | group-by {$in.success}
    let failed = $res | get -o false | default []
    let passed = $res | get -o true | default []
    print $"\n($passed | length) passed, ($failed | length) failed"

    try { ^pkill "hydrant" }

    if ($failed | length) > 0 { exit 1 }
}
