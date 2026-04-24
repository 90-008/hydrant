#!/usr/bin/env nu
# run all hydrant integration tests in parallel with automatically assigned free ports.
#
# usage:
#   nu tests/run_all.nu
#   nu tests/run_all.nu --only [stream repos_api]

def get_free_ports [count: int] {
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

def test-needs-relay-binary [name: string] {
    # tests that build a relay-only binary must run last (and serially) to avoid racing on
    # `target/` artifacts while other tests are executing.
    try {
        open --raw $"tests/($name).nu" | str contains "build-hydrant-relay"
    } catch {
        false
    }
}

def main [--only: list<string> = [], --skip-creds] {
    print "building hydrant..."
    # build default features
    cargo build
    # build backlinks
    cargo build --features backlinks
    print ""

    # discover all test scripts, excluding infrastructure files
    mut excluded = ["common", "mock_relay", "mock_pds", "run_all"]
    if $skip_creds {
        $excluded = ($excluded | append ["authenticated_stream", "count_tracking", "repo_sync_integrity"])
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
    let ports = get_free_ports (($tests | length) * 3)

    let relay_tests = $tests | where {|t| test-needs-relay-binary $t }

    mut assigned = []
    for test in ($tests | enumerate) {
        let p = {($test | get index) * 3 + $in}
        let name = ($test | get item)
        let binary = if ($relay_tests | any {$in == $name}) { null } else { "target/x86_64-unknown-linux-gnu/debug/hydrant" }
        let entry = {
            name: $name,
            api: ($ports | get (0 | do $p)),
            debug: ($ports | get (1 | do $p)),
            mock: ($ports | get (2 | do $p)),
            binary: $binary,
        }
        $assigned = ($assigned | append $entry)
    }

    let relay_assigned = $assigned | where {|t| $t.binary == null }
    let parallel_assigned = $assigned | where {|t| $t.binary != null }

    let groups = {
        "authenticated_stream": "event_dependent",
        "count_tracking": "event_dependent",
        "signal_filter": "event_dependent",
    }
    let grouped = $parallel_assigned | group-by {
        let name = $in.name
        $groups | get -o $name | default $name
    }

    print $"running ($assigned | length) tests...\n"
    if not ($relay_assigned | is-empty) {
        print $"note: relay-binary tests will run last and not in parallel: (($relay_assigned | get name) | str join ', ')\n"
    }

    let run_group = {each {timeit -o {run-test} | {time: $in.time, ...$in.output}}};
    let parallel_results = $grouped | values | par-each {do $run_group} | flatten
    let relay_results = if ($relay_assigned | is-empty) { [] } else { $relay_assigned | do $run_group }
    let results = $parallel_results | append $relay_results

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
