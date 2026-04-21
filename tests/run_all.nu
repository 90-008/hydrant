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
    let result = (with-env {
        HYDRANT_API_PORT: $in.api
        HYDRANT_DEBUG_PORT: $in.debug
        HYDRANT_TEST_MOCK_PORT: $in.mock
        HYDRANT_BINARY: "target/x86_64-unknown-linux-gnu/debug/hydrant"
    } {
        ^nu $"tests/($in.name).nu" | complete
    })
    {
        name: $in.name
        success: ($result.exit_code == 0)
        output: $result.stdout
        stderr: $result.stderr
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

    mut assigned = []
    for test in ($tests | enumerate) {
        let p = {($test | get index) * 3 + $in}
        let entry = {
            name: ($test | get item),
            api: ($ports | get (0 | do $p)),
            debug: ($ports | get (1 | do $p)),
            mock: ($ports | get (2 | do $p))
        }
        $assigned = ($assigned | append $entry)
    }

    let groups = {
        "authenticated_stream": "event_dependent",
        "count_tracking": "event_dependent",
        "signal_filter": "event_dependent",
    }
    let grouped = $assigned | group-by {|t| $groups | get -o $t.name | default $t.name}

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
