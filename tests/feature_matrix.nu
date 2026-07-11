#!/usr/bin/env nu
# check every supported cargo feature combination compiles without errors or warnings.
#
# usage:
#   nu tests/feature_matrix.nu
#   nu tests/feature_matrix.nu --only [relay indexer]

# supported combinations. indexer and relay are mutually exclusive modes
# (enforced by compile_error! in src/lib.rs); everything else composes on
# top of exactly one mode (or none, for the bare shared core).
const combos = [
    { name: "none", features: "" }
    { name: "indexer", features: "indexer" }
    { name: "default", features: "indexer,indexer_stream" }
    { name: "indexer-jetstream", features: "indexer,indexer_stream,jetstream" }
    { name: "indexer-backlinks", features: "indexer,indexer_stream,backlinks" }
    { name: "indexer-backlinks-nostream", features: "indexer,backlinks" }
    { name: "indexer-user-keyspace", features: "indexer,indexer_stream,user-keyspace" }
    { name: "indexer-diagnostics", features: "indexer,indexer_stream,firehose-diagnostics" }
    { name: "indexer-persist-sync-all", features: "indexer,indexer_stream,__persist_sync_all" }
    { name: "relay", features: "relay" }
    { name: "relay-jetstream", features: "relay,jetstream" }
    { name: "relay-diagnostics", features: "relay,firehose-diagnostics" }
    { name: "relay-user-keyspace", features: "relay,user-keyspace" }
]

def check-combo [combo: record] {
    mut args = [check --all-targets --no-default-features]
    if ($combo.features | is-not-empty) {
        $args = $args | append [--features $combo.features]
    }

    let result = (cargo ...$args | complete)
    let diagnostics = $result.stderr
        | lines
        | where {|line|
            ($line starts-with "error") or (
                ($line starts-with "warning:")
                and not ($line | str contains "generated")
                and not ($line | str contains "build failed")
            )
        }

    {
        name: $combo.name
        features: $combo.features
        success: ($result.exit_code == 0 and ($diagnostics | is-empty))
        diagnostics: $diagnostics
        stderr: $result.stderr
    }
}

def main [--only: list<string> = []] {
    let selected = if ($only | is-empty) {
        $combos
    } else {
        $combos | where {|c| $c.name in $only }
    }
    if ($selected | is-empty) {
        print $"no combos match ($only); known: ($combos | get name | str join ', ')"
        exit 1
    }

    mut failures = []
    for combo in $selected {
        print $"checking ($combo.name) [($combo.features)]..."
        let result = (check-combo $combo)
        if not $result.success {
            print $"  FAILED:"
            for line in $result.diagnostics {
                print $"    ($line)"
            }
            $failures = $failures | append $result
        }
    }

    print ""
    if ($failures | is-empty) {
        print $"feature matrix OK: ($selected | length) combos error- and warning-free"
    } else {
        print $"feature matrix FAILED: ($failures | get name | str join ', ')"
        exit 1
    }
}
