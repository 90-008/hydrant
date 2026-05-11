#!/usr/bin/env nu
use common.nu *
use helpers/authenticated_stream.nu [load-authenticated-stream-config run-authenticated-stream-test]

def main [] {
    let config = load-authenticated-stream-config
    let port = resolve-test-port 3005
    let relay = "wss://relay.fire.hose.cam"
    let success = run-authenticated-stream-test $config.binary $config.did $config.password $config.pds_url $relay $port "single relay"

    if not $success {
        exit 1
    }
}
