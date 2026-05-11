#!/usr/bin/env nu
use common.nu *
use helpers/authenticated_stream.nu [load-authenticated-stream-config run-authenticated-stream-test]

def main [] {
    let config = load-authenticated-stream-config
    let port = resolve-test-port 3016
    let relays = "wss://relay.fire.hose.cam,wss://relay3.fr.hose.cam,wss://relay1.us-west.bsky.network,wss://relay1.us-east.bsky.network"
    let success = run-authenticated-stream-test $config.binary $config.did $config.password $config.pds_url $relays $port "multi relay"

    if not $success {
        exit 1
    }
}
