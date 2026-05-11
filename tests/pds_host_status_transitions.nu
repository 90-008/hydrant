#!/usr/bin/env nu
use common.nu *
use helpers/pds_status.nu [run-pds-status-lifecycle]

def main [] {
    let port = resolve-test-port 3033
    let mock_port = resolve-test-mock-port 9999
    let binary = build-hydrant

    run-pds-status-lifecycle $binary $port $mock_port
    print "pds host status transition test passed!"
}
