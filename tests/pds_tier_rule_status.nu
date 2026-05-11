#!/usr/bin/env nu
use common.nu *
use helpers/pds_status.nu [run-pds-tier-rule-status]

def main [] {
    let port = resolve-test-port 3133
    let mock_port = resolve-test-mock-port 9999
    let binary = build-hydrant

    run-pds-tier-rule-status $binary $port $mock_port
    print "pds tier rule status test passed!"
}
