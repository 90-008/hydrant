#!/usr/bin/env nu

def fetch-failing-pds-sources [api_base: string] {
    let res = (http get -f -e $"($api_base)/firehose/sources?failing=true&is_pds=true")
    if $res.status != 200 {
        error make {
            msg: $"failed to fetch failing PDS sources from ($api_base)"
            label: {
                text: $"http status ($res.status)"
                span: { start: 0, end: 0 }
            }
        }
    }

    $res.body
}

def dedupe-hosts [sources: list<any>] {
    let grouped = (
        $sources
        | each { |source|
            {
                host: ($source.pds?.host? | default ""),
                url: $source.url,
                running: ($source.running | default false),
                local_status: ($source.host_status? | default ($source.pds?.status? | default null)),
                consecutive_failures: ($source.consecutive_failures | default 0),
                retry_in_secs: ($source.retry_in_secs? | default null)
            }
        }
        | where host != ""
        | group-by host
    )

    $grouped | items { |host, entries|
        let first = ($entries | first)
        let retry_values = ($entries | get retry_in_secs | where $it != null)

        {
            host: $host,
            source_count: ($entries | length),
            urls: ($entries | get url),
            local_status: $first.local_status,
            running: (($entries | where running == true | length) > 0),
            max_consecutive_failures: (($entries | get consecutive_failures) | math max),
            min_retry_in_secs: (if ($retry_values | is-empty) { null } else { $retry_values | math min }),
        }
    }
}

def fetch-relay-host-status [relay_base: string, host: string, timeout_secs: int] {
    let result = (
        ^curl
        --silent
        --show-error
        --max-time ($timeout_secs | into string)
        --get
        --data-urlencode $"hostname=($host)"
        --write-out "\n%{http_code}"
        $"($relay_base)/xrpc/com.atproto.sync.getHostStatus"
        | complete
    )

    if $result.exit_code != 0 {
        return {
            relay_http: null,
            relay_status: null,
            relay_seq: null,
            relay_account_count: null,
            relay_error: ($result.stderr | str trim)
        }
    }

    let lines = ($result.stdout | lines)
    let http_code = ($lines | last | into int)
    let body = ($lines | drop nth (($lines | length) - 1) | str join "\n")
    let payload = (try { $body | from json } catch { null })

    if $http_code == 200 {
        {
            relay_http: $http_code,
            relay_status: ($payload.status? | default null),
            relay_seq: ($payload.seq? | default null),
            relay_account_count: ($payload.account_count? | default null),
            relay_error: null
        }
    } else {
        {
            relay_http: $http_code,
            relay_status: null,
            relay_seq: null,
            relay_account_count: null,
            relay_error: ($payload.error? | default ($payload.message? | default ($body | str trim)))
        }
    }
}

def classify-host [relay_http: any, relay_status: any] {
    if $relay_http == 200 {
        match $relay_status {
            "active" => "active_elsewhere",
            "idle" => "idle_elsewhere",
            "offline" => "offline_elsewhere",
            "banned" => "banned_elsewhere",
            _ => "other_relay_status",
        }
    } else if $relay_http == 400 or $relay_http == 404 {
        "unknown_to_relay"
    } else {
        "relay_error"
    }
}

def summarize [rows: list<any>] {
    let total = ($rows | length)
    let active_elsewhere = ($rows | where classification == "active_elsewhere" | length)
    let idle_elsewhere = ($rows | where classification == "idle_elsewhere" | length)
    let offline_elsewhere = ($rows | where classification == "offline_elsewhere" | length)
    let banned_elsewhere = ($rows | where classification == "banned_elsewhere" | length)
    let other_relay_status = ($rows | where classification == "other_relay_status" | length)
    let unknown = ($rows | where classification == "unknown_to_relay" | length)
    let relay_errors = ($rows | where classification == "relay_error" | length)

    {
        total_hosts: $total,
        active_elsewhere: $active_elsewhere,
        idle_elsewhere: $idle_elsewhere,
        offline_elsewhere: $offline_elsewhere,
        banned_elsewhere: $banned_elsewhere,
        other_relay_status: $other_relay_status,
        unknown_to_relay: $unknown,
        relay_errors: $relay_errors
    }
}

def main [
    api_base: string = "http://127.0.0.1:13579",
    relay_base: string = "https://bsky.network",
    --threads: int = 32,
    --timeout-secs: int = 10,
    --json
] {
    let sources = (fetch-failing-pds-sources $api_base)
    let rows = (
        dedupe-hosts $sources
        | par-each --threads $threads --keep-order { |row|
            let relay = (fetch-relay-host-status $relay_base $row.host $timeout_secs)
            let classification = (classify-host $relay.relay_http $relay.relay_status)

            $row
            | merge $relay
            | insert classification $classification
        }
        | sort-by classification host
    )

    if $json {
        let output = ({
            api_base: $api_base,
            relay_base: $relay_base,
            summary: (summarize $rows),
            hosts: $rows
        } | to json -r)

        print $output
        return
    }

    print $"api base:   ($api_base)"
    print $"relay base: ($relay_base)"
    print ""
    print "summary:"
    summarize $rows | table
    print ""
    print "hosts:"
    $rows
    | select host classification local_status relay_status relay_http source_count running max_consecutive_failures min_retry_in_secs urls relay_error
    | table -e
}
