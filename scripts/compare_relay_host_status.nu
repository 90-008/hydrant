#!/usr/bin/env nu

def featured-relays [] {
    [
        { relay: "https://relay.bas.sh", host: "relay.bas.sh", run_by: "@bas.sh", impl: "indigo" }
        { relay: "https://relay1.us-east.bsky.network", host: "relay1.us-east.bsky.network", run_by: "Bluesky PBC", impl: "indigo" }
        { relay: "https://relay3.fr.hose.cam", host: "relay3.fr.hose.cam", run_by: "@bad-example.com", impl: "indigo" }
        { relay: "https://relay.waow.tech", host: "relay.waow.tech", run_by: "@zzstoatzz.io", impl: "indigo" }
        { relay: "https://asia.firehose.network", host: "asia.firehose.network", run_by: "@sri.xyz", impl: "indigo" }
        { relay: "https://bsky.network", host: "bsky.network", run_by: "Bluesky PBC", impl: "indigo" }
        { relay: "https://relay1.us-west.bsky.network", host: "relay1.us-west.bsky.network", run_by: "Bluesky PBC", impl: "indigo" }
        { relay: "https://relay.fire.hose.cam", host: "relay.fire.hose.cam", run_by: "@bad-example.com", impl: "indigo" }
        { relay: "https://europe.firehose.network", host: "europe.firehose.network", run_by: "@sri.xyz", impl: "indigo" }
        { relay: "https://zlay.waow.tech", host: "zlay.waow.tech", run_by: "@zzstoatzz.io", impl: "zlay" }
        { relay: "https://relay.klbr.net", host: "relay.klbr.net", run_by: "@klbr.net", impl: "hydrant" }
        { relay: "https://northamerica.firehose.network", host: "northamerica.firehose.network", run_by: "@sri.xyz", impl: "indigo" }
        { relay: "https://atproto.africa", host: "atproto.africa", run_by: "blacksky", impl: "rsky" }
    ]
}

def curl-json [url: string, timeout_secs: int] {
    let result = (
        ^curl
        --silent
        --show-error
        --max-time ($timeout_secs | into string)
        $url
        | complete
    )

    if $result.exit_code != 0 {
        error make {
            msg: $"request failed for ($url)"
            label: {
                text: ($result.stderr | str trim)
                span: { start: 0, end: 0 }
            }
        }
    }

    $result.stdout | from json
}

def fetch-all-hosts [relay_base: string, page_size: int, timeout_secs: int] {
    mut cursor = ""
    mut hosts = []

    loop {
        let url = if $cursor == "" {
            $"($relay_base)/xrpc/com.atproto.sync.listHosts?limit=($page_size)"
        } else {
            let encoded_cursor = ($cursor | url encode)
            $"($relay_base)/xrpc/com.atproto.sync.listHosts?limit=($page_size)&cursor=($encoded_cursor)"
        }

        let page = (curl-json $url $timeout_secs)
        let page_hosts = ($page.hosts? | default [])

        if ($page_hosts | is-empty) {
            break
        }

        $hosts = ($hosts | append $page_hosts)

        if ($page.cursor? | default "") == "" {
            break
        }

        $cursor = $page.cursor
    }

    $hosts
    | uniq-by hostname
    | sort-by hostname
}

def fetch-host-status [relay_base: string, host: string, timeout_secs: int] {
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
            http_status: null,
            hostname: $host,
            status: null,
            seq: null,
            accountCount: null,
            error: ($result.stderr | str trim)
        }
    }

    let lines = ($result.stdout | lines)
    let http_status = ($lines | last | into int)
    let body = ($lines | drop nth (($lines | length) - 1) | str join "\n")
    let payload = (try { $body | from json } catch { null })

    if $http_status == 200 {
        {
            http_status: $http_status,
            hostname: ($payload.hostname? | default $host),
            status: ($payload.status? | default null),
            seq: ($payload.seq? | default null),
            accountCount: ($payload.accountCount? | default null),
            error: null
        }
    } else {
        {
            http_status: $http_status,
            hostname: $host,
            status: null,
            seq: null,
            accountCount: null,
            error: ($payload.error? | default ($payload.message? | default ($body | str trim)))
        }
    }
}

def host-status-from-list [hosts: list<any>, host: string] {
    let matches = ($hosts | where hostname == $host)

    if ($matches | is-empty) {
        {
            http_status: 404,
            hostname: $host,
            status: null,
            seq: null,
            accountCount: null,
            error: "not found in listHosts"
        }
    } else {
        let listed = ($matches | first)
        $listed
        | merge {
            http_status: 200,
            hostname: ($listed.hostname? | default $host),
            status: ($listed.status? | default null),
            seq: ($listed.seq? | default null),
            accountCount: ($listed.accountCount? | default null),
            error: null
        }
    }
}

def compare-statuses [left: record, right: record] {
    {
        status_match: ($left.status == $right.status),
        seq_match: ($left.seq == $right.seq),
        account_count_match: ($left.accountCount == $right.accountCount),
        status_delta: (if $left.status == $right.status { null } else { $"($left.status) != ($right.status)" }),
        seq_delta: (if $left.seq == $right.seq { 0 } else { (($left.seq? | default 0) - ($right.seq? | default 0)) }),
        account_count_delta: (if $left.accountCount == $right.accountCount { 0 } else { (($left.accountCount? | default 0) - ($right.accountCount? | default 0)) })
    }
}

def summarize [rows: list<any>] {
    let same = ($rows | where comparison == "same" | length)
    let diffs = ($rows | where comparison == "different" | length)
    let unavailable = ($rows | where comparison == "unavailable" | length)
    let left_only = ($rows | where comparison == "missing_on_right" | length)
    let right_only = ($rows | where comparison == "missing_on_left" | length)
    let right_errors = ($rows | where comparison == "right_error" | length)
    let left_errors = ($rows | where comparison == "left_error" | length)

    {
        total_hosts: ($rows | length),
        same: $same,
        different: $diffs,
        missing_on_right: $left_only,
        missing_on_left: $right_only,
        left_error: $left_errors,
        right_error: $right_errors,
        unavailable: $unavailable
    }
}

def classify-comparison [left: record, right: record, cmp: record] {
    if $left.http_status == null {
        "left_error"
    } else if $right.http_status == null {
        "right_error"
    } else if $left.http_status != 200 and $right.http_status != 200 {
        "unavailable"
    } else if $left.http_status == 200 and $right.http_status != 200 {
        "missing_on_right"
    } else if $left.http_status != 200 and $right.http_status == 200 {
        "missing_on_left"
    } else if $cmp.status_match and $cmp.seq_match and $cmp.account_count_match {
        "same"
    } else {
        "different"
    }
}

def summarize-single [rows: list<any>] {
    let ok_rows = ($rows | where http_status == 200)
    {
        relays_checked: ($rows | length),
        successful: ($ok_rows | length),
        unsuccessful: (($rows | length) - ($ok_rows | length)),
        unique_statuses: ($ok_rows | get status | uniq | sort),
        unique_seq_count: (($ok_rows | get seq | uniq | length)),
        unique_account_count_count: (($ok_rows | get accountCount | uniq | length)),
        status_consistent: ((($ok_rows | get status | uniq | length)) <= 1),
        seq_consistent: ((($ok_rows | get seq | uniq | length)) <= 1),
        account_count_consistent: ((($ok_rows | get accountCount | uniq | length)) <= 1)
    }
}

def main [
    --source-relay: string = "https://relay.klbr.net",
    --compare-relay: string = "https://bsky.network",
    --threads: int = 32,
    --timeout-secs: int = 8,
    --page-size: int = 1000,
    --host-limit: int,
    --only-diff,
    --json
] {
    let source_hosts = (fetch-all-hosts $source_relay $page_size $timeout_secs)
    let compare_hosts = (fetch-all-hosts $compare_relay $page_size $timeout_secs)
    let source_hostnames = ($source_hosts | get hostname)
    let compare_hostnames = ($compare_hosts | get hostname)
    let union_hosts = (
        ($source_hostnames | append $compare_hostnames)
        | uniq
        | sort
    )
    let host_rows = if ($host_limit | default null) == null {
        $union_hosts
    } else {
        $union_hosts | first $host_limit
    }
    let listed_on_both = ($union_hosts | where {|host| ($source_hostnames | any {|it| $it == $host }) and ($compare_hostnames | any {|it| $it == $host }) } | length)
    let listed_only_on_source = ($union_hosts | where {|host| ($source_hostnames | any {|it| $it == $host }) and not ($compare_hostnames | any {|it| $it == $host }) } | length)
    let listed_only_on_compare = ($union_hosts | where {|host| not ($source_hostnames | any {|it| $it == $host }) and ($compare_hostnames | any {|it| $it == $host }) } | length)

    let rows = (
        $host_rows
        | each { |host|
            let left = (host-status-from-list $source_hosts $host)
            let right = (host-status-from-list $compare_hosts $host)
            let cmp = (compare-statuses $left $right)
            let comparison = (classify-comparison $left $right $cmp)
            let source_listed = ($source_hostnames | any {|it| $it == $host })
            let compare_listed = ($compare_hostnames | any {|it| $it == $host })

            {
                host: $host,
                source_listed: $source_listed,
                compare_listed: $compare_listed,
                source: $left,
                compare: $right,
                comparison: $comparison,
                status_match: $cmp.status_match,
                seq_match: $cmp.seq_match,
                account_count_match: $cmp.account_count_match,
                status_delta: $cmp.status_delta,
                seq_delta: $cmp.seq_delta,
                account_count_delta: $cmp.account_count_delta
            }
        }
    )

    let filtered = if $only_diff {
        $rows | where comparison != "same"
    } else {
        $rows
    }

    if $json {
        {
            source_relay: $source_relay,
            compare_relay: $compare_relay,
            source_discovered_hosts: ($source_hosts | length),
            compare_discovered_hosts: ($compare_hosts | length),
            union_hosts: ($union_hosts | length),
            listed_on_both: $listed_on_both,
            listed_only_on_source: $listed_only_on_source,
            listed_only_on_compare: $listed_only_on_compare,
            checked_hosts: ($host_rows | length),
            summary: (summarize $rows),
            rows: $filtered
        }
        | to json -r
        | print
        return
    }

    print $"source relay:  ($source_relay)"
    print $"compare relay: ($compare_relay)"
    print $"source discovered hosts:  ($source_hosts | length)"
    print $"compare discovered hosts: ($compare_hosts | length)"
    print $"union hosts:              ($union_hosts | length)"
    print $"listed on both:           ($listed_on_both)"
    print $"listed only on source:    ($listed_only_on_source)"
    print $"listed only on compare:   ($listed_only_on_compare)"
    print $"checked hosts:    ($host_rows | length)"
    print ""
    print "summary:"
    summarize $rows | table
    print ""
    print "rows:"
    $filtered
    | select host source_listed compare_listed comparison source.status compare.status source.seq compare.seq seq_delta source.accountCount compare.accountCount account_count_delta source.http_status compare.http_status source.error compare.error
    | table -e
}

def "main compare single" [
    host: string,
    --timeout-secs: int = 8,
    --threads: int = 16,
    --json
] {
    let relays = (featured-relays)
    let rows = (
        $relays
        | par-each --threads $threads --keep-order { |relay|
            let status = (fetch-host-status $relay.relay $host $timeout_secs)
            $relay | merge $status
        }
    )

    if $json {
        {
            host: $host,
            summary: (summarize-single $rows),
            rows: $rows
        }
        | to json -r
        | print
        return
    }

    print $"host: ($host)"
    print ""
    print "summary:"
    summarize-single $rows | table
    print ""
    print "rows:"
    $rows
    | select host run_by impl http_status status seq accountCount error
    | table -e
}
