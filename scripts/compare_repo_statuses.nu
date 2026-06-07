#!/usr/bin/env nu

def curl-json-status [url: string, timeout_secs: int] {
    let result = (
        ^curl
        --silent
        --show-error
        --max-time ($timeout_secs | into string)
        --write-out "\n%{http_code}"
        $url
        | complete
    )

    if $result.exit_code != 0 {
        return {
            http: null,
            body: null,
            error: ($result.stderr | str trim)
        }
    }

    let lines = ($result.stdout | lines)
    let http = ($lines | last | into int)
    let body_text = ($lines | drop nth (($lines | length) - 1) | str join "\n")
    let body = (try { $body_text | from json } catch { null })

    {
        http: $http,
        body: $body,
        error: (if $http == 200 { null } else { ($body.error? | default ($body.message? | default ($body_text | str trim))) })
    }
}

def fetch-repo-status [relay_base: string, did: string, timeout_secs: int] {
    let encoded_did = ($did | url encode)
    let res = (curl-json-status $"($relay_base)/xrpc/com.atproto.sync.getRepoStatus?did=($encoded_did)" $timeout_secs)
    {
        http: $res.http,
        active: ($res.body.active? | default null),
        rev: ($res.body.rev? | default null),
        status: ($res.body.status? | default null),
        error: $res.error
    }
}

def resolve-pds-host [did: string, timeout_secs: int] {
    let res = (curl-json-status $"https://plc.directory/($did)" $timeout_secs)
    if $res.http != 200 or $res.body == null {
        return {
            pds: null,
            host: null,
            plc_error: $res.error
        }
    }

    let service = (
        $res.body.service?
        | default []
        | where id == "#atproto_pds"
    )
    let pds = (if ($service | is-empty) { null } else { $service | get serviceEndpoint | first })
    let host = (
        if $pds == null {
            null
        } else {
            try { $pds | url parse | get host } catch { null }
        }
    )

    {
        pds: $pds,
        host: $host,
        plc_error: null
    }
}

def classify-row [left: record, right: record] {
    if $left.http == 200 and $right.http == 200 and $left.active == $right.active and $left.rev == $right.rev and $left.status == $right.status {
        "same"
    } else if $left.http == 404 and $right.http == 200 {
        "missing_on_source"
    } else if $left.http == 200 and $right.http == 404 {
        "missing_on_compare"
    } else if $left.http == 200 and $right.http == 200 and $left.active != $right.active {
        "active_mismatch"
    } else if $left.http == 200 and $right.http == 200 and $left.status != $right.status {
        "status_mismatch"
    } else if $left.http == 200 and $right.http == 200 and $left.rev != $right.rev {
        "rev_mismatch"
    } else {
        "other"
    }
}

def summarize [rows: list<any>] {
    let grouped = ($rows | group-by classification | transpose classification entries)
    let counts = ($grouped | each {|row| {classification: $row.classification count: ($row.entries | length)}} | sort-by classification)
    {
        total: ($rows | length),
        counts: $counts
    }
}

def main [
    dids_file?: string,
    --source-relay: string = "http://volsinii:13579",
    --compare-relay: string = "https://bsky.network",
    --timeout-secs: int = 15,
    --threads: int = 16,
    --json
] {
    let default_dids = [
        "did:web:prod-sea0.stream.place"
        "did:plc:v54cvudsxw5bsdb53rz3rpgw"
        "did:plc:wc3uljbzjvw6xqq3b2yoirgf"
        "did:plc:mdnglmdfkeyskljqgm6urpz6"
        "did:plc:2yyvxfpy4s63ars26dzlpbf2"
        "did:plc:p3xq5k27o4od3ttbvmd6cvrq"
        "did:plc:4mnxkcsm6rvflpfujxvykald"
        "did:plc:j2vrb6so7edkfy4ax6vztdke"
        "did:plc:b6dl7ze2sawauxyacb6xy3cf"
        "did:plc:5roprrkswuzn5cyvtddpvszh"
        "did:plc:p5pcswhxiigbi4lefml2qktv"
        "did:plc:xb2urvqt5f4zzccjs46hysbf"
        "did:plc:vm3bwfvsdsfxiyqwepdxchmx"
        "did:plc:waekfwabp6s6e546dozi5ryy"
        "did:plc:bhvrrngtey3i3amzy4ndb5aq"
        "did:plc:uiv7petmywo5zjcmburvucbp"
        "did:plc:rajqfvoufme434pc55wqed7x"
        "did:plc:oztyg3gp2jt74y6k53n2q3cu"
        "did:plc:jvlk3extyrhndypkhu3372yz"
        "did:plc:lkfazkg3ejp3oosadwmq6hhj"
        "did:plc:4wc4qpf7gpsktpri4lnmqykn"
        "did:plc:clii72ny72fpagrywk36ypig"
        "did:plc:pppq65ew7bw6tyeppgmcwojh"
        "did:plc:64p3c6ff3ccxc3kwftctvzx3"
        "did:plc:rtawxkoomhd4c5kpvn5vp3kf"
        "did:plc:z37762lrjhrqr4ghs7v2i3vj"
        "did:plc:chewvd2le7fnipvfdnku3omq"
        "did:plc:4wbbbi22wxvaneqlokv4rayc"
        "did:plc:yiwegfgtew7hhra7f2fuhqha"
        "did:plc:75kzcv3wdwajpgazpywco6d7"
    ]

    let dids = (
        if $dids_file == null {
            $default_dids
        } else {
            open $dids_file | lines | each { |line| $line | str trim } | where $it != ""
        }
    )

    let rows = (
        $dids
        | par-each --threads $threads --keep-order { |did|
            let source = (fetch-repo-status $source_relay $did $timeout_secs)
            let compare = (fetch-repo-status $compare_relay $did $timeout_secs)
            let resolved = (resolve-pds-host $did $timeout_secs)
            let classification = (classify-row $source $compare)

            {
                did: $did,
                host: $resolved.host,
                pds: $resolved.pds,
                classification: $classification,
                source: $source,
                compare: $compare,
                plc_error: $resolved.plc_error
            }
        }
    )

    let grouped_by_host = (
        $rows
        | group-by host
        | transpose host entries
        | each {|row|
            {
                host: $row.host,
                count: ($row.entries | length),
                classifications: (
                    $row.entries
                    | group-by classification
                    | transpose classification vals
                    | each {|c| {classification: $c.classification count: ($c.vals | length)}}
                    | sort-by classification
                ),
                dids: ($row.entries | get did)
            }
        }
        | sort-by count -r
    )

    if $json {
        {
            source_relay: $source_relay,
            compare_relay: $compare_relay,
            summary: (summarize $rows),
            grouped_by_host: $grouped_by_host,
            rows: $rows
        }
        | to json -r
        | print
        return
    }

    print $"source relay:  ($source_relay)"
    print $"compare relay: ($compare_relay)"
    print ""
    print "summary:"
    summarize $rows | table
    print ""
    print "grouped by host:"
    $grouped_by_host | table -e
    print ""
    print "rows:"
    $rows
    | select did host classification source.http compare.http source.active compare.active source.status compare.status source.rev compare.rev source.error compare.error plc_error
    | table -e
}
