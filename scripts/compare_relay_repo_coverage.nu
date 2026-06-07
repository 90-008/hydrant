#!/usr/bin/env nu

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

def list-repos-url [relay_base: string, page_size: int, cursor: string = ""] {
    if $cursor == "" {
        $"($relay_base)/xrpc/com.atproto.sync.listRepos?limit=($page_size)"
    } else {
        let encoded_cursor = ($cursor | url encode)
        $"($relay_base)/xrpc/com.atproto.sync.listRepos?limit=($page_size)&cursor=($encoded_cursor)"
    }
}

def normalize-repo [repo: record] {
    [
        $repo.did
        ($repo.active? | default null | into string)
        ($repo.status? | default "")
        ($repo.rev? | default "")
        ($repo.head? | default "")
    ] | str join "\t"
}

def dump-relay-repos [
    relay_base: string,
    out_path: string,
    page_size: int,
    timeout_secs: int,
    progress_interval: int
] {
    mut cursor = null
    mut pages = 0
    mut total = 0

    [] | save --force $out_path

    loop {
        let page = (curl-json (list-repos-url $relay_base $page_size ($cursor | default "")) $timeout_secs)
        let repos = ($page.repos? | default [])

        if ($repos | is-empty) {
            break
        }

        $repos
        | each { |repo| (normalize-repo $repo) }
        | str join "\n"
        | $"($in)\n"
        | save --append $out_path

        $pages = ($pages + 1)
        $total = ($total + ($repos | length))
        $cursor = ($page.cursor? | default null)

        if $progress_interval > 0 and ($pages mod $progress_interval) == 0 {
            print --stderr $"[repo-compare] relay=($relay_base) pages=($pages) repos=($total) cursor=($cursor | default '<done>')"
        }

        if $cursor == null {
            break
        }
    }

    {
        relay: $relay_base,
        pages: $pages,
        repos: $total,
        out_path: $out_path
    }
}

def sort-repo-file [src: string, dst: string] {
    let result = (
        ^sort
        --field-separator $'\t'
        --key 1,1
        $src
        | save --force $dst
    )
    $result
}

def trim-lines [path: string, count: int] {
    if $count <= 0 {
        []
    } else {
        open $path | lines | first $count
    }
}

def count-lines [path: string] {
    (
        ^wc
        -l
        $path
        | complete
    ).stdout
    | str trim
    | split row " "
    | where $it != ""
    | first
    | into int
}

def compare-sorted-repo-files [
    source_sorted: string,
    compare_sorted: string,
    work_dir: string,
    sample_size: int
] {
    let source_dids = $"($work_dir)/source.dids.tsv"
    let compare_dids = $"($work_dir)/compare.dids.tsv"
    let shared_dids = $"($work_dir)/shared.dids.tsv"
    let source_only = $"($work_dir)/source_only.tsv"
    let compare_only = $"($work_dir)/compare_only.tsv"
    let joined = $"($work_dir)/joined.tsv"
    let field_mismatches = $"($work_dir)/field_mismatches.tsv"

    ^cut -f1 $source_sorted | save --force $source_dids
    ^cut -f1 $compare_sorted | save --force $compare_dids

    ^comm -12 $source_dids $compare_dids | save --force $shared_dids
    ^comm -23 $source_dids $compare_dids | save --force $source_only
    ^comm -13 $source_dids $compare_dids | save --force $compare_only

    ^join -t $'\t' $source_sorted $compare_sorted | save --force $joined
    ^awk -F $'\t' '($2 != $6) || ($3 != $7) || ($4 != $8) || ($5 != $9)' $joined | save --force $field_mismatches

    let source_total = (count-lines $source_sorted)
    let compare_total = (count-lines $compare_sorted)
    let shared_total = (count-lines $shared_dids)
    let source_only_total = (count-lines $source_only)
    let compare_only_total = (count-lines $compare_only)
    let mismatch_total = (count-lines $field_mismatches)

    {
        source_total: $source_total,
        compare_total: $compare_total,
        shared_total: $shared_total,
        source_only_total: $source_only_total,
        compare_only_total: $compare_only_total,
        field_mismatch_total: $mismatch_total,
        source_coverage_vs_compare: (
            if $compare_total == 0 {
                null
            } else {
                ((($shared_total | into float) / ($compare_total | into float)) * 100.0)
            }
        ),
        samples: {
            source_only: (trim-lines $source_only $sample_size),
            compare_only: (trim-lines $compare_only $sample_size),
            field_mismatches: (trim-lines $field_mismatches $sample_size)
        },
        paths: {
            source_sorted: $source_sorted,
            compare_sorted: $compare_sorted,
            shared_dids: $shared_dids,
            source_only: $source_only,
            compare_only: $compare_only,
            joined: $joined,
            field_mismatches: $field_mismatches
        }
    }
}

def main [
    --source-relay: string = "https://relay.klbr.net",
    --compare-relay: string = "https://bsky.network",
    --page-size: int = 1000,
    --timeout-secs: int = 20,
    --sample-size: int = 20,
    --progress-interval: int = 100,
    --keep-workdir,
    --workdir: string,
    --json
] {
    let tmp = ($workdir | default (mktemp -d -t hydrant_repo_compare.XXXXXX))
    let source_raw = $"($tmp)/source.raw.tsv"
    let compare_raw = $"($tmp)/compare.raw.tsv"
    let source_sorted = $"($tmp)/source.sorted.tsv"
    let compare_sorted = $"($tmp)/compare.sorted.tsv"

    let source_dump = (dump-relay-repos $source_relay $source_raw $page_size $timeout_secs $progress_interval)
    let compare_dump = (dump-relay-repos $compare_relay $compare_raw $page_size $timeout_secs $progress_interval)

    sort-repo-file $source_raw $source_sorted
    sort-repo-file $compare_raw $compare_sorted

    let comparison = (compare-sorted-repo-files $source_sorted $compare_sorted $tmp $sample_size)
    let output = {
        source_relay: $source_relay,
        compare_relay: $compare_relay,
        page_size: $page_size,
        timeout_secs: $timeout_secs,
        sample_size: $sample_size,
        progress_interval: $progress_interval,
        source_dump: $source_dump,
        compare_dump: $compare_dump,
        comparison: $comparison,
        workdir: $tmp
    }

    if (not $keep_workdir) and (not $json) {
        print $"workdir: ($tmp)"
    }

    if $json {
        $output | to json -r | print
    } else {
        print $"source relay:  ($source_relay)"
        print $"compare relay: ($compare_relay)"
        print $"source repos:  ($comparison.source_total)"
        print $"compare repos: ($comparison.compare_total)"
        print $"shared repos:  ($comparison.shared_total)"
        print $"source only:   ($comparison.source_only_total)"
        print $"compare only:  ($comparison.compare_only_total)"
        print $"field mismatches on shared repos: ($comparison.field_mismatch_total)"
        if $comparison.source_coverage_vs_compare != null {
            print $"source coverage vs compare: (($comparison.source_coverage_vs_compare | math round -p 4))%"
        }
        print $"workdir: ($tmp)"
        print ""
        print "sample compare-only dids:"
        ($comparison.samples.compare_only | each { |line| $line } | str join "\n") | print
        print ""
        print "sample source-only dids:"
        ($comparison.samples.source_only | each { |line| $line } | str join "\n") | print
        print ""
        print "sample field mismatches:"
        ($comparison.samples.field_mismatches | each { |line| $line } | str join "\n") | print
    }
}
