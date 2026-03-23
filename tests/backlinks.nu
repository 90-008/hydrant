#!/usr/bin/env nu
# tests the blue.microcosm.links backlinks API endpoints.
#
# backfills a single repo and verifies:
#   1. getBacklinks returns a result for a subject URI extracted from an indexed like record
#   2. getBacklinksCount is >= number of getBacklinks results for the same subject
#   3. cursor pagination returns each result exactly once with no duplicates
#   4. source filter restricts results to the specified collection
#   5. reverse=true returns the same set as forward, with inverted order verified when
#      a subject with 2+ backlinks is found
#
# usage: nu tests/backlinks_test.nu
use common.nu *

# paginate through all backlinks for subject+source using limit=2, return all entries
def get-all-backlinks [url: string, subject: string, source: string] {
    mut all = []
    mut cursor = null
    loop {
        let params = if $cursor != null {
            $"subject=($subject | url encode)&source=($source)&limit=2&cursor=($cursor)"
        } else {
            $"subject=($subject | url encode)&source=($source)&limit=2"
        }
        let resp = (http get $"($url)/xrpc/blue.microcosm.links.getBacklinks?($params)")
        $all = ($all | append $resp.backlinks)
        let next = ($resp | get --optional cursor)
        if ($next | is-empty) { break }
        $cursor = $next
    }
    $all
}

# scan up to `limit` reply posts looking for a thread root that has 2+ backlinks
# from app.bsky.feed.post. returns a record {subject, count} or null.
def find-multi-backlink-subject [url: string, did: string, limit: int] {
    let posts_resp = (try {
        http get $"($url)/xrpc/com.atproto.repo.listRecords?repo=($did)&collection=app.bsky.feed.post&limit=($limit)"
    } catch {
        return null
    })

    for post in $posts_resp.records {
        let reply = ($post.value | get --optional reply)
        if ($reply | is-empty) { continue }
        let root_uri = $reply.root.uri
        let count_resp = (try {
            http get $"($url)/xrpc/blue.microcosm.links.getBacklinksCount?subject=($root_uri | url encode)"
        } catch {
            continue
        })
        if $count_resp.count >= 2 {
            return {subject: $root_uri, count: $count_resp.count}
        }
    }

    null
}

# verify getBacklinks and getBacklinksCount return sensible results for sampled likes.
# returns an error string on failure, or empty string on success.
def check-basic [url: string, likes: list] {
    print "checking basic backlinks and count..."

    for like in $likes {
        let subject = $like.value.subject.uri
        print $"  subject: ($subject)"

        let bl_resp = (try {
            http get $"($url)/xrpc/blue.microcosm.links.getBacklinks?subject=($subject | url encode)&source=app.bsky.feed.like"
        } catch {
            return $"getBacklinks request failed for ($subject)"
        })

        if ($bl_resp.backlinks | is-empty) {
            return $"expected at least 1 backlink for ($subject)"
        }
        let bl_count = ($bl_resp.backlinks | length)
        print $"    getBacklinks: ($bl_count) results"

        let ct_resp = (try {
            http get $"($url)/xrpc/blue.microcosm.links.getBacklinksCount?subject=($subject | url encode)&source=app.bsky.feed.like"
        } catch {
            return $"getBacklinksCount request failed for ($subject)"
        })

        # count may exceed backlinks results because stale index entries (records deleted
        # after indexing) are counted but skipped during result collection
        if $ct_resp.count < ($bl_resp.backlinks | length) {
            return $"count ($ct_resp.count) < backlinks result count ($bl_resp.backlinks | length) for ($subject)"
        }
        print $"    getBacklinksCount: ($ct_resp.count)"
    }

    ""
}

# verify that cursor pagination returns every result exactly once with no duplicates.
# returns an error string on failure, or empty string on success.
def check-pagination [url: string, likes: list] {
    print "checking cursor pagination..."
    let subject = ($likes | first).value.subject.uri
    print $"  subject: ($subject)"

    # fetch all in a single shot with a large limit
    let full = (http get $"($url)/xrpc/blue.microcosm.links.getBacklinks?subject=($subject | url encode)&source=app.bsky.feed.like&limit=100")
    let full_uris = ($full.backlinks | each { |b| $b.uri } | sort)

    # paginate through the same subject with limit=2
    let paged = (get-all-backlinks $url $subject "app.bsky.feed.like")
    let paged_uris = ($paged | each { |b| $b.uri } | sort)

    print $"    full fetch: ($full_uris | length), paginated: ($paged_uris | length)"

    if $full_uris != $paged_uris {
        return "paginated results differ from single-page results"
    }

    let unique_count = ($paged_uris | uniq | length)
    if $unique_count != ($paged_uris | length) {
        return "duplicate URIs found in paginated results"
    }

    print "    pagination OK"
    ""
}

# verify source filter and that forward/reverse return the same set.
# returns an error string on failure, or empty string on success.
def check-source-filter [url: string, likes: list] {
    print "checking source filter..."
    let subject = ($likes | first).value.subject.uri
    print $"  subject: ($subject)"

    let fwd = (http get $"($url)/xrpc/blue.microcosm.links.getBacklinks?subject=($subject | url encode)&source=app.bsky.feed.like&limit=50")
    let rev = (http get $"($url)/xrpc/blue.microcosm.links.getBacklinks?subject=($subject | url encode)&source=app.bsky.feed.like&limit=50&reverse=true")

    # source filter: all returned URIs must belong to app.bsky.feed.like
    let bad = ($fwd.backlinks | where { |b| not ($b.uri | str contains "/app.bsky.feed.like/") })
    if not ($bad | is-empty) {
        return $"source filter returned non-like records: ($bad)"
    }
    let fwd_count = ($fwd.backlinks | length)
    print $"    source filter OK: ($fwd_count) likes"

    # reverse must return the same set
    let fwd_sorted = ($fwd.backlinks | each { |b| $b.uri } | sort)
    let rev_sorted = ($rev.backlinks | each { |b| $b.uri } | sort)
    if $fwd_sorted != $rev_sorted {
        return "forward and reverse scans returned different sets"
    }

    print "    reverse set equality OK"
    ""
}

# verify that reverse=true actually inverts the order using a subject with 2+ backlinks.
# returns an error string on failure, or empty string on success.
def check-reverse-ordering [url: string, subject: string, expected_count: int] {
    print $"checking reverse ordering — subject has ($expected_count) backlinks..."
    print $"  subject: ($subject)"

    let fwd = (http get $"($url)/xrpc/blue.microcosm.links.getBacklinks?subject=($subject | url encode)&limit=50")
    let rev = (http get $"($url)/xrpc/blue.microcosm.links.getBacklinks?subject=($subject | url encode)&limit=50&reverse=true")

    let fwd_uris = ($fwd.backlinks | each { |b| $b.uri })
    let rev_uris = ($rev.backlinks | each { |b| $b.uri })

    if ($fwd_uris | length) < 2 {
        return $"expected >= 2 forward results for ordering test, got ($fwd_uris | length)"
    }

    if $fwd_uris != ($rev_uris | reverse) {
        return "reverse order is not the inverse of forward order"
    }

    print $"    order inversion verified with ($fwd_uris | length) results"
    ""
}

def main [] {
    let did = "did:plc:dfl62fgb7wtjj3fcbb72naae"
    let port = resolve-test-port 3020
    let url = $"http://localhost:($port)"
    let db_path = (mktemp -d -t hydrant_backlinks_test.XXXXXX)

    print $"database path: ($db_path)"

    let binary = (build-hydrant-features "backlinks")
    let instance = (start-hydrant $binary $db_path $port)

    if not (wait-for-api $url) {
        print "ERROR: hydrant failed to start"
        try { kill -9 $instance.pid }
        rm -rf $db_path
        exit 1
    }

    print $"adding ($did) to tracking..."
    http put -t application/json $"($url)/repos" [{ did: $did }]

    if not (wait-for-backfill $url) {
        print "ERROR: backfill timed out or failed"
        try { kill -9 $instance.pid }
        rm -rf $db_path
        exit 1
    }

    # fetch a small set of like records to use as test subjects
    print "fetching likes from indexed repo..."
    let likes_resp = (try {
        http get $"($url)/xrpc/com.atproto.repo.listRecords?repo=($did)&collection=app.bsky.feed.like&limit=5"
    } catch {
        print "ERROR: could not fetch like records"
        try { kill -9 $instance.pid }
        rm -rf $db_path
        exit 1
    })

    let likes = $likes_resp.records
    if ($likes | is-empty) {
        print "SKIP: no like records found for test DID. cannot verify backlinks"
        try { kill -9 $instance.pid }
        rm -rf $db_path
        exit 0
    }
    print $"found ($likes | length) likes for testing"

    # find a thread root with 2+ backlinks for ordering verification
    print "scanning posts for a thread root with 2+ backlinks..."
    let multi = (find-multi-backlink-subject $url $did 100)
    if ($multi | is-empty) {
        print "  note: no thread root with 2+ backlinks found, ordering verification will be skipped"
    } else {
        print $"  found subject with ($multi.count) backlinks: ($multi.subject)"
    }

    let basic_err      = (check-basic $url $likes)
    let paging_err     = (check-pagination $url $likes)
    let srcfilt_err    = (check-source-filter $url $likes)
    let ordering_err   = if not ($multi | is-empty) {
        check-reverse-ordering $url $multi.subject $multi.count
    } else {
        print "skipping reverse ordering check (no multi-backlink subject found)"
        ""
    }

    let failed = ([$basic_err, $paging_err, $srcfilt_err, $ordering_err] | where { |e| not ($e | is-empty) })

    print "stopping hydrant..."
    try { kill -9 $instance.pid }
    rm -rf $db_path

    if ($failed | is-empty) {
        print "all backlinks tests PASSED"
        exit 0
    } else {
        for err in $failed { print $"FAILED: ($err)" }
        exit 1
    }
}
