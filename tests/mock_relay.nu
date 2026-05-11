# mock_relay.nu

# A closure that handles HTTP requests
{|req|


    # check path
    if ($req.path | str starts-with "/xrpc/com.atproto.sync.listRepos") {
        
        let cursor = ($req.query | get --optional cursor)

        # define some mock repos
        let all_repos = [
            { did: "did:web:mock1.com", head: "bafyreidf747c4x3lps3k4n357l3a3r57k3k465743k573k465743k5", rev: "3j6s746574657" },
            { did: "did:web:mock2.com", head: "bafyreidf747c4x3lps3k4n357l3a3r57k3k465743k573k465743k5", rev: "3j6s746574657" },
            { did: "did:web:mock3.com", head: "bafyreidf747c4x3lps3k4n357l3a3r57k3k465743k573k465743k5", rev: "3j6s746574657" },
            { did: "did:web:mock4.com", head: "bafyreidf747c4x3lps3k4n357l3a3r57k3k465743k573k465743k5", rev: "3j6s746574657" },
            { did: "did:web:mock5.com", head: "bafyreidf747c4x3lps3k4n357l3a3r57k3k465743k573k465743k5", rev: "3j6s746574657" }
        ]

        let repos = if ($cursor == "50") {
             [
                { did: "did:web:mock6.com", head: "bafyreidf747c4x3lps3k4n357l3a3r57k3k465743k573k465743k5", rev: "3j6s746574657" }
             ]
        } else if ($cursor == "100") {
            []
        } else {
             $all_repos
        }

        let next_cursor = if ($cursor == "50") {
             "100"
        } else if ($cursor == "100") {
             null
        } else {
             "50"
        }

        {
            cursor: $next_cursor,
            repos: $repos
        } 
        | to json
        | metadata set --merge {
            http.response: {
                headers: {
                    "Content-Type": "application/json"
                }
            }
        }

    } else {
        # 404
        "not found" 
        | metadata set --merge {
            http.response: {
                status: 404
            }
        }
    }
}
