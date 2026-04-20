# repository management

all `/repos` endpoints that return lists respond with NDJSON by default. send `Accept: application/json` or `Content-Type: application/json` to get a JSON array instead.

- `GET /repos`: get a list of repositories and their sync status. supports pagination and filtering:
    - `limit`: max results (default 100, max 1000)
    - `cursor`: did key for paginating.
- `GET /repos/{did}`: get the sync status and metadata of a specific repository. also returns the handle, PDS URL and the atproto signing key (these won't be available before the repo has been backfilled once at least).
- `PUT /repos`: explicitly track repositories. accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same). only affects repositories that are not known or are untracked. returns a list of the DIDs that were queued for backfill.
- `DELETE /repos`: untrack repositories. accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same). only affects repositories that are currently tracked. returns a list of the DIDs that were untracked.
- `POST /repos/resync`: force a new backfill for one or more repositories. accepts an NDJSON body of `{"did": "..."}` (or JSON array of the same). only affects repositories hydrant already knows about. returns a list of the DIDs that were queued.
