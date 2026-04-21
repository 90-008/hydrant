# hydrant

`hydrant` is an AT Protocol indexer built on the `fjall` database. it's built to
be flexible, supporting both full-network indexing and filtered indexing (e.g.,
by DID), allowing querying with XRPCs (not only `com.atproto.*`!), providing an
ordered event stream, etc. oh and it can also act as a relay!

you can see
[random.wisp.place](https://tangled.org/did:plc:dfl62fgb7wtjj3fcbb72naae/random.wisp.place)
(standalone binary using http API) or the [statusphere
example](./examples/statusphere.rs) (hydrant-as-library) for examples on how to
use hydrant.

for more information, look at the [documentation](https://hydrant.klbr.net).
