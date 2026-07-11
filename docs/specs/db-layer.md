# Spec: db layer restructuring

Status: draft
Tracking: beads epic (see `bd show`, filed alongside this spec)

## Motivation

The db layer works, but its correctness rests on conventions that every
callsite must independently remember. Concretely:

- `Db` (`src/db/mod.rs:48`) exposes every keyspace as a public `Keyspace`
  field plus `pub inner: Arc<Database>`. Any module can write raw bytes to
  any keyspace with hand-built keys; `db.inner.batch()` appears in 27+ files.
- Four access styles coexist: half-dead static async helpers (`Db::get`,
  with `insert`/`remove`/`contains_key` marked `#[allow(dead_code)]`),
  direct sync calls inside `spawn_blocking` closures, direct sync calls on
  dedicated blocking threads, and free functions taking `(batch, db, ...)`.
- Key formats live in free functions returning `Vec<u8>` (`src/db/keys/`)
  with prefix constants scattered per module — `db/filter.rs` maintains its
  own `SEP`/prefix constants separate from `keys::SEP`. The authoritative
  documentation of key formats is comments (duplicated again in AGENTS.md).
- Serialization helpers exist (`ser_repo_state` etc.) but ~31 files call
  `rmp_serde` directly, several deserializing `RepoState` raw and bypassing
  the helpers (`backfill/worker/process.rs:58`, `backfill/manager.rs:122`,
  `backfill/worker/task.rs:187`).

None of these are hypothetical risks:

1. **The record-import invariant is duplicated three times.** A record write
   must keep in sync: `records` ks, `blocks` ks, per-collection counts,
   backlinks (cfg), a `StoredEvent`, and a jetstream event (cfg). This is
   implemented once properly in `ops::apply_commit` (via `RecordEmitter`),
   then re-implemented by hand in `backfill/sparse.rs` (`persist_sparse_backfill`
   builds `StoredEvent` + event key + `jetstream::stage_event` manually, and
   carries its own cfg'd backlinks block) and twice more in
   `backfill/worker/process.rs`. Divergence between these is silent index
   drift — missing events, missing backlinks, wrong counts.
2. **Three manual registries per keyspace.** `keyspace_by_name`
   (`db/keyspaces.rs`), the `/debug` name→type deserializer match
   (`api/debug.rs:99`), and the `/stats` endpoint must each be updated when a
   keyspace is added. `open.rs:209` admits this with a "when adding new
   keyspaces, make sure to..." comment. That comment is the bug report.
3. **The counts protocol is convention-only.** Every write site must
   remember: stage deltas into the batch → `batch.commit()` →
   `apply_count_deltas` (plus lifecycle reservation apply). The pairing is
   unenforced; a missed apply drifts in-memory counts from persisted state
   until restart replay. Three separate subsystems (count deltas + watermarks,
   `LifecycleCountBatch`, per-collection record count free fns) each carry
   their own call-order rules.
4. **Nothing pairs a keyspace to its key format or value type.** Inserting
   `RepoState` bytes into `cursors`, or building a record key against the
   `repos` keyspace, compiles fine and fails at read time — or worse, doesn't.
5. `Db::open` is a 260-line monolith; `check_poisoned` is called at some
   iteration sites and not others; every key construction heap-allocates.

What is already good and stays untouched: the migration framework (frozen
versioned wire types, ordered forward migrations), the per-mode keyspace
groups (`IndexerDb`/`StreamDb`/`JetstreamDb`/`RelayDb` + `OpenCx`), the
`RecordEmitter` ZST facade pattern, the count-delta crash-safe replay design,
and the per-keyspace tuning rationale.

## Design

One principle: **a keyspace is one type, declared in one table, and its
invariant-bearing writes have exactly one owner.** Everything else derives.

### 1. Keyspace schemas: `Ks<S>`

Each keyspace gets a schema type pairing name, key codec, value codec, tuning,
and debug rendering:

```rust
pub trait Schema {
    const NAME: &'static str;
    type Key: KeyCodec;
    type Value: ValueCodec;

    /// per-keyspace tuning, applied at open (moves out of open.rs).
    fn options(cx: &OpenCx) -> KsOptions;

    /// debug rendering; default impl transcodes msgpack -> json.
    fn render(value: &[u8]) -> Result<serde_json::Value> { ... }
}

/// zero-cost typed handle; Clone is cheap (Keyspace is an Arc'd handle).
pub struct Ks<S: Schema> {
    inner: Keyspace,
    _s: PhantomData<S>,
}

impl<S: Schema> Ks<S> {
    pub fn get(&self, key: &S::Key) -> Result<Option<S::Value>>;
    /// zero-copy escape for CAS blocks and other raw-value reads.
    pub fn get_raw(&self, key: &S::Key) -> Result<Option<Slice>>;
    pub fn contains(&self, key: &S::Key) -> Result<bool>;
    pub fn insert(&self, txn: &mut Txn, key: &S::Key, value: &S::Value);
    pub fn remove(&self, txn: &mut Txn, key: &S::Key);
    pub fn prefix(&self, p: impl Prefix<S>) -> impl Iterator<Item = Result<(S::Key, S::Value)>>;
    /// greppable escape hatch (migrations, debug endpoints).
    pub fn raw(&self) -> &Keyspace;
}
```

Poison checks (`check_poisoned`) move into the read/iterator adapters so
every access is covered instead of the current ad-hoc sites.

`KeyCodec` replaces the `keys/` free functions with structs that *are* the
documented format:

```rust
pub trait KeyCodec: Sized {
    /// KeyBuf = SmallVec<[u8; 64]>: typical keys never touch the heap.
    fn encode_into(&self, buf: &mut KeyBuf);
    fn decode(bytes: &[u8]) -> Result<Self, KeyError>;
}

/// {DID}|{COL}|{RKey} — the definition is the documentation.
pub struct RecordKey<'a> {
    pub did: &'a str,
    pub collection: &'a str,
    pub rkey: &'a str,
}
```

This deletes the duplicate `SEP`/prefix constants in `db/filter.rs` and makes
"wrong key against wrong keyspace" a type error.

`ValueCodec` has three impls: msgpack via `rmp_serde` (blanket over
`Serialize + DeserializeOwned` wire types), `RawBytes` (CAS blocks), and
`BeU64`/`BeI64` (cursors, counts). The ~31 scattered `rmp_serde` callsites
and the `ser_/deser_` helper pair in `mod.rs` collapse into this.

### 2. One declarative registry

A single table declares every keyspace, grouped exactly like the existing
mode groups:

```rust
keyspaces! {
    core {
        repos: Repos,
        repo_metadata: RepoMetadata,
        cursors: Cursors,
        counts: Counts,
        filter: Filter,
        crawler: Crawler,
    }
    #[cfg(feature = "indexer")]
    indexer {
        records: Records,
        blocks: Blocks,
        pending: Pending,
        resync: Resync,
        resync_buffer: ResyncBuffer,
    }
    #[cfg(feature = "indexer_stream")]
    stream { events: Events, ... }
    #[cfg(feature = "jetstream")]
    jetstream { ... }
    #[cfg(feature = "relay")]
    relay { relay_events: RelayEvents, ... }
    #[cfg(feature = "backlinks")]
    backlinks { backlinks: Backlinks }
}
```

The macro generates:

- the `Db` field groups (matching today's `IndexerDb` etc. — this extends the
  existing seam, it does not replace it),
- open-time creation with `S::options(cx)` tuning (guts the open.rs monolith),
- `keyspace_by_name(&str)`,
- `fn all() -> impl Iterator<...>` for `/stats`,
- `/debug` rendering via `S::render`.

Adding a keyspace becomes: write the schema type, add one row. Forgetting the
row means the keyspace doesn't exist at all — no silent partial registration.
The `open.rs:209` comment dies.

### 3. Visibility is the enforcement mechanism

Registry-generated fields are **private to `db::`**. Read access is exposed
per keyspace where legitimately needed; the invariant-bearing keyspaces
(`records`, `blocks`, `counts`, `events`, `jetstream_events`, `backlinks`)
get **no public write access at all**. The only way to mutate a record is the
op type below. `raw()` escape hatches exist for `db::migration` and the
`/debug` write endpoint — both inside or routed through `db::`, both
greppable.

This is the "use the type system to encode correctness constraints" rule
applied with module privacy, which is the only enforcement Rust actually
gives us.

### 4. One write path for records: `RecordTxn`

Layered write handles:

```rust
/// typed batch over fjall::Batch; plain writes (cursors, filter, pds_meta...).
pub struct Txn<'db> { batch: fjall::Batch, db: &'db Db }

impl Txn<'_> {
    pub fn commit(self, mode: PersistMode) -> Result<()>;
}

/// the ONLY path that may mutate records/blocks/counts/events/backlinks.
pub struct RecordTxn<'db> {
    txn: Txn<'db>,
    counts: CountDeltas,
    lifecycle: LifecycleCountBatch,
    emitter: RecordEmitter, // existing ZST facade, reused as-is
}

impl RecordTxn<'_> {
    pub fn put_record(&mut self, did, coll, rkey, cid, block, action) -> Result<()>;
    pub fn delete_record(&mut self, did, coll, rkey, prev_cid) -> Result<()>;
    pub fn update_repo_state(&mut self, did, f: impl FnOnce(&mut RepoState)) -> Result<()>;
    /// stage deltas -> batch.commit -> apply_count_deltas -> lifecycle apply.
    /// the ordering exists exactly once, here.
    pub fn commit(self, mode: PersistMode) -> Result<()>;
}
```

- `put_record`/`delete_record` own the full invariant: records ks, blocks ks,
  per-collection counts, backlinks (one cfg site, inside), `StoredEvent`
  emission and jetstream staging via `RecordEmitter`.
- `ops::apply_commit`, `backfill/sparse.rs::persist_sparse_backfill`, and both
  `backfill/worker/process.rs` sites route through it. The three hand-rolled
  `StoredEvent` constructions and sparse.rs's private backlinks cfg block are
  deleted, not deprecated.
- Drop-without-commit is the only failure mode, and it is exactly the case
  the existing count-delta restart replay already handles. No new crash-safety
  machinery — the existing design, minus the per-callsite ceremony.

This is not a transaction abstraction over fjall; batches are already atomic.
It is the existing protocol given a shape that cannot be half-followed.

### 5. Blocking discipline

One rule: `Ks` methods are sync. Async contexts use one wrapper:

```rust
impl Db {
    /// spawn_blocking with the Db handle; the one async<->sync seam.
    pub async fn run<T>(&self, f: impl FnOnce(&Db) -> T + Send) -> T;
}
```

Dedicated blocking threads (`ingest/indexer/shard.rs`, backfill retry worker)
call sync methods directly, unchanged. The static async helpers on `Db`
(`get`/`insert`/`remove`/`contains_key`, mostly dead code today) are removed.

## Non-goals

- No storage-engine abstraction, no fjall replacement, no mock-store trait
  for tests (fjall in a tempdir is fast; tests keep using the real thing).
- No runtime mode selection. Modes stay compile-time; the registry table
  keeps cfg at the composition root per the feature-gating conventions.
- The migration framework is untouched. Migrations deliberately read old
  formats and keep raw keyspace access via `raw()`.
- No change to on-disk formats, key layouts, or wire types. This is a
  code-shape refactor; a fresh build must open an existing database and
  produce byte-identical writes.

## Phasing

Each phase lands independently, warning-free through
`nu tests/feature_matrix.nu` (all 13 combos) and `nu tests/run_all.nu`.
Within a phase, old and new APIs may coexist; at each phase boundary the old
path is deleted — no lingering dual conventions.

1. **Schema + registry.** `Schema`, `Ks<S>`, `ValueCodec`, the `keyspaces!`
   table; port every keyspace; derive `keyspace_by_name`, `/stats`, `/debug`
   from it; move tuning out of `open.rs`. Pure mechanical, no behavior change.
   (Fields stay `pub(crate)` during this phase.)
2. **Key codecs.** Key structs replace `keys/` free functions; delete the
   duplicate separator/prefix constants in `db/filter.rs`; poison checks fold
   into accessors/iterators.
3. **`RecordTxn`.** Route `apply_commit`, sparse backfill, and backfill worker
   processing through `put_record`/`delete_record`; delete the duplicated
   `StoredEvent`/backlinks/counts blocks; flip invariant keyspaces to private.
4. **Counts + cleanup.** Fold count staging/apply and lifecycle reservations
   into `Txn`/`RecordTxn` commit; remove loose `apply_count_deltas` callsites;
   `Db::run` replaces the static async helpers; decompose what remains of
   `open.rs`.

Ordering: 1–2 are low-risk mechanical enablers; 3 is the highest-value
correctness fix and depends on them; 4 rides on 3's types.

## Verification

- Per phase: `nu tests/feature_matrix.nu`, `nu tests/run_all.nu --skip-creds`.
- Phase 3 specifically (invariant-touching): `repo_sync_integrity.nu`,
  `backlinks.nu`, `stream_live_backfill.nu`, `stream_cursor_replay.nu`, and
  `repo_count_resync.nu` with credentials — these are exactly the tests that
  observe the record-import invariant from outside.
- Phase 4: `repo_count_resync.nu` again (count drift is its purpose), plus
  `ephemeral_ttl_*.nu` for event pruning paths.
- Open an existing pre-refactor database directory and verify `/stats` and
  `/debug/iter` parity against a pre-refactor build (no format changes means
  byte-identical output).

## Risks

- **Callsite breadth.** Dozens of files touch keyspaces directly. Mitigation:
  phase boundaries are keyspace-by-keyspace mechanical ports; the compiler
  drives (private fields = exhaustive callsite list as errors).
- **Iterator decode overhead.** Typed iterators decode per item; hot scans
  that only need raw slices use `prefix_raw` variants. CAS block reads keep
  zero-copy `Slice` via `get_raw`.
- **Monomorphization.** `Ks<S>` per keyspace is ~20 instantiations of small
  methods; negligible.
- **`RecordTxn` shape vs. sparse backfill.** Sparse import builds records from
  proof CARs, not commit diffs; `put_record` must accept a pre-parsed block
  without forcing a re-parse. The API above takes `(cid, block)` raw for this
  reason — verify during phase 3 that sparse's zero-copy `Bytes::slice` path
  survives.
